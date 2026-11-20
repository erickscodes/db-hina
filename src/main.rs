//! Shopping Sessions pipeline: parses the CSV logs, links IDs into people,
//! dedups and validates events, sessionizes, attributes, and writes Parquet.

mod parquet;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NP_BITS: u32 = 8; // event_id partitions
const NP: usize = 1 << NP_BITS;
const NQ_BITS: u32 = 8; // person partitions
const NQ: usize = 1 << NQ_BITS;
const CHUNK: u64 = 8 << 20;

// ---------------------------------------------------------------- keys

/// Exact 128-bit key for a byte string of up to 15 bytes: the bytes big-endian
/// in the high 15 bytes (so integer order is byte-wise order), the length in
/// the low byte. Longer strings get a 120-bit hash with low byte 0xFF and are
/// remembered in a side table. The empty string is 0.
static LONG: OnceLock<Mutex<HashMap<u128, Vec<u8>>>> = OnceLock::new();

fn long_table() -> &'static Mutex<HashMap<u128, Vec<u8>>> {
    LONG.get_or_init(|| Mutex::new(HashMap::new()))
}

#[inline(always)]
fn pack(s: &[u8]) -> u128 {
    let n = s.len();
    if n == 0 {
        return 0;
    }
    if n <= 15 {
        let mut k: u128 = 0;
        for &b in s {
            k = (k << 8) | b as u128;
        }
        (k << (8 * (15 - n) + 8)) | n as u128
    } else {
        pack_long(s)
    }
}

#[cold]
fn pack_long(s: &[u8]) -> u128 {
    let mut a: u64 = 0xcbf29ce484222325;
    let mut b: u64 = 0x84222325cbf29ce4;
    for &c in s {
        a = (a ^ c as u64).wrapping_mul(0x100000001b3);
        b = (b ^ c as u64).wrapping_mul(0x00000100000001b3).rotate_left(17) ^ 0x9E3779B97F4A7C15;
    }
    let k = (((a as u128) << 64 | b as u128) << 8) | 0xFF;
    long_table().lock().unwrap().entry(k).or_insert_with(|| s.to_vec());
    k
}

fn key_bytes(k: u128) -> Vec<u8> {
    let n = (k & 0xFF) as usize;
    if n == 0xFF {
        return long_table().lock().unwrap().get(&k).cloned().unwrap_or_default();
    }
    (0..n).map(|i| (k >> (120 - 8 * i)) as u8).collect()
}

#[inline]
fn key_cmp(a: u128, b: u128) -> Ordering {
    if (a & 0xFF) == 0xFF || (b & 0xFF) == 0xFF {
        key_bytes(a).cmp(&key_bytes(b))
    } else {
        a.cmp(&b)
    }
}

#[inline(always)]
fn h64(k: u128) -> u64 {
    let x = (k as u64) ^ ((k >> 64) as u64).wrapping_mul(0x9E3779B97F4A7C15);
    let mut z = x.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Open-addressing map from non-zero u128 keys.
struct Map<V: Copy> {
    s: Vec<(u128, V)>,
    mask: usize,
}

impl<V: Copy + Default> Map<V> {
    fn new(n: usize) -> Self {
        let cap = (n * 2).max(16).next_power_of_two();
        Map { s: vec![(0, V::default()); cap], mask: cap - 1 }
    }
    #[inline(always)]
    fn get(&self, k: u128) -> Option<V> {
        if k == 0 {
            return None;
        }
        let mut i = h64(k) as usize & self.mask;
        loop {
            let (kk, v) = unsafe { *self.s.get_unchecked(i) };
            if kk == k {
                return Some(v);
            }
            if kk == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Index of k's slot, inserting it with `V::default()` if absent; true if new.
    #[inline]
    fn slot(&mut self, k: u128) -> (usize, bool) {
        let mut i = h64(k) as usize & self.mask;
        loop {
            let kk = self.s[i].0;
            if kk == k {
                return (i, false);
            }
            if kk == 0 {
                self.s[i].0 = k;
                return (i, true);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- parsing

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a * 10 + b) as i64)
    } else {
        None
    }
}

/// Parses F1, F2 or F3 into epoch ms. None for a bad format; a value too
/// large for i64 becomes i64::MAX (outside every window).
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if s[4] == b'-' {
            return parse_civil(s);
        }
    }
    // F3: digits only
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig > 19 {
                // keep scanning for non-digits
                for &c in s {
                    if !c.is_ascii_digit() {
                        return None;
                    }
                }
                return Some(i64::MAX);
            }
            v = v * 10 + d as u64;
        }
    }
    Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 })
}

fn parse_civil(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let mut off = 0i64;
    if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = sign * (oh * 60 + om) * 60_000;
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se) * 1000 - off)
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep checking digits only matters for validity; any value > 1e7 is invalid anyway
            return None;
        }
    }
    if v >= 1 {
        Some(v as u32)
    } else {
        None
    }
}

#[inline(always)]
fn unq(f: &[u8], q: u32) -> Option<&[u8]> {
    if q == 0 {
        Some(f)
    } else if q == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        Some(&f[1..f.len() - 1])
    } else {
        None
    }
}

/// Splits the line starting at `i` (ending at '\n' or `end`) into exactly N
/// fields. Returns whether it is well-formed, and the start of the next line.
#[inline(always)]
fn split<'a, const N: usize>(buf: &'a [u8], start: usize, end: usize, f: &mut [&'a [u8]; N]) -> (bool, usize) {
    let s = &buf[..end];
    let mut i = start;
    let mut nf = 0usize;
    let mut fs = i;
    let mut q = 0u32;
    let mut ok = true;
    while i < s.len() {
        let b = s[i];
        if b == b'\n' {
            break;
        }
        if b == b',' {
            if nf < N - 1 {
                match unq(&s[fs..i], q) {
                    Some(v) => f[nf] = v,
                    None => ok = false,
                }
            } else {
                ok = false;
            }
            nf += 1;
            fs = i + 1;
            q = 0;
        } else if b == b'"' {
            q += 1;
        }
        i += 1;
    }
    let next = i + 1;
    if i == start || !ok || nf != N - 1 {
        return (false, next);
    }
    match unq(&s[fs..i], q) {
        Some(v) => {
            f[N - 1] = v;
            (true, next)
        }
        None => (false, next),
    }
}

#[inline]
fn find_nl(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..].iter().position(|&b| b == b'\n').map(|p| p + from)
}

/// Reads the lines that start in [start, end) of a file (skipping the header
/// line at offset 0) into `buf`, and returns the byte range to scan.
fn read_chunk(f: &File, size: u64, start: u64, end: u64, buf: &mut Vec<u8>) -> (usize, usize) {
    let rs = if start == 0 { 0 } else { start - 1 };
    let mut extra: u64 = 1 << 16;
    let region_end;
    loop {
        let re = (end + extra).min(size);
        buf.resize((re - rs) as usize, 0);
        f.read_exact_at(buf, rs).expect("read");
        let from = (end - 1 - rs) as usize;
        match find_nl(buf, from) {
            Some(i) => {
                region_end = i + 1;
                break;
            }
            None => {
                if re == size {
                    region_end = buf.len();
                    break;
                }
                extra *= 4;
            }
        }
    }
    let pos = match find_nl(&buf[..region_end], 0) {
        Some(i) => i + 1,
        None => region_end,
    };
    (pos, region_end)
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct EvRaw {
    eid: u128,
    user: u128,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u16,
    flags: u8, // 1 valid, 2 purchase
}

#[derive(Clone, Copy)]
struct ClickRaw {
    user: u128,
    cid: u128,
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct EvP {
    person: u128,
    ts: i64,
    price: u32,
    cat: u16,
    purch: u8,
}

#[derive(Clone, Copy)]
struct ClP {
    person: u128,
    cid: u128,
    ts: i64,
    camp: u32,
}

struct Ctx {
    ws: i64,
    we: i64,
    products: Map<u16>,
    unknown_cat: u16,
    campaigns: Map<u32>,
}

// ---------------------------------------------------------------- parallel helper

fn run_par<S: Send, I: Fn() -> S + Sync, F: Fn(&mut S, usize) + Sync>(w: usize, ntasks: usize, init: I, f: F) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..w)
            .map(|_| {
                sc.spawn(|| {
                    let mut s = init();
                    loop {
                        let t = next.fetch_add(1, AO::Relaxed);
                        if t >= ntasks {
                            break;
                        }
                        f(&mut s, t);
                    }
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- small inputs

fn csv_rows<const N: usize>(path: &Path) -> Vec<[Vec<u8>; N]> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut out = Vec::new();
    let mut pos = match find_nl(&data, 0) {
        Some(i) => i + 1,
        None => data.len(),
    };
    let mut f: [&[u8]; N] = [&[]; N];
    while pos < data.len() {
        let (ok, next) = split::<N>(&data, pos, data.len(), &mut f);
        if ok {
            out.push(std::array::from_fn(|i| f[i].to_vec()));
        }
        pos = next;
    }
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key") + key.len() + 2;
    let rest = &text[at..];
    let colon = rest.find(':').unwrap();
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    parse_ts(rest[..q2].as_bytes()).expect("manifest timestamp")
}

fn list_csv(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "csv") && p.is_file() {
                let size = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                v.push((p, size));
            }
        }
    }
    v.sort();
    v
}

// ---------------------------------------------------------------- main

struct P1 {
    ev: Vec<Vec<EvRaw>>,
    cl: Vec<ClickRaw>,
    links: Vec<(u128, u128)>,
    buf: Vec<u8>,
}

struct P2 {
    ev: Vec<Vec<EvP>>,
    cl: Vec<Vec<ClP>>,
}

struct UserRows {
    name: Vec<Vec<u8>>,
    day: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    top: Vec<Option<u32>>,
}

struct P3 {
    acc: Vec<[i64; 4]>,
    counts: Vec<u32>,
    pos: Vec<u32>,
    list: Vec<u32>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
    rows: usize,
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let w: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()))
        .max(1);
    let log = |m: &str| eprintln!("[{:7.3}s] {m}", t0.elapsed().as_secs_f64());

    // --- small inputs
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    let prod_rows = csv_rows::<2>(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = prod_rows.iter().map(|r| r[1].clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let cat_index: HashMap<Vec<u8>, u16> = cat_names.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let unknown_cat = cat_index[&b"unknown"[..]];
    let mut products = Map::<u16>::new(prod_rows.len());
    for r in &prod_rows {
        let k = pack(&r[0]);
        if k != 0 {
            let (i, _) = products.slot(k);
            products.s[i].1 = cat_index[&r[1]];
        }
    }
    let camp_rows = csv_rows::<2>(&data.join("campaigns.csv"));
    let mut camp_names: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    let mut campaigns = Map::<u32>::new(camp_rows.len());
    for r in &camp_rows {
        let k = pack(&r[0]);
        if k == 0 {
            continue;
        }
        let (i, new) = campaigns.slot(k);
        if new {
            campaigns.s[i].1 = camp_names.len() as u32;
            camp_names.push(r[0].clone());
            camp_channels.push(r[1].clone());
        }
    }
    let ncamp = camp_names.len();
    let ctx = Ctx { ws, we, products, unknown_cat, campaigns };

    // --- tasks
    let ev_files = list_csv(&data.join("events"));
    let cl_files = list_csv(&data.join("clicks"));
    let ln_files = list_csv(&data.join("links"));
    let mut files: Vec<(u8, File, u64)> = Vec::new();
    let mut tasks: Vec<(usize, u64, u64)> = Vec::new();
    let mut ev_bytes = 0u64;
    for (kind, list) in [(2u8, &ln_files), (0u8, &ev_files), (1u8, &cl_files)] {
        for (p, size) in list {
            let fi = files.len();
            files.push((kind, File::open(p).expect("open"), *size));
            if kind == 0 {
                ev_bytes += size;
            }
            let mut s = 0;
            while s < *size {
                let e = (s + CHUNK).min(*size);
                tasks.push((fi, s, e));
                s = e;
            }
        }
    }
    let est_ev = (ev_bytes / 70) as usize;
    let per = est_ev * 11 / 10 / NP / w + 16;
    log(&format!("{} files, {} tasks, {} event bytes, {} workers", files.len(), tasks.len(), ev_bytes, w));

    // --- phase 1: parse
    let p1s = run_par(
        w,
        tasks.len(),
        || P1 { ev: (0..NP).map(|_| Vec::with_capacity(per)).collect(), cl: Vec::new(), links: Vec::new(), buf: Vec::new() },
        |st, t| {
            let (fi, s, e) = tasks[t];
            let (kind, ref file, size) = files[fi];
            let mut buf = std::mem::take(&mut st.buf);
            let (mut pos, end) = read_chunk(file, size, s, e, &mut buf);
            match kind {
                0 => {
                    let mut f: [&[u8]; 7] = [&[]; 7];
                    while pos < end {
                        let (ok, next) = split::<7>(&buf, pos, end, &mut f);
                        pos = next;
                        if !ok || f[0].is_empty() {
                            continue;
                        }
                        let ingest = match parse_ts(f[3]) {
                            Some(v) => v,
                            None => continue,
                        };
                        let mut flags = 0u8;
                        let mut price = 0u32;
                        let mut cat = 0u16;
                        let mut ts = 0i64;
                        let ty = match f[4] {
                            b"view" => 0u8,
                            b"cart" => 1,
                            b"purchase" => 2,
                            _ => 255,
                        };
                        if ty != 255 && !f[1].is_empty() && !f[5].is_empty() {
                            if let Some(v) = parse_ts(f[2]) {
                                if v >= ctx.ws && v < ctx.we && ingest - v <= DAY {
                                    ts = v;
                                    if ty == 2 {
                                        if let Some(p) = parse_price(f[6]) {
                                            price = p;
                                            cat = ctx.products.get(pack(f[5])).unwrap_or(ctx.unknown_cat);
                                            flags = 3;
                                        }
                                    } else {
                                        flags = 1;
                                    }
                                }
                            }
                        }
                        let eid = pack(f[0]);
                        let user = if flags != 0 { pack(f[1]) } else { 0 };
                        let p = (h64(eid) >> (64 - NP_BITS)) as usize;
                        st.ev[p].push(EvRaw { eid, user, ts, ingest, price, cat, flags });
                    }
                }
                1 => {
                    let mut f: [&[u8]; 4] = [&[]; 4];
                    let lo = ctx.ws - WEEK;
                    while pos < end {
                        let (ok, next) = split::<4>(&buf, pos, end, &mut f);
                        pos = next;
                        if !ok || f[0].is_empty() || f[1].is_empty() {
                            continue;
                        }
                        let ts = match parse_ts(f[3]) {
                            Some(v) if v >= lo && v < ctx.we => v,
                            _ => continue,
                        };
                        let camp = match ctx.campaigns.get(pack(f[2])) {
                            Some(c) => c,
                            None => continue,
                        };
                        st.cl.push(ClickRaw { user: pack(f[1]), cid: pack(f[0]), ts, camp });
                    }
                }
                _ => {
                    let mut f: [&[u8]; 2] = [&[]; 2];
                    while pos < end {
                        let (ok, next) = split::<2>(&buf, pos, end, &mut f);
                        pos = next;
                        if !ok || f[0].is_empty() || f[1].is_empty() {
                            continue;
                        }
                        st.links.push((pack(f[0]), pack(f[1])));
                    }
                }
            }
            st.buf = buf;
        },
    );
    log("parsed");

    // --- links: union-find
    let npairs: usize = p1s.iter().map(|p| p.links.len()).sum();
    let mut pmap = Map::<u128>::new(npairs * 2);
    let mut ids: Vec<u128> = Vec::with_capacity(npairs);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for p in &p1s {
        for &(a, b) in &p.links {
            let mut idx = [0u32; 2];
            for (j, k) in [a, b].into_iter().enumerate() {
                let (s, new) = pmap.slot(k);
                if new {
                    pmap.s[s].1 = ids.len() as u128;
                    parent.push(ids.len() as u32);
                    ids.push(k);
                }
                idx[j] = pmap.s[s].1 as u32;
            }
            let ra = find(&mut parent, idx[0]);
            let rb = find(&mut parent, idx[1]);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let mut name: Vec<u128> = ids.clone();
    for i in 0..ids.len() {
        let r = find(&mut parent, i as u32) as usize;
        if key_cmp(ids[i], name[r]) == Ordering::Less {
            name[r] = ids[i];
        }
    }
    let roots: Vec<u32> = (0..ids.len() as u32).map(|i| find(&mut parent, i)).collect();
    for slot in pmap.s.iter_mut() {
        if slot.0 != 0 {
            slot.1 = name[roots[slot.1 as usize] as usize];
        }
    }
    drop(roots);
    drop(parent);
    drop(ids);
    drop(name);
    log(&format!("linked {} pairs", npairs));

    // --- phase 2: dedup events, map to people, partition by person
    let ncl_chunks: Vec<(usize, usize, usize)> = {
        let mut v = Vec::new();
        for (wi, p) in p1s.iter().enumerate() {
            let mut s = 0;
            while s < p.cl.len() {
                let e = (s + (1 << 18)).min(p.cl.len());
                v.push((wi, s, e));
                s = e;
            }
        }
        v
    };
    let est_p = est_ev / NQ / w + 16;
    let p2s = run_par(
        w,
        NP + ncl_chunks.len(),
        || P2 { ev: (0..NQ).map(|_| Vec::with_capacity(est_p)).collect(), cl: (0..NQ).map(|_| Vec::new()).collect() },
        |st, t| {
            if t < NP {
                let parts: Vec<&[EvRaw]> = p1s.iter().map(|p| &p.ev[t][..]).collect();
                let n: usize = parts.iter().map(|p| p.len()).sum();
                let cap = (n * 2).max(16).next_power_of_two();
                let mask = cap - 1;
                let mut table: Vec<u32> = vec![0; cap];
                let get = |id: u32| -> &EvRaw {
                    let id = id - 1;
                    &parts[(id >> 24) as usize][(id & 0xFF_FFFF) as usize]
                };
                for (wi, part) in parts.iter().enumerate() {
                    assert!(part.len() < (1 << 24));
                    for (i, r) in part.iter().enumerate() {
                        let id = ((wi as u32) << 24 | i as u32) + 1;
                        let mut s = h64(r.eid) as usize & mask;
                        loop {
                            let cur = table[s];
                            if cur == 0 {
                                table[s] = id;
                                break;
                            }
                            let c = get(cur);
                            if c.eid == r.eid {
                                if r.ingest < c.ingest {
                                    table[s] = id;
                                }
                                break;
                            }
                            s = (s + 1) & mask;
                        }
                    }
                }
                for &id in &table {
                    if id == 0 {
                        continue;
                    }
                    let r = get(id);
                    if r.flags & 1 == 0 {
                        continue;
                    }
                    let person = pmap.get(r.user).unwrap_or(r.user);
                    let q = (h64(person) >> (64 - NQ_BITS)) as usize;
                    st.ev[q].push(EvP { person, ts: r.ts, price: r.price, cat: r.cat, purch: r.flags >> 1 });
                }
            } else {
                let (wi, s, e) = ncl_chunks[t - NP];
                for c in &p1s[wi].cl[s..e] {
                    let person = pmap.get(c.user).unwrap_or(c.user);
                    let q = (h64(person) >> (64 - NQ_BITS)) as usize;
                    st.cl[q].push(ClP { person, cid: c.cid, ts: c.ts, camp: c.camp });
                }
            }
        },
    );
    drop(p1s);
    drop(pmap);
    log("deduped and partitioned");

    // --- phase 3: sessionize, attribute, aggregate, write user_daily
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ud_dir = output.join("user_daily");
    let cd_dir = output.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir");
    std::fs::create_dir_all(&cd_dir).expect("mkdir");
    let mut order: Vec<usize> = (0..NQ).collect();
    let qsize = |q: usize| -> usize { p2s.iter().map(|p| p.ev[q].len() + p.cl[q].len()).sum() };
    let sizes: Vec<usize> = (0..NQ).map(qsize).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(sizes[q]));
    let ncat = cat_names.len();
    let p3s = run_par(
        w,
        NQ,
        || P3 {
            acc: vec![[0i64; 4]; (ncamp + 1) * ndays],
            counts: vec![0; ncamp],
            pos: vec![0; ncamp],
            list: Vec::new(),
            catsum: vec![0; ncat],
            touched: Vec::new(),
            rows: 0,
        },
        |st, t| {
            let q = order[t];
            let mut ev: Vec<EvP> = Vec::with_capacity(p2s.iter().map(|p| p.ev[q].len()).sum());
            for p in &p2s {
                ev.extend_from_slice(&p.ev[q]);
            }
            let mut cl: Vec<ClP> = Vec::with_capacity(p2s.iter().map(|p| p.cl[q].len()).sum());
            for p in &p2s {
                cl.extend_from_slice(&p.cl[q]);
            }
            ev.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
            cl.sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
            let mut rows = UserRows { name: Vec::new(), day: Vec::new(), sessions: Vec::new(), events: Vec::new(), revenue: Vec::new(), top: Vec::new() };
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let person = ev[i].person;
                let mut j = i + 1;
                while j < ev.len() && ev[j].person == person {
                    j += 1;
                }
                while ci < cl.len() && cl[ci].person < person {
                    ci += 1;
                }
                let cs = ci;
                while ci < cl.len() && cl[ci].person == person {
                    ci += 1;
                }
                walk_person(person, &ev[i..j], &cl[cs..ci], st, ncamp, ndays, day0, &mut rows);
                i = j;
            }
            st.rows += rows.day.len();
            if !rows.day.is_empty() {
                let cols = [
                    parquet::Column::Str("user_id", rows.name),
                    parquet::Column::I32("day", rows.day, true),
                    parquet::Column::I32("sessions", rows.sessions, false),
                    parquet::Column::I32("events", rows.events, false),
                    parquet::Column::I64("revenue_cents", rows.revenue),
                    parquet::Column::DictStr("top_category", rows.top, &cat_names),
                ];
                parquet::write_file(&ud_dir.join(format!("part-{q:04}.parquet")), &cols).expect("write user_daily");
            }
        },
    );
    let total_rows: usize = p3s.iter().map(|p| p.rows).sum();
    if total_rows == 0 {
        let cols = [
            parquet::Column::Str("user_id", vec![]),
            parquet::Column::I32("day", vec![], true),
            parquet::Column::I32("sessions", vec![], false),
            parquet::Column::I32("events", vec![], false),
            parquet::Column::I64("revenue_cents", vec![]),
            parquet::Column::DictStr("top_category", vec![], &cat_names),
        ];
        parquet::write_file(&ud_dir.join("part-empty.parquet"), &cols).expect("write user_daily");
    }
    log(&format!("sessions done, {total_rows} user_daily rows"));

    // --- campaign_daily
    let mut acc = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for p in &p3s {
        for (a, b) in acc.iter_mut().zip(&p.acc) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let (mut cid, mut ch, mut dy, mut se, mut ps, mut rv, mut asg) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = acc[c * ndays + d];
            if a[0] > 0 || a[3] > 0 {
                if c == ncamp {
                    cid.push(b"organic".to_vec());
                    ch.push(b"organic".to_vec());
                } else {
                    cid.push(camp_names[c].clone());
                    ch.push(camp_channels[c].clone());
                }
                dy.push((day0 + d as i64) as i32);
                se.push(a[0]);
                ps.push(a[1]);
                rv.push(a[2]);
                asg.push(a[3]);
            }
        }
    }
    let cols = [
        parquet::Column::Str("campaign_id", cid),
        parquet::Column::Str("channel", ch),
        parquet::Column::I32("day", dy, true),
        parquet::Column::I64("sessions", se),
        parquet::Column::I64("purchasing_sessions", ps),
        parquet::Column::I64("revenue_cents", rv),
        parquet::Column::I64("assisted_sessions", asg),
    ];
    parquet::write_file(&cd_dir.join("part-0000.parquet"), &cols).expect("write campaign_daily");
    log("done");
}

#[allow(clippy::too_many_arguments)]
fn walk_person(person: u128, evs: &[EvP], cls: &[ClP], st: &mut P3, ncamp: usize, ndays: usize, day0: i64, rows: &mut UserRows) {
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut k = 0usize;
    let mut cur_day = i64::MIN;
    let (mut ds, mut de, mut dr) = (0i32, 0i32, 0i64);
    let name = key_bytes(person);
    macro_rules! flush {
        () => {
            if ds > 0 {
                let mut top: Option<u32> = None;
                let mut best = 0i64;
                for &c in &st.touched {
                    let v = st.catsum[c as usize];
                    if top.is_none() || v > best || (v == best && (c as u32) < top.unwrap()) {
                        top = Some(c as u32);
                        best = v;
                    }
                    st.catsum[c as usize] = 0;
                }
                st.touched.clear();
                rows.name.push(name.clone());
                rows.day.push(cur_day as i32);
                rows.sessions.push(ds);
                rows.events.push(de);
                rows.revenue.push(dr);
                rows.top.push(top);
                ds = 0;
                de = 0;
                dr = 0;
            }
        };
    }
    while k < evs.len() {
        let start = evs[k].ts;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            flush!();
            cur_day = day;
        }
        let mut last = start;
        let mut m = k;
        let mut rev = 0i64;
        let mut purch = false;
        while m < evs.len() && evs[m].ts - last <= GAP {
            let e = &evs[m];
            last = e.ts;
            if e.purch != 0 {
                rev += e.price as i64;
                purch = true;
                let c = e.cat as usize;
                if st.catsum[c] == 0 {
                    st.touched.push(e.cat);
                }
                st.catsum[c] += e.price as i64;
            }
            m += 1;
        }
        while hi < cls.len() && cls[hi].ts <= start {
            let c = cls[hi].camp as usize;
            if st.counts[c] == 0 {
                st.pos[c] = st.list.len() as u32;
                st.list.push(c as u32);
            }
            st.counts[c] += 1;
            hi += 1;
        }
        while lo < hi && cls[lo].ts < start - WEEK {
            let c = cls[lo].camp as usize;
            st.counts[c] -= 1;
            if st.counts[c] == 0 {
                let p = st.pos[c] as usize;
                let moved = *st.list.last().unwrap();
                st.list[p] = moved;
                st.pos[moved as usize] = p as u32;
                st.list.pop();
            }
            lo += 1;
        }
        let credit = if lo < hi {
            let t = cls[hi - 1].ts;
            let mut b = hi - 1;
            let mut j = hi - 1;
            while j > lo && cls[j - 1].ts == t {
                j -= 1;
                if key_cmp(cls[j].cid, cls[b].cid) == Ordering::Less {
                    b = j;
                }
            }
            cls[b].camp as usize
        } else {
            ncamp
        };
        let di = (day - day0) as usize;
        let a = &mut st.acc[credit * ndays + di];
        a[0] += 1;
        if purch {
            a[1] += 1;
        }
        a[2] += rev;
        for &c in &st.list {
            st.acc[c as usize * ndays + di][3] += 1;
        }
        ds += 1;
        de += (m - k) as i32;
        dr += rev;
        k = m;
    }
    flush!();
    for &c in &st.list {
        st.counts[c as usize] = 0;
    }
    st.list.clear();
}
