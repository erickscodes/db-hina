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


// ---------------------------------------------------------------- allocator

/// The system allocator, asking for transparent huge pages on big blocks:
/// fewer page faults when buffers are first touched, and a cheaper exit.
struct HugeAlloc;

extern "C" {
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
}

const MADV_POPULATE_READ: i32 = 22;
const MADV_POPULATE_WRITE: i32 = 23;

/// Faults in [p, p+n) with one call (Linux 5.14+); harmless if unsupported.
fn populate(p: *const u8, n: usize, write: bool) {
    let a = p as usize & !4095;
    let e = (p as usize + n + 4095) & !4095;
    if e > a {
        unsafe {
            madvise(a as *mut u8, e - a, if write { MADV_POPULATE_WRITE } else { MADV_POPULATE_READ });
        }
    }
}

/// Maps a whole file read-only; None if empty or mmap fails.
fn map_file(f: &File, size: u64) -> Option<&'static [u8]> {
    use std::os::unix::io::AsRawFd;
    if size == 0 {
        return None;
    }
    let p = unsafe { mmap(std::ptr::null_mut(), size as usize, 1, 2, f.as_raw_fd(), 0) };
    if p as isize == -1 || p.is_null() {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(p, size as usize) })
}

/// The lines starting in [start, end) of a mapped file, as (first line start,
/// region end), if the region ends at least 64 bytes before the end of the
/// file (so `scan` can over-read). The header line at offset 0 is skipped.
fn map_chunk(m: &[u8], start: usize, end: usize) -> Option<(usize, usize)> {
    let rs = start.saturating_sub(1);
    let region_end = find_nl(m, end - 1)? + 1;
    if region_end + 64 > m.len() {
        return None;
    }
    populate(m[rs..].as_ptr(), region_end - rs, false);
    let pos = match find_nl(&m[..region_end], rs) {
        Some(i) => i + 1,
        None => region_end,
    };
    Some((pos, region_end))
}

static HUGE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn advise(p: *mut u8, n: usize) {
    if n >= 4 << 20 && !p.is_null() && HUGE.load(AO::Relaxed) {
        let a = (p as usize + 4095) & !4095;
        let e = (p as usize + n) & !4095;
        if e > a {
            unsafe {
                madvise(a as *mut u8, e - a, 14); // MADV_HUGEPAGE
            }
        }
    }
}

unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        let p = std::alloc::System.alloc(l);
        advise(p, l.size());
        p
    }
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        let p = std::alloc::System.alloc_zeroed(l);
        advise(p, l.size());
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        std::alloc::System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, n: usize) -> *mut u8 {
        let q = std::alloc::System.realloc(p, l, n);
        advise(q, n);
        q
    }
}

#[global_allocator]
static GLOBAL: HugeAlloc = HugeAlloc;

// ---------------------------------------------------------------- keys

/// Every ID becomes a 64-bit key, exactly:
/// - Up to 9 bytes from `0-9 A-Z _ a-z` (every ID the generator writes): each
///   byte as a 6-bit code that keeps byte order, left-aligned in bits 62..9,
///   the length in bits 3..0. Integer order is then byte-wise string order.
/// - Anything else: bit 63 set and an index into a global intern table, so
///   equality stays exact; ordering goes through the strings (`key_cmp`).
/// The empty string is 0.
const fn code_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut c = 1u8;
    let mut b = 0usize;
    while b < 256 {
        let ch = b as u8;
        if ch.is_ascii_digit() || ch.is_ascii_uppercase() || ch == b'_' || ch.is_ascii_lowercase() {
            t[b] = c;
            c += 1;
        }
        b += 1;
    }
    t
}
static CODE: [u8; 256] = code_table();

const fn decode_table() -> [u8; 64] {
    let code = code_table();
    let mut d = [0u8; 64];
    let mut b = 0usize;
    while b < 256 {
        let c = code[b];
        if c != 0 {
            d[c as usize] = b as u8;
        }
        b += 1;
    }
    d
}
static DECODE: [u8; 64] = decode_table();

const OTHER: u64 = 1 << 63;

struct Interned {
    ids: HashMap<Vec<u8>, u64>,
    strings: Vec<Vec<u8>>,
}

static OTHERS: OnceLock<Mutex<Interned>> = OnceLock::new();

fn others() -> &'static Mutex<Interned> {
    OTHERS.get_or_init(|| Mutex::new(Interned { ids: HashMap::new(), strings: Vec::new() }))
}

#[inline(always)]
fn pack(s: &[u8]) -> u64 {
    let n = s.len();
    if n == 0 {
        return 0;
    }
    if n <= 9 {
        let mut k: u64 = 0;
        for &b in s {
            let c = CODE[b as usize];
            if c == 0 {
                return pack_other(s);
            }
            k = (k << 6) | c as u64;
        }
        (k << (6 * (9 - n) + 9)) | n as u64
    } else {
        pack_other(s)
    }
}

#[cold]
#[inline(never)]
fn pack_other(s: &[u8]) -> u64 {
    let mut g = others().lock().unwrap();
    if let Some(&k) = g.ids.get(s) {
        return k;
    }
    let k = OTHER | g.strings.len() as u64;
    g.strings.push(s.to_vec());
    g.ids.insert(s.to_vec(), k);
    k
}

fn key_bytes(k: u64) -> Vec<u8> {
    if k & OTHER != 0 {
        return others().lock().unwrap().strings[(k & !OTHER) as usize].clone();
    }
    let n = (k & 15) as usize;
    (0..n).map(|i| DECODE[((k >> (57 - 6 * i)) & 63) as usize]).collect()
}

#[inline]
fn key_cmp(a: u64, b: u64) -> Ordering {
    if (a | b) & OTHER != 0 {
        key_bytes(a).cmp(&key_bytes(b))
    } else {
        a.cmp(&b)
    }
}

#[inline(always)]
fn h64(k: u64) -> u64 {
    let mut z = k.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// Open-addressing map from non-zero u64 keys.
struct Map<V: Copy> {
    s: Vec<(u64, V)>,
    mask: usize,
}

impl<V: Copy + Default> Map<V> {
    fn new(n: usize) -> Self {
        let cap = (n * 2).max(16).next_power_of_two();
        Map { s: vec![(0, V::default()); cap], mask: cap - 1 }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<V> {
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
    #[inline(always)]
    fn get_h(&self, k: u64, h: u64) -> Option<V> {
        if k == 0 {
            return None;
        }
        let mut i = h as usize & self.mask;
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
    #[inline(always)]
    fn prefetch_h(&self, h: u64) {
        let i = h as usize & self.mask;
        unsafe {
            core::arch::x86_64::_mm_prefetch(self.s.as_ptr().add(i) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
    }
    /// Index of k's slot, inserting it with `V::default()` if absent; true if new.
    #[inline]
    fn slot(&mut self, k: u64) -> (usize, bool) {
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

/// Remembers validated `YYYY-MM-DD` prefixes and their day numbers: a
/// dataset's timestamps fall on a few weeks of dates, so most lookups hit.
struct DateCache {
    keys: [u64; 64],
    dd: [u16; 64],
    days: [i64; 64],
}

const BAD_DATE: i64 = i64::MIN;

impl DateCache {
    fn new() -> Self {
        DateCache { keys: [0; 64], dd: [0; 64], days: [0; 64] }
    }
    /// Day number of the first 10 bytes of `s` (`YYYY-MM-DD`), or None if
    /// they are not a valid date. `s` has at least 10 bytes.
    #[inline(always)]
    fn days(&mut self, s: &[u8]) -> Option<i64> {
        let k = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let d = u16::from_le_bytes(s[8..10].try_into().unwrap());
        let i = ((k ^ ((d as u64) << 40)).wrapping_mul(0x9E3779B97F4A7C15) >> 58) as usize;
        if self.keys[i] == k && self.dd[i] == d {
            let v = self.days[i];
            return if v == BAD_DATE { None } else { Some(v) };
        }
        let v = civil_days(s).unwrap_or(BAD_DATE);
        self.keys[i] = k;
        self.dd[i] = d;
        self.days[i] = v;
        if v == BAD_DATE {
            None
        } else {
            Some(v)
        }
    }
}

/// Validates `YYYY-MM-DD` and returns its day number.
fn civil_days(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    if !(1..=12).contains(&mo) || d < 1 {
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
    Some(days_from_civil(y, mo, d))
}

/// Parses F1, F2 or F3 into epoch ms. None for a bad format; a value too
/// large for i64 becomes i64::MAX (outside every window).
#[inline]
fn parse_ts(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if s[4] == b'-' {
            return parse_civil(s, dc);
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

#[inline(always)]
fn parse_civil(s: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let days = dc.days(s)?;
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
    Some((days * 86_400 + h * 3600 + mi * 60 + se) * 1000 - off)
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
    // 64 bytes of slack so `scan` can load whole 64-byte blocks.
    let dl = buf.len();
    buf.resize(dl + 64, 0);
    (pos, region_end)
}

/// Comma, newline and quote bitmasks of the 64 bytes at `p`.
#[inline(always)]
fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use core::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |ch: u8| -> u64 {
            let v = _mm256_set1_epi8(ch as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
            lo | hi << 32
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let b = unsafe { *p.add(i) };
            c |= ((b == b',') as u64) << i;
            n |= ((b == b'\n') as u64) << i;
            q |= ((b == b'"') as u64) << i;
        }
        (c, n, q)
    }
}

/// Builds the N fields of one line and calls `on` if the line is well-formed.
/// `lq` is the number of quotes in the whole line. A field is quoted when its
/// first and last bytes are both quotes (length >= 2); every other quote makes
/// the line malformed, which is exactly "quotes only around a whole field".
#[inline(always)]
fn finish_line<'a, const N: usize>(buf: &'a [u8], ls: usize, le: usize, nf: usize, ends: &[usize; N], lq: u32, on: &mut impl FnMut(&[&'a [u8]; N])) {
    if le == ls || nf != N {
        return;
    }
    let mut f: [&[u8]; N] = [&[]; N];
    let mut s = ls;
    if lq == 0 {
        for k in 0..N {
            let e = ends[k];
            f[k] = unsafe { buf.get_unchecked(s..e) };
            s = e + 1;
        }
        on(&f);
        return;
    }
    let mut ok = true;
    let mut expected = 0u32;
    for k in 0..N {
        let e = ends[k];
        let (mut lo, mut hi) = (s, e);
        if e > s {
            let first = unsafe { *buf.get_unchecked(s) } == b'"';
            let last = unsafe { *buf.get_unchecked(e - 1) } == b'"';
            if first || last {
                ok &= first && last && e >= s + 2;
                expected += 2;
                lo = s + 1;
                hi = if e >= s + 2 { e - 1 } else { lo };
            }
        }
        f[k] = unsafe { buf.get_unchecked(lo..hi) };
        s = e + 1;
    }
    if ok && expected == lq {
        on(&f);
    }
}

/// Calls `on` with the N fields of every well-formed line in buf[pos..end],
/// finding delimiters 64 bytes at a time. `buf` needs 64 bytes past `end`.
#[inline(always)]
fn scan<'a, const N: usize>(buf: &'a [u8], pos: usize, end: usize, mut on: impl FnMut(&[&'a [u8]; N])) {
    assert!(buf.len() >= end + 64);
    let mut ends = [0usize; N];
    let mut nf = 0usize;
    let mut line_start = pos;
    // Quotes seen before the current 64-byte block, and before the current line.
    let mut qbase: u32 = 0;
    let mut qline: u32 = 0;
    let mut base = pos;
    while base < end {
        let (c, n, q) = masks64(unsafe { buf.as_ptr().add(base) });
        let lim = if end - base < 64 { (1u64 << (end - base)) - 1 } else { !0 };
        let (n, q) = (n & lim, q & lim);
        let mut d = (c & lim) | n;
        while d != 0 {
            let b = d.trailing_zeros();
            let at = base + b as usize;
            if nf < N {
                ends[nf] = at;
            }
            nf += 1;
            if (n >> b) & 1 == 1 {
                let qc = qbase + (q & ((1u64 << b) - 1)).count_ones();
                finish_line::<N>(buf, line_start, at, nf, &ends, qc - qline, &mut on);
                qline = qc;
                line_start = at + 1;
                nf = 0;
            }
            d &= d - 1;
        }
        qbase += q.count_ones();
        base += 64;
    }
    if line_start < end {
        if nf < N {
            ends[nf] = end;
        }
        nf += 1;
        finish_line::<N>(buf, line_start, end, nf, &ends, qbase - qline, &mut on);
    }
}

#[cfg(test)]
mod scan_tests {
    use super::*;

    /// Every line through the scalar `split` (the reference) and through `scan`.
    fn both<const N: usize>(text: &[u8]) -> (Vec<Vec<Vec<u8>>>, Vec<Vec<Vec<u8>>>) {
        let mut want = Vec::new();
        let mut pos = 0;
        let mut f: [&[u8]; N] = [&[]; N];
        while pos < text.len() {
            let (ok, next) = split::<N>(text, pos, text.len(), &mut f);
            if ok {
                want.push(f.iter().map(|x| x.to_vec()).collect());
            }
            pos = next;
        }
        let mut buf = text.to_vec();
        buf.resize(text.len() + 64, 0);
        let mut got = Vec::new();
        scan::<N>(&buf, 0, text.len(), |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
        (want, got)
    }

    #[test]
    fn scan_matches_scalar_split_on_random_lines() {
        let alphabet = b"ab,,\"\"\n\r x";
        let mut seed: u64 = 0x1234_5678;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200_000 {
            let len = (next() % 160) as usize;
            let text: Vec<u8> = (0..len).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let (w2, g2) = both::<2>(&text);
            assert_eq!(w2, g2, "N=2 round {round} text {:?}", String::from_utf8_lossy(&text));
            let (w4, g4) = both::<4>(&text);
            assert_eq!(w4, g4, "N=4 round {round} text {:?}", String::from_utf8_lossy(&text));
            let (w7, g7) = both::<7>(&text);
            assert_eq!(w7, g7, "N=7 round {round} text {:?}", String::from_utf8_lossy(&text));
        }
    }

    #[test]
    fn scan_edge_cases() {
        for text in [
            &b""[..], b"\n", b"a,b", b"a,b\n", b"\"\",\"\"\n", b"\",\"\n", b"\"\"\",x\n", b"a\"b,c\n",
            b"\"a\",b\r\n", b"\"a,b\n", b"a\",b\n", b"\"\"\"\",b\n", b",\n", b"\"\"\n",
        ] {
            let (w, g) = both::<2>(text);
            assert_eq!(w, g, "{:?}", String::from_utf8_lossy(text));
        }
        // Lines spanning several 64-byte blocks, quotes on both sides of a block edge.
        let mut long = Vec::new();
        for i in 0..50 {
            long.extend_from_slice(format!("\"{}\",{}\n", "x".repeat(i * 3), "\"y\"").as_bytes());
        }
        let (w, g) = both::<2>(&long);
        assert_eq!(w, g);
    }
}

// ---------------------------------------------------------------- records

/// An event row before dedup. Valid rows have `user != 0`; purchases have a
/// non-zero price. `tsc` is `ts - ws` in the low 48 bits and the category
/// above; `ip` is the ingest time in the high 40 bits (seconds since
/// 0000-01-01, exact for every valid F1 timestamp) and the price in the low 24.
#[derive(Clone, Copy)]
struct EvRaw {
    eid: u64,
    user: u64,
    tsc: u64,
    ip: u64,
}

#[derive(Clone, Copy)]
struct ClickRaw {
    user: u64,
    cid: u64,
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct EvP {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    /// Phase-3 sub-bucket of `person` (see `bucket_of`), cached in padding.
    bk: u16,
}
const _: () = assert!(std::mem::size_of::<EvP>() == 24);

#[derive(Clone, Copy)]
struct ClP {
    person: u64,
    cid: u64,
    ts: i64,
    camp: u32,
    bk: u16,
}
const _: () = assert!(std::mem::size_of::<ClP>() == 32);

struct Ctx {
    ws: i64,
    we: i64,
    products: Map<u16>,
    unknown_cat: u16,
    campaigns: Map<u32>,
}

// ---------------------------------------------------------------- partitioned buffers

/// Bytes per block. Blocks hold records of one partition.
const BLOCK_BYTES: usize = 32 << 10;

/// Blocks whose records were consumed, ready to be written again: reusing
/// memory that is already faulted in costs no page faults.
static POOL: Mutex<Vec<usize>> = Mutex::new(Vec::new());
/// Blocks in POOL, read without the lock to skip it when empty.
static POOL_LEN: AtomicUsize = AtomicUsize::new(0);

/// Records appended to many partitions, in fixed-size blocks carved from a few
/// large chunks or taken from `POOL`, so no per-partition vector grows.
struct Parted<T: Copy> {
    chunks: Vec<Vec<u64>>,
    chunk_bytes: usize,
    used: usize,
    populated: usize,
    spare: Vec<usize>,
    cap: u32,
    /// Per partition: the open block and its length (null = none yet).
    cur: Vec<(*mut T, u32)>,
    done: Vec<Vec<*mut T>>,
}

unsafe impl<T: Copy> Send for Parted<T> {}
unsafe impl<T: Copy> Sync for Parted<T> {}

impl<T: Copy> Parted<T> {
    fn new(parts: usize, expect: usize) -> Self {
        let cap = (BLOCK_BYTES / std::mem::size_of::<T>()) as u32;
        let blocks = expect.div_ceil(cap as usize) + parts;
        Parted {
            chunks: Vec::new(),
            chunk_bytes: (blocks * BLOCK_BYTES).max(4 * BLOCK_BYTES),
            used: usize::MAX,
            populated: 0,
            spare: Vec::new(),
            cap,
            cur: vec![(std::ptr::null_mut(), cap); parts],
            done: vec![Vec::new(); parts],
        }
    }
    #[cold]
    fn new_block(&mut self) -> *mut T {
        if self.spare.is_empty() && POOL_LEN.load(AO::Relaxed) > 0 {
            let mut g = POOL.lock().unwrap();
            let take = g.len().min(64);
            let at = g.len() - take;
            self.spare.extend(g.drain(at..));
            POOL_LEN.store(g.len(), AO::Relaxed);
        }
        if let Some(b) = self.spare.pop() {
            return b as *mut T;
        }
        if self.used >= self.chunk_bytes {
            // u64 storage: blocks start 8-byte aligned (BLOCK_BYTES is a multiple of 8).
            self.chunks.push(Vec::with_capacity(self.chunk_bytes / 8));
            self.used = 0;
            self.populated = 0;
        }
        let base = self.chunks.last_mut().unwrap().as_mut_ptr() as *mut u8;
        if self.used + BLOCK_BYTES > self.populated {
            // Fault in the next window with one call instead of page by page.
            let upto = (self.populated + (4 << 20)).min(self.chunk_bytes);
            populate(unsafe { base.add(self.populated) }, upto - self.populated, true);
            self.populated = upto;
        }
        let b = unsafe { base.add(self.used) } as *mut T;
        self.used += BLOCK_BYTES;
        b
    }
    #[inline(always)]
    fn push(&mut self, p: usize, v: T) {
        let c = unsafe { *self.cur.get_unchecked(p) };
        if c.1 == self.cap {
            let nb = self.new_block();
            if !c.0.is_null() {
                self.done[p].push(c.0);
            }
            self.cur[p] = (nb, 0);
        }
        let c = unsafe { self.cur.get_unchecked_mut(p) };
        unsafe { c.0.add(c.1 as usize).write(v) };
        c.1 += 1;
    }
    fn len(&self, p: usize) -> usize {
        let c = self.cur[p];
        self.done[p].len() * self.cap as usize + if c.0.is_null() { 0 } else { c.1 as usize }
    }
    fn for_each_slice(&self, p: usize, mut f: impl FnMut(&[T])) {
        for &b in &self.done[p] {
            f(unsafe { std::slice::from_raw_parts(b, self.cap as usize) });
        }
        let c = self.cur[p];
        if !c.0.is_null() {
            f(unsafe { std::slice::from_raw_parts(c.0, c.1 as usize) });
        }
    }
    /// Appends partition p's blocks as (address, records).
    fn raw_blocks(&self, p: usize, out: &mut Vec<(usize, u32)>) {
        out.extend(self.done[p].iter().map(|&b| (b as usize, self.cap)));
        let c = self.cur[p];
        if !c.0.is_null() {
            out.push((c.0 as usize, c.1));
        }
    }
    /// Hands partition p's blocks to `POOL`. The caller must not read them again.
    fn recycle(&self, p: usize) {
        let mut g = POOL.lock().unwrap();
        g.extend(self.done[p].iter().map(|&b| b as usize));
        let c = self.cur[p];
        if !c.0.is_null() {
            g.push(c.0 as usize);
        }
        POOL_LEN.store(g.len(), AO::Relaxed);
    }
}

fn parted<T: Copy>(parts: usize, expect: usize) -> Parted<T> {
    Parted::new(parts, expect)
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
    parse_ts(rest[..q2].as_bytes(), &mut DateCache::new()).expect("manifest timestamp")
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

// ---------------------------------------------------------------- linking

/// ID -> person name, for the IDs whose name is a different ID. Every other
/// ID (unlinked, or the smallest of its person) is its own name. A blocked
/// Bloom filter in front rejects most such IDs without touching the table.
struct People {
    map: Map<u64>,
    bloom: Vec<u64>,
    shift: u32,
}

impl People {
    #[inline(always)]
    fn bits(h: u64) -> u64 {
        (1u64 << (h & 63)) | (1u64 << ((h >> 6) & 63))
    }
    #[inline(always)]
    fn maybe(&self, h: u64) -> bool {
        let b = Self::bits(h);
        unsafe { *self.bloom.get_unchecked((h >> self.shift) as usize) & b == b }
    }
    /// (person, h64(person)) for an ID whose hash `h` is known.
    #[inline(always)]
    fn person_h(&self, k: u64, h: u64) -> (u64, u64) {
        if !self.maybe(h) {
            return (k, h);
        }
        match self.map.get_h(k, h) {
            Some(p) => (p, h64(p)),
            None => (k, h),
        }
    }
    #[inline(always)]
    fn prefetch_bloom_h(&self, h: u64) {
        unsafe {
            core::arch::x86_64::_mm_prefetch(self.bloom.as_ptr().add((h >> self.shift) as usize) as *const i8, core::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn prefetch_h(&self, h: u64) {
        if self.maybe(h) {
            self.map.prefetch_h(h);
        }
    }
    #[inline(always)]
    #[allow(dead_code)]
    fn person(&self, k: u64) -> u64 {
        let h = h64(k);
        if !self.maybe(h) {
            return k;
        }
        self.map.get_h(k, h).unwrap_or(k)
    }
}

/// Union-find over every linked ID; names each person by its smallest ID.
fn build_people(links: &[Vec<(u64, u64)>]) -> People {
    let npairs: usize = links.iter().map(|l| l.len()).sum();
    let mut pmap = Map::<u64>::new(npairs + 16);
    let mut ids: Vec<u64> = Vec::with_capacity(npairs);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for l in links {
        for &(a, b) in l {
            let mut idx = [0u32; 2];
            for (j, k) in [a, b].into_iter().enumerate() {
                let (s, new) = pmap.slot(k);
                if new {
                    pmap.s[s].1 = ids.len() as u64;
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
    let roots: Vec<u32> = (0..ids.len() as u32).map(|i| find(&mut parent, i)).collect();
    let mut name: Vec<u64> = ids.clone();
    for i in 0..ids.len() {
        let r = roots[i] as usize;
        if key_cmp(ids[i], name[r]) == Ordering::Less {
            name[r] = ids[i];
        }
    }
    let renamed: Vec<(u64, u64)> = (0..ids.len()).map(|i| (ids[i], name[roots[i] as usize])).filter(|&(k, n)| k != n).collect();
    drop(pmap);
    let mut map = Map::<u64>::new(renamed.len() + 16);
    // About 16 bits per entry, at least 1024 words.
    let words = (renamed.len() / 4).max(1024).next_power_of_two();
    let mut bloom = vec![0u64; words];
    let shift = 64 - words.trailing_zeros();
    for &(k, n) in &renamed {
        let (s, _) = map.slot(k);
        map.s[s].1 = n;
        let h = h64(k);
        bloom[(h >> shift) as usize] |= People::bits(h);
    }
    People { map, bloom, shift }
}

#[repr(C)]
struct RUsage {
    utime: [i64; 2],
    stime: [i64; 2],
    rest: [i64; 14],
}
extern "C" {
    fn getrusage(who: i32, usage: *mut RUsage) -> i32;
}
/// Process CPU seconds so far: (user, system).
fn cpu_times() -> (f64, f64) {
    let mut r = RUsage { utime: [0; 2], stime: [0; 2], rest: [0; 14] };
    unsafe { getrusage(0, &mut r) };
    (r.utime[0] as f64 + r.utime[1] as f64 * 1e-6, r.stime[0] as f64 + r.stime[1] as f64 * 1e-6)
}

const SUB_BITS: u32 = 12;

/// The bucket of a person inside its phase-3 partition: the hash bits right
/// below the ones that chose the partition.
#[inline(always)]
fn bucket_of(person_hash: u64) -> u16 {
    ((person_hash >> (64 - NQ_BITS - SUB_BITS)) & ((1 << SUB_BITS) - 1)) as u16
}

#[inline(always)]
#[allow(dead_code)]
fn sub_bucket(person: u64) -> usize {
    ((h64(person) >> (64 - NQ_BITS - SUB_BITS)) & ((1 << SUB_BITS) - 1)) as usize
}

/// Copies partition q of every worker's `Parted` into `out`, grouped by
/// `sub_bucket`; `bounds` gets the bucket boundaries (len = buckets + 1).
fn bucket_gather<P, T: Copy>(src: &[P], q: usize, get: impl Fn(&P) -> &Parted<T>, bucket: impl Fn(&T) -> usize, out: &mut Vec<T>, bounds: &mut Vec<u32>) {
    let nb = 1usize << SUB_BITS;
    bounds.clear();
    bounds.resize(nb + 1, 0);
    for p in src {
        get(p).for_each_slice(q, |s| {
            for r in s {
                bounds[bucket(r) + 1] += 1;
            }
        });
    }
    for b in 1..=nb {
        bounds[b] += bounds[b - 1];
    }
    let n = bounds[nb] as usize;
    out.clear();
    out.reserve(n);
    let mut at: Vec<u32> = bounds[..nb].to_vec();
    let dst = out.as_mut_ptr();
    for p in src {
        get(p).for_each_slice(q, |s| {
            for r in s {
                let b = bucket(r);
                unsafe { dst.add(at[b] as usize).write(*r) };
                at[b] += 1;
            }
        });
    }
    // Every slot in 0..n was written exactly once above.
    unsafe { out.set_len(n) };
}

// ---------------------------------------------------------------- main

struct P1 {
    ev: Parted<EvRaw>,
    cl: Vec<ClickRaw>,
    buf: Vec<u8>,
    dc: DateCache,
}

struct P2 {
    ev: Parted<EvP>,
    cl: Parted<ClP>,
    blocks: Vec<(usize, u32)>,
    table: Vec<u64>,
}

struct UserRows {
    /// user_id column, PLAIN-encoded.
    name: Vec<u8>,
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
    ev: Vec<EvP>,
    cl: Vec<ClP>,
    counts_ev: Vec<u32>,
    counts_cl: Vec<u32>,
}

static PT: [AtomicUsize; 5] = [AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0)];
/// Seconds from 0000-01-01T00:00:00Z to the epoch, rounded up: F1 years run
/// 0000-9999, so ingest seconds + this fit in 40 bits.
const INGEST_BASE: i64 = 62_200_000_000;
const TS_MASK: u64 = (1 << 48) - 1;

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let output = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let w: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()))
        .max(1);
    // Huge pages are opt-in: on a fragmented machine the kernel stalls compacting
    // memory for them (60M events: 45 s with, 15 s without, locally).
    if std::env::var("HUGE").is_ok() {
        HUGE.store(true, AO::Relaxed);
    }
    let log = |m: &str| {
        let (u, sy) = cpu_times();
        eprintln!("[{:7.3}s user {:6.2} sys {:6.2}] {m}", t0.elapsed().as_secs_f64(), u, sy)
    };

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

    // --- tasks: link files first, so one worker can build the people while the rest parse
    let ev_files = list_csv(&data.join("events"));
    let cl_files = list_csv(&data.join("clicks"));
    let ln_files = list_csv(&data.join("links"));
    let mut files: Vec<(u8, File, u64, Option<&'static [u8]>)> = Vec::new();
    let mut tasks: Vec<(usize, u64, u64)> = Vec::new();
    let (mut ev_bytes, mut cl_bytes) = (0u64, 0u64);
    let mut link_tasks = 0usize;
    for (kind, list) in [(2u8, &ln_files), (0u8, &ev_files), (1u8, &cl_files)] {
        for (p, size) in list {
            let fi = files.len();
            let f = File::open(p).expect("open");
            let m = map_file(&f, *size);
            files.push((kind, f, *size, m));
            match kind {
                0 => ev_bytes += size,
                1 => cl_bytes += size,
                _ => {}
            }
            let mut s = 0;
            while s < *size {
                let e = (s + CHUNK).min(*size);
                tasks.push((fi, s, e));
                if kind == 2 {
                    link_tasks += 1;
                }
                s = e;
            }
        }
    }
    // Start reading every input file now, in the order workers will need them:
    // many requests in flight if the data is not in the page cache yet.
    {
        use std::os::unix::io::AsRawFd;
        let fds: Vec<i32> = files.iter().map(|f| f.1.as_raw_fd()).collect();
        std::thread::spawn(move || {
            for fd in fds {
                unsafe {
                    posix_fadvise(fd, 0, 0, 3); // POSIX_FADV_WILLNEED
                }
            }
        });
    }
    let est_ev = (ev_bytes / 64) as usize;
    let est_cl = (cl_bytes / 40) as usize;
    log(&format!("{} files, {} tasks, {} event bytes, {} workers", files.len(), tasks.len(), ev_bytes, w));

    // --- phase 1: parse (and link)
    let links_store: Mutex<Vec<Vec<(u64, u64)>>> = Mutex::new(Vec::new());
    let links_left = AtomicUsize::new(link_tasks);
    let people: OnceLock<People> = OnceLock::new();
    if link_tasks == 0 {
        let _ = people.set(build_people(&[]));
    }
    let p1s = run_par(
        w,
        tasks.len(),
        || P1 { ev: parted(NP, est_ev * 11 / 10 / w), cl: Vec::with_capacity(est_cl * 11 / 10 / w), buf: Vec::new(), dc: DateCache::new() },
        |st, t| {
            let (fi, s, e) = tasks[t];
            let (kind, ref file, size, map) = files[fi];
            let mut owned = std::mem::take(&mut st.buf);
            let dc = &mut st.dc;
            let (buf, pos, end): (&[u8], usize, usize) = match map.and_then(|m| map_chunk(m, s as usize, e as usize).map(|r| (m, r))) {
                Some((m, (pos, end))) => (m, pos, end),
                None => {
                    let (pos, end) = read_chunk(file, size, s, e, &mut owned);
                    (&owned[..], pos, end)
                }
            };
            match kind {
                0 => {
                    scan::<7>(&buf, pos, end, |f| {
                        if f[0].is_empty() {
                            return;
                        }
                        let ingest = match parse_ts(f[3], dc) {
                            Some(v) => v,
                            None => return,
                        };
                        let mut user = 0u64;
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
                            if let Some(v) = parse_ts(f[2], dc) {
                                if v >= ctx.ws && v < ctx.we && ingest - v <= DAY {
                                    if ty == 2 {
                                        if let Some(p) = parse_price(f[6]) {
                                            price = p;
                                            cat = ctx.products.get(pack(f[5])).unwrap_or(ctx.unknown_cat);
                                            user = pack(f[1]);
                                            ts = v - ctx.ws;
                                        }
                                    } else {
                                        user = pack(f[1]);
                                        ts = v - ctx.ws;
                                    }
                                }
                            }
                        }
                        let eid = pack(f[0]);
                        let ing = (ingest.div_euclid(1000) + INGEST_BASE).clamp(0, (1i64 << 40) - 1) as u64;
                        let p = (h64(eid) >> (64 - NP_BITS)) as usize;
                        st.ev.push(p, EvRaw { eid, user, tsc: ts as u64 | (cat as u64) << 48, ip: (ing << 24) | price as u64 });
                    });
                }
                1 => {
                    let lo = ctx.ws - WEEK;
                    scan::<4>(&buf, pos, end, |f| {
                        if f[0].is_empty() || f[1].is_empty() {
                            return;
                        }
                        let ts = match parse_ts(f[3], dc) {
                            Some(v) if v >= lo && v < ctx.we => v,
                            _ => return,
                        };
                        let camp = match ctx.campaigns.get(pack(f[2])) {
                            Some(c) => c,
                            None => return,
                        };
                        st.cl.push(ClickRaw { user: pack(f[1]), cid: pack(f[0]), ts, camp });
                    });
                }
                _ => {
                    let mut local = Vec::new();
                    scan::<2>(&buf, pos, end, |f| {
                        if !f[0].is_empty() && !f[1].is_empty() {
                            local.push((pack(f[0]), pack(f[1])));
                        }
                    });
                    links_store.lock().unwrap().push(local);
                    if links_left.fetch_sub(1, AO::AcqRel) == 1 {
                        let all = std::mem::take(&mut *links_store.lock().unwrap());
                        let _ = people.set(build_people(&all));
                    }
                }
            }
            st.buf = owned;
        },
    );
    let pmap = people.into_inner().expect("people built");
    log("parsed and linked");

    // --- phase 2: dedup events, map to people, partition by person
    let ncl_chunks: Vec<(usize, usize, usize)> = {
        let mut v = Vec::new();
        for (wi, p) in p1s.iter().enumerate() {
            let mut s = 0;
            while s < p.cl.len() {
                let e = (s + (1 << 16)).min(p.cl.len());
                v.push((wi, s, e));
                s = e;
            }
        }
        v
    };
    let p2s = run_par(
        w,
        NP + ncl_chunks.len(),
        || P2 { ev: parted(NQ, est_ev / w), cl: parted(NQ, est_cl * 11 / 10 / w), blocks: Vec::new(), table: Vec::new() },
        |st, t| {
            if t < NP {
                let tt = Instant::now();
                // Dedup in place, over the phase-1 blocks of partition t (no copy).
                // Only this task touches partition t, so it owns these blocks.
                // A losing copy gets user = 0 (the same mark as an invalid row).
                const _: () = assert!(std::mem::size_of::<EvRaw>() * 1024 == BLOCK_BYTES);
                let blocks = &mut st.blocks;
                blocks.clear();
                for p in &p1s {
                    p.ev.raw_blocks(t, blocks);
                }
                let n: usize = blocks.iter().map(|b| b.1 as usize).sum();
                PT[0].fetch_add(tt.elapsed().as_nanos() as usize, AO::Relaxed);
                let tt = Instant::now();
                let cap = (n * 2).max(16).next_power_of_two();
                let mask = cap - 1;
                // Each slot: hash fingerprint (high 32 bits) and location + 1,
                // location = block index << 10 | offset in block.
                let table = &mut st.table;
                table.clear();
                table.resize(cap, 0);
                const DAHEAD: usize = 16;
                for (bi, &(bp, bl)) in blocks.iter().enumerate() {
                    let bp = bp as *mut EvRaw;
                    let bl = bl as usize;
                    for o in 0..bl {
                        unsafe {
                            if o + DAHEAD < bl {
                                let hs = h64((*bp.add(o + DAHEAD)).eid) as usize & mask;
                                core::arch::x86_64::_mm_prefetch(table.as_ptr().add(hs) as *const i8, core::arch::x86_64::_MM_HINT_T0);
                            }
                            let r = bp.add(o);
                            let eid = (*r).eid;
                            let ing = (*r).ip >> 24;
                            let loc = ((bi << 10) | o) as u64 + 1;
                            let h = h64(eid);
                            let fp = h & 0xFFFF_FFFF_0000_0000;
                            let mut sl = h as usize & mask;
                            loop {
                                let cur = table[sl];
                                if cur == 0 {
                                    table[sl] = fp | loc;
                                    break;
                                }
                                if cur & 0xFFFF_FFFF_0000_0000 == fp {
                                    let cl = (cur & 0xFFFF_FFFF) - 1;
                                    let c = (blocks[(cl >> 10) as usize].0 as *mut EvRaw).add((cl & 1023) as usize);
                                    if (*c).eid == eid {
                                        if ing < (*c).ip >> 24 {
                                            table[sl] = fp | loc;
                                            (*c).user = 0;
                                        } else {
                                            (*r).user = 0;
                                        }
                                        break;
                                    }
                                }
                                sl = (sl + 1) & mask;
                            }
                        }
                    }
                }
                PT[1].fetch_add(tt.elapsed().as_nanos() as usize, AO::Relaxed);
                let tt = Instant::now();
                PT[2].fetch_add(tt.elapsed().as_nanos() as usize, AO::Relaxed);
                let tt = Instant::now();
                // Survivors go straight from their phase-1 block to the person
                // partition (no survivor copy). Per block: list survivor offsets,
                // then run the hash-once prefetch pipeline over them.
                const FAR: usize = 32;
                const NEAR: usize = 16;
                let mut idx = [0u16; 1024];
                for &(bp, bl) in blocks.iter() {
                    let blk = unsafe { std::slice::from_raw_parts(bp as *const EvRaw, bl as usize) };
                    let mut m = 0usize;
                    for (o, r) in blk.iter().enumerate() {
                        idx[m] = o as u16;
                        m += (r.user != 0) as usize;
                    }
                    let mut ring = [0u64; FAR];
                    for i in 0..FAR.min(m) {
                        ring[i] = h64(blk[idx[i] as usize].user);
                        pmap.prefetch_bloom_h(ring[i]);
                    }
                    for i in 0..m {
                        let h = ring[i % FAR];
                        if i + FAR < m {
                            let hf = h64(blk[idx[i + FAR] as usize].user);
                            ring[i % FAR] = hf;
                            pmap.prefetch_bloom_h(hf);
                        }
                        if i + NEAR < m {
                            pmap.prefetch_h(ring[(i + NEAR) % FAR]);
                        }
                        let r = blk[idx[i] as usize];
                        let (person, ph) = pmap.person_h(r.user, h);
                        let q = (ph >> (64 - NQ_BITS)) as usize;
                        st.ev.push(q, EvP { person, ts: (r.tsc & TS_MASK) as i64 + ws, price: (r.ip & 0xFF_FFFF) as u32, cat: (r.tsc >> 48) as u16, bk: bucket_of(ph) });
                    }
                }
                // Partition t is consumed; its blocks can hold phase-2 output.
                for p in &p1s {
                    p.ev.recycle(t);
                }
                PT[3].fetch_add(tt.elapsed().as_nanos() as usize, AO::Relaxed);
            } else {
                let tt = Instant::now();
                let (wi, s, e) = ncl_chunks[t - NP];
                let cl = &p1s[wi].cl[s..e];
                const FAR: usize = 32;
                const NEAR: usize = 16;
                // Each user is hashed once: a ring keeps hashes from the far stage.
                let n = cl.len();
                let mut ring = [0u64; FAR];
                for i in 0..FAR.min(n) {
                    ring[i] = h64(cl[i].user);
                    pmap.prefetch_bloom_h(ring[i]);
                }
                for i in 0..n {
                    let h = ring[i % FAR];
                    if i + FAR < n {
                        let hf = h64(cl[i + FAR].user);
                        ring[i % FAR] = hf;
                        pmap.prefetch_bloom_h(hf);
                    }
                    if i + NEAR < n {
                        pmap.prefetch_h(ring[(i + NEAR) % FAR]);
                    }
                    let c = cl[i];
                    let (person, ph) = pmap.person_h(c.user, h);
                    let q = (ph >> (64 - NQ_BITS)) as usize;
                    st.cl.push(q, ClP { person, cid: c.cid, ts: c.ts, camp: c.camp, bk: bucket_of(ph) });
                }
                PT[4].fetch_add(tt.elapsed().as_nanos() as usize, AO::Relaxed);
            }
        },
    );
    std::mem::forget(p1s);
    std::mem::forget(pmap);
    log(&format!("deduped and partitioned; thread-ms gather {} dedup {} surv {} map+push {} clicks {}", PT[0].load(AO::Relaxed)/1000000, PT[1].load(AO::Relaxed)/1000000, PT[2].load(AO::Relaxed)/1000000, PT[3].load(AO::Relaxed)/1000000, PT[4].load(AO::Relaxed)/1000000));

    // --- phase 3: sessionize, attribute, aggregate, write user_daily
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ud_dir = output.join("user_daily");
    let cd_dir = output.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir");
    std::fs::create_dir_all(&cd_dir).expect("mkdir");
    let mut order: Vec<usize> = (0..NQ).collect();
    let sizes: Vec<usize> = (0..NQ).map(|q| p2s.iter().map(|p| p.ev.len(q) + p.cl.len(q)).sum()).collect();
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
            ev: Vec::new(),
            cl: Vec::new(),
            counts_ev: Vec::new(),
            counts_cl: Vec::new(),
        },
        |st, t| {
            let q = order[t];
            let mut ev = std::mem::take(&mut st.ev);
            let mut cl = std::mem::take(&mut st.cl);
            // Gather by a counting sort on further person-hash bits, then sort
            // each small bucket exactly by (person, ts). People end up ordered by
            // (bucket, person), the same for events and clicks.
            bucket_gather(&p2s, q, |p| &p.ev, |e| e.bk as usize, &mut ev, &mut st.counts_ev);
            bucket_gather(&p2s, q, |p| &p.cl, |c| c.bk as usize, &mut cl, &mut st.counts_cl);
            for w in st.counts_ev.windows(2) {
                ev[w[0] as usize..w[1] as usize].sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
            }
            for w in st.counts_cl.windows(2) {
                cl[w[0] as usize..w[1] as usize].sort_unstable_by(|a, b| a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)));
            }
            let mut rows = UserRows { name: Vec::new(), day: Vec::new(), sessions: Vec::new(), events: Vec::new(), revenue: Vec::new(), top: Vec::new() };
            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let person = ev[i].person;
                let mut j = i + 1;
                while j < ev.len() && ev[j].person == person {
                    j += 1;
                }
                let pk = (ev[i].bk, person);
                while ci < cl.len() && (cl[ci].bk, cl[ci].person) < pk {
                    ci += 1;
                }
                let cs = ci;
                while ci < cl.len() && cl[ci].person == person {
                    ci += 1;
                }
                walk_person(person, &ev[i..j], &cl[cs..ci], st, ncamp, ndays, day0, &mut rows);
                i = j;
            }
            st.ev = ev;
            st.cl = cl;
            st.rows += rows.day.len();
            if !rows.day.is_empty() {
                let cols = [
                    parquet::Column::PlainStr("user_id", rows.name, rows.day.len()),
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
    // Skip freeing gigabytes of buffers one by one: the process is finished.
    std::process::exit(0);
}

#[allow(clippy::too_many_arguments)]
fn walk_person(person: u64, evs: &[EvP], cls: &[ClP], st: &mut P3, ncamp: usize, ndays: usize, day0: i64, rows: &mut UserRows) {
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
                rows.name.extend_from_slice(&(name.len() as u32).to_le_bytes());
                rows.name.extend_from_slice(&name);
                rows.day.push(cur_day as i32);
                rows.sessions.push(ds);
                rows.events.push(de);
                rows.revenue.push(dr);
                rows.top.push(top);
            }
        };
    }
    while k < evs.len() {
        let start = evs[k].ts;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            flush!();
            ds = 0;
            de = 0;
            dr = 0;
            cur_day = day;
        }
        let mut last = start;
        let mut m = k;
        let mut rev = 0i64;
        let mut purch = false;
        while m < evs.len() && evs[m].ts - last <= GAP {
            let e = &evs[m];
            last = e.ts;
            if e.price != 0 {
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
