//! A minimal Parquet writer: one row group, one uncompressed v1 data page per
//! column. Supports required BYTE_ARRAY (UTF8), INT32 (optionally DATE), INT64,
//! and an optional dictionary-encoded UTF8 column with a small dictionary.

use std::io::Write;

pub enum Column<'a> {
    /// Required UTF8 strings.
    Str(&'a str, Vec<Vec<u8>>),
    /// Required UTF8 strings already PLAIN-encoded (4-byte length + bytes each), and their count.
    PlainStr(&'a str, Vec<u8>, usize),
    /// Required INT32; `true` marks it as a DATE.
    I32(&'a str, Vec<i32>, bool),
    /// Required INT64.
    I64(&'a str, Vec<i64>),
    /// Optional UTF8 from a dictionary: per row an index into `dict`, or None.
    DictStr(&'a str, Vec<Option<u32>>, &'a [Vec<u8>]),
}

impl Column<'_> {
    fn name(&self) -> &str {
        match self {
            Column::Str(n, _) | Column::PlainStr(n, _, _) | Column::I32(n, _, _) | Column::I64(n, _) | Column::DictStr(n, _, _) => n,
        }
    }
    fn len(&self) -> usize {
        match self {
            Column::Str(_, v) => v.len(),
            Column::PlainStr(_, _, n) => *n,
            Column::I32(_, v, _) => v.len(),
            Column::I64(_, v) => v.len(),
            Column::DictStr(_, v, _) => v.len(),
        }
    }
}

// ---------------- Thrift compact protocol ----------------

struct T {
    b: Vec<u8>,
    stack: Vec<i16>,
    cur: i16,
}

const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

impl T {
    fn new() -> Self {
        T { b: Vec::new(), stack: Vec::new(), cur: 0 }
    }
    fn varint(&mut self, mut v: u64) {
        loop {
            if v < 0x80 {
                self.b.push(v as u8);
                return;
            }
            self.b.push((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, ty: u8, id: i16) {
        let d = id - self.cur;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        self.cur = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.fh(CT_I32, id);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.fh(CT_I64, id);
        self.zz(v);
    }
    fn binf(&mut self, id: i16, v: &[u8]) {
        self.fh(CT_BINARY, id);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn sbegin(&mut self, id: i16) {
        self.fh(CT_STRUCT, id);
        self.stack.push(self.cur);
        self.cur = 0;
    }
    /// A struct that is a list element (no field header).
    fn ebegin(&mut self) {
        self.stack.push(self.cur);
        self.cur = 0;
    }
    fn send(&mut self) {
        self.b.push(0);
        self.cur = self.stack.pop().unwrap();
    }
    fn lbegin(&mut self, id: i16, ety: u8, n: usize) {
        self.fh(CT_LIST, id);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xf0 | ety);
            self.varint(n as u64);
        }
    }
}

// ---------------- encodings ----------------

/// RLE/bit-packed hybrid, bit-packed runs only.
fn bitpack(out: &mut Vec<u8>, vals: impl Iterator<Item = u32>, n: usize, width: u32) {
    let groups = n.div_ceil(8);
    let mut t = T::new();
    t.varint(((groups as u64) << 1) | 1);
    out.extend_from_slice(&t.b);
    // Values LSB-first, `width` bits each, accumulated in a u64 and flushed a
    // byte at a time; the last group is zero-padded to whole bytes.
    out.reserve(groups * width as usize);
    let start = out.len();
    let mut acc: u64 = 0;
    let mut nbits = 0u32;
    let mask = if width >= 32 { u32::MAX } else { (1u32 << width) - 1 };
    for v in vals {
        acc |= ((v & mask) as u64) << nbits;
        nbits += width;
        while nbits >= 8 {
            out.push(acc as u8);
            acc >>= 8;
            nbits -= 8;
        }
    }
    if nbits > 0 {
        out.push(acc as u8);
    }
    out.resize(start + groups * width as usize, 0);
}

fn page_header(kind: i32, size: usize, num_values: usize, data_encoding: i32) -> Vec<u8> {
    let mut t = T::new();
    t.i32f(1, kind);
    t.i32f(2, size as i32);
    t.i32f(3, size as i32);
    if kind == 0 {
        t.sbegin(5);
        t.i32f(1, num_values as i32);
        t.i32f(2, data_encoding);
        t.i32f(3, 3);
        t.i32f(4, 3);
        t.send();
    } else {
        t.sbegin(7);
        t.i32f(1, num_values as i32);
        t.i32f(2, 0);
        t.send();
    }
    t.b.push(0);
    t.b
}

struct ChunkMeta {
    ptype: i32,
    encodings: Vec<i32>,
    name: String,
    num_values: i64,
    size: i64,
    data_off: i64,
    dict_off: Option<i64>,
}

pub fn write_file(path: &std::path::Path, cols: &[Column]) -> std::io::Result<()> {
    let nrows = cols.first().map_or(0, |c| c.len());
    let mut out: Vec<u8> = Vec::with_capacity(64 + nrows * 48);
    out.extend_from_slice(b"PAR1");
    let mut metas = Vec::new();
    if nrows > 0 {
        for c in cols {
            assert_eq!(c.len(), nrows);
            let start = out.len() as i64;
            match c {
                Column::Str(n, v) => {
                    let mut body = Vec::with_capacity(v.iter().map(|s| s.len() + 4).sum());
                    for s in v {
                        body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        body.extend_from_slice(s);
                    }
                    out.extend_from_slice(&page_header(0, body.len(), nrows, 0));
                    out.extend_from_slice(&body);
                    metas.push(ChunkMeta { ptype: 6, encodings: vec![0, 3], name: n.to_string(), num_values: nrows as i64, size: out.len() as i64 - start, data_off: start, dict_off: None });
                }
                Column::PlainStr(n, body, _) => {
                    out.extend_from_slice(&page_header(0, body.len(), nrows, 0));
                    out.extend_from_slice(body);
                    metas.push(ChunkMeta { ptype: 6, encodings: vec![0, 3], name: n.to_string(), num_values: nrows as i64, size: out.len() as i64 - start, data_off: start, dict_off: None });
                }
                Column::I32(n, v, _) => {
                    out.extend_from_slice(&page_header(0, v.len() * 4, nrows, 0));
                    for x in v {
                        out.extend_from_slice(&x.to_le_bytes());
                    }
                    metas.push(ChunkMeta { ptype: 1, encodings: vec![0, 3], name: n.to_string(), num_values: nrows as i64, size: out.len() as i64 - start, data_off: start, dict_off: None });
                }
                Column::I64(n, v) => {
                    out.extend_from_slice(&page_header(0, v.len() * 8, nrows, 0));
                    for x in v {
                        out.extend_from_slice(&x.to_le_bytes());
                    }
                    metas.push(ChunkMeta { ptype: 2, encodings: vec![0, 3], name: n.to_string(), num_values: nrows as i64, size: out.len() as i64 - start, data_off: start, dict_off: None });
                }
                Column::DictStr(n, v, dict) => {
                    // dictionary page
                    let mut dbody = Vec::new();
                    for s in dict.iter() {
                        dbody.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        dbody.extend_from_slice(s);
                    }
                    out.extend_from_slice(&page_header(2, dbody.len(), dict.len(), 0));
                    out.extend_from_slice(&dbody);
                    let data_off = out.len() as i64;
                    // data page: def levels (length-prefixed), then indices
                    let mut body = Vec::new();
                    let mut lv = Vec::new();
                    bitpack(&mut lv, v.iter().map(|x| x.is_some() as u32), nrows, 1);
                    body.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    body.extend_from_slice(&lv);
                    let width = (32 - (dict.len().max(2) as u32 - 1).leading_zeros()).max(1);
                    body.push(width as u8);
                    let nn = v.iter().filter(|x| x.is_some()).count();
                    if nn > 0 {
                        bitpack(&mut body, v.iter().filter_map(|x| *x), nn, width);
                    }
                    out.extend_from_slice(&page_header(0, body.len(), nrows, 8));
                    out.extend_from_slice(&body);
                    metas.push(ChunkMeta { ptype: 6, encodings: vec![0, 3, 8], name: n.to_string(), num_values: nrows as i64, size: out.len() as i64 - start, data_off, dict_off: Some(start) });
                }
            }
        }
    }
    // footer
    let mut t = T::new();
    t.i32f(1, 1);
    t.lbegin(2, CT_STRUCT, cols.len() + 1);
    t.ebegin();
    t.binf(4, b"schema");
    t.i32f(5, cols.len() as i32);
    t.send();
    for c in cols {
        t.ebegin();
        let (ptype, opt, conv, logical) = match c {
            Column::Str(..) | Column::PlainStr(..) => (6, false, Some(0), Some(1)),
            Column::I32(_, _, true) => (1, false, Some(6), Some(6)),
            Column::I32(_, _, false) => (1, false, None, None),
            Column::I64(..) => (2, false, None, None),
            Column::DictStr(..) => (6, true, Some(0), Some(1)),
        };
        t.i32f(1, ptype);
        t.i32f(3, if opt { 1 } else { 0 });
        t.binf(4, c.name().as_bytes());
        if let Some(cv) = conv {
            t.i32f(6, cv);
        }
        if let Some(lt) = logical {
            t.sbegin(10);
            t.sbegin(lt);
            t.send();
            t.send();
        }
        t.send();
    }
    t.i64f(3, nrows as i64);
    if nrows > 0 {
        t.lbegin(4, CT_STRUCT, 1);
        t.ebegin();
        t.lbegin(1, CT_STRUCT, metas.len());
        let mut total = 0i64;
        for m in &metas {
            total += m.size;
            t.ebegin();
            t.i64f(2, m.dict_off.unwrap_or(m.data_off));
            t.sbegin(3);
            t.i32f(1, m.ptype);
            t.lbegin(2, CT_I32, m.encodings.len());
            for &e in &m.encodings {
                t.zz(e as i64);
            }
            t.lbegin(3, CT_BINARY, 1);
            t.varint(m.name.len() as u64);
            t.b.extend_from_slice(m.name.as_bytes());
            t.i32f(4, 0);
            t.i64f(5, m.num_values);
            t.i64f(6, m.size);
            t.i64f(7, m.size);
            t.i64f(9, m.data_off);
            if let Some(d) = m.dict_off {
                t.i64f(11, d);
            }
            t.send();
            t.send();
        }
        t.i64f(2, total);
        t.i64f(3, nrows as i64);
        t.send();
    } else {
        t.lbegin(4, CT_STRUCT, 0);
    }
    t.binf(6, b"sessions-rs");
    t.b.push(0);
    out.extend_from_slice(&t.b);
    out.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&out)?;
    Ok(())
}
