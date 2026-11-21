# Project context: Shopping Sessions (Databricks buildathon)

Read this first if you are picking up the work without the original chat.

## The task
- Rules: `rules.md` (authoritative). Fixtures: `fixtures/` (36 official cases + `X1`, our own regression case). Official dev answer: `../dev-answer/dev-answer/*.csv` (outside the repo, not committed).
- Score = wall time from container start to exit on hidden data (~20M events practice, ~60M events final, 3-minute cap per run, final top-5 re-timed 5x on an idle machine). Any wrong row = no score.
- Limits: linux/amd64, 8 vCPU, 24 GB RAM, read-only root, `/tmp` 64 MB, uid 1000, no network at run time, data in `/data`, write Parquet to `/output/user_daily/` and `/output/campaign_daily/`.
- Submit: public repo `https://github.com/erickscodes/db-hina` + a commit SHA. 2 submissions/hour; a rejected submission never lowers the best score. Practice scores vary up to ~20% by machine.
- Cutoff: Sunday 11:00 AM ET.

## Scores so far
| Commit | What | Scorer time (20M practice) |
|---|---|---|
| `2fb8e19` | AVX2 CSV scanner | 1.66 s |
| `b67b114` | exact 8-byte IDs, dedup prefetch, huge pages off | 1.64 s |
| `50898c8` | zero-copy dedup (+ mmap, recycled blocks, date cache, Bloom person map, exact ingest) | 1.43 s |
| `c6c0f90` | + readahead, survivors straight from blocks, cached bucket, quote-once scanner | 1.40 s |
| `aa5d095` | + parallel memory release before exit, 16-byte packed event records | **1.35 s (best; the final uses it unless another is chosen)** |
| `c3f5c59` | + phase 3 bucket-by-bucket, 8 output files | 1.38 s (same as aa5d095 within noise) |
| Baseline (PySpark starter) | | 171.91 s |
Leaderboard at the time: 0.93 / 1.01 / 1.05 / 1.14 / 1.17 s. Practice scores vary up to ~20% by machine.
Local 20M wall (median, quiet machine) is ~1.6-1.7 s for aa5d095/c3f5c59; the scorer gives only about half of any local gain.

## Design (src/main.rs, src/parquet.rs): one dependency-free Rust binary
- Docker: `rust:1.85-slim-bookworm` build stage, `-C target-cpu=x86-64-v3 -C target-feature=+crt-static`, final image `FROM scratch` (~2.3 MB).
- **Keys:** every ID becomes an exact `u64`. IDs of 1-9 bytes from `0-9 A-Z _ a-z` (all generator IDs) are packed as 6-bit order-preserving codes + length, so integer order == byte-wise order. Anything else (spaces, `\r`, quotes, long IDs) gets bit 63 + an index into a global intern table (exact equality; ordering via the strings in `key_cmp`). Empty string = 0.
- **Phase 1 (parallel parse):** input files are mmapped (final chunk of each file falls back to a padded `pread` copy); 8 MB chunks; `scan::<N>` finds commas/newlines/quotes with AVX2 64 bytes at a time and enforces the CSV dialect. Events validated at parse time (validity = `user != 0`), stored as 32-byte `EvRaw` (ingest = 40-bit seconds since 0000-01-01, exact for any F1; price 24 bits) in 256 partitions by hash(event_id), in 32 KB blocks (`Parted`, populated with `MADV_POPULATE_WRITE`). Clicks validated (dedup skipped: identical copies cannot change output). Link files first; the worker finishing the last link chunk builds the union-find while others parse. Timestamps use a per-worker date-prefix cache.
- **Phase 2:** per event_id partition: gather, hash-table dedup (fingerprint + index slots, prefetched), keep min ingest; survivors mapped user → person via `People` (map holds only IDs whose name differs, behind a blocked Bloom filter; two-stage prefetch with a hash ring, each hash computed once); scatter by hash(person) into 256 partitions, reusing consumed phase-1 blocks from `POOL` (no fresh page faults).
- **Phase 3:** per person partition (largest first): counting-sort gather by 12 more person-hash bits, sort each small bucket by (person, ts); walk each person: sessions, day, credit (latest click in [start-7d, start], tie → smallest click_id), assists (sliding window, distinct campaigns). user_daily Parquet per partition (user_id PLAIN buffer built once per person); campaign_daily merged at the end.
- **Parquet writer:** hand-written Thrift compact protocol, uncompressed PLAIN pages, `day` INT32 DATE, `top_category` dictionary-encoded optional.
- Huge pages off by default (env `HUGE=1`); `process::exit(0)` skips freeing memory.

## Traps in the rules (all handled, all covered by tests)
Dedup before validation (earliest copy wins even if invalid or a different user); late = strictly > 24 h; F3 timestamps may have leading zeros or overflow i64 (= out of window); only ASCII digits; offsets 00-23:00-59 with colon; Feb 29 2028; spaces and case are significant; equal timestamps share a session; session day = start day; credit window inclusive at both ends; click window starts 7 days before the event window; assists include the crediting campaign once; assist-only campaigns get rows with 0 credited; person name = smallest ID including IDs only seen in links; only the first line of a file is a header; at least one Parquet file per table even if empty; ingest_ts can be any valid F1 year (X1 covers ingests in 0001/1800/1850/9999).

## How we verify (never push without all of these)
1. All fixtures incl. X1 (`wsltools/fixtures.sh` + `wsltools/check.sh`, or `make fixtures` in WSL after `source ~/venv/bin/activate`).
2. Exact match with the official dev answer (1M events, seed 1).
3. Independent slow Python reference (`../wsltools/ref.py`, itself matches the dev answer) on 13 other seeds incl. a Feb-29-2028 window (`wsltools/seeds2.sh`). One command for all three: `bash wsltools/verify.sh`.
4. Before a submission: build the real Docker image and run the fixtures through `scripts/docker_run.sh`.

Helper scripts: `C:\Users\zevys\Downloads\databricks\wsltools\` (outside the repo). WSL datasets: `~/dev` (1M), `~/d20` (20M), `~/d60` (60M). Generator: `~/gen/target/release/generate`. perf: `~/perfroot` (`wsltools/prof3.sh`, `prof4.sh`). CPU A/B of two builds: `wsltools/old.sh <commit>` then `wsltools/cpu.sh`.

## Measured results (local, keep / drop)
- Phase-1 profile (perf annotate): ~80% of phase-1 time is the two AVX2 loads of input bytes in `masks64` waiting on memory; parsing logic is nearly free. Software prefetch and streaming via a 256 KB `pread` buffer both made no difference, so phase 1 is bound by memory bandwidth/latency, not instructions.
- Exit teardown was 280-480 ms (single-threaded munmap of ~2 GB at exit): fixed by releasing registered regions with MADV_DONTNEED in parallel / overlapped (`aa5d095`). Never MADV_DONTNEED from /proc/self/maps: it hits glibc arenas (segfault).
- Dropped after measurement: fusing phase-2 dedup and emit into one pass with tombstones (3% slower: the second pass reads a ~2.5 MB partition from cache); phase-2 (partition, bucket) histograms to skip the phase-3 counting pass (14% slower: random counter increments miss cache); NQ 64/128 (no gain); 1.5x dedup table (no gain); `pread` instead of mmap (slower); MADV_POPULATE off (neutral).
- Kept: AVX2 scanner; exact u64 IDs (-22% CPU); dedup-table prefetch; mmap + batched population (kernel time 5.5 -> 3.5 CPU-s); block recycling (20M wall ~2.5 -> ~1.7 s); date cache, flat user_id (-2-3%); Bloom-filtered person map (-6%); bucketed phase-3 sort (-3-4%); hash-once lookup pipeline + exact 40-bit ingest + 8-byte-aligned blocks + lock-free empty-pool check (~-5%).
- Dropped: huge pages (60M: 45 s vs 15 s); sorting lookups by table slot (slower); split lookup/scatter loops (no gain); block sizes 128/256/1024 (no gain); PGO (no measurable gain); scanning winners straight from the dedup table (random reads, slower).
- Local timing is noisy (±10%); compare by total CPU over 5 alternating runs.

## Backlog from the two external audits (checked against the rules)
Status: [ ] open, [x] done, [-] rejected.
- [x] Hash each user once in the Bloom/person pipeline (hash ring), reuse the hash for partitioning.
- [x] Exact ingest ordering for any F1 year (was an i32 clamp). Regression fixture X1.
- [x] Aligned block storage (Vec<u64>), skip POOL lock when empty.
- [x] Phase-3 hash sub-bucketing before exact (person, ts) sort.
- [ ] Zero-copy phase-2 dedup directly over phase-1 blocks (zero losers' `user` in place, sequential second pass, hand blocks to the worker's own spare list). Biggest remaining idea (claimed 8-18%). Needs exclusive per-partition block ownership.
- [ ] mmap the final chunk of each file too (today it is copied with pread). Must keep `finish_line` in bounds: an empty last field at exact EOF must not read past the mapping; use a padded stack copy for the final <64-byte block only.
- [ ] Pack EvP to 16 B and ClP/ClickRaw to 24 B, with runtime checks (window < 2^32 ms, <= 256 categories, campaigns fit) and an exact wide fallback.
- [ ] Cache-line staging buffers for the 256-way scatters (TLB pressure).
- [ ] Merge quote handling into the structural bit loop (count each quote once).
- [ ] Fixed-width/SWAR timestamp parsing; specialized F1 parser for ingest_ts.
- [ ] Isolated AVX-512 masks64 experiment (watch downclocking).
- [ ] Fewer user_daily files (one per worker) and raw-copy/writev output.
- [ ] Generator-shaped direct arrays for products/campaigns with exact map fallback.
- [-] Scan dedup winners from the table (measured slower).
- [-] PGO (measured no gain; revisit last).
- Fuzz the unsafe scanner against the scalar splitter (empty file, no final newline, final empty field, chunk boundaries, quote patterns, CRLF, long lines).
