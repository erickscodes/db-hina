# Project context: Shopping Sessions (Databricks buildathon)

Read this first if you are picking up the work without the original chat.

## The task
- Rules: `rules.md` (authoritative). Fixtures: `fixtures/` (36 hand-made cases). Official dev answer: `../dev-answer/dev-answer/*.csv` (outside the repo, not committed).
- Score = wall time from container start to exit on hidden data (~20M events practice, ~60M events final, 3-minute cap per run). Any wrong row = no score.
- Limits: linux/amd64, 8 vCPU, 24 GB RAM, read-only root, `/tmp` 64 MB, uid 1000, no network at run time, data in `/data`, write Parquet to `/output/user_daily/` and `/output/campaign_daily/`.
- Submit: public repo `https://github.com/erickscodes/db-hina` + a commit SHA. 2 submissions/hour; a rejected submission never lowers the best score.
- Cutoff: Sunday 11:00 AM ET.

## Scores so far
| Commit | What | Scorer time (20M practice) |
|---|---|---|
| `2fb8e19` | AVX2 CSV scanner on top of `8dc5783` | **1.66 s** (3rd place at the time; leader 1.52 s) |
| `<next>` | exact 8-byte IDs (fast path for `[0-9A-Z_a-z]{1,9}`, exact intern table otherwise), dedup prefetch, huge pages off by default | not yet scored (local CPU -22% vs `415479e`; 60M events: ~15 s locally) |
| Baseline (PySpark starter) | | 171.91 s |

## Design (src/main.rs, src/parquet.rs) — one dependency-free Rust binary
- Docker: `rust:1.85-slim-bookworm` build stage, `-C target-cpu=x86-64-v3 -C target-feature=+crt-static`, final image `FROM scratch` (~2.3 MB) → near-instant container start.
- **Keys:** every ID becomes an exact `u64`. IDs of 1-9 bytes from `0-9 A-Z _ a-z` (all generator IDs) are packed as 6-bit order-preserving codes + length, so integer order == byte-wise order. Anything else (spaces, ``, quotes, long IDs) gets bit 63 + an index into a global intern table (exact equality; ordering via the strings in `key_cmp`). Empty string = 0.
- **Phase 1 (parallel parse):** files are split into 8 MB chunks read with `pread`; `scan::<N>` finds commas/newlines/quotes with AVX2 64 bytes at a time and enforces the CSV dialect (exact field count, quotes only around a whole field, empty line malformed, `\r` is data). Events are validated at parse time (validity folded into `user != 0`), stored as 48-byte `EvRaw` records into 256 partitions by hash(event_id) using block arenas (`Parted`). Clicks are validated and stored per worker (click dedup is skipped: copies are identical by rule, so they cannot change any output). Link files are parsed first; the worker that finishes the last link chunk builds the union-find (`build_people`) while others keep parsing.
- **Phase 2:** per event_id partition: hash-table dedup keeping min ingest (ingest stored as i32 seconds from window start), then map user → person name via `pmap` (prefetched), re-partition events and clicks by hash(person) into 256 partitions.
- **Phase 3:** per person partition (largest first): sort events (person, ts) and clicks (person, ts); walk each person: sessions (gap > 30 min), day = UTC date of session start, credit = latest click in [start-7d, start] with tie → smallest click_id, assists = distinct campaigns in that window (sliding window with per-campaign counts and an O(1) active list). user_daily rows are written per partition as Parquet; campaign_daily accumulated per worker then merged.
- **Parquet writer:** hand-written (Thrift compact protocol), uncompressed PLAIN pages, `day` as INT32 DATE, `top_category` dictionary-encoded optional column.
- Misc: global allocator asks for transparent huge pages on big blocks; `process::exit(0)` at the end skips freeing gigabytes.

## Traps in the rules (all handled, all covered by tests)
Dedup before validation (earliest copy wins even if invalid or a different user); late = strictly > 24 h; F3 timestamps may have leading zeros or overflow i64 (= out of window); only ASCII digits; offsets 00-23:00-59 with colon; Feb 29 2028; spaces and case are significant; equal timestamps share a session; session day = start day; credit window inclusive at both ends; click window starts 7 days before the event window; assists include the crediting campaign once; assist-only campaigns get rows with 0 credited; person name = smallest ID including IDs only seen in links; only the first line of a file is a header; at least one Parquet file per table even if empty.

## How we verify (never push without all three)
1. All 36 fixtures (`wsltools/fixtures.sh` in WSL, then `wsltools/check.sh` on Windows; or `make fixtures` in WSL after `source ~/venv/bin/activate`).
2. Exact match with the official dev answer (1M events, seed 1).
3. An independent slow Python reference (`../wsltools/ref.py`, matches the dev answer itself) compared on 13 other seeds incl. a Feb-29-2028 window (`wsltools/seeds.sh`).

Helper scripts live in `C:\Users\zevys\Downloads\databricks\wsltools\` (outside the repo). Datasets in WSL: `~/dev` (1M), `~/d20` (20M), `~/d60` (60M). Generator binary: `~/gen/target/release/generate`. Profiler: perf unpacked at `~/perfroot` (see `wsltools/prof.sh`).

## Local performance notes
- Local machine (Ryzen 5600G in WSL2) is noisy: the same binary varies 2.2-4.7 s on 20M, and WSL's clocks can disagree. Compare builds by total CPU time (`wsltools/cpu.sh`) and perf profiles, not single wall times.
- perf profile on 20M (commit `2fb8e19`): phase 2 ~41% (person lookup `Map::get` ~14%, dedup table probing, scatter), phase 1 parse ~35% (`parse_ts` 7%, field extraction ~12%), phase 3 ~15% (sorting ~9%).
- Huge pages (MADV_HUGEPAGE) made the 60M run 3x slower locally (kernel compaction stalls), so they are off by default (env `HUGE=1` turns them on).
- Sorting person lookups by table slot (radix-ordered probes) made phase 2 slower; reverted. Prefetching dedup-table slots 16 rows ahead halved dedup time; kept.
- Tried, no measurable gain: partition block sizes 128/256/1024; huge pages on vs off was within noise (kept on).

## Ideas not done yet
- Faster timestamp parsing (fixed-width F1/F2 via SWAR, cached day numbers).
- Cheaper person lookup (smaller table, or avoid per-row lookups).
- Radix sort instead of comparison sort in phase 3.
- Fewer passes over memory (e.g. skip the event_id shuffle for IDs that are provably unique).
