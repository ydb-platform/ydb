# TEvRead allocation and fragmentation benchmark

Standalone allocation/fragmentation benchmark for YDBDUTY-300. It is not registered in normal test runs. The production integration keeps the existing wire format and decodes large remote reads into one schema-normalized cell array.

## Compared paths

- `legacy`: real `TEvRead` serialization followed by a frozen copy of the original receiver algorithm (protobuf plus typed per-endpoint cell vectors).
- `contiguous`: the same wire/protobuf decoder, followed by one contiguous cell array and endpoint descriptors.
- `arena`: the same wire/protobuf decoder, followed by endpoint cells allocated from a request-owned `TMemoryPool` and the same descriptors.
- `protobuf-arena`: the existing `TEventPBWithArena` decoder with legacy typed key/range vectors and auxiliary event fields; varies protobuf object ownership while retaining the same cell-vector allocation path.
- `production`: real `TEvRead::Load()` and schema preparation, including the compact path for large remote requests. Checksums use original endpoint widths so normalization padding does not change the comparison.

The contiguous, arena and protobuf-arena alternatives are measurement prototypes. The production mode exercises the integrated event implementation. The contiguous/arena paths retain the protobuf backing strings and use the shared serialized-cell decoder through `TryParseCellsInto()`. They isolate decoded-cell/range metadata allocation; they do not eliminate protobuf message/string allocations or implement iterator normalization, locks, retries or resends. Every loaded request is checked against the logical input checksum.

The target explicitly selects `TCMALLOC_256K`, matching Linux `ydbd`. Measure without sanitizers. Run each mode/configuration in a fresh process; use cycles within each run to observe warm allocator behavior.

## Build and run

```bash
set -o pipefail
./ya make --build relwithdebinfo ydb/core/tx/datashard/bench_read_memory 2>&1 | tail -n 80
./ya make --build relwithdebinfo -tA ydb/core/scheme/ut -F 'Scheme::CellVecParseInto*' 2>&1 | tail -n 80
./ya make --build relwithdebinfo -tA ydb/core/tx/datashard/ut_read_events 2>&1 | tail -n 80
./ya make --build relwithdebinfo -tA ydb/core/tx/datashard/ut_read_iterator -F '*DataShardReadIterator*' 2>&1 | tail -n 80
```

After a successful build, example individual runs from the repository root:

```bash
ydb/core/tx/datashard/bench_read_memory/datashard_read_memory_bench --mode legacy --entries 4096 > /tmp/read-legacy.csv
ydb/core/tx/datashard/bench_read_memory/datashard_read_memory_bench --mode contiguous --entries 4096 > /tmp/read-contiguous.csv
ydb/core/tx/datashard/bench_read_memory/datashard_read_memory_bench --mode arena --entries 4096 > /tmp/read-arena.csv
```

Sweep `--entries` over 1K, 4K, 16K, 64K and 256K, then larger isolated stress cases if the memory budget permits. Vary `--requests`, `--columns`, `--to-columns`, `--cell-bytes` and `--arena-chunk-bytes`. `--kind keys` tests keys-only events; `--kind ranges --to-columns 0` tests an absent upper endpoint, and `--columns 0` tests an absent lower endpoint. Each invocation intentionally runs only one mode.

Use a local cgroup/container memory limit for large cases. No production data or cluster is required. This benchmark does not derive a production-safe concurrency limit without the actual node budget, baseline services and representative end-to-end workload.

## Mixed lifetimes and diagnostics

`--background-per-request` retains small allocations of 8, 32, 64, 80, 96, 112, 128 and 256 bytes between individual read loads. Reads drain each cycle; background objects survive across cycles until the final release. Compare with `--background-per-request 0` to separate lifetime mixing from a homogeneous burst. These objects are synthetic; the run does not claim to reproduce production's exact survivor pattern or actor concurrency.

`--stats-file PATH` writes full allocator stats at wire-only, loaded, drained-with-background and final-release stages. Inspect per-class live spans and utilization alongside central/CPU/transfer cache sizes. The per-class free-object table combines several cache levels. Stats formatting allocates memory, so collect these in separate diagnostic runs.

`--profile` enables sampled allocation profiling for each load phase. The default profiling interval is 64 KiB (`--profile-sampling-bytes`); the rate is recorded in CSV, and unprofiled runs retain the allocator default. Estimated counts/bytes include the fixed background allocations, checksum work and phase measurement overhead; they are not exact allocation counts. Profiling also perturbs the allocator. Compare allocation counts in separate runs from unprofiled footprint measurements. Unprofiled rows use `-1` for these estimates.

## Output and interpretation

CSV includes allocator application/physical bytes, central/transfer/CPU/thread/page-heap free bytes, RSS, arena requested chunk bytes and chunk count, load time and optional allocation estimates. `load_us` covers only request loading, excluding checksum validation and synthetic background creation. `wire_bytes` is the size of the common serialized input fixture.

Measurement helpers themselves have a small allocation/CPU cost common to the modes. Rows distinguish producer construction, serialization overlap, retained wire input, loaded reads, drained reads with background survivors, and final release. The serialized fixture remains alive during the read cycles; the sender event is destroyed before receiver measurements. Pointer-container capacities and measurement storage are reserved before the baseline and remain allocated throughout the run.

The `raw_tcmalloc_peak_memory_usage_bytes` column reports the raw `generic.peak_memory_usage` property. This sampled application estimate is updated at new backed-memory high-water marks and can miss application peaks while reusing existing backed pages. `lifetime_peak_rss_bytes` is the process-lifetime RSS high-water mark. Both can reflect fixture generation. Stage samples are not guaranteed to capture short intermediate peaks inside decoding; none of these values is a guaranteed isolated receiver-phase peak. Compare the raw property, RSS and phase snapshots together, using diagnostic profiles when attribution is needed.

Arena requested chunk bytes include pool headers/tail space but exclude additional tcmalloc size-class rounding; total allocator application bytes include the actual backing allocation cost. The arena's data hint is not a guarantee of an exact allocation size or immediate OS reclamation.

Keep database page-cache configuration out of this isolated comparison. Success means fewer small allocations and lower peak/post-burst physical footprint or central span slack without unacceptable load-time regression. A low live heap alone is insufficient evidence of reduced fragmentation.

## Measured production result (2026-11-11)

Configuration: eight simultaneously retained requests, 65,536 ranges per request, five 16-byte cells per endpoint, five load/drain cycles, and 32 synthetic long-lived small allocations added between request loads. Both modes run in fresh processes with `TCMALLOC_256K`. These are isolated benchmark results, not whole-node memory limits.

| Metric | Frozen legacy | Production | Reduction |
|---|---:|---:|---:|
| Maximum loaded-stage application bytes | 357.60 MiB | 325.35 MiB | 9.0% |
| Maximum loaded-stage allocator physical bytes | 381.84 MiB | 343.84 MiB | 10.0% |
| Maximum central freelist bytes after reads drain | 55.02 MiB | 44.78 MiB | 18.6% |
| Median loading time per eight-request burst | 151.89 ms | 145.45 ms | 4.2% |
| Median estimated allocations per burst (separate profiling run) | 4,638,617 | 3,628,972 | 21.8% |

Memory and timing values above come from unprofiled runs. Allocation counts come from separate 64 KiB sampling runs; profiling strongly perturbs memory, so profiled footprint values are excluded. Loaded-stage maxima are phase snapshots, not guaranteed transient process peaks. Small-read timings approach the timer resolution and are not used to claim a speedup.

The separate allocator diagnostic after the fifth drain showed the 80-byte class falling from 43 live 256 KiB pages to 2. Its spans with fewer than eight slots allocated outside the central freelist fell from 40 to 0. These slots can belong to application objects or other allocator caches. This reproduces the suspected span-fragmentation mechanism in the synthetic workload.

Validation passed: 3 caller-storage parser tests, 5 protocol/lifetime/normalization tests, and 184 read-iterator tests. The iterator gate includes explicit deserialization tests for compact key prefixes, reverse range continuations and sampled range continuations. All fresh-process legacy/production matrix and production edge cases completed without checksum failures or crashes.

This change removes per-endpoint cell-vector allocations for large remote reads and avoids copying endpoint vectors during range execution. The protobuf object graph remains, and aggregate request-memory admission is a separate follow-up; these measurements do not establish that every OOM workload is bounded.
