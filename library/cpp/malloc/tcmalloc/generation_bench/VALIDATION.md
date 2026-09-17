# Validation: 2026-09-17

Host: build-host-004, Linux x86-64, `--build relwithdebinfo`.
Base: e78b15d22b8 (upstream/main when the diagnostic branch was created).

## Correctness

4 suites, **176 GOOD**:

- 18 generation tests with ALLOCATOR(TCMALLOC_TC).
- The same 18 tests with ALLOCATOR(TCMALLOC_256K), used by ydbd.
- 139 existing ydb/library/actors/util/ut tests.
- 1 existing library/cpp/malloc/api/ut test.

Coverage includes fixed slot geometry, multiple request sizes and alignments,
100 generation cycles, exact quarantine deadlines with an injected clock,
budget exhaustion without premature reuse, distinct fatal reasons, simultaneous
double free, cross-thread legitimate free, header and poison corruption, realloc
OOM preservation, calloc, sized/aligned delete, size-returning new, nallocx,
large allocations and alignment, concurrent churn and fork.

The existing queue suite could not compile due to missing standard includes in
mpmc_bitmap_buffer.h. It was not changed or counted as passing. Actor utility
validation exposed the HPAA alignment limit; backing over-reservation now handles
large slots, with padding charged to the budget and an explicit regression test.

No production YDB workload, TSAN run, non-x86 target or full ydbd build was validated.

## Microbenchmark

Fresh process per run, three runs per case, 200,000 allocation/free pairs per
thread, sizes cycling through 16, 32, 64, 128, 512 and 4096 bytes. Each allocation
touches its first and last byte. Default one-second quarantine, poison disabled.
Latency samples use a deterministic pseudorandom selection to avoid synchronizing
with pool-refill periods. No CPU affinity or statistical confidence bounds.

These short runs include pool growth and mostly precede quarantine expiry. They
are not steady-state or application-throughput measurements. `ns/pair` is elapsed
time divided by total operations across all threads, not per-thread latency.
Values below are medians of three runs. The variant names describe build selection;
they do not assert that the baseline used per-CPU caches on this host.

| Allocator build | Threads | Diagnostic ns/pair | Baseline ns/pair | Ratio | Diagnostic max RSS MiB | Baseline max RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TCMALLOC_TC | 1 | 87.0 | 24.3 | 3.58x | 15.83 | 9.13 |
| TCMALLOC_TC | 8 | 13.8 | 7.0 | 1.97x | 71.83 | 9.13 |
| TCMALLOC_256K | 1 | 88.2 | 21.2 | 4.16x | 15.82 | 9.13 |
| TCMALLOC_256K | 8 | 14.7 | 6.9 | 2.13x | 71.80 | 9.13 |

The mechanism is functionally validated within this scope, but it is not a
negligible-overhead production allocator. It retains pool backing, increases
memory consumption and disables ordinary allocation profiling. The extended
allocation/free stack-history mode is not implemented.

## Source fingerprints

- `contrib/libs/tcmalloc/tcmalloc/tcmalloc.cc`: `8767862e2b4d328099eb7570c70304382e4a4e5048abb645bcf39a314ffdb1d0`
- `contrib/libs/tcmalloc/tcmalloc/generation_allocator.h`: `3edd7e2054cb3087ea5bfe4bb293662b871f782901d6d1a149782063ae1fe476`
- `contrib/libs/tcmalloc/tcmalloc/generation_allocator.cc`: `2d31190447c0d1e2f1d17a6053ef9a923ca7dbdebd0ddaa3d8aad0c0960b77d5`
- `library/cpp/malloc/tcmalloc/generation_bench/main.cpp`: `884ba8b5a2883cedd3114e6857f03bf57f64ae490aaf15a5a3a48c17b743b2ec`

## Raw measurements

```text
thread generation 1 0 threads=1 operations=200000 seconds=0.017691 ns_per_pair=88.5 sample_p99_ns=136 maxrss_kib=16080 reserved=9318400
thread generation 1 1 threads=1 operations=200000 seconds=0.017406 ns_per_pair=87.0 sample_p99_ns=138 maxrss_kib=16308 reserved=9318400
thread generation 1 2 threads=1 operations=200000 seconds=0.017220 ns_per_pair=86.1 sample_p99_ns=137 maxrss_kib=16208 reserved=9318400
thread generation 8 0 threads=8 operations=1600000 seconds=0.022035 ns_per_pair=13.8 sample_p99_ns=151 maxrss_kib=73556 reserved=69222400
thread generation 8 1 threads=8 operations=1600000 seconds=0.021960 ns_per_pair=13.7 sample_p99_ns=145 maxrss_kib=73696 reserved=69222400
thread generation 8 2 threads=8 operations=1600000 seconds=0.022613 ns_per_pair=14.1 sample_p99_ns=162 maxrss_kib=73476 reserved=69222400
thread baseline 1 0 threads=1 operations=200000 seconds=0.005065 ns_per_pair=25.3 sample_p99_ns=80 maxrss_kib=9348 reserved=0
thread baseline 1 1 threads=1 operations=200000 seconds=0.004854 ns_per_pair=24.3 sample_p99_ns=78 maxrss_kib=9348 reserved=0
thread baseline 1 2 threads=1 operations=200000 seconds=0.004261 ns_per_pair=21.3 sample_p99_ns=69 maxrss_kib=9348 reserved=0
thread baseline 8 0 threads=8 operations=1600000 seconds=0.011279 ns_per_pair=7.0 sample_p99_ns=234 maxrss_kib=9348 reserved=0
thread baseline 8 1 threads=8 operations=1600000 seconds=0.011104 ns_per_pair=6.9 sample_p99_ns=252 maxrss_kib=9348 reserved=0
thread baseline 8 2 threads=8 operations=1600000 seconds=0.011327 ns_per_pair=7.1 sample_p99_ns=292 maxrss_kib=9348 reserved=0
cpu generation 1 0 threads=1 operations=200000 seconds=0.020186 ns_per_pair=100.9 sample_p99_ns=221 maxrss_kib=16200 reserved=9318400
cpu generation 1 1 threads=1 operations=200000 seconds=0.017645 ns_per_pair=88.2 sample_p99_ns=141 maxrss_kib=16084 reserved=9318400
cpu generation 1 2 threads=1 operations=200000 seconds=0.017631 ns_per_pair=88.2 sample_p99_ns=142 maxrss_kib=16292 reserved=9318400
cpu generation 8 0 threads=8 operations=1600000 seconds=0.023668 ns_per_pair=14.8 sample_p99_ns=176 maxrss_kib=73528 reserved=69222400
cpu generation 8 1 threads=8 operations=1600000 seconds=0.023518 ns_per_pair=14.7 sample_p99_ns=161 maxrss_kib=73712 reserved=69222400
cpu generation 8 2 threads=8 operations=1600000 seconds=0.023187 ns_per_pair=14.5 sample_p99_ns=160 maxrss_kib=73424 reserved=69222400
cpu baseline 1 0 threads=1 operations=200000 seconds=0.004308 ns_per_pair=21.5 sample_p99_ns=81 maxrss_kib=9348 reserved=0
cpu baseline 1 1 threads=1 operations=200000 seconds=0.004245 ns_per_pair=21.2 sample_p99_ns=87 maxrss_kib=9348 reserved=0
cpu baseline 1 2 threads=1 operations=200000 seconds=0.004244 ns_per_pair=21.2 sample_p99_ns=73 maxrss_kib=9348 reserved=0
cpu baseline 8 0 threads=8 operations=1600000 seconds=0.011056 ns_per_pair=6.9 sample_p99_ns=265 maxrss_kib=9348 reserved=0
cpu baseline 8 1 threads=8 operations=1600000 seconds=0.011040 ns_per_pair=6.9 sample_p99_ns=281 maxrss_kib=9348 reserved=0
cpu baseline 8 2 threads=8 operations=1600000 seconds=0.011653 ns_per_pair=7.3 sample_p99_ns=312 maxrss_kib=9348 reserved=0
```
