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

## Zero-duration quarantine comparison (2026-09-17)

Source commit: `834ffa04fa9`. Same build-host-004 and unchanged benchmark/code.
Both allocator variants were rebuilt, each binary copied to a temporary path,
and the three modes interleaved in a deterministic shuffled order. Five fresh
processes per case, 200,000 pairs per thread, 1 or 8 threads, poison off, 16 GiB
budget. The ordinary baseline was rebuilt and remeasured in this session.

`0 ms` sets `TCMALLOC_GENERATION_QUARANTINE_MS=0`. It removes the waiting period,
not the queue, clock calls or drain work. Full queue bypass was not implemented
or measured. Generation reset can therefore repeat an old pointer almost
immediately after the final position is freed.

Medians of five runs; ns/pair is wall time divided by the total pair count across
all threads. RSS is process high-water RSS and includes startup costs. The
reserved column counts only diagnostic backing and registry storage, not RSS.
These are short microbenchmarks; near-baseline throughput in eight threads is
not proof of negligible overhead on a YDB workload.

| Build | Threads | Mode | ns/pair | Peak RSS MiB | Diagnostic reserved MiB |
| --- | ---: | --- | ---: | ---: | ---: |
| TCMALLOC_TC | 1 | ordinary | 23.5 | 10.89 | 0.00 |
| TCMALLOC_TC | 1 | 1000 ms | 89.1 | 15.96 | 8.89 |
| TCMALLOC_TC | 1 | 0 ms | 56.6 | 10.89 | 1.52 |
| TCMALLOC_TC | 8 | ordinary | 7.2 | 10.89 | 0.00 |
| TCMALLOC_TC | 8 | 1000 ms | 14.3 | 71.96 | 66.02 |
| TCMALLOC_TC | 8 | 0 ms | 8.0 | 12.90 | 7.11 |
| TCMALLOC_256K | 1 | ordinary | 21.9 | 10.89 | 0.00 |
| TCMALLOC_256K | 1 | 1000 ms | 87.8 | 15.83 | 8.89 |
| TCMALLOC_256K | 1 | 0 ms | 56.4 | 10.89 | 1.52 |
| TCMALLOC_256K | 8 | ordinary | 7.5 | 10.89 | 0.00 |
| TCMALLOC_256K | 8 | 1000 ms | 14.0 | 71.96 | 66.02 |
| TCMALLOC_256K | 8 | 0 ms | 7.7 | 12.89 | 7.11 |

Raw records for all 60 runs:

```jsonl
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "23.1", "operations": "200000", "repeat": 0, "reserved": "0", "sample_p99_ns": "82", "seconds": "0.004611", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "55.8", "operations": "200000", "repeat": 0, "reserved": "1597440", "sample_p99_ns": "115", "seconds": "0.011166", "threads": "1", "variant": "thread"}
{"maxrss_kib": "16344", "mode": "1000", "ns_per_pair": "89.2", "operations": "200000", "repeat": 0, "reserved": "9318400", "sample_p99_ns": "139", "seconds": "0.017845", "threads": "1", "variant": "thread"}
{"maxrss_kib": "16372", "mode": "1000", "ns_per_pair": "89.1", "operations": "200000", "repeat": 1, "reserved": "9318400", "sample_p99_ns": "141", "seconds": "0.017822", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "55.7", "operations": "200000", "repeat": 1, "reserved": "1597440", "sample_p99_ns": "121", "seconds": "0.011132", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "22.0", "operations": "200000", "repeat": 1, "reserved": "0", "sample_p99_ns": "108", "seconds": "0.004401", "threads": "1", "variant": "thread"}
{"maxrss_kib": "16196", "mode": "1000", "ns_per_pair": "85.7", "operations": "200000", "repeat": 2, "reserved": "9318400", "sample_p99_ns": "132", "seconds": "0.017147", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "59.5", "operations": "200000", "repeat": 2, "reserved": "1597440", "sample_p99_ns": "153", "seconds": "0.011899", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "24.1", "operations": "200000", "repeat": 2, "reserved": "0", "sample_p99_ns": "138", "seconds": "0.004826", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "24.0", "operations": "200000", "repeat": 3, "reserved": "0", "sample_p99_ns": "91", "seconds": "0.004806", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "56.6", "operations": "200000", "repeat": 3, "reserved": "1597440", "sample_p99_ns": "120", "seconds": "0.011319", "threads": "1", "variant": "thread"}
{"maxrss_kib": "16364", "mode": "1000", "ns_per_pair": "87.1", "operations": "200000", "repeat": 3, "reserved": "9318400", "sample_p99_ns": "134", "seconds": "0.017423", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "23.5", "operations": "200000", "repeat": 4, "reserved": "0", "sample_p99_ns": "78", "seconds": "0.004705", "threads": "1", "variant": "thread"}
{"maxrss_kib": "16200", "mode": "1000", "ns_per_pair": "91.3", "operations": "200000", "repeat": 4, "reserved": "9318400", "sample_p99_ns": "144", "seconds": "0.018254", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "56.6", "operations": "200000", "repeat": 4, "reserved": "1597440", "sample_p99_ns": "123", "seconds": "0.011312", "threads": "1", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.1", "operations": "1600000", "repeat": 0, "reserved": "0", "sample_p99_ns": "257", "seconds": "0.011329", "threads": "8", "variant": "thread"}
{"maxrss_kib": "12996", "mode": "0", "ns_per_pair": "8.0", "operations": "1600000", "repeat": 0, "reserved": "7454720", "sample_p99_ns": "139", "seconds": "0.012855", "threads": "8", "variant": "thread"}
{"maxrss_kib": "73688", "mode": "1000", "ns_per_pair": "14.0", "operations": "1600000", "repeat": 0, "reserved": "69222400", "sample_p99_ns": "163", "seconds": "0.022336", "threads": "8", "variant": "thread"}
{"maxrss_kib": "73720", "mode": "1000", "ns_per_pair": "14.3", "operations": "1600000", "repeat": 1, "reserved": "69222400", "sample_p99_ns": "160", "seconds": "0.022874", "threads": "8", "variant": "thread"}
{"maxrss_kib": "13264", "mode": "0", "ns_per_pair": "8.0", "operations": "1600000", "repeat": 1, "reserved": "7454720", "sample_p99_ns": "130", "seconds": "0.012814", "threads": "8", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.5", "operations": "1600000", "repeat": 1, "reserved": "0", "sample_p99_ns": "282", "seconds": "0.011966", "threads": "8", "variant": "thread"}
{"maxrss_kib": "73688", "mode": "1000", "ns_per_pair": "14.2", "operations": "1600000", "repeat": 2, "reserved": "69222400", "sample_p99_ns": "162", "seconds": "0.022645", "threads": "8", "variant": "thread"}
{"maxrss_kib": "13208", "mode": "0", "ns_per_pair": "8.1", "operations": "1600000", "repeat": 2, "reserved": "7454720", "sample_p99_ns": "127", "seconds": "0.012930", "threads": "8", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.2", "operations": "1600000", "repeat": 2, "reserved": "0", "sample_p99_ns": "274", "seconds": "0.011448", "threads": "8", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.2", "operations": "1600000", "repeat": 3, "reserved": "0", "sample_p99_ns": "284", "seconds": "0.011507", "threads": "8", "variant": "thread"}
{"maxrss_kib": "13116", "mode": "0", "ns_per_pair": "7.9", "operations": "1600000", "repeat": 3, "reserved": "7454720", "sample_p99_ns": "127", "seconds": "0.012625", "threads": "8", "variant": "thread"}
{"maxrss_kib": "73724", "mode": "1000", "ns_per_pair": "14.3", "operations": "1600000", "repeat": 3, "reserved": "69222400", "sample_p99_ns": "161", "seconds": "0.022937", "threads": "8", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.3", "operations": "1600000", "repeat": 4, "reserved": "0", "sample_p99_ns": "285", "seconds": "0.011712", "threads": "8", "variant": "thread"}
{"maxrss_kib": "73480", "mode": "1000", "ns_per_pair": "14.3", "operations": "1600000", "repeat": 4, "reserved": "69222400", "sample_p99_ns": "170", "seconds": "0.022922", "threads": "8", "variant": "thread"}
{"maxrss_kib": "13228", "mode": "0", "ns_per_pair": "7.7", "operations": "1600000", "repeat": 4, "reserved": "7454720", "sample_p99_ns": "120", "seconds": "0.012307", "threads": "8", "variant": "thread"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "25.4", "operations": "200000", "repeat": 0, "reserved": "0", "sample_p99_ns": "81", "seconds": "0.005088", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "60.2", "operations": "200000", "repeat": 0, "reserved": "1597440", "sample_p99_ns": "126", "seconds": "0.012049", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "16132", "mode": "1000", "ns_per_pair": "90.9", "operations": "200000", "repeat": 0, "reserved": "9318400", "sample_p99_ns": "146", "seconds": "0.018174", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "16212", "mode": "1000", "ns_per_pair": "88.0", "operations": "200000", "repeat": 1, "reserved": "9318400", "sample_p99_ns": "138", "seconds": "0.017608", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "56.0", "operations": "200000", "repeat": 1, "reserved": "1597440", "sample_p99_ns": "113", "seconds": "0.011205", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "21.9", "operations": "200000", "repeat": 1, "reserved": "0", "sample_p99_ns": "72", "seconds": "0.004389", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "16408", "mode": "1000", "ns_per_pair": "86.7", "operations": "200000", "repeat": 2, "reserved": "9318400", "sample_p99_ns": "141", "seconds": "0.017344", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "55.9", "operations": "200000", "repeat": 2, "reserved": "1597440", "sample_p99_ns": "115", "seconds": "0.011182", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "21.9", "operations": "200000", "repeat": 2, "reserved": "0", "sample_p99_ns": "66", "seconds": "0.004386", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "21.7", "operations": "200000", "repeat": 3, "reserved": "0", "sample_p99_ns": "65", "seconds": "0.004348", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "56.4", "operations": "200000", "repeat": 3, "reserved": "1597440", "sample_p99_ns": "122", "seconds": "0.011272", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "16372", "mode": "1000", "ns_per_pair": "87.8", "operations": "200000", "repeat": 3, "reserved": "9318400", "sample_p99_ns": "140", "seconds": "0.017555", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "22.2", "operations": "200000", "repeat": 4, "reserved": "0", "sample_p99_ns": "64", "seconds": "0.004432", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "16200", "mode": "1000", "ns_per_pair": "85.9", "operations": "200000", "repeat": 4, "reserved": "9318400", "sample_p99_ns": "146", "seconds": "0.017175", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "0", "ns_per_pair": "57.0", "operations": "200000", "repeat": 4, "reserved": "1597440", "sample_p99_ns": "122", "seconds": "0.011404", "threads": "1", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.7", "operations": "1600000", "repeat": 0, "reserved": "0", "sample_p99_ns": "273", "seconds": "0.012246", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "13204", "mode": "0", "ns_per_pair": "7.7", "operations": "1600000", "repeat": 0, "reserved": "7454720", "sample_p99_ns": "131", "seconds": "0.012349", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "73548", "mode": "1000", "ns_per_pair": "14.3", "operations": "1600000", "repeat": 0, "reserved": "69222400", "sample_p99_ns": "164", "seconds": "0.022842", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "73708", "mode": "1000", "ns_per_pair": "13.9", "operations": "1600000", "repeat": 1, "reserved": "69222400", "sample_p99_ns": "164", "seconds": "0.022169", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "13284", "mode": "0", "ns_per_pair": "8.0", "operations": "1600000", "repeat": 1, "reserved": "7454720", "sample_p99_ns": "136", "seconds": "0.012855", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.2", "operations": "1600000", "repeat": 1, "reserved": "0", "sample_p99_ns": "263", "seconds": "0.011494", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "73688", "mode": "1000", "ns_per_pair": "14.0", "operations": "1600000", "repeat": 2, "reserved": "69222400", "sample_p99_ns": "156", "seconds": "0.022352", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "13284", "mode": "0", "ns_per_pair": "7.9", "operations": "1600000", "repeat": 2, "reserved": "7454720", "sample_p99_ns": "128", "seconds": "0.012680", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.7", "operations": "1600000", "repeat": 2, "reserved": "0", "sample_p99_ns": "275", "seconds": "0.012266", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.5", "operations": "1600000", "repeat": 3, "reserved": "0", "sample_p99_ns": "312", "seconds": "0.012075", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "13116", "mode": "0", "ns_per_pair": "7.7", "operations": "1600000", "repeat": 3, "reserved": "7454720", "sample_p99_ns": "122", "seconds": "0.012275", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "73476", "mode": "1000", "ns_per_pair": "14.2", "operations": "1600000", "repeat": 3, "reserved": "69222400", "sample_p99_ns": "156", "seconds": "0.022730", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "11152", "mode": "baseline", "ns_per_pair": "7.4", "operations": "1600000", "repeat": 4, "reserved": "0", "sample_p99_ns": "271", "seconds": "0.011811", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "73692", "mode": "1000", "ns_per_pair": "13.9", "operations": "1600000", "repeat": 4, "reserved": "69222400", "sample_p99_ns": "177", "seconds": "0.022305", "threads": "8", "variant": "cpu"}
{"maxrss_kib": "13184", "mode": "0", "ns_per_pair": "7.7", "operations": "1600000", "repeat": 4, "reserved": "7454720", "sample_p99_ns": "126", "seconds": "0.012389", "threads": "8", "variant": "cpu"}
```
