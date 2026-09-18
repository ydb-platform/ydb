# Block erasure benchmark

Build from the YDB repository root:

```sh
./ya make --build relwithdebinfo ydb/core/erasure/benchmark
```

The `Y_CPU_BENCHMARK` entrypoints `Block42` and `Block82` use one parameterized
harness and the actual production kernels or canonical `ErasureSplit` and
`ErasureRestore`. The kernel adapters allocate their parts before timing and
reuse production arithmetic. The ISA-L singleton and runtime dispatcher are
warmed before measurement. Deterministic xorshift data fills every input and
the full ring is warmed twice. Correctness is checked before measurement and
on the final corpus outside timing. Run ordinary erasure UT before benchmarking.

Run a single cell with one entrypoint per process:

```sh
ERASURE_BENCH_RESULT=/tmp/block82.json \
ERASURE_BENCH_OPERATION=restore ERASURE_BENCH_LOSS=DD \
taskset -c 0 ydb/core/erasure/benchmark/benchmark --budget 1 --format json Block82
```

The JSON sidecar contains totals over **all** measured invocations, without
outlier rejection, including monotonic wall time, thread CPU time, timer ticks,
logical throughput, actual part sizes and physical/logical size ratio. Timer
ticks are architecture timer units, not CPU cycles. Framework JSON is retained
as a diagnostic only: its regression estimator removes internal outliers and
its outer timer includes first-call initialization. The sidecar excludes that
initialization and result checks. The first measured call runs for at least
`ERASURE_BENCH_MIN_NS` (50 ms by default), checking elapsed time once per 64
operations. This guarantees actual steady-state samples even when large-ring
initialization exceeds the framework budget. The process runner sets this
minimum to `--budget`. Clock reads add a small per-batch cost.

The process runner captures commands, environment, source and binary identity,
CPU/topology, paired samples, median, median absolute deviation and paired
throughput/CPU ratios. It alternates process order and keeps every sample.
Existing output directories are rejected to preserve previous runs.

```sh
python3 ydb/core/erasure/benchmark/run.py \
  ydb/core/erasure/benchmark/benchmark \
  /path/to/experiments/2026-09-08-block-4-2-vs-block-8-2-x86_64 \
  --suite full --repeats 10 --budget 0.2 --cpu 0 --numa-node 0
```

`smoke` exercises kernel/API encode, restore and glue on 64 KiB; `baseline` adds all four large-blob
anchors, short-size diagnostics and adapter/output controls; `full` additionally
runs the streaming ring and all single/double loss masks at 1 MiB. A streaming
ring defaults to 128 MiB of logical input; confirm that it exceeds the target
host LLC and adjust it if necessary. The matrix uses representatives of D/P/DD/
DP/PP, and numeric masks are available for any additional slowest-mask sweep.
The full mask rows have no cross-species ratio because their numeric indices
do not describe identical roles in the two species.

After the full suite, select the largest median `ns/blob` from each species and
level's all-mask sweep and measure that mask at the remaining three anchors:

```sh
python3 ydb/core/erasure/benchmark/run.py \
  ydb/core/erasure/benchmark/benchmark /path/to/experiments/slowest-masks \
  --slowest-from /path/to/experiments/full-suite --repeats 10 --budget 0.05
```

The selected masks, candidate counts and source medians are saved in
`slowest-selection.json`; exact follow-up cells are in `cells.json`.
`--cells-file` also accepts an explicit JSON cell list for other reproducible
follow-ups.

Parameters are passed as environment variables:

| Suffix after `ERASURE_BENCH_` | Values / default |
|---|---|
| `SIZE` | Logical bytes; `1048576` |
| `LEVEL` | `api`, `kernel`; `api` |
| `OPERATION` | `encode`, `restore`, `fragment`, `glue`; `encode` |
| `LOSS` | `D`, `P`, `DD`, `DP`, `PP`, decimal mask; `DD` |
| `OUTPUT` | `parts`, `whole`, `both`, `first`; `parts` |
| `AVAILABILITY` | `all`, `k`; `all` |
| `RING_BYTES` | Logical working set; `0` means one hot blob |
| `FRAGMENTED` | `1` for fragmented input ropes; `0` |
| `FRAGMENT_BYTES` | Input rope segment bytes; `4093` |
| `INCREMENTAL` | `1` for split with the production 256 KiB quantum; `0` |
| `CRC` | `1` for WholePart, `0` for None |
| `RESULT` | Sidecar JSON path, one entrypoint per process |
| `MIN_NS` | Minimum first-call steady-state duration; `50000000` |

Kernel mode supports preallocated encode and requested-part restore with
contiguous buffers and no CRC. `first` requests one output of a double loss.
`k` additionally hides the highest-index survivor for single-loss scenarios.
The fragment scenario uses a part-relative offset of 32 bytes when possible,
with up to 4096 aligned bytes. Fragment restore with whole-blob output or CRC
is rejected. `glue`, and parity-only whole restore, are no-decode controls.

CPU affinity uses `numactl` for worker and memory binding when installed;
otherwise `taskset` plus first touch is used and recorded. Each process is a
single worker. `concurrency.py BINARY OUTPUT --repeats 10 --budget 0.5` measures
kernel/API encode and DD repair at 1 MiB with 1/2/4 workers. It discovers four
physical cores in one NUMA node, avoids SMT siblings and starts workers through
a launch barrier. Process lifetimes and the single contiguous measured call
bound a conservative guaranteed common measurement window. Every run is kept;
coverage below 80% is marked in JSON. The framework `--threads` option
parallelizes different cases and does not represent same-workload scaling.
If any group is marked, rerun the affected complete paired cells with a longer
window in a separate directory, preserving all original samples:

```sh
python3 ydb/core/erasure/benchmark/concurrency.py \
  ydb/core/erasure/benchmark/benchmark /path/to/experiments/concurrency-retry \
  --retry-from /path/to/experiments/concurrency --budget 1 --repeats 10
```

When available, `perf stat` captures task-clock, cycles/reference cycles,
instructions, cache and branch misses, context switches and migrations. These
process counters include initialization and cleanup and must not be silently
equated with the explicit steady-state sidecar region. Record a sufficiently
long measurement budget to make startup amortization meaningful. Add
architecture-specific PMU/cache/TLB events separately for each host.

The runner explicitly records unavailable PMU tooling, strict NUMA binding,
frequency metadata and unobserved runtime ISA. Allocated/copy/zero byte counts
are null because this harness does not instrument production buffer ownership.
Linux perf_event_open also collects steady-state user-space hardware/software
counters around the explicit measurement region, with multiplexing scaling
and per-event errors. Counter start/stop syscalls are outside wall/CPU timing;
counter intervals include the surrounding userspace instrumentation. Compare
short-size PMU diagnostics with this instrumentation floor in mind.

For Linux x86_64, a benchmark-only read-only diagnostic resolves the initialized
ISA-L 2.31 dispatcher pointer and records the actual high-level encode target.
It validates the known stub and reports an unobserved target if it changed;
it never invokes or selects a specific implementation. Short-length scalar
fallback within the selected function remains dependent on each encode span.
CPU capabilities alone do not prove which ISA-L kernel ran. Native AArch64
must run the same suite; QEMU correctness is not native ARM performance data.
There is no numeric performance pass/fail threshold. Storage-path measurement
belongs to the later BlobStorage integration stages.
