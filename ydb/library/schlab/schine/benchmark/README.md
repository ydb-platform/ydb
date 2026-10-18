# Forseti scheduler microbenchmark

Build from the repository root:

```sh
./ya make -k --build relwithdebinfo ydb/library/schlab/schine/benchmark
```

The `forseti_bench` program links the production `schlab/schine` library. It does
not depend on PDisk, actor runtime, disk devices or the PDisk test suite. The
library's existing tracing dependencies are retained. No new benchmark framework
is needed.

```sh
ydb/library/schlab/schine/benchmark/forseti_bench > results.tsv
ydb/library/schlab/schine/benchmark/forseti_bench --owners 128 --gates 10 --active 8 --depth 16 --jobs 1000000 --repeats 7
ydb/library/schlab/schine/benchmark/forseti_bench --help
```

Options (defaults):

| Option | Meaning |
| --- | --- |
| `--owners 8` | Registered owners, 1–255 |
| `--gates 10` | Queues per owner, 1–255 |
| `--active 0` | Active queues spread across registered queues; 0 means all |
| `--depth 4` | Jobs per active queue |
| `--jobs 200000` | Completed jobs per measured sample |
| `--warmup 10000` | Jobs before each measured sample |
| `--repeats 5` | Independent samples, with a fresh scheduler each time |
| `--mode both` | `steady`, `burst`, or both |

`steady` pre-fills the queues, then completes one selected job and appends a new
job to the same queue. `burst` submits a batch of `active * depth` jobs and drains
it completely before the next batch; the last batch may be partial. Both modes
measure job allocation, enqueue, selection, completion and job destruction.
Creation/registration of queues, warmup, final draining and output are outside
the timed region. The harness also performs integer bookkeeping in the loop;
`ns_per_job` is the cost of the whole cycle, not `SelectJob` alone.

Costs cycle deterministically through 10–80 microseconds of **virtual** service
time. Weights are `1 + gate`; gate 0 has a 2 ms minimum budget, others have zero.
These are synthetic workloads, not a reconstruction of a particular disk's
traffic. Virtual time advances on enqueue and by job cost on completion, as in
PDisk. The benchmark never sleeps to simulate I/O. Binary scheduling logs are
disabled, as in PDisk's default configuration; tracing is not activated.

Output is TSV, one row per sample: workload parameters, elapsed milliseconds,
wall-clock nanoseconds per completed job, jobs per second, thread CPU nanoseconds
per job (`cpu_ns_per_job`) and a checksum. Thread CPU time excludes descheduling;
it still depends on CPU frequency, cache contention and profiling overhead.
The two CPU clock reads are outside the measured CPU interval. Each run checks
that all submitted jobs have been drained and that completed counts and ID sums
match. There are no performance pass/fail thresholds.

For a scaling experiment, vary registered owners (e.g. 1, 8, 32, 128), first with
all queues active, then with `--active 1`. Vary depth separately. Compare medians
from repeated samples on the same machine and build type; keep the machine idle
and record the revision/compiler alongside the TSV. A concurrent build makes
these measurements unsuitable for comparing scheduler changes. Jobs/s here is
scheduler throughput, not disk IOPS.
