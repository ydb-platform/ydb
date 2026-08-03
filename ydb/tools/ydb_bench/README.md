# YDB benchmark

`ydb_bench` packages benchmark executables into a single Python tool and runs
reproducible platform-acceptance scenarios. The initial `actors-core` scenario
bundles the program variant of `ydb/library/actors/core/ut_fat` and runs only
`HeavyActorBenchmark::SendActivateReceiveCSVManual`.

Build and inspect the tool:

```bash
./ya make --build relwithdebinfo ydb/tools/ydb_bench
ydb/tools/ydb_bench/ydb_bench list
ydb/tools/ydb_bench/ydb_bench describe actors-core
```

Run a short check or the fixed comparison profile:

```bash
ydb/tools/ydb_bench/ydb_bench run actors-core \
    --profile smoke \
    --output ydb-bench-smoke

ydb/tools/ydb_bench/ydb_bench run actors-core \
    --profile baseline \
    --output ydb-bench-baseline
```

Every scenario is run in five placement modes by default:

- `none`: no explicit affinity;
- `single-numa`: CPUs from one NUMA node;
- `multi-numa`: CPUs spread across NUMA nodes;
- `single-chiplet`: CPUs from one L3-cache group (chiplet);
- `multi-chiplet`: CPUs spread across chiplets inside one NUMA node.

Pinned modes use the same number of CPUs as the largest requested thread count.
Topology is read from Linux sysfs and intersected with the process's allowed CPU
set. A mode that the machine cannot provide is recorded as `unsupported` in
`run.json`; it is never silently replaced with a different placement. Use, for
example, `--affinity none,single-numa` to run a subset.

Use `--work-dir` when the system temporary directory is mounted with `noexec`.
The tool extracts the bundled executable atomically into a unique temporary
subdirectory and removes it after the run.

The output contains:

- `run.json` with the tool revision, binary SHA-256, platform description,
  command, benchmark parameters, whitelisted environment, and status;
- `<affinity>/repeat-NNN/stdout.txt` and `stderr.txt` with unmodified process
  output;
- `<affinity>/repeat-NNN/metrics.csv` with CSV rows extracted from the unit-test
  output;
- `summary.csv` with median, minimum, and maximum throughput per affinity mode
  across external repetitions.

The run fails if the process exits unsuccessfully, times out, or produces an
empty or parameter-mismatched CSV result. Timeout and interruption stop the
whole process group so that scenarios can safely grow to include a server and
workload processes.
