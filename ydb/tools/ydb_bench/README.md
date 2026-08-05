# YDB benchmark

`ydb_bench` packages benchmark executables into a single Python tool and runs
reproducible platform-acceptance scenarios. The initial `actors-core` scenario
bundles the program variant of `ydb/library/actors/core/ut_fat` and runs only
`HeavyActorBenchmark::SendActivateReceiveCSVManual`.

Build and inspect the tool:

```bash
./ya make --build=profile ydb/tools/ydb_bench
ydb/tools/ydb_bench/ydb_bench list
ydb/tools/ydb_bench/ydb_bench describe actors-core
```

The profile build is required for profiler runs so the bundled benchmark ELF
retains stable symbols, source information, and a build ID.

Run a short check or the fixed comparison profile:

```bash
ydb/tools/ydb_bench/ydb_bench run actors-core \
    --profile smoke \
    --output ydb-bench-smoke

ydb/tools/ydb_bench/ydb_bench run actors-core \
    --profile baseline \
    --output ydb-bench-baseline
```

Record a profiling run with the same `cycles:u`, 99 Hz, DWARF-call-stack setup
used by the YDB platform investigation:

```bash
ydb/tools/ydb_bench/ydb_bench run actors-core \
    --profile smoke \
    --threads 16 \
    --duration 20 \
    --repetitions 1 \
    --affinity one-whole-chiplet \
    --perf \
    --output ydb-bench-profile
```

`--perf` is rejected unless `ydb_bench` itself was built with
`--build=profile`. Profiling changes both the build and runtime overhead, so its
throughput must not be mixed with the non-profile baseline.

Every scenario is run in five placement modes by default:

- `none`: no explicit affinity;
- `one-whole-numa`: all allowed CPUs from one NUMA node;
- `multi-numa`: CPUs spread across NUMA nodes;
- `one-whole-chiplet`: all allowed CPUs from one L3-cache group (chiplet);
- `multi-chiplet`: CPUs spread across chiplets inside one NUMA node.

The `one-whole-*` modes use the complete allowed CPU set of the selected topology
group, independently of the largest requested thread count. The `multi-*` modes
use the same number of CPUs as that thread count. Topology is read from Linux
sysfs and intersected with the process's allowed CPU set. A mode that the machine
cannot provide is recorded as `unsupported` in `run.json`; it is never silently
replaced with a different placement. Use, for example,
`--affinity none,one-whole-numa` to run a subset.

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

With `--perf`, the output also contains the exact bundled profile ELF and, for
each repetition, raw `perf.data`, a symbolized flat `perf-report.txt`, and
`perf-buildids.txt`. The report and build-ID list are generated before the
temporary executable is removed.

The run fails if the process exits unsuccessfully, times out, or produces an
empty or parameter-mismatched CSV result. Timeout and interruption stop the
whole process group so that scenarios can safely grow to include a server and
workload processes.
