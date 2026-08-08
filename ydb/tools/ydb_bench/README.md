# YDB benchmark

`ydb_bench` packages actor benchmark executables into one Python tool and runs
reproducible benchmark profiles described by a YAML file. Build it with the
profile build type so the same binary can also be used with `perf`:

```bash
./ya make --build=profile ydb/tools/ydb_bench
```

The tool currently provides two benchmarks:

- `ping-bench`: pairwise actor ping throughput;
- `star-ping-bench`: star-topology actor ping throughput.

Inspect them and print the standard JSON Schema for the YAML configuration:

```bash
ydb/tools/ydb_bench/ydb_bench list
ydb/tools/ydb_bench/ydb_bench describe ping-bench
ydb/tools/ydb_bench/ydb_bench describe star-ping-bench
ydb/tools/ydb_bench/ydb_bench config-schema
```

A configuration can contain multiple benchmarks and multiple arbitrarily named
profiles for each benchmark:

```yaml
ping-bench:
  baseline:
    threads: [1, 2, 4, 8, 16]
    actor-pairs: [512]
    inflight: [1]
    duration: 3
    repetitions: 5
    affinity: [none, one-whole-numa, one-whole-chiplet, multi-chiplet]
  focused:
    threads: [16]
    duration: 20
    repetitions: 1
    affinity: [one-whole-chiplet]

star-ping-bench:
  star-sweep:
    threads: [4, 8, 16]
    actor-pairs: [512]
    stars: [1, 2, 4]
    duration: 3
    repetitions: 5
    affinity: [none, one-whole-chiplet]
```

`threads`, `duration`, `repetitions`, and `affinity` are required and arrays
must be non-empty. `actor-pairs` defaults to `[512]`; `inflight` and `stars`
default to `[1]` for their respective benchmarks. A per-process `timeout` can
be specified; otherwise it is computed from the requested parameter matrix and
duration. Unknown fields, benchmark names, affinity modes, and unsafe profile
names are rejected before a result directory is created.

Run every benchmark/profile pair from the file:

```bash
ydb/tools/ydb_bench/ydb_bench run \
    --config bench.yaml \
    --output ydb-bench-results
```

Add `--perf` to record each repetition with the same `cycles:u`, 99 Hz, and
DWARF-call-stack setup used by the YDB platform investigation:

```bash
ydb/tools/ydb_bench/ydb_bench run \
    --config bench.yaml \
    --perf \
    --output ydb-bench-profile
```

`--perf` is rejected unless `ydb_bench` itself was built with
`--build=profile`. Profiling changes both the build and runtime overhead, so its
throughput must not be mixed with a non-profile baseline.

The available placement modes are:

- `none`: no explicit affinity;
- `one-whole-numa`: all allowed CPUs from one NUMA node;
- `one-whole-chiplet`: all allowed CPUs from one L3-cache group (chiplet);
- `multi-chiplet`: CPUs spread across chiplets inside one NUMA node.

The `one-whole-*` modes use the complete allowed CPU set of the selected
topology group, independently of the largest requested thread count. The
`multi-chiplet` mode uses the same number of CPUs as that thread count. Topology
is read from Linux sysfs and intersected with the process's allowed CPU set. A
mode that the machine cannot provide is recorded as `unsupported` in the
profile's `run.json`; it is never silently replaced with another placement.

The top-level output contains:

- `run.json` with the config hash, tool revision, binary hash, and status of
  every benchmark/profile pair and paths to their individual summaries;
- `<benchmark>/<profile>/run.json` and `summary.csv` with the benchmark-specific
  columns and parameters; results from different profiles are not combined;
- `<benchmark>/<profile>/<affinity>/repeat-NNN/` with raw stdout, stderr, and
  extracted `metrics.csv`.

With `--perf`, the exact bundled profile ELF is saved once under `profiler/`.
Each repetition also contains raw `perf.data`, a symbolized flat
`perf-report.txt`, and `perf-buildids.txt`. These artifacts are generated before
the temporary executable is removed.

Use `--work-dir` when the system temporary directory is mounted with `noexec`.
The run fails if a process exits unsuccessfully, times out, is interrupted, or
produces empty or parameter-mismatched CSV data.
