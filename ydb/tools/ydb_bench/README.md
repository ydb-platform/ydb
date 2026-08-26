# YDB benchmark

`ydb_bench` packages actor benchmark executables into one Python tool and runs
reproducible benchmark profiles described by a YAML file. Build it with the
profile build type so the same binary can also be used with `perf`:

```bash
./ya make --build=profile ydb/tools/ydb_bench
```

The tool provides three benchmarks:

- `ping-bench`: pairwise actor ping throughput;
- `star-ping-bench`: star-topology actor ping throughput.
- `memory-bandwidth-bench`: mixed sequential-copy and random copy/write memory workload.

Inspect them and print the standard JSON Schema for the YAML configuration:

```bash
ydb/tools/ydb_bench/ydb_bench list
ydb/tools/ydb_bench/ydb_bench describe ping-bench
ydb/tools/ydb_bench/ydb_bench describe star-ping-bench
ydb/tools/ydb_bench/ydb_bench config-schema
```

For automation, use JSON discovery and validate the YAML before allocating any
result directory:

```bash
ydb/tools/ydb_bench/ydb_bench list --json
ydb/tools/ydb_bench/ydb_bench describe ping-bench --json
ydb/tools/ydb_bench/ydb_bench validate --config bench.yaml --json
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
    affinity: [none, pack-numa, pack-numa-pack-chiplet, spread-numa-pack-chiplet]
  focused:
    threads: [16]
    duration: 20
    repetitions: 1
    affinity: [pack-numa-pack-chiplet]

star-ping-bench:
  star-sweep:
    threads: [4, 8, 16]
    actor-pairs: [512]
    stars: [1, 2, 4]
    duration: 3
    repetitions: 5
    affinity: [none, pack-numa-pack-chiplet]

memory-bandwidth-bench:
  mixed-memory:
    threads: [1, 2, 4, 8, 16]
    random-percent: [0, 25, 50, 75, 100]
    random-mode: [copy, write]
    buffer-size-mb: [256]
    part-size-kb: [2048]
    duration: 3
    repetitions: 3
    affinity: [none, pack-numa, spread-numa-pack-chiplet]
```

The memory benchmark runs every matrix combination in a separate process. Each
worker owns and first-touches its private buffer after process affinity has been
applied. `random-percent` controls the deterministic interleaving of sequential
and random workers; `random-mode` selects byte copy or byte write. Before
allocating, the executable rejects a requested private-buffer footprint above
80% of Linux `MemAvailable` to avoid silently benchmarking swap activity.

Memory results retain both work counts and memory volume: sequential/random
operations, payload bytes, read bytes, written bytes, operations per second,
payload MB/s, read/write MB/s, and estimated program memory traffic MB/s.
Estimated traffic is `read_bytes + written_bytes`; it is not hardware DRAM
traffic because caches, prefetching, and write allocation can change physical
traffic. Hardware counters can therefore be added later as separate metrics
without changing the benchmark contract.

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

The default queue is fail-fast. Add `--continue-on-error` to attempt later
profiles after one fails; the final top-level status remains `failed` if any
profile failed. For an automation-friendly final report, use `--report-json`:

```bash
ydb/tools/ydb_bench/ydb_bench run --config bench.yaml --output results \
    --report-json results/report.json
# stdout is exactly one JSON value; progress and diagnostics use stderr.
ydb/tools/ydb_bench/ydb_bench run --config bench.yaml --output results-stdout \
    --report-json - > run.json
```

The path report is atomically written and has the exact same value as the
top-level `results/run.json`. YAML remains the portable input contract;
`config-schema` is provided only to help tools generate or validate it.

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

The available placement modes are `none`, `pack-numa`,
`pack-numa-pack-chiplet`, `spread-numa-pack-chiplet`,
`pack-numa-pack-chiplet-pack-core`, `pack-numa-pack-chiplet-spread-core`,
`pack-numa-spread-chiplet-pack-core`, `pack-numa-spread-chiplet-spread-core`,
`spread-numa-pack-chiplet-pack-core`, `spread-numa-pack-chiplet-spread-core`,
`spread-numa-spread-chiplet-pack-core`, and
`spread-numa-spread-chiplet-spread-core`.

They compose placement policies over NUMA nodes, chiplets, physical cores, and
vCPUs. `pack` exhausts the current entity; `spread` round-robins entities and
continues to the next level. `pack-core` keeps all allowed SMT siblings of a
core together, while `spread-core` takes one vCPU from every core before its
siblings. Topology is read from Linux sysfs and intersected with the process's
allowed CPU set. A mode that the machine cannot provide is recorded as
`unsupported` in the profile's `run.json`; it is never silently replaced with
another placement.

The top-level output contains:

- `run.json` with the config hash, tool revision, binary hash, and status of
  every benchmark/profile pair and paths to their individual summaries;
- `<benchmark>/<profile>/run.json` and `summary.csv` with the benchmark-specific
  columns and parameters; results from different profiles are not combined;
- `<benchmark>/<profile>/<affinity>/threads-NNN[/case-NNN]/repeat-NNN/` with raw stdout, stderr, and
  extracted `metrics.csv`.

With `--perf`, the exact bundled profile ELF is saved once under `profiler/`.
Each repetition also contains raw `perf.data`, a symbolized flat
`perf-report.txt`, and `perf-buildids.txt`. These artifacts are generated before
the temporary executable is removed.

Use `--work-dir` when the system temporary directory is mounted with `noexec`.
The run fails if a process exits unsuccessfully, times out, is interrupted, or
produces empty or parameter-mismatched CSV data.

## Local run control

Serve completed, active, or imported schema-v4 result directories locally:

```bash
ydb/tools/ydb_bench/ydb_bench web --output ydb-bench-results --no-open
```

The command prints its loopback URL and serves bundled HTML, CSS, and JavaScript
without external resources. The Builder and YAML pages validate and preview the
same parsed YAML and immutable `RunPlan` used by the CLI. Invalid YAML stays in
the editor and is never replaced by Builder data. Starting a valid plan creates
a durable `run.json` and navigates to its run detail.

The API provides `POST /api/validate`, `POST /api/plan`, `POST /api/runs`,
`POST /api/runs/<id>/cancel`, `GET /api/runs/<id>`, and replayable
`GET /api/runs/<id>/events`. Runs are owned by the application service rather
than a request handler. Event history and bounded stdout/stderr tails reconnect
after a page reload; cancellation is idempotent. The detail view shows the
benchmark/profile/affinity/repeat queue, active timeout, progress, and published
artifacts. On service recovery an in-progress manifest is marked
`recovery_required`: it is never restarted unless an executor can prove its
previous process stopped.

The server binds to `127.0.0.1` on a free port by default. A non-loopback
listener requires the explicit `--allow-remote` opt-in.

The offline UI has four persistent navigation sections:

- **Runs** is the local/imported run journal. It filters by status, benchmark,
  profile, source, and period; provides YAML, `run.json`, and portable archive
  downloads; and can import a ZIP from another machine.
- **New run** provides synchronized Builder and YAML tabs. Builder edits the
  selected benchmark/profile matrix, affinity modes, duration, repetition,
  timeout, perf, and queue policy. YAML is always the source of portable
  configuration; invalid YAML remains editable. A draft can be downloaded or
  stored beneath the configured output root before starting it.
- **System topology** displays the cpuset-filtered NUMA, chiplet, physical-core
  and SMT hierarchy, every affinity mode's first usable mask or rejection
  reason, and can seed a New run affinity template.
- **Comparisons** persists a local choice of local/imported runs and displays
  only the compatibility keys supported by the selected manifests.

Run detail has a durable queue grouped by `benchmark/profile`, current-step
placement and timeout, live stdout/stderr tails, and direct links to all
published artifacts. It also supports idempotent cancellation and reopening
the original YAML as a new draft.

## Portable result imports and comparisons

The Runs page accepts a portable ZIP through `POST /api/import`. A portable
archive contains a root `import.json`, `run.json`, and its related artifacts.
`import.json` is format version 1 and lists every other member with its exact
relative POSIX path, byte size, and SHA-256 hash. Only regular files are
accepted; absolute paths, traversal, duplicate entries, symlinks, unlisted
files, unknown member types, oversized archives, bad hashes, malformed import
manifests, and non-v4 result manifests are rejected before extraction.

Accepted results are installed under `OUTPUT/imports/import-<id>` without
changing `run.json`; files are made read-only and a collision never overwrites
an existing import. The Runs list labels them `imported` while local results
remain `local`. The Comparisons page persists a chosen run set locally and
shows only availability keys: shared benchmark/profile/affinity keys, shared
benchmark/profile keys where that shared affinity is unique, and each run's
own benchmark/profile keys. It intentionally performs no charting or metric
calculation.
# Result manifest compatibility

Run manifests use schema version 4. Earlier manifests are intentionally not
read as resumable results because they lack the immutable step plan and durable
per-step artifact contract.
