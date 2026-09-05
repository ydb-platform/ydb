# YDB benchmark

`ydb_bench` packages actor benchmark executables into one Python tool and runs
reproducible benchmark profiles described by a YAML file. Build it with the
profile build type when embedded `ydb` and `ydbd` symbols are needed. Other
build types store stripped server and CLI binaries to keep the bundle compact:

```bash
./ya make --build=profile ydb/tools/ydb_bench
```

The tool provides four benchmarks:

- `ping-bench`: pairwise actor ping throughput;
- `star-ping-bench`: star-topology actor ping throughput.
- `memory-bandwidth-bench`: mixed sequential-copy and random copy/write memory workload.
- `local-ydb`: a local static/dynamic YDB cluster driven by the `kv`, `stock`,
  `log`, `tpcc`, or `topic` YDB CLI workload.

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

local-ydb:
  storage-capacity:
    workload:
      type: kv
      operation: upsert
      options:
        init-upserts: 1000
    geometry:
      preset: storage
      static-nodes: 2
      dynamic-nodes: 1
      max-dynamic-nodes: 8
      disk-size-gb: 64
      storage-groups: 1
    client:
      threads: 64
    load:
      parameter: rate
      allow-errors: false
      search:
        start: 1000
        maximum: 1000000
        resolution-percent: 2
      objective:
        type: maximize-throughput
        target-role: static
    measurement:
      warmup: 10
      duration: 30
      repetitions: 3
      verification-repetitions: 3
    affinity:
      ydb-cli:
        mode: pack-numa-pack-chiplet-spread-core
        cpus: one-chiplet
      static-nodes:
        mode: none
      dynamic-nodes:
        mode: none
```

The local YDB benchmark bundles `ydbd` and the YDB CLI, creates an isolated
Config V2 cluster backed by in-memory SectorMap PDisks, and stops it after the
profile. `single` always uses one dynamic node. `storage` may grow the
dynamic-node count up to `max-dynamic-nodes` when dynamic CPU is saturated but
static/storage CPU is not; `custom` keeps the explicitly requested geometry.
Each static node gets its own `NONE`-profile SectorMap with the virtual size
specified by `disk-size-gb`, so benchmark results are not limited by a host
block device.

`load.parameter` selects the one monotonic YDB CLI setting controlled by the
benchmark: `rate` maps to `--rate`, while `threads` maps to `--threads`. Topic
also searches `rate`, but maps it to the Topic CLI's `--message-rate`. The `kv`
and `stock` workloads accept either parameter; `log` searches `threads` because
its CLI does not expose `--rate`. Log throughput is the number of successful
batches per second; `rows-per-operation` controls how many rows are written by
each batch. Its default `ttl-minutes: 0` is a zero-minute table TTL, not disabled
TTL. A `values` list measures exact points. For adaptive runs,
`search` defines the range and resolution. `maximize-throughput` uses a
discrete ternary search and, after confirming a plateau, selects the lowest
CPU-saturated load within the configured throughput tolerance of the best
saturated measurement. A plateau is confirmed only when the selected role's
CPU is saturated. `latency-slo` uses the configured `multiplier` to find the
first failing point, then a binary search to find the highest load whose
millisecond percentile, error count, and achieved-rate ratio satisfy the SLO.
For example:

```yaml
    load:
      parameter: rate
      search:
        start: 1000
        maximum: 1000000
        multiplier: 2
        resolution-percent: 2
      objective:
        type: latency-slo
        percentile: p99
        max-ms: 10
        max-errors: 0
        min-achieved-rate-ratio: 0.98
```

TPC-C uses `load.parameter: max-sessions`, which maps to the CLI
`--max-sessions` limit and must not exceed `warehouses * 10`. The benchmark
runs `tpcc init` and `tpcc import` once for each dynamic-node geometry, reuses
that dataset for every search and verification sample at the geometry, then
runs both `tpcc clean` and recursive `scheme rmdir` cleanup. `client.threads`
maps to the TPC-C runner's `--threads`; it defaults to 2, while
`import-threads: 0` asks the CLI to select import concurrency automatically.
The result is rejected if the CLI reports a different number of execution
threads than `client.threads`, which prevents CPU-based CLI clamping from
silently changing the configuration being compared.

TPC-C warmup is inline. The CLI receives the explicit value
`max(measurement.warmup, floor(warehouses / 10) + 1)` so terminal startup is
complete before measurement; YAML `warmup: 0` requests that minimum rather
than the CLI's adaptive warmup. Measurement duration must be at least two
seconds. Canonical throughput is uncapped successful NewOrder transactions per
measured second. The CLI-reported, warehouse-capped `tpmC` remains available as
the separate `tpcc_tpmc` metric. Latency SLOs use the admitted latency of
`latency-transaction`: it excludes the queue created by the configured
`max-sessions` limit, but includes session acquisition and SDK retries. This
keeps the latency signal monotonic enough for load search; the full terminal
latency and pure transaction time remain in the raw CLI result. `p50`, `p90`,
`p95`, `p99`, and `p999` are available. See `examples/local-ydb-tpcc.yaml` for
a small manual smoke configuration.

Topic supports the CLI `full` producer-and-consumer workload. Every sample
creates a fresh generated topic, runs one inline warmup and measurement, and
then drops that topic, so consumer offsets and unread backlog cannot leak into
the next search or verification sample. `partitions`, `consumers`,
`message-size`, and the `raw`, `gzip`, or `zstd` codec are configurable.
`client.threads` defaults to 1 and is passed to both `--producer-threads` and
`--consumer-threads`; with multiple consumers, each consumer receives that many
reader threads. The CLI receives `--seconds` equal to warmup plus measurement
duration, one-second reporting windows with UTC timestamps, and a fixed p99
percentile. CPU aggregation is anchored to the CLI timestamps at windows
`warmup + 1` and `warmup + duration`; this excludes topic/session startup and
the first measurement interval from the approximate CPU window.

Canonical Topic throughput is the smaller of the write rate and the aggregate
read rate divided by `consumers`. The raw aggregate read rate and normalized
per-consumer rate remain separate result metrics. Latency SLOs therefore expose
only `p99`, backed by the CLI's full end-to-end p99 latency. The CLI output does
not contain a trustworthy request-error count, so Topic requires
`allow-errors: false` and `max-errors: 0`; a zero-throughput sample is still
ineligible. Measurement duration must be at least two seconds. See
`examples/local-ydb-topic.yaml` for a small manual smoke configuration.

For workloads which report an `errors` metric, set `load.allow-errors: true`
when request-level errors reported by `ydb workload` are an expected part of
the experiment. Such points remain
eligible for selection and the error counts stay in CSV, manifests, tables,
and charts, provided every repetition completed at least one successful
operation. A repetition with zero successful operations makes the whole point
ineligible, even when errors are allowed. It remains in the raw repetition and
attempt diagnostics, but is omitted from summary comparison rows and its
latency is not plotted as a zero. The flag does not hide or tolerate a failed
CLI process, timeout, malformed output, cluster failure, or workload
setup/cleanup failure. For a latency SLO, it disables the `max-errors`
rejection while keeping the successful-operation, latency, and achieved-rate
checks active.

The previous flat `mode`, `start`, and `slo` fields remain accepted for config
compatibility, but newly generated YAML uses `search` and `objective`.

`measurement.repetitions` controls how many samples contribute to every search
point. Set `measurement.verification-repetitions` to run additional independent
samples at the load selected by the search; it defaults to `0` so existing
configurations keep their previous runtime and is limited to 20. These
post-search samples contribute to the automatically computed default command
timeout; `timeout` remains a per-command safety bound rather than an absolute
profile deadline. Verification never
changes the selected load or dynamic-node scaling decision. Its holdout samples
are written separately to `verification-repetitions.csv` and
`verification-summary.csv`; a completed holdout becomes the reported metric
source while the search measurements remain intact for diagnostics. Latency
holdout metrics are evaluated with the same aggregate SLO contract as a search
point. A throughput holdout is diagnostic: its request-error acceptance,
throughput drift, and CPU saturation do not claim statistical reproducibility.

Workloads with geometry-scoped datasets initialize and import once for each
dynamic-node count, reuse that dataset across every search attempt at the same
geometry, and clean it before adding nodes. The final geometry remains open for
verification and is cleaned only after the holdout finishes. Shared setup and
cleanup commands are stored under that geometry's `workload/commands.json`.

During a local YDB run, the CLI reports cluster startup, workload initialization,
warmup, measurement, cleanup, evaluation, and dynamic-node scaling milestones.
The web profile page shows the same live phase with elapsed time and a countdown
for warmup and measurement. Completed attempts appear immediately on synchronized
search-order charts for candidate load, current best load, throughput, latency,
CPU by role, errors, and retries. Geometry stages and the chronological attempt
table remain available after completion. A bounded recent-activity log replays
profile phase transitions and commands after a page reload without exposing the
full event payload. Profile `run.json` stores attempt and
stage timestamps, durations, structured decisions, scaling actions, and the
final outcome so consumers do not have to parse diagnostic text.

Linux CPU metrics are sampled independently for static nodes, dynamic nodes,
the YDB CLI, and the whole host. Role affinity uses the existing placement
modes. `mode: none` deliberately leaves YDB server placement to Linux; the CLI
is pinned to one chiplet by default and its mask stays fixed throughout the
search. The web UI Builder edits workload, geometry, load controller,
measurement, and per-role affinity settings; the YAML tab exposes the same
portable configuration directly.

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
remain `local`. The Comparisons page persists a chosen run set locally. For
local YDB results it provides a compact baseline table with selected load,
throughput, latency, errors, CPU usage, dynamic-node count, and directional
deltas. Deltas are suppressed for semantically incompatible workload,
load-parameter, or latency-percentile combinations. Configuration,
environment, affinity, and binary differences remain visible next to every
candidate so that a confounded comparison is not mistaken for a regression.
Compatible local YDB profiles also get synchronized search curves for
throughput, latency, CPU by process role, and errors. Curves use the actual
searched load on the X axis, split geometry stages by dynamic-node count, and
connect only each profile's own measured loads; another profile's intermediate
load does not create a false gap or a synthesized value.
Generic configurable summary charts remain available below the baseline table.
# Result manifest compatibility

Run manifests use schema version 4. Earlier manifests are intentionally not
read as resumable results because they lack the immutable step plan and durable
per-step artifact contract.
