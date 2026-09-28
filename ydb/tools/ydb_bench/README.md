# YDB benchmark

`ydb_bench` packages actor benchmark executables into one Python tool and runs
reproducible benchmark profiles described by a YAML file. Build it with the
profile build type when embedded `ydb` and `ydbd` symbols are needed. Other
build types store stripped server and CLI binaries to keep the bundle compact:

```bash
./ya make --build=profile ydb/tools/ydb_bench
```

The tool provides five benchmarks:

- `ping-bench`: pairwise actor ping throughput;
- `star-ping-bench`: star-topology actor ping throughput.
- `memory-bandwidth-bench`: mixed sequential-copy and random copy/write memory workload.
- `local-ydb`: a local static/dynamic YDB cluster driven by the `kv` or `stock` YDB CLI workload.
- `distributed-ydb`: an experimental fixed multi-host YDB cluster with configurable CLI generators.

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
    actor-system:
      use-shared-threads: false
      use-united-pool: false
      use-ring-queue: true
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
Across scaled stages, latency search selects the highest feasible load, while
manual points and throughput search select the highest observed throughput;
ties prefer fewer dynamic nodes.
Each static node gets its own `NONE`-profile SectorMap with the virtual size
specified by `disk-size-gb`, so benchmark results are not limited by a host
block device.

`actor-system.use-shared-threads`, `actor-system.use-united-pool` and
`actor-system.use-ring-queue` are independent boolean switches. The first two
default to `false`; `use-ring-queue` defaults to `true`, matching YDBD.
They set YDBD's `use_shared_threads`, `use_united_pool` and `use_ring_queue` in `actor_system_config` for all
static and dynamic nodes, including scaled and verification clusters, while
keeping automatic pool sizing enabled. They do not affect the YDB CLI.
The Builder exposes all three switches; saved profile parameters and comparisons
retain their values.

For `local-ydb`, `actor-system.use-waker: true` enables the experimental waker
for automatically configured BASIC executor pools through YDBD's `use_waker`.
It defaults to `false` and requires a YDBD build that supports this field.
When disabled, the field is omitted from the generated YDB configuration so
older external binaries keep working. The saved profile and Builder retain the
explicit boolean value.

Set `ydbd-binary: /absolute/path/to/ydbd` in a `local-ydb` profile to
use a different YDBD build. The Builder exposes the same optional executable
path. It refers to a readable executable on the benchmark host (not the browser
machine); relative paths and `~` are not accepted. Omit it to use bundled YDBD.
The YDB CLI remains bundled. All static, dynamic, scaled, and verification
nodes use the selected binary. At the first use of each distinct path in a run,
the executable is copied into the temporary run directory without stripping;
subsequent profiles using that path reuse the snapshot. Profile manifests
record its original path, SHA-256 and size. The original file is never modified.

For a version selector in Builder, arrange executable files as
`bin/ydbd/<version>` and start the server with
`ydb_bench web --binaries-dir /absolute/path/to/bin` (default: `./bin`).
For example, `bin/ydbd/stable-26-3-1` is an executable file, not a directory.
The catalog is refreshed when Builder loads; only readable executable files
are listed. Selecting a version writes its absolute path to `ydbd-binary`.
Manual paths and bundled YDBD remain available.

An explicitly configured profile `timeout` caps every YDB CLI setup, warmup,
measurement, and cleanup command. Workload-specific safety limits still apply
when they are shorter. Without an explicit cap, setup and cleanup retain their
workload-specific budgets. The computed default is used as a conservative
per-command budget for cluster control; it is not a deadline or an estimate of
the total profile runtime.

`load.parameter` selects the one monotonic YDB CLI setting controlled by the
benchmark: `rate` maps to `--rate`, while `threads` maps to `--threads`.
Both `kv` and `stock` accept either parameter. Stock throughput counts successful
query operations per second.
A `values` list measures exact points. For adaptive runs,
`search` defines the range and resolution. `maximize-throughput` uses a
discrete ternary search and, after confirming a plateau, selects the lowest
CPU-saturated load within the configured throughput tolerance of the best
saturated measurement. A plateau is confirmed only when the selected role's
CPU is saturated. `latency-slo` uses the configured `multiplier` to find the
first failing point, then alternates linear interpolation of the nearest latency measurements with binary steps to find the highest load whose millisecond percentile, error count, and achieved-rate ratio satisfy the SLO. It finishes only when that load and the next integer have been measured: the selected load passes and the next one fails. `resolution-percent` applies only to throughput search; older SLO configurations may still contain it, but it no longer stops refinement early. A passing configured maximum remains a lower bound, not a discovered capacity limit. This is an observed discrete boundary, not a guarantee against latency noise or nonmonotonic workloads.
Automatic search is limited to 64 measurements per cluster-geometry stage.
The `storage` preset can run a separate search after each dynamic-node scaling
step, so a complete profile can contain more than 64 measurements.
Latency-SLO configuration validation rejects ranges, multipliers, or
resolutions whose deterministic worst-case search path can exceed that limit.
Throughput search stops at the limit and reports the best point measured so far
with a `search-limit-reached` outcome, because its path depends on measured
throughput, feasibility, and cached probes.
For example:

```yaml
    load:
      parameter: rate
      search:
        start: 1000
        maximum: 1000000
        multiplier: 2
      objective:
        type: latency-slo
        percentile: p99
        max-ms: 10
        max-errors: 0
        min-achieved-rate-ratio: 0.98
```

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
post-search samples are included when deriving the conservative default
cluster-control command budget. An explicitly configured `timeout` also caps
each workload command; it remains a per-command safety bound rather than an
absolute profile deadline.
For automatic latency-SLO search, verification participates in selection: a rejected candidate is marked failed and search resumes below it, reusing existing measurements in the selected geometry. Rejected verification samples and commands are retained in `verification-rejected-NNN/`, with their paths recorded in `run.json`. The final accepted samples are written to `verification-repetitions.csv` and `verification-summary.csv` and become the reported metrics. This adaptive verification is not an independent holdout. If no feasible point remains, no passing result is published. Cancellation, command failure and malformed output still fail the execution rather than being treated as latency evidence. The 64-search-measurement safety limit still applies across resumptions; exhausting it does not publish a precise boundary.

For explicit points and throughput searches, verification does not change the selected load or dynamic-node scaling decision and remains an independent holdout. A throughput holdout is diagnostic: its request-error acceptance,
throughput drift, and CPU saturation do not claim statistical reproducibility.

When the winning stage is the last one, its cluster remains open until
verification finishes. If an earlier stage wins, verification recreates its
geometry on a fresh cluster; that cluster's configuration is stored in
`verification-cluster/cluster.yaml`.

The profile page separates the final **Result** from the **Discovery** process.
Result presents the selected load, throughput, latency, errors, and CPU metrics;
Discovery keeps the attempt history, synchronized search charts, and commands.
Each attempt links to a separate page with YDB executor-pool counters, grouped by
node and measurement repetition. Verification has its own metrics page.
The collector samples the local monitoring endpoints every two seconds during
measurements and saves `ydb-metrics.jsonl` with the profile artifacts.
Thread-count gauges are displayed as threads (the original counters use threads
multiplied by 100); elapsed and CPU microseconds can be displayed as raw counters
or per-second deltas. Counter resets and failed samples break the rate series.
Each counter has its own chart with lines for all pools of the selected
node, including both microsecond counters. Hover values use
two decimal places and share a time cursor across charts.
Collection is best-effort, limited to 32 MiB per profile, 64 nodes and 32 pools
per node. The attempt view retains at most 300 samples / 2 MiB and reports
truncation; the full saved file can be downloaded. Historical runs without the
artifact show an empty metrics page.

Alongside the chart projection, local and distributed measurements archive full
YDB dynamic-counter snapshots every five seconds in `ydb-counters/*.jsonl.gz`.
Each record identifies the host, node role/index, monitoring port, timestamp and
attempt/repetition context. `/counters/json?@private=1` includes public and private
counter groups, labels and histograms without the chart counter whitelist.
These archives do not add charts. No uncompressed snapshot files are retained.
The versioned `ydb-counters-delta-v1` format uses a per-part metric dictionary
(`definitions`: ID and labels/type) and per-node `changes` (ID and new value or
histogram, not arithmetic differences). `present`, emitted initially and when
membership/order changes, lists the complete ordered set of metric IDs. Newly
present scalar metrics default to integer zero; later transitions to zero are
explicit changes. Missing IDs in a new `present` list cease to exist. Failed
polls contain an error and do not change the last successful state. Duplicate
label sets retain separate IDs; negative gauges and histograms are preserved.
Each node's first successful record in a part is a checkpoint; subsequent parts
never depend on earlier files. A sequence number detects missing/reordered records.
`gzip -dc` exposes the encoded records; `read_counters_archive(path)` in
`ydb/tools/ydb_bench/lib/ydb_telemetry.py` reconstructs complete snapshots and
also accepts legacy full-snapshot gzip files. The reader supports concatenated
gzip members. Archives rotate around 16 MiB compressed, or when dictionary/value
JSON state reaches 32 MiB (not a strict Python heap limit), and are copied with distributed results.
They are excluded from the 128 MiB aggregate workload-transfer budget, while the
32 MiB per-file and 1000-file transfer safety limits still apply. Portable ZIP
export retains its separate archive-size limit.
Collection is best-effort: individual requests have a 16 MiB response bound and
network deadlines; failed samples contain an explicit error instead of counters.
Slow sampling cycles do not overlap. Disk errors stop collection and are logged;
collected archive parts are retained rather than discarded.

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

### Distributed YDB (experimental)

Select **Deploy cluster** in the distributed Builder to reserve and deploy a
cluster without running a workload. This is a dedicated run with one profile:
its YAML contains `mode: deploy`, `cluster-template`, `storage`, and `tenants`.
CLI nodes in the snapshot are ignored; no load generator or search is started.
The cluster remains active until **Release cluster** is pressed in Runs or the
run page. Connection endpoints and the actual launch YAML remain available in
the profile. Other runs can be queued on the coordinator while it is held.
The queue advances only after worker cleanup is confirmed. Cancel, controller
shutdown and lease expiration retain the existing interruption/recovery rules;
an interrupted deployment is not automatically restarted.

While ready, a reservation records per-host CPU telemetry and all YDB counters
(every five seconds, using the compressed dictionary/delta archive format).
Completed one-minute intervals are copied into the profile's `telemetry/`
directory while the cluster is held; Release saves the final interval before
stopping nodes. Run downloads and run-level Prometheus export include these
archives. Collection/transfer errors are reported explicitly; an interrupted
transfer can leave the latest interval incomplete. Older reservations without
telemetry remain readable and are labelled as having no recorded metrics.

The template **Configuration** tab edits all message types reachable from the
bundled YDB `TAppConfig` and `TEphemeralInputFields` protobuf descriptors. The latter
covers YAML input fields such as `hosts`, `host_configs`, `fail_domain_type`,
`default_disk_type`, `erasure` and top-level `storage_pool_types`.
Nested messages, repeated fields,
maps and oneof alternatives are discovered at runtime, not maintained as a
separate field list. The schema fingerprint identifies the editor schema; it
does not establish compatibility with an external YDB binary.

Only explicitly added fields are saved in the template's `ydb_config` mapping.
Removing a field unsets it; an unchecked boolean remains explicitly false.
Use **Add section** or the **+** beside a section to search available fields.
Scalar values can be set before adding; Cancel leaves the draft unchanged.
Tenant **Effective configuration** shows inherited values read-only; **Override**
enables a local value and **Reset** returns a field to inheritance.
**Overrides only** hides inherited values without removing them from the field picker.
64-bit integer values are kept as strings across the browser boundary. The
top-level YAML tab accepts/exports a configuration V2 draft with `metadata`,
`config`, `allowed_labels` and `selector_config`. Metadata starts at revision 0
with an empty cluster identity; the runner supplies the actual session identity.
Configuration scope selects cluster defaults or a tenant's overrides, stored in
`ydb_tenant_configs`. Each tenant has one exact `tenant` selector. Nested maps
use `!inherit` by default. The checkbox beside each tenant mapping controls
whether it inherits or replaces the entire section; replacement paths are stored
in `ydb_tenant_replacements`. YAML imports preserve this choice. Lists replace
the corresponding list (their items do not inherit). Other selector predicates
and multiple selectors for one tenant are rejected
rather than silently converted. Legacy bare mappings can still be imported.
Unknown fields are retained
and reported, not silently removed. Comments and YAML formatting are not retained.
Validation checks known protobuf field types, not full YDB cluster semantics.

The tabs are Cluster, Physical, Logical, Tenants, Configuration and YAML.
Applying YAML reconciles missing DCs/racks from `hosts` or `nameservice_config.node`,
and tenants from exact tenant selectors or explicit tenant-pool slots. Registered
hosts are matched by exact name, ID or endpoint hostname (case-insensitive DNS names);
unknown or ambiguous hosts reject the entire operation with a dialog. No hosts are
registered automatically. Existing nodes, disks and assignments are preserved.
New tenants default to SSD and one storage group; review them before saving.
Application changes only the draft, not the saved template. Imported managed or
unknown config sections retain the existing execution-validation restrictions.
Cluster is a projection of the same `ydb_config` mapping: domain name,
self-management erasure, state storage and storage pool types. Renaming the
domain in this form updates template tenant paths, not existing run drafts.
Absent state storage and pools keep YDB/benchmark automatic generation.
Execution supports one domain (ID 1), erasure `none`, `block-4-2` or
`mirror-3-dc`, standard SSD/HDD pools and a single flat state-storage ring
using static node IDs. Geometry is checked before worker preparation.
Advanced configurations remain editable but may be rejected for execution.

During distributed execution supported configuration overrides are recursively
merged into each generated node config; lists replace existing lists. Unknown
fields and benchmark-owned placement, disk, endpoint, system bootstrap and
actor-system sections reject execution rather than bypassing admission or
silently overriding the run Builder. These sections remain editable/exportable
for standalone configuration work. Importing configuration does not reconstruct
physical placement yet. No resources are opened or modified by the editor.

`distributed-ydb` runs one fixed YDB cluster across the hosts in a placement
template. All participating benchmark servers must run a compatible distributed
peer protocol on Linux and be registered with the coordinator. The coordinator
checks every host's identity and protocol before reserving any participant.
Peer HTTP requests go server-to-server; the browser only talks to its own server.
YDB nodes and the CLI also need direct network connectivity to the advertised
hostnames and dynamically allocated gRPC, interconnect and monitoring ports.

In **Cluster templates**, choose **New run**, select the workload's target tenant,
then confirm **Prepare run**. This copies the current placement (including unsaved
edits) into the New run YAML draft; it neither saves the template nor launches
processes. The confirmation explicitly replaces any previous New run draft.
Review the generated configuration in Builder or YAML, validate it, and use **Start run** separately.
The initial draft uses the `kv` upsert workload, one thread per CLI, 4 vCPU
per static/dynamic node, and no verification repetition. These are editable
starting values, not recommendations for a particular machine.

In the **Physical** view, select a static node and use **Add disk** to configure
individual SectorMap, file, block-device or PARTLABEL entries. Each entry specifies
SSD/HDD media; SectorMap and file entries also specify size in GiB. Paths belong
to the node's host; PARTLABEL stores a partition label, not a device path.
Saving a template never creates files or opens/formats devices. Legacy SectorMap
count/size settings migrate to an explicit disk list. Identical paths on one host
are rejected, including PARTLABEL and its explicit `/dev/disk/by-partlabel/` path.
Other aliases require host-side device identity checks and are not resolved by
template validation. Execution supports all four sources with SSD or HDD media.
Temporary files use `temporary: true` without a path; each generation creates its
own files in `file-disks/<session-id>/` next to the history database and removes
them after processes stop, including cancellation and recovery cleanup.
Persistent files use `name` and reside directly in `file-disks/`; they remain after
cleanup. Existing files must have the configured size. Existing files and block
devices require the run-level `reset-disks: true` permission: YDB metadata is
cleared before each cluster start, including search repetitions. Use only dedicated
benchmark disks; their previous data is lost. New files need no reset permission.
Workers reject mounted devices, active holders, duplicate device identities and
overlapping disk/partition assignments, and hold generation-scoped disk locks.
This does not replace reserving the devices against unrelated external workloads.
Disks appear inside their storage node in Physical. Drag a disk onto another
static node to reassign it within the same physical host.
Individual disk cross-host moves are rejected. Whole nodes may move between hosts
when all their disks are SectorMap or temporary files: configuration moves, not data. Empty
storage nodes may be saved while editing; execution requires at least one disk.

The legacy single-generator format uses the YAML editor. Its
`cluster-template` field contains the complete placement snapshot, and `tenant`
selects a database from that snapshot. `workload`, `actor-system`, `client`,
`load`, `measurement`, and `timeout` reuse the local-YDB configuration contract.
Actor-system vCPU is independent of affinity. Binary selection, node counts,
logical locations, tenant assignments and CPU masks come from the template;
there is no separate run-level geometry or affinity override.

Supported legacy scope is storage with erasure `NONE`, one CLI
generator, at least one static node, and a target tenant with dynamic nodes.
Other tenant definitions are allowed, but only the selected tenant receives
the workload. Geometry is fixed during search and verification. This is not a
multi-generator throughput test or a durability/failure-tolerance benchmark.
Launch through the web coordinator, not the standalone `run` command.

#### Multi-generator Builder

The Builder supports KV and stock workloads with up to 32 CLI generators,
with fixed loads or search assigned to one generator.
Its sections are Cluster, Storage, Tenants, Load generators and Run policy;
YAML remains a separate top-level tab. Each CLI has its own target tenant,
dataset, operation, client threads and thread/rate load. Storage and
each tenant have separate actor-system flags and per-node `cpu-count` values.
Placement and affinity remain in the template snapshot.

The new format uses `cli-nodes` instead of the legacy profile-level
`tenant`, `workload`, `client`, `actor-system` and `load` fields:

```yaml
distributed-ydb:
  baseline:
    cluster-template: # complete placement snapshot, supplied by the Builder
      # ...
    storage: {cpu-count: 8, use-shared-threads: true}
    tenants:
      /Root/bench: {cpu-count: 16, use-united-pool: true}
    cli-nodes:
      cli-write:
        tenant: /Root/bench
        dataset: shared-kv
        workload: {type: kv, operation: upsert, options: {init-upserts: 1000}}
        client: {threads: 32}
        load: {parameter: threads, values: [32]}
      cli-read:
        tenant: /Root/bench
        dataset: shared-kv
        workload: {type: kv, operation: select, options: {init-upserts: 1000}}
        client: {threads: 64}
        load: {parameter: threads, values: [64]}
    measurement: {warmup: 2, duration: 30, repetitions: 1, verification-repetitions: 0}
```

Every CLI in the template must have an entry. Dataset identity is the pair
`(tenant, dataset)`: matching pairs share one initialization and cleanup, while
different pairs are independent for KV. Shared workload types and options must match, including
`init-upserts`; operations and loads can differ. Builder edits to shared dataset
options apply to all generators using that pair.

All datasets are initialized before any generator runs. CLI samples run
concurrently, including multiple CLI nodes on the same host. Per-CLI results,
latencies and measurement clocks are stored in `cli-results.json` beside each
sample's host metrics; complete individual artifacts are in CLI-named directories.
All generators currently must use the same `allow-errors` policy.
For fixed load, summary throughput is the sum of individual rates. Percentiles are not merged;
whole-cluster CPU aggregation is unavailable for these separate measurement
windows. This is concurrent fixed load, not clock-synchronized traffic replay.

The legacy single-CLI YAML and its search/verification remain supported.
Conversion to the fixed-load Builder is explicit and replaces load settings
with defaults. At most one CLI may own `load.search` and `load.objective`;
all other generators must have one fixed load value. The selected CLI uses the
same latency-SLO or maximize-throughput search and verification as local-ydb.
Search decisions and headline workload metrics describe that CLI only, not the
sum of foreground and background throughput. Individual results remain available
in `cli-results.json`. Background generators run at their fixed load in each
sample; this is not a continuous or clock-synchronized background workload.

Both KV and stock are supported. Stock uses fixed table names, so all stock
generators targeting one tenant must share the same dataset and options. Separate
stock datasets require separate tenants. Initialization and cleanup run once per
shared dataset, before and after the measurements.

For example, replace one CLI's fixed `load` with:

```yaml
load:
  parameter: threads
  search: {start: 1, maximum: 256, multiplier: 2}
  objective: {type: latency-slo, percentile: p99, max-ms: 20}
```

Use `measurement.verification-repetitions` to enable final verification. In the
Builder, choose the workload and search objective under **Load generators**;
verification is configured in **Run policy**. YAML remains a separate top-level tab.
All participant servers must use the same distributed protocol version (12).

Each worker freezes the selected binaries, resolves placement from its own
topology and reserves ports. The coordinator retains that execution plan,
binary checksums, results and host-qualified telemetry in one canonical run.
Each distributed profile retains `profile.yaml` with its input configuration,
including the embedded cluster template. Before starting YDB nodes, the coordinator
checks that all workers generated identical YAML and downloads one complete
`cluster/configuration/cluster.yaml` (and
`verification-cluster/configuration/cluster.yaml` for a fresh verification cluster).
All static and dynamic nodes use this same document: storage actor-system settings
are in the base configuration, and each tenant's settings are in its selector.
These are the same bytes supplied to YDB, including resolved placement and tenant
overrides, not a later reconstruction from the saved template.
The run Configuration page and profile result link to these retained artifacts.
Older runs without these artifacts are explicitly reported as unavailable.
Counters are viewed per host, without merging unrelated wall clocks. Aggregate
CPU metrics require sufficient common measurement coverage and bounded clock
uncertainty; missing coverage is not reported as zero utilization.

Workers hold renewable leases. Cancellation or lease expiry stops their managed
processes; stale requests cannot reopen a finished generation. Temporary binary
copies are deleted after stopping, while original binaries remain unchanged.
The coordinator downloads bounded diagnostic log tails and configurations after
release. Unconfirmed cleanup is reported as `recovery_required`, not success.
After a worker server restart, unfinished sessions are cleaned up automatically
on Linux when their durable process ownership journal is available. Recovery
uses process identity and pidfds, and retries unresolved cleanup every ten seconds.
The coordinator waits for confirmed release from every participant before
finalizing the interrupted run. Unreachable participants keep admission blocked.
Older sessions without an ownership journal still require manual recovery.

### Other benchmark profiles and CLI execution

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
`recovery_required`. Automatic recovery stops only processes identified by the
run's durable ownership journal, preserves artifacts, and marks the interrupted
run as failed after cleanup is confirmed. It does not restart the benchmark.
Missing ownership evidence or unsupported process recovery keeps the run blocked
for manual recovery rather than risking another workload.

The server binds to `127.0.0.1` on a free port by default. A non-loopback
listener requires the explicit `--allow-remote` opt-in.

Run lists use a rebuildable `.run-index.sqlite3` database in the output directory.
Only list/search metadata is indexed; manifests, logs and metrics remain in files.
Startup reconciles the index with existing manifests. Known service runs and UI
imports are refreshed before listing; a background reconciliation discovers other
file changes every five seconds. To rebuild manually, stop the service and move
the index (including any `-wal` and `-shm` sidecars) aside, then restart. Corrupt
SQLite files are preserved with an `.invalid-*` suffix when automatically rebuilt.

Runs and the comparison run picker use server-side filters and cursor pages of
50 records. Each host returns a bounded page, merged by sort value, host ID and
run ID. Both hosts must support `/api/run-page`; an older/unavailable peer is
reported as incomplete, and advancing is disabled until it can be read. Refresh
returns to the first page. Pages are a live view, not an immutable snapshot:
changing durations or timestamps may move active runs between pages.
The legacy `/api/runs` list response is retained for older clients.

The gear icon opens **Settings → Monitoring**. Monitoring settings are stored
separately from host availability. Each server maintains a shared in-memory peer
availability state, checked in the background every 10 seconds after each check
cycle. Failed network requests mark peers unavailable immediately; federation and
UI proxy requests skip them until a successful authenticated identity probe.
Partial run pages retain an error and do not advance their cursor. A notification
appears once per transition to unavailable (also on initial observation of an
unavailable peer), while the results retain their incomplete-data warning.

Monitoring settings are stored
separately from run configurations in `.monitoring-settings.json`. The initial
owner is selected by host ID and remains the owner after synchronization;
all peers must be reachable for initialization. Later edits use optimistic
revision checks and are forwarded to that owner. Peers pull updates every 30
seconds. Unavailable peers remain pending; conflicting independent owners are
reported rather than silently overwriting configuration. Keep the owner in the
host directory; automatic owner failover is not supported.

The Prometheus URL must be reachable from benchmark hosts; an empty URL disables
the integration. An optional bearer token is stored in the server-side settings
file and synchronized with peers. The browser receives a token-presence flag and
a mask with four asterisks and the last four characters (short tokens are fully masked);
an empty token input preserves the saved token, with a separate removal control.
Peer transport must be trusted (use HTTPS outside a trusted network).
Legacy import-service URLs are not reused as Prometheus URLs during migration.
The Grafana URL must be reachable from the user's browser.
Finished runs and attempts offer **Export metrics to Prometheus** when the URL
is configured. Export runs on the owning benchmark server in the background,
decodes archived counters without modifying them and sends Remote Write 1.0 to
`PROMETHEUS_URL/api/v1/write`. Enable `--web.enable-remote-write-receiver` and
configure `storage.tsdb.out_of_order_time_window` to cover historical samples.
Original timestamps, zero values and histogram bucket counts are preserved;
no histogram sum is invented. Series include `bench_host`, `bench_run`,
`bench_benchmark`, `bench_profile`, `bench_attempt`, and `bench_repetition`.
Exports require a finished run so archives remain stable. One export runs per
server; temporary SQLite staging is limited to 2 GiB. Progress is stored under
`.metrics-exports`. Every new or retried export checks raw Prometheus samples
over the archived time interval: identical labels, timestamps and values are
skipped; missing samples are sent; conflicting values stop the export.
Query failures do not fall back to blind writes. A changed archive fingerprint
prevents unsafe retries. Changing the Prometheus URL or token invalidates saved
export statuses across hosts after settings synchronization, even when changing
back to an earlier URL. Grafana-only changes leave statuses intact.
An interrupted request can be replayed with identical timestamps and values.
Partial failure does not roll back samples already accepted by Prometheus.

Attempt pages also offer **Open in Grafana** when a Grafana URL and recorded
counter timestamps are available. The chooser lists existing Grafana dashboards
and bundled benchmark versions of the YDB dashboards. Links carry
the attempt time range and host/run/profile/attempt variables. Bundled panels
use those variables in every metric selector; existing third-party dashboards
must implement the variables themselves to isolate a benchmark attempt.

Configure `grafana_url` as the browser-facing base URL and `grafana_api_url`
as the base URL reachable from the monitoring settings owner (empty means use
`grafana_url`). All hosts forward Grafana operations to that owner, so Grafana
can remain on its loopback listener. The optional `grafana_token` is a separate
Grafana service-account token, stored and redacted like the Prometheus token.
Its permissions must allow reading dashboards/datasources and creating dashboards.
`grafana_datasource_uid` selects the default Prometheus datasource in the chooser.
The user can choose another datasource before opening or installing a dashboard.
The binary bundles all JSON dashboards in `ydb/deploy/helm/ydb-prometheus/dashboards/`.
New files are discovered automatically during the next build; deployed binaries
do not fetch new templates at runtime. Dashboard names come from their JSON titles.
**Install and open** uses a separate versioned UID and `overwrite=false`;
existing dashboards are never overwritten. Installation
does not export metrics. Export counters separately before viewing archived data.

The offline UI has the following persistent navigation sections:

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
# Actor-system capacity and CPU placement

For local YDB, `actor-system.static-nodes.cpu-count` and `actor-system.dynamic-nodes.cpu-count` independently set the vCPU count used by YDB automatic actor-system configuration **per node**. They do not set an OS affinity mask or an exact executor thread count. These positive integers remain unchanged when dynamic nodes are added. `affinity` only controls eligible logical CPUs; its mask can be larger or smaller than the configured actor-system capacity. For example:

```yaml
actor-system:
  static-nodes: {cpu-count: 8}
  dynamic-nodes: {cpu-count: 8}
affinity:
  static-nodes: {mode: pack-numa-pack-chiplet, cpus: 16}
  dynamic-nodes: {mode: pack-numa-pack-chiplet, cpus: 32}
```

An explicit actor-system count also works with `mode: none`. Omitting it preserves YDB's automatic detection from the process affinity (or available host CPUs). Linux CPU usage remains relative to the assigned CPUs, not this actor-system setting.
