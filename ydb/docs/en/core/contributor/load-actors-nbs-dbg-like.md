# NbsDbgLikeLoad

`NbsDbgLikeLoad` measures a simplified Network Block Store (NBS) write/read path through [Direct Block Groups (DBGs)](distributed-storage/direct-block-groups.md). A persistent load tablet owns the DBGs and sends requests to [PersistentBuffer (PB)](distributed-storage/persistent-buffer.md) and [DDisk](distributed-storage/ddisk.md). Separate load actors generate requests and measure their completion latency.

Use this actor to compare PB replication, background copying to DDisk, batching, and concurrent I/O. For the DSProxy/VDisk blob-storage path, use [{#T}](load-actors-storage.md).

## Prerequisites {#prerequisites}

Use a test cluster with DDisk and PB pools configured in the BlobStorage Controller (BSC). The allocation must return the number of DDisk/PB pairs expected by `HostsPerDbg`. The load tablet also needs ordinary tablet-channel storage and a working Hive; DDisk pools do not provide that storage.

Open `http://<node>:8765/actors/load?mode=tablet` on a node in the intended database. This page creates tablets, lists their Hive IDs and placement, and starts runs. The monitoring port may differ in your cluster.

{% note warning %}

Run one workload at a time per load tablet, and finish it before deleting the tablet. The tablet stays in its `Ready` phase during a run and does not enforce exclusive access. A later run reconfigures the same per-DBG actors.

{% endnote %}

## Automation with dstool {#automation}

Use `ydb-dstool cluster workload nbs-dbg-like` for scripts and recoverable runs. It calls the existing legacy `TestShardControl` gRPC method; it never falls back to HTTP. Deploy server support first, including the load service on the coordinator and generator nodes and tablets supporting automation protocol version 1. Each command requires an explicit database (or a saved handle containing it) and administrator authorization. Capability checks reject unsupported servers and stale coordinator incarnations.

The legacy gRPC service is not enabled by default on TLS endpoints or when an explicit service list excludes it. Add `legacy` to `GRpcConfig.ServicesEnabled` (textproto), or merge this setting into the server YAML configuration:

```yaml
grpc_config:
  services_enabled:
    - legacy
```

Preserve other enabled services and ensure `legacy` is absent from `services_disabled`, which takes precedence. The `test_shard` service is a separate service and does not enable this legacy RPC. This workflow does not change server service-enablement defaults.

Use dedicated load tablets. Serialize create and delete for each Hive owner index, including operations from the HTTP interface. Reserve distinct owner indices across databases that share a Hive. Concurrent lifecycle mutations from independent controllers are unsupported. The coordinator rejects overlapping runs it knows about and deletion of a tablet with a known active run; this is not a cluster-wide lock. Do not concurrently run workloads through another coordinator against the same tablets.

### Configuration and commands

The gRPC `create` command builds `TAllocConfig` from flags; it needs no allocation file. `--pool-name` selects both the DDisk and persistent-buffer pools and defaults to `ddp1`. `--num-groups` defaults to 32; `--target-num-vchunks` defaults to 1 vChunk per group; `--vchunk-size-bytes` defaults to 134217728 (128 MiB); `--hosts-per-dbg` defaults to 5. `TargetNumVChunks` is also the per-group vChunk claim sent to BSC, and the workload address space per group is `TargetNumVChunks * VChunkSizeBytes`. Use the actual pool and geometry supported by your cluster.

If the database has no default tablet channel pools, repeat `--tablet-storage-pool POOL` in channel order. For three channels using the same ordinary tablet-storage pool, pass `--tablet-storage-pool tablet-storage` three times. This is separate from `--pool-name`, which selects DDisk/PB pools. Repeated create reuses an allocation only when its database/domain, effective allocation, and actual ordered bindings match; other reuse conflicts. The preflight checks do not make creation atomic against another controller. Each operation sends at most one create or delete request; after an ambiguous transport failure, inspect the owner index and reported tablet ID before deciding whether to repeat it. A partial failure reports the tablet identity for recovery. An already absent tablet can be deleted again.

`run` builds its `TEvLoadTestRequest` from flags and accepts no `--config`. Supply `--tablet-id ID` for one existing tablet, or repeat `--target TABLET_ID[@NODE_ID]` for multiple existing tablets. Without a target, `run` creates a dedicated tablet using the same allocation flags as `create`, waits for readiness, runs the workload, and deletes that tablet after termination is confirmed. `--pool-name` selects both DDisk and PB pools; `--ddisk-pool-name` and `--pb-pool-name` can override either pool, as on the monitoring page. Omit `@NODE_ID` to use current Hive placement, use `@0` for coordinator-local generation, or specify a nonzero node to override placement. Target tablet IDs must be unique. Each run or trial lasts 10 seconds by default. Duration must be positive and exceed the measurement delay. The CLI uses monitoring-page defaults for duration, warmup, inflight, and max inflight LSNs; other omitted tuning flags use protobuf defaults.

Omitting inflight options runs once with 2048 inflight per tablet. Specify `--inflight N` for another single value, or both `--inflight-from A --inflight-to B` for a sweep. The sweep runs A, 2A, 4A, and so on while the value is at most B; it does not insert B if B is between doubling steps. `--trials` defaults to 1 and must be odd when greater than 1, matching the monitoring page. The CLI preserves the page's multi-tablet targets, read/write ratio, I/O size, DBG prefix, sequential access, LSN cap, checksums, replication toggle, and median trial by write IOPS. The automation path assigns service tags itself.

| Flag | Protobuf field | Default |
| --- | --- | --- |
| `--duration-seconds` | `DurationSeconds` | 10 seconds per run or trial |
| `--delay-before-measurements-seconds` | `DelayBeforeMeasurementsSeconds` | 0 seconds |
| `--num-groups-to-use` | `NumDirectBlockGroupsToUse` | 0 (all groups) |
| `--inflight` or `--inflight-from` / `--inflight-to` | Trial `MaxInFlight` values | 2048 per tablet for a single run |
| `--read-ratio` | `ReadRatio` | 0 reads per 100 writes |
| `--sequential` | `Sequential` | Random addresses |
| `--read-write-size-kib` | `ReadWriteSizeKiB` | 4 KiB |
| `--stop-on-writes-done-count` | `StopOnWritesDoneCount` | 0 (duration only) |
| `--max-inflight-lsns` | `TabletConfig.MaxInflightLsns` | 65536 |
| `--flush-batch-size` / `--erase-batch-size` | Tablet batch sizes | 10000 each |
| `--sync-requests-batch-size` | `TabletConfig.SyncRequestsBatchSize` | 10 |
| `--pbuffer-reply-timeout-us` | `TabletConfig.PBufferReplyTimeoutMicroseconds` | 50000 µs |
| `--disable-replication` | `TabletConfig.DisableReplication` | Replication enabled; reads cannot be used when set |
| `--disable-checksums` | `TabletConfig.EnableChecksums` | Checksums enabled |

`--read-ratio` counts reads per 100 writes, not as a percentage of all operations, and accepts 0–100. `--read-write-size-kib` must be at least 4, a multiple of 4, and fit evenly inside the allocated vChunk size. A nonzero `--stop-on-writes-done-count` is an additional stopping condition; the duration still bounds the run.

Use the usual dstool TLS and credential options and an explicit `grpc://` or `grpcs://` endpoint. Replace the database, endpoint, owner index, and tablet ID in these recipes:

```bash
DSTOOL_ENDPOINT=grpcs://node.example:2135
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like create --database /Root/test --owner-index 1 --pool-name ddp1 --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like list --database /Root/test --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like describe --database /Root/test --owner-index 1 --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like run --database /Root/test --tablet-id 72057594000000001 --duration-seconds 60 --inflight 32 --output-dir ./run-01 --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like results --handle ./run-01 --wait --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like stop --handle ./run-01 --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like run --database /Root/test --duration-seconds 60 --inflight-from 1 --inflight-to 32 --trials 3 --output-dir ./sweep-01 --format jsonl
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like delete --database /Root/test --owner-index 1 --format json
```

`create` waits for readiness. `run` waits by default; `--no-wait` is available only for a single run on an explicit tablet and returns the handle and artifact location. `results` retrieves the current state; `--wait` waits for a terminal result. `stop` addresses one run and waits for confirmed termination. For a trial in a range, use `results` or `stop` with `--database`, `--node-id`, `--incarnation`, and `--request-id` from its checkpoint instead of `--handle`.

The receiving node is the default coordinator; `--node-id` selects another. The client pins the coordinator node and service incarnation before submission. Later requests may enter through another gateway and are forwarded to that coordinator. Missing load services or database context cause an error. Each new run, including every sweep trial, resolves current Hive placement. Omitted target `NodeId` colocates the generator with its tablet; an explicit nonzero value overrides placement, and explicit zero uses the coordinator node. Accepted runs retain their placement snapshot on retry and do not automatically relocate or restart.

Automation waits for the entire requested DBG prefix and acknowledged configuration of its per-DBG actors before generating load. It neither uses the HTTP zero-ready fallback nor silently reduces the requested workload. A `NumDirectBlockGroupsToUse` greater than the allocated count is rejected. Defaults are a 90-second RPC timeout, a 60-second startup budget, and a 2-second polling interval; override with `--rpc-timeout`, `--startup-timeout`, and `--poll-interval`. The startup budget is a single deadline measured from START admission that covers placement lookup, capability checks, DBG readiness, and the configuration acknowledgement; a retried START does not extend it, and it cannot exceed 3600 seconds. One RPC timeout is shared across gateway retries and capped by the remaining overall operation budget; both gRPC and gateway `RpcTimeoutMs` receive that cap. Trial budgets begin before START or recovery GET, result waits before the initial GET, readiness before DESCRIBE, and stops before STOP. The runner's default overall wait is startup budget + workload duration + 210 seconds for drain and bounded transport slack; `--wait-timeout` overrides it. Confirmed completion means client-request drain, not completion of background flush or erase. Check `Run.TerminationConfirmed`: without confirmed drain, the service keeps the run in `STOPPING` with `ExecutionError`, `TerminationConfirmed: false`, and no `FinishedAtMs`, even after a worker watchdog reports failure. Such unresolved runs continue blocking reuse of their tablets on the same coordinator. A late terminal reply is saved but still has a timeout verdict. Cancellation has its own bounded budget; a failed or unconfirmed outcome must not permit the next trial.

### Results, recovery, and sweeps

`--format json` keeps stdout machine-readable; progress and diagnostics go to stderr. Pretty sweep output prints tablet placement once, then `MaxInFlight`, direction, IOPS, p50, p95, p99 (microseconds), measured error count, and error percentage for each trial. Reads appear when configured or measured. JSON and JSONL trial results include `measured_io_errors` by direction, with string counts and totals and a numeric percentage; failed verdicts include `failure_reason`. The percentage is errors divided by successful plus failed measured operations for that direction. Protobuf JSON represents 64-bit identifiers and counters as strings. The CLI result contains `response.Run` and a separate `passed` verdict (pending results have no final verdict). Execution states are `IN_PROGRESS`, `STOPPING`, `SUCCEEDED`, `FAILED`, and `CANCELLED`. `SUCCEEDED` means workload execution completed. Partial measured I/O errors remain visible but pass the CLI verdict; a direction fails that check only when it has errors and zero successful measured operations (100% errors). Execution failure, cancellation, timeout, and missing/lost results still cause a nonzero exit. `--allow-io-errors` relaxes only the 100%-error check.

The typed result includes execution errors, effective configuration with resolved placement, build information, timestamps, measured milliseconds, counts, bytes, rates, latency histograms in microseconds, and per-tablet statistics. Histograms are merged for aggregate latency, not averaged as percentiles. Browser metric fields remain a separate compatibility projection.

`run` defaults to `./nbs-dbg-like-load-results/<client-generated-uuid>/`. An explicit output directory must not already exist unless resuming. The generated `config.json` and atomically written `checkpoint.json` preserve settings, pinned coordinator identity, request IDs, targets, and trial states before submission. Automatic allocation also saves `auto.json` before CREATE, including its owner index and allocation. Each `result-NNNN.json` is saved before the checkpoint marks that trial complete. Artifacts contain no credentials.

```bash
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like run --resume ./run-01 --format json
ydb-dstool -e "$DSTOOL_ENDPOINT" cluster workload nbs-dbg-like run --resume ./sweep-01 --format jsonl
```

Resume uses saved run settings; conflicting new settings are rejected. `results --handle` and `stop --handle` also save terminal results before marking the checkpoint complete. They reconcile an interrupted result/checkpoint write and serve saved terminal results offline, including after coordinator history is lost. Dry runs do not alter artifacts. Nonterminal replies remain pending. Resume skips saved completed trials, retrieves previously submitted work, and starts only never-submitted trials. A fully completed run or sweep can be resumed from saved results without a live coordinator. An interrupted submission is ambiguous: an unknown or expired handle must never trigger replacement work automatically. Accepted bounded work belongs to the service and continues after client disconnection. Completed server results and deduplication records are retained in memory for 24 hours; each coordinator accepts at most 1,024 active/retained records and rejects new runs at capacity. A coordinator restart loses this history and changes its incarnation. Coordinator loss does not prove remote workers stopped; investigate before launching more work against those tablets.

Sweeps preserve all doubling inflight values and trials, changing only `WorkloadConfig.MaxInFlight`. They run sequentially, stop at the first failed verdict, and save `summary.json` selecting the median trial by write IOPS. On timeout or interruption the runner requests scoped cancellation and confirms termination before it can continue; an unconfirmed outcome remains unresolved. An automatically created tablet is deleted only after every submitted run has a saved terminal result with confirmed drain; otherwise its identity remains in `auto.json` for recovery. Existing tablets are never automatically deleted. The global `--dry-run` option validates and displays intended requests without mutations or simulated results.

For agent-assisted operation, see the scoped [ydb-nbs-dbg-like-load skill](https://github.com/ydb-platform/ydb/blob/main/ydb/apps/dstool/.agents/skills/ydb-nbs-dbg-like-load/SKILL.md). The remaining sections describe the compatible HTTP workflow and the shared workload parameters.

## Create a Tablet {#create}

Choose a distinct `owner_idx` for each tablet within the selected Hive. Hive maps this index, together with the load service's fixed owner ID, to the actual tablet ID. Keep both values: deletion uses `owner_idx`, while workloads use the Hive-assigned tablet ID.

Save the following allocation configuration as `nbs-dbg-alloc.txt`. Replace the pool names with pools configured in your cluster. This is a `TAllocConfig` message, without an enclosing `NbsDbgLikeLoad` block.

```proto
DDiskPoolName: "ddp1"
PersistentBufferDDiskPoolName: "ddp1"
NumDirectBlockGroups: 1
TargetNumVChunks: 1
VChunkSizeBytes: 134217728
HostsPerDbg: 5
```

Create the tablet through the page's form, or submit the same request:

```bash
curl --fail-with-body 'http://<node>:8765/actors/load' \
  -H 'Content-Type: application/x-protobuf-text' \
  --data-urlencode 'mode=tablet_create' \
  --data-urlencode 'owner_idx=1' \
  --data-urlencode 'config@nbs-dbg-alloc.txt'
```

The response is HTML containing `TabletId=<id>`. Repeating Create for an initialized tablet returns HTTP 409 with "Tablet was already initialized"; it does not change the existing allocation.

For explicit tablet-channel bindings, pass the `storage_pools` form field as a newline-separated list of pool names, one per channel. For example, add `--data-urlencode 'storage_pools@tablet-storage-pools.txt'`. With no explicit bindings, the helper uses the tenant's storage pools for three channels when available, then falls back to a default Hive binding. The HTTP helper reads this separate form field; putting only `TabletStoragePools` in the allocation text does not select its Hive bindings.

### Allocation Parameters {#allocation-parameters}

Allocation persists across runs. Change it by deleting and recreating the load tablet.

The tablet page pre-fills `NumDirectBlockGroups: 32`. The table below lists protobuf defaults, which apply when a field is omitted from a configuration message.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `DDiskPoolName` | `"ddp1"` | BSC pool for DDisk data. |
| `PersistentBufferDDiskPoolName` | `"ddp1"` | BSC pool for PB placement. |
| `NumDirectBlockGroups` | `1` | Number of logical DBGs to allocate; must be positive. |
| `TargetNumVChunks` | `1` | vChunks per DBG in the workload address space and the requested data-side chunk claim. Use a positive value. |
| `VChunkSizeBytes` | `134217728` | Bytes per vChunk; use the cluster's supported chunk size. Must be positive and a multiple of 4096. |
| `HostsPerDbg` | `5` | Expected DDisk/PB pairs per allocation response, from 3 to 5. The workload uses the first three pairs for replicated writes and flushes. This field validates the response; it does not configure BSC pool geometry. |
| `TabletId` | Assigned by the tablet | The tablet overwrites this field with its own Hive-assigned ID for storage ownership. Omit it from the input. |
| `TabletStoragePools` | Empty | Stored configuration field. For the HTTP Create workflow, supply bindings through the separate `storage_pools` form field described above. |

## Check the Allocation {#summary}

Refresh the tablet page or fetch its listing:

```bash
curl --fail-with-body 'http://<node>:8765/actors/load?mode=tablet_list'
```

The HTML listing combines Hive information with each tablet's `TEvNbsLoadTabletGetSummary` response. It shows tablet placement, pools, and DBG counts. This also works after creating multiple tablets; there is no separate multi-tablet allocation message.

The actor protocol's summary contains allocated DBG count, vChunk geometry, and `NumReadyDirectBlockGroups`: the longest consecutive prefix of DBGs with at least three connected PB peers. Create completes before these connections finish. A positive ready count limits a run's address space. For compatibility, the current load proxy falls back to the allocated count when the ready count is zero, so starting immediately after Create can still produce "peers not ready" errors. PB readiness also does not guarantee that all DDisk connections required by reads and flushes are ready.

## Run a Workload {#run}

Replace the example tablet ID below with the ID returned by Create, and save this complete load-service request as `nbs-dbg-run.txt`:

```proto
NbsDbgLikeLoad {
  NbsDbgLikeTabletId: 72057594000000001
  WorkloadConfig {
    DurationSeconds: 60
    DelayBeforeMeasurementsSeconds: 15
    MaxInFlight: 32
    ReadRatio: 0
    Sequential: false
    ReadWriteSizeKiB: 4
    TabletConfig {
      MaxInflightLsns: 4096
      FlushBatchSize: 10000
      EraseBatchSize: 10000
      SyncRequestsBatchSize: 10
      PBufferReplyTimeoutMicroseconds: 50000
      EnableChecksums: true
    }
  }
}
```

Paste it into the general load-actor configuration form and start it on the current node, or submit it directly:

```bash
curl --fail-with-body 'http://<node>:8765/actors/load' \
  -H 'Content-Type: application/x-protobuf-text' \
  --data-urlencode 'mode=start' \
  --data-urlencode 'config@nbs-dbg-run.txt'
```

Check that the JSON reply has `"status": "ok"` and retain the assigned `tag` and `uuid`. This endpoint returns HTTP 200 even for parse/start errors, so `curl --fail-with-body` alone does not establish success. Worker initialization errors can still appear later in the final result.

Poll completed results using the returned UUID:

```bash
curl --fail-with-body \
  -H 'Accept: application/json' \
  'http://<node>:8765/actors/load?mode=results&uuid=<uuid>'
```

An empty result array means that no completed result for that UUID is available yet. Use JSON while the workload is active: the NBS DBG like load proxies do not implement live HTML status requests. After completion, the same URL without the JSON header displays the final HTML report. Start another run after completion to reuse the allocation with different workload settings.

### Workload Parameters {#workload-parameters}

These fields belong to `NbsDbgLikeLoad.WorkloadConfig`.

The tablet page pre-fills `MaxInFlight: 2048` and `MaxInflightLsns: 65536` for a run. The `tablet_run` HTTP endpoint uses the same values when these parameters are omitted. The tables below list protobuf defaults for configuration messages.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `DurationSeconds` | `0` | Time from workload start to stopping request generation, including warm-up. Set this or `StopOnWritesDoneCount` to a positive value. |
| `DelayBeforeMeasurementsSeconds` | `15` | Warm-up interval excluded from measured results. Must be less than a positive `DurationSeconds`. |
| `MaxInFlight` | `32` | Combined concurrent write/read limit per target tablet. Use a positive value. Random workloads split large limits among workers; sequential workloads use one worker. |
| `ReadRatio` | `0` | Reads issued per 100 writes, based on issued request counts. `100` means approximately one read per write, or half of all requests. Values above `100` are allowed; `200` means approximately two reads per write. It does not select a read-only workload. |
| `Sequential` | `false` | Sequential traversal of the flat address space when true; uniform random selection otherwise. |
| `ReadWriteSizeKiB` | `4` | Fixed I/O size, at least 4 KiB and a multiple of 4 KiB. It must fit in and evenly divide a vChunk. |
| `NumDirectBlockGroupsToUse` | `0` | Use a prefix of the available DBGs. Zero or a value exceeding the available count uses all available DBGs. |
| `StopOnWritesDoneCount` | `0` | Successful-write completion target, split among workers. Zero disables the count limit. Also set a duration: failed writes can exhaust the current implementation's issue limit before its successful-write target is reached. |
| `TabletConfig` | See below | Configuration installed on the tablet's per-DBG actors before generating load. |

Warm-up and drain completions remain in lifetime counters but are excluded from measured throughput and latency. Random reads can access unwritten addresses, and the generator does not verify returned data against an expected block image. Use the integration tests for correctness checks.

When DBGs of one load tablet share a data DDisk, their DBG-local vChunk indexes
can refer to the same underlying blocks. Do not assume that distinct logical
DBG address ranges isolate stored data; see the [workload implementation notes](https://github.com/ydb-platform/ydb/blob/main/ydb/core/load_test/rfc/nbs_dbg_like/workload.md).

### Tablet Parameters {#tablet-parameters}

These fields belong to `WorkloadConfig.TabletConfig`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `MaxInflightLsns` | `4096` | Budget for tracked log sequence numbers (LSNs). Each DBG actor independently enforces `max(1, budget / allocated_DBG_count)`. The divisor includes inactive DBGs. Zero rejects writes; this is not a single shared global counter. |
| `FlushBatchSize` | `10000` | Maximum LSNs scheduled per destination in one flush batch; normalized to at least one. |
| `EraseBatchSize` | `10000` | Maximum LSNs scheduled per destination in one erase batch; normalized to at least one. |
| `SyncRequestsBatchSize` | `10` | Per-DBG threshold of ready LSNs before scheduling flush or erase. Set to `1` to process tails promptly. The gate remains enabled after load generation stops. |
| `PBufferReplyTimeoutMicroseconds` | `50000` | Timeout passed to the PB plural-write coordinator. |
| `DisableReplication` | `false` | Write only to the coordinator PB, acknowledge its confirmation, and erase without copying to DDisk. Requires `ReadRatio: 0`. |
| `EnableChecksums` | `true` | Calculate one checksum per 4 KiB payload block in the load worker and forward the checksums through the load tablet to PersistentBuffer. Set this to the DDisk/PersistentBuffer checksum mode. |

The proxy sets `TabletConfig.IoSizeBytes` from `ReadWriteSizeKiB` and `TabletConfig.NumDirectBlockGroupsToUse` from the selected DBG count. Configure those through the workload fields, not the internal tablet fields.

Keep the per-DBG LSN budget comfortably above the sync threshold: LSNs remain tracked through write, flush, and erase, and both queues need enough work to pass their gates.

## Run Against Multiple Tablets {#multiple-tablets}

Create each tablet independently using distinct owner indices, then obtain their tablet IDs and current node IDs from the listing. Replace the example IDs in this request:

```proto
NbsDbgLikeLoad {
  Targets { TabletId: 72057594000000001 NodeId: 1 }
  Targets { TabletId: 72057594000000002 NodeId: 2 }
  WorkloadConfig {
    DurationSeconds: 60
    DelayBeforeMeasurementsSeconds: 15
    MaxInFlight: 64
    ReadRatio: 100
    ReadWriteSizeKiB: 4
    TabletConfig { SyncRequestsBatchSize: 1 }
  }
}
```

Submit it using the same `mode=start` workflow. A nonempty `Targets` list selects coordinator mode and takes precedence over `NbsDbgLikeTabletId`. Each target receives the whole workload configuration: here each tablet gets a limit of 64 concurrent operations, for 128 in total. `StopOnWritesDoneCount`, when set, also applies separately to each target.

`NodeId` selects the node on which to start that tablet's load proxy. Zero or an omitted value uses the coordinator's node; the actor does not look up or continuously track placement in Hive. Use the current hosting node to keep load generation colocated. The target load service must be available. Include each tablet only once and start this coordinator on one node.

## Results and Cleanup {#results}

Results include write/read requests per second, p50/p95/p99 completion latencies in microseconds, configured `max_in_flight`, combined successful request count (`txs`), throughput (`rps`), and errors per second (`errors`). Multi-tablet results also include a `tablets` array. The coordinator merges latency histograms; it does not average per-tablet percentiles.

Write completion measures PB confirmation, before the background copy to DDisk and PB erase. Read results aggregate both PB and DDisk routes. The last HTML report contains counts, byte totals, measurement duration, and latency percentiles. Tablet and per-DBG counters expose write, flush, erase, and read activity through the `load_actor` counter service.

A duration limit, count target, or [{#T}](load-actors-stop.md) stops new requests and allows up to 30 seconds for outstanding client replies. This drain does not wait for all background flushes or erases. In particular, a sync threshold greater than one can leave a short tail queued until more work arrives.

After the run finishes, delete each tablet with the owner index used to create it:

```bash
curl --fail-with-body 'http://<node>:8765/actors/load' \
  -H 'Content-Type: application/x-protobuf-text' \
  --data-urlencode 'mode=tablet_delete' \
  --data-urlencode 'owner_idx=1'
```

The helper requests DBG deallocation from the tablet and then deletion from Hive. This is allocation cleanup, not a data sanitization operation. If allocation or deletion fails, inspect the BSC and tablet logs before retrying; the current implementation has incomplete recovery for interrupted allocation and can clear local state after a BSC deallocation error.

## Source and Implementation Notes {#source}

- [Current configuration and actor protocol](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/load_test.proto).
- [Load-actor implementation guide](https://github.com/ydb-platform/ydb/blob/main/ydb/core/load_test/rfc/nbs_dbg_like/README.md), including workload mechanics, lifecycle limitations, and test targets.
- [{#T}](distributed-storage/direct-block-groups.md).
- [{#T}](load-actors-overview.md).
