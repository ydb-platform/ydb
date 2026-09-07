# NbsDbgLikeLoad

`NbsDbgLikeLoad` measures a simplified Network Block Store (NBS) write/read path through [Direct Block Groups (DBGs)](distributed-storage/direct-block-groups.md). A persistent load tablet owns the DBGs and sends requests to [PersistentBuffer (PB)](distributed-storage/persistent-buffer.md) and [DDisk](distributed-storage/ddisk.md). Separate load actors generate requests and measure their completion latency.

Use this actor to compare PB replication, background copying to DDisk, batching, and concurrent I/O. For the DSProxy/VDisk blob-storage path, use [{#T}](load-actors-storage.md).

## Prerequisites {#prerequisites}

Use a test cluster with DDisk and PB pools configured in the BlobStorage Controller (BSC). The allocation must return the number of DDisk/PB pairs expected by `HostsPerDbg`. The load tablet also needs ordinary tablet-channel storage and a working Hive; DDisk pools do not provide that storage.

Open `http://<node>:8765/actors/load?mode=tablet` on a node in the intended database. This page creates tablets, lists their Hive IDs and placement, and starts runs. The monitoring port may differ in your cluster.

{% note warning %}

Run one workload at a time per load tablet, and finish it before deleting the tablet. The tablet stays in its `Ready` phase during a run and does not enforce exclusive access. A later run reconfigures the same per-DBG actors.

{% endnote %}

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

An empty result array means that no completed result for that UUID is available yet. Use JSON while the workload is active: the NBS load proxies do not implement live HTML status requests. After completion, the same URL without the JSON header displays the final HTML report. Start another run after completion to reuse the allocation with different workload settings.

### Workload Parameters {#workload-parameters}

These fields belong to `NbsDbgLikeLoad.WorkloadConfig`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `DurationSeconds` | `0` | Time from workload start to stopping request generation, including warm-up. Set this or `StopOnWritesDoneCount` to a positive value. |
| `DelayBeforeMeasurementsSeconds` | `15` | Warm-up interval excluded from measured results. Must be less than a positive `DurationSeconds`. |
| `MaxInFlight` | `32` | Combined concurrent write/read limit per target tablet. Use a positive value. Random workloads split large limits among workers; sequential workloads use one worker. |
| `ReadRatio` | `0` | Reads issued per 100 writes, based on issued request counts. `100` means approximately one read per write, or half of all requests. It does not select a read-only workload. |
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
