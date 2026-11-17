# DataShard snapshot retention

The shared workload is in `ydb/tests/library/snapshot_retention/workload`.
Functional tests, compatibility tests and the standalone CLI use the same implementation.

The reusable workload writes two row tables atomically. A private Table Service
snapshot transaction reads only the anchor table, then reads the target after
its ordinary MVCC window. The anchor's generation determines the exact target
rows, including values, deleted keys and newly inserted keys. Point, range and
secondary-index reads use the same explicit transaction without SDK retries.

The functional suites, with and without concurrent load, enable both
`EnableSnapshotsLocking` and `EnableDataShardSnapshotLocking`. They use a
five-second `KeepSnapshotTimeout` and shorter registry intervals. The original
three-node fixtures keep KQP
sessions on node 1 and move target tablets to node 2. The tests check the session
owner and actual tablet placement.

Both functional suites use `retention_cluster_config` and `pinned_driver` from
`ydb.tests.library.snapshot_retention`. Functional fixtures override this common
configuration for isolation modes, topology, and registry limits. Their
additional compaction settings remain explicit overrides. `HeldSnapshot.owner_node_id`
provides the original session owner's runtime node ID, including after close.

`SnapshotRetentionMonitor` reads the persisted DataShard `Sys` table through the
existing `/tablets/db` inspector. After a subsequent acknowledged generation
write, it captures that write's MVCC edge, then waits until the target's actual
`LowWatermark` passes the captured edge. It does not infer expiration from a
fixed sleep or from the client's wall clock. Schema and HTML changes fail the
monitor explicitly.

Callers capture an explicit bound with `snapshot_bound` and pass it to
`wait_low_watermark`, sharing one bound across the table and index. A later
cleanup phase captures a new bound after a fault or release; the monitor keeps
no per-transaction bound cache.

Each monitor reuses one `QuerySessionPool` for partition polling, including
concurrent readers. Test cleanup and compatibility context managers close the
pool before its driver stops. Tablet lists are still queried on every poll.
Hive placement uses the shared `SwaggerClient` with a ten-second HTTP timeout;
polling retries transient errors from both HTTP clients.

Functional tests cover first point/range/index reads below the watermark,
independent held snapshots and release, compaction and tablet restart,
split/merge, CopyTable, rejection of held snapshots after TRUNCATE, and
disabled/default feature flags. TRUNCATE cases check range, point, and index
reads independently: a registered old snapshot must fail even when fresh reads
see an empty table. Registry counts check eventual handle release; they do not
prove physical removal of every historical row. Exact removed-version ranges
remain covered by C++ tests.

The first range/point/index reads also run in a one-node fixture, with the KQP
owner, target table and index on the same process. These cases require a promoted
local snapshot in a ready registry and empty remote peer/snapshot sets. They
check that the local holding disappears after release.

Serializable and SnapshotRW cases also write and commit after the first target
read below LowWatermark. They check successful commits without a conflict,
Serializable read conflicts, and SnapshotRW conflicts on the same written key.
Unrelated marker keys advance cleanup without changing the anchor's read key.
Fresh reads verify committed data and absence of partial writes after ABORTED.

Isolated overflow cases use `MaxRemoteSnapshots` limits of one and three, with
three and five distinct held snapshots respectively. They check the explicit
oldest points and the inclusive `SnapshotBorder` protecting newer versions,
wait for base/index cleanup to reach that border, compact, and read all held
generations. Partial releases must advance/remove the border while preserving
survivors. The three-point limit also checks promotion of a range-protected
version into the explicit set after releasing a middle point. The final release
must allow cleanup past a fresh acknowledged write.

Node-process fault cases use one static storage node and three tenant compute
nodes. They cover a DataShard node restart, abrupt loss of the KQP owner, and
expiration of an unavailable peer's collection time. Their unavailable-node
snapshot timeout is 30 seconds. The expiration checks leave the failed process
down, observe its removal from the surviving registries, and require new cleanup
progress on both the main table and index before compaction. The restart case
checks real registry reconstruction and retained reads; deterministic coverage
of the exact empty-registry startup window remains in C++.

The functional suite `snapshot_retention_with_load` keeps four concurrent readers
in its normal profile for the configured duration. Fault profiles are explicit
sequences: two remote tablet restarts, or a split followed by merge, with a
background writer running throughout. Two groups of three snapshots are held
before the first fault. Each group covers
point, range and index reads. The second group first accesses target after the
second restart or merge; the first group is checked after both faults. Actual
watermarks are checked before the first fault and again with a refreshed write
bound before each read on the recovered tablets.

Checked reads keep their original transaction and never retry it. Both groups
exercise rollback, commit and session deletion before checking registry release.
The shared writer helper propagates failures and stops and joins its thread
before final fresh reads. Only the fault-mode writer may retry an uncertain
acknowledgement of its idempotent generation replacement.

The split/merge load profile disables `EnableDataShardUncommittedWriteSeqNum`:
the default protocol exposed a separate writer failure, `Ancestor shard ... not
found in lock`, during rerouting after a split. The normal and restart profiles
keep that protocol enabled. No `BadRequest` is suppressed or retried, and the
snapshot-retention flags and checked read policy stay the same in all profiles.

Compatibility tests exercise default flags through mixed clusters, restart to
another binary, and rolling upgrade/downgrade. Mixed clusters additionally split
and merge a table and verify its exact current data. These scenarios leave
`EnableDataShardSnapshotLocking` unspecified. A stopped KQP owner invalidates
the checked transaction: the tests start a new transaction after a full restart
rather than claiming the old in-memory handle survived it.

Run the new tests (tests include their build):

```bash
./ya make --build relwithdebinfo -tA \
    ydb/tests/functional/snapshot_retention \
    ydb/tests/functional/snapshot_retention_with_load \
    ydb/tests/compatibility/workloads \
    -DYDB_COMPAT_TARGET_REF=current \
    -F '*SnapshotRetention*' 2>&1 | tail -n 100
```

Build the standalone utility to use it against an existing cluster:

```bash
./ya make --build relwithdebinfo \
    ydb/tests/functional/snapshot_retention_with_load/runner
```

```bash
ydb/tests/functional/snapshot_retention_with_load/runner/snapshot_retention \
    --endpoint localhost:2135 --database /Root \
    --path retention_check --duration 900 --hold-seconds 360
```

Enable both flags on that cluster and choose `--hold-seconds` beyond its MVCC
and registry safety windows. The external utility has no local cluster
administration object and therefore uses the requested hold duration instead
of the persisted-watermark check used by the local tests. Every reader must
complete an aged read; reader failures fail the workload.
