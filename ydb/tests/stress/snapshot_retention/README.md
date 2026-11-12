# DataShard snapshot retention

The reusable workload writes two row tables atomically. A private Table Service
snapshot transaction reads only the anchor table, then reads the target after
its ordinary MVCC window. The anchor's generation determines the exact target
rows, including values, deleted keys and newly inserted keys. Point, range and
secondary-index reads use the same explicit transaction without SDK retries.

The local stress runner and functional tests enable both `EnableSnapshotsLocking`
and `EnableDataShardSnapshotLocking`. They use a five-second `KeepSnapshotTimeout`
and shorter registry intervals. KQP sessions stay on node 1; target tablets are
moved to node 2. The test checks the session owner and actual tablet placement.

`SnapshotRetentionMonitor` reads the persisted DataShard `Sys` table through the
existing `/tablets/db` inspector. After a subsequent acknowledged generation
write, it captures that write's MVCC edge, then waits until the target's actual
`LowWatermark` passes the captured edge. It does not infer expiration from a
fixed sleep or from the client's wall clock. Schema and HTML changes fail the
monitor explicitly.

Functional tests cover first point/range/index reads below the watermark,
independent held snapshots and release, compaction and tablet restart,
split/merge, CopyTable, and disabled/default feature flags. Registry counts
check eventual handle release; they do not prove physical removal of every
historical row. Exact removed-version ranges remain covered by C++ tests.

Stress also covers two controlled remote tablet restarts and a split followed
by merge while the writer continues running. Checked reads are serialized with
fault injection; they keep their original transaction and never retry it. Only
the fault-mode writer may retry an uncertain acknowledgement of its idempotent
generation replacement.

The split/merge stress profile disables `EnableDataShardUncommittedWriteSeqNum`:
the default protocol exposed a separate writer failure, `Ancestor shard ... not
found in lock`, during rerouting after a split. The normal and restart profiles
keep that protocol enabled. No `BadRequest` is suppressed or retried, and the
snapshot-retention flags and checked read policy stay the same in all profiles.

Compatibility tests exercise default flags through mixed clusters, restart to
another binary, and rolling upgrade/downgrade. Mixed clusters additionally split
and merge a table and verify its exact current data. The enabled case only runs when
every binary is built from current sources. Older binaries never receive the
new flag. A stopped KQP owner invalidates the checked transaction: the tests
start a new transaction after a full restart rather than claiming the old
in-memory handle survived it.

Run the new tests (tests include their build):

```bash
./ya make --build relwithdebinfo -tA \
    ydb/tests/functional/snapshot_retention \
    ydb/tests/stress/snapshot_retention/tests \
    ydb/tests/compatibility/workloads \
    -DYDB_COMPAT_TARGET_REF=current \
    -F '*SnapshotRetention*' 2>&1 | tail -n 100
```

The standalone utility can use an existing cluster:

```bash
snapshot_retention --endpoint localhost:2135 --database /Root \
    --path retention_check --duration 900 --hold-seconds 360
```

Enable both flags on that cluster and choose `--hold-seconds` beyond its MVCC
and registry safety windows. The external utility has no local cluster
administration object and therefore uses the requested hold duration instead
of the persisted-watermark check used by the local tests. Every reader must
complete an aged read; reader failures fail the workload.
