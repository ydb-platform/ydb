# NBS 2.0 — Functional Test Plan

Status: draft. Owner: `g:cloud-nbs`.

This directory is both the pytest suite and the functional-level plan. It
sits in the L2/L3 band of the parent high-level plan in Arcadia
(`cloud/nbs_internal/blockstore/tests/nbs2/test_plan/`): a local 9-node
KiKiMR cluster, `dstool` as the client, in-process `ydbd`. Lab-stand
suites (L4+) stay in that repo's `suites.md`.

| File | Contents |
| --- | --- |
| `README.md` | Scope, how to run, fault toolbox, config axes, harness prerequisites |
| `F1_user_scenarios/F1_user_scenarios.md` | Disk lifecycle, IO correctness, vhost, load actor |
| `F2_fault_injection/F2_fault_injection.md` | Tablet kill, node stop, PDisk fail, slowdown, pipeline-phase faults |
| `F3_node_down_and_data_copy/F3_node_down_and_data_copy.md` | Temporary vs permanent host loss, handoff promotion, `TDDiskDataCopier` |
| `F4_throttling_and_limits/F4_throttling_and_limits.md` | Copy-range leaky bucket, flush pacing, DDisk rejection |
| `F5_observability/F5_observability_and_oracles.md` | Mon pages, counters, wait-and-assert |
| `matrix.md` | Per-case index: priority, primitive, config, coverage, blocking hook |

---

## 1. Scope

**In scope.** Behaviour that can be driven from pytest against a local cluster
started by `NbsTestBase` in [`common.py`](common.py):

* disk create / delete / IO via `dstool nbs partition`
* vhost-user-blk on `/tmp/<disk_id>.sock`
* NBS2Load actor via HTTP `/actors/load`
* host loss, tablet kill, PDisk `BROKEN`, `SIGSTOP` slowdown
* handoff promotion and sequential DDisk copy
* copy-range bandwidth, flush batching, PBuffer/DDisk rejection

**Out of scope.** Nightly fio/pgbench on `load_cluster`, rack-level blast
radius, checksum/scrub (feature not implemented), CMS maintenance permissions,
checkpoints, adaptive backpressure (RFC 009). Those remain in the parent
plan's `suites.md`.

---

## 2. How to run

From `ydb_main`, no `-j`, no force rebuild:

```bash
./ya make --build relwithdebinfo -tA ydb/tests/functional/nbs
./ya make --build relwithdebinfo -tA ydb/tests/functional/nbs -F *test-filter*
```

`ya.make` today: `PY3TEST()`, `SIZE(MEDIUM)`, `REQUIREMENTS(cpu:4)` +
`REQUIREMENTS(ram:16)`, `DEPENDS(ydb/apps/dstool)`,
`PEERDIR(ydb/tests/library)`. No `FORK_SUBTESTS`, no `SPLIT_FACTOR`.

As the suite grows, add `FORK_SUBTESTS()` and `SPLIT_FACTOR` so cases do not
share a process after a killed node. F1–F5 use `SIZE(LARGE)` with
`TIMEOUT(600)` as the chunk budget. Fail-fast is the per-call 60s
dstool/ydbd/vhost timeout plus `PYTEST_TIMEOUT=60` (test function only);
a hung case fails in a minute, not the whole chunk.

A host-loss case must drive IO while waiting for Offline: `TOracle::Think`
demotes a host from consecutive request failures, and `OnDDiskDisconnected`
is a no-op. Use `wait_host_offline`.

---

## 3. Environment today

`NbsTestBase.setup` is an **autouse, per-test-method** fixture:

```python
KiKiMR(KikimrConfigGenerator(
    erasure=Erasure.MIRROR_3_DC,   # 9 static nodes
    enable_nbs=True,
    nbs_database_name="/Root/NBS",
))
cluster.start()
start_nbs("/Root/NBS")   # create_database + register_and_start_slots(count=1)
create_ddisk_pool()      # DefineDDiskPool ddp1, 1x5 geometry, NumDDiskGroups: 10
```

NBS 2.0 runs inside `ydbd`. One dynamic slot hosts the partition tablet.
DDisk and PBuffer actors live on the 9 static nodes. The pool geometry is
**5 fail domains**, so a DBG uses 5 of the 9 nodes and 4 spares remain for
`AddHost`.

Helpers to reuse, not reinvent: `create_disk` / `delete_disk` /
`delete_disk_expect_failure`, `write` / `read` via
`get_load_actor_adapter_actor_id`, `VhostUserBlkClient`,
`run_nbs_load_test` / `verify_load_test_results`,
`fetch_partition_dbg_page`, `fetch_pbuffer_page`, `parse_dbg_indexes`,
`parse_pbuffer_service_ids`, `wait_until`.

Twelve tests exist (`TestNbs` 8, `TestNbsLoadActor` 4). All are happy-path.

### Fixture scoping

* Keep **per-test** cluster for F2/F3/F4: a stopped node or a killed tablet
  dirties the cluster.
* Move read-only F1 cases (create/delete/IO without faults) to `setup_class`
  or a module-scoped cluster once the suite is large enough that 9-node
  startup dominates. Sibling suites (`functional/restarts`, `functional/hive`)
  already do this.
* Do not share a cluster across a test that called `node.stop()` and a later
  test that assumes all 9 nodes are up.

---

## 4. Fault-injection toolbox

All of these are available today at pytest level. Use `node.stop()`, not
`node.kill()`: `KiKiMRNode.kill()` restarts immediately.

| Primitive | Call | What it hits |
| --- | --- | --- |
| Storage host down | `cluster.nodes[i].stop()` / `.start()` | DDisk + PBuffer on that static node |
| Frozen / slow host | `cluster.nodes[i].send_signal(SIGSTOP)` / `SIGCONT` | Same process, no disconnect; only coarse slowdown |
| Partition tablet kill | `cluster.client.tablet_kill(tablet_id)` | `tabletId` already returned by `create_disk` |
| Slot restart | slot `stop()` / `start()`, or `unregister_and_stop_slots` | Partition host; tenant `/Root/NBS` |
| Device fail | `dstool pdisk stop` / `set --status BROKEN`, or `cluster.client.update_drive_status(..., EDriveStatus.BROKEN)` | One PDisk / DDisk |
| Device recover | `dstool pdisk restart` or `update_drive_status(..., ACTIVE)` | Same |

`ydb/tests/library/nemesis/` and `ydb/tests/tools/nemesis/` are SSH /
external-cluster oriented. `KillBlocktoreVolume` / `KillBlocktorePartition`
target NBS 1.0 tablet types, not `BlockStorePartitionDirect`. Cited as
reference only.

Where faults attach:

```
Guest (dstool / vhost / NBS2Load)
        |
        v
  dynamic slot: TPartitionActor + TFastPathService + DBGs
        |                    ^
        |                    | tablet_kill / slot stop
        v
  9 static nodes: DDisk + PBuffer
                  ^
                  | node.stop / SIGSTOP / pdisk BROKEN
```

---

## 5. Config axes

Single-axis deviations on top of the baseline (IndirectWrite, 4 KiB blocks,
4 GiB disk, in-memory PDisks, default oracle thresholds):

| Knob | Why vary it |
| --- | --- |
| `WriteMode` (`IndirectWrite` / `DirectWrite`) | Both write executors exist; functional coverage today is whatever the default is |
| `use_direct_session_transport` | Direct vs actor-path transport |
| Block size 4 / 8 / 16 / 32 / 64 / 128 KiB | Geometry; max volume is 2³¹ blocks (8–256 TiB) |
| Disk size: 4 GiB (default) vs 500 GiB | Region / many-vchunk routing |
| `PBufferCleanupLsnStep` | 0 disables barrier cleanup |
| `CopyRangeBandwidthMbs` | Copy pacing (F4) |
| `SyncRequestsBatchSize` | Flush/erase batching (F4) |
| `oracle_config.*` duration / error thresholds | TemporaryOffline vs Offline split (F3) |

---

## 6. Harness prerequisites

Most of F3 and F4 cannot be written until these two exist. They are not
blocked on product code.

### 6.1 Per-test `nbs_config` override

`enable_nbs=True` in `ydb/tests/library/harness/kikimr_config.py` writes a
fixed `nbs_config`. Tests must be able to set, at least:

* `oracle_config.max_duration_before_going_temporary_offline`
* `oracle_config.max_duration_before_going_offline`
* `oracle_config.min_errors_count_before_going_offline`
* `copy_range_bandwidth_mbs`
* `sync_requests_batch_size`
* `write_mode`

Without this, TemporaryOffline is **unobservable**. Both duration defaults
are 10 s, so `Think` can move a host `Online → TemporaryOffline → Offline`
in the same tick.

### 6.2 `use_in_memory_pdisks=False` for return-with-data cases

Default in-memory PDisks lose all DDisk content when the node process
stops. "Temporarily down, comes back with its data" cannot be expressed.
`functional/restarts` already passes `use_in_memory_pdisks=False`. F3
return-and-catch-up cases need the same.

---

## 7. Oracle strategy

`write` / `read` today compare a literal string trimmed to the written
length. That is not enough once faults interleave with IO.

1. **Self-describing payload.** Every block carries LBA, a sequence number
   and a checksum. A wrong read is attributable (stale / torn / misplaced).
2. **Invariant checkers on mon pages.** Host state, watermark, PBuffer
   occupancy, flush pending — see F5. Prefer `wait_until` with a deadline
   over `time.sleep`.
3. **Load-actor counters.** `RequestsFailed == 0`, `Iops > 0` — liveness
   only; not a data oracle.

A case that only checks (3) is a smoke test and does not count as
correctness coverage.

---

## 8. Blocked cases

Cases that need a capability the harness does not have are still listed,
marked **blocked**, with the required hook named. They live in F2 and F4.
Do not drop them: they are the specification for the next harness change.

| Hook | Unblocks |
| --- | --- |
| DDisk / PBuffer per-request error injection (test hook or ICB) | F2.15 |
| Per-request latency injection | F2.16 |
| Precise-point kill (torn write / power loss) | F2.17 |
| Local netem / iptables (unavailable in sandbox) | F2.18 |
| On-disk block corruption tool + `use_in_memory_pdisks=False` | F2.19 (checksums also unimplemented) |
| Copy-rate / leaky-bucket delay counter | F4.13 |

---

## 9. How to use the matrix

[matrix.md](matrix.md) is the file to update when a test lands. A release
review should be able to read it and see which cases are implemented, which
are ready to implement, and which wait on a harness hook.
