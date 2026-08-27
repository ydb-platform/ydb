# F2 — Fault injection and slowdown

Priority **P0** (data loss / fencing) and **P1** (availability). Existing
coverage: none. Every case in this file is new.

Primitives are listed in [README.md](../README.md) §4. Use `node.stop()`, not
`node.kill()`. Drive a verifying load (self-describing payload or at least
a seed-and-check around the fault) for every case that claims "no data
loss". IO that is expected to succeed is **write then immediate
read-after-write** (`write_and_verify`), not write-only. After the fault
is lifted, re-read the seed and do another write-and-verify.

Pick storage nodes from the disk's DBG, not at random. The pool geometry
is 5 fail domains on a 9-node cluster; stopping a spare that is not in
the DBG is a no-op for that disk. Discover membership from the partition
DBG mon page (`fetch_partition_dbg_page` + host table).

---

## Tablet and slot

| # | Case | Fault | Assertion |
| --- | --- | --- | --- |
| F2.1 | Partition tablet kill under load | `cluster.client.tablet_kill(tablet_id)` while vhost write+read-after-write is in flight | new generation comes up; previously acked writes readable; in-flight writes either acked and durable or failed retriably (`E_REJECTED` / `E_TIMEOUT`); after restore, a fresh write-and-verify succeeds; PBuffer restore + flush resume (see F3/S4); no mixed replicas |
| F2.2 | Dynamic slot stop + start | stop the `/Root/NBS` slot, wait, start it | same as F2.1; vhost socket `/tmp/<disk_id>.sock` returns (ties to F1.15); seed re-read plus a new write-and-verify |

`tablet_id` is already returned by `create_disk`. There is no
`BLOCKSTORE_PARTITION_DIRECT` value in the Python `TabletTypes` enum;
pass the numeric id.

---

## Static node loss

| # | Case | Fault | Assertion |
| --- | --- | --- | --- |
| F2.3 | One DBG host down | `nodes[i].stop()` on a host that is in the disk's DBG | write-and-verify still succeeds (quorum of 3); reads served from a remaining valid replica; acked seed still matches |
| F2.4 | Two DBG hosts down | stop two hosts in every role combination that the test can identify (primary/primary, primary/handoff, handoff/handoff) | write-and-verify still succeeds; seed still matches |
| F2.5 | Three DBG hosts down | stop three **PBuffer** nodes of the DBG (not DDisk nodes: they can be different machines in a 9-node / 5-domain pool) | writes fail cleanly with a retriable status; no `SUCCESS` for a write that did not reach quorum; a read of the seed either fails or returns the acked bytes (no silent corruption); after restart, seed matches and a new write-and-verify succeeds |

F2.4/F2.5 need the DBG host-role table on the mon page (F5.1). If roles
are not yet parseable, start with "any two / any three of the five" and
tighten later.

---

## Device

| # | Case | Fault | Assertion |
| --- | --- | --- | --- |
| F2.6 | PDisk `BROKEN` | `dstool pdisk set --status BROKEN` (or `update_drive_status`) on a PDisk backing a DBG DDisk | `OnDDiskBroken` path: host goes `Broken` / `Offline` and stays there (F3.5); write-and-verify continues on the remaining quorum; seed still matches |
| F2.7 | PDisk restart after `stop` | `dstool pdisk stop` then `restart` (or ACTIVE) | sessions reconnect; seed re-read matches; a new write-and-verify succeeds |

In-memory PDisks make F2.7 "return with data" meaningless — the content
is gone when the process dies. For a process-level stop use F2.3. For a
PDisk-only bounce, `dstool pdisk stop/restart` does not kill `ydbd`, so
it is valid on in-memory PDisks.

---

## Slowdown

| # | Case | Fault | Assertion |
| --- | --- | --- | --- |
| F2.8 | Frozen node | `send_signal(SIGSTOP)` on one DBG host for longer than `WriteHedgingDelay` (default 1 ms) and `PBufferReplyTimeoutMicroseconds` (default 50 ms), then `SIGCONT` | write-and-verify still completes via hedge / remaining replicas; host may enter `Sufferer`; after `SIGCONT` seed and the in-fault writes still match. This is the only slowdown primitive available |

`SIGSTOP` is coarse: the whole `ydbd` on that node freezes, including
unrelated tablets. Acceptable for functional tests; do not treat it as a
model of device latency.

---

## Fault during a pipeline phase

Seed enough writes that flush / erase / restore / AddHost are in flight,
then inject. Observe phase via F5 (`Pending` flush/erase, `TAddHostInProgress`
on local-db mon).

| # | Case | Fault | Assertion |
| --- | --- | --- | --- |
| F2.9 | Kill tablet mid-flush | `tablet_kill` while `TVChunkCounters.Flush.Pending > 0` | flush is idempotent; no LSN forgotten; no rewrite of newer data on DDisk; seed re-read plus a new write-and-verify |
| F2.10 | Kill tablet mid-erase | same, `Erase.Pending > 0` | LSN retried, not dropped; PBuffer occupancy eventually drains (F4 / S6); seed re-read plus a new write-and-verify |
| F2.11 | Kill tablet mid-restore | `tablet_kill` during the first seconds after a previous kill, while restore is listing PBuffers | second recovery converges; dirty map matches the reference; seed re-read plus a new write-and-verify |
| F2.12 | Kill tablet mid-`AddHost` | trigger Offline so `QueryAddHost` runs (F3.4), then `tablet_kill` | `TAddHostInProgress` is persisted and replayed (`ShouldReplayInFlightAddHostAfterRestart` exists at unit level; this is the functional counterpart) |

---

## Repeated and combined

| # | Case | Fault | Assertion |
| --- | --- | --- | --- |
| F2.13 | N cycles of (stop one host → wait IO → start) | `node.stop` / `node.start` | no monotonic growth of PBuffer occupancy, DBG connect errors, or tablet memory; every cycle ends with a write-and-verify |
| F2.14 | Combined: tablet kill + one host `SIGSTOP` + one host `stop` | all three | no deadlock; no data mismatch; after faults are lifted, seed re-read plus a new write-and-verify |

---

## Blocked — harness hook required

Keep these in the matrix. Do not implement a weak substitute that pretends
to cover them.

| # | Case | Required hook | Why it matters |
| --- | --- | --- | --- |
| F2.15 | Per-request IO error from one DDisk or PBuffer (next N requests fail with `E_IO` / timeout / undelivered) | DDisk / PBuffer test hook or ImmediateControlBoard control that fails the next request without killing the process | Distinguishes "host gone" from "one RPC failed"; drives oracle error counters and flush cooldown (F4.7) without `SIGSTOP` |
| F2.16 | Per-request latency injection (delay one replica by X ms) | same hook, delay instead of fail | Hedging correctness under a slow-not-dead replica; `SIGSTOP` is too coarse |
| F2.17 | Torn write / power loss at a precise point (after PBuffer ack, before DDisk flush; mid integrity-slot write) | precise-point kill or a crash hook in the flush/copy path | Product SLO: no torn write visible. Checksums (S7) are unimplemented, so this is also blocked on that feature |
| F2.18 | Targeted interconnect loss between partition slot and one storage node | local netem / iptables — **unavailable in the yatest sandbox** | Session fencing (S3) under a real partition, not a process kill |
| F2.19 | Corrupt a 4 KiB block on a real PDisk | `use_in_memory_pdisks=False` plus a corruption tool | Checksum / never-return-corrupt-data. Feature not implemented |

---

## Suggested pytest layout

* `test_nbs_faults.py` — F2.1–F2.8
* `test_nbs_faults_pipeline.py` — F2.9–F2.12
* `test_nbs_faults_cycles.py` — F2.13, F2.14 (`SIZE(LARGE)`)

Share a small mixin on `NbsTestBase`: `stop_dbg_host(disk_id, index)`,
`dbg_hosts(tablet_id)`, `wait_io_ok(actor_id)`. Do not fork a new cluster
factory until the `nbs_config` override (README §6.1) lands.
