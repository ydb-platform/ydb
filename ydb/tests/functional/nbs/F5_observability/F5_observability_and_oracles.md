# F5 — Observability and oracles

Priority **P3**, but a prerequisite for asserting F2–F4 at pytest level.
Without parseable mon pages, host state, watermark and PBuffer fill are
guesswork.

Existing helpers in `common.py`: `fetch_mon`, `fetch_partition_dbg_page`,
`fetch_pbuffer_page`, `parse_dbg_indexes`, `parse_pbuffer_service_ids`,
`collect_pbuffer_service_ids`, `wait_until`. Used today only by
`test_nbs_disk_deletion`.

---

## 1. Wait-and-assert

| # | Rule |
| --- | --- |
| F5.6 | Every condition that is eventually true uses `wait_until(predicate, timeout_seconds=..., description=...)`. Never `time.sleep` then a single assert (the load-actor helper's `duration + 2` sleep is the existing exception; do not copy it). Timeouts are part of the assertion: a hang is a failure with the last `AssertionError` attached |

---

## 2. Partition DBG page

URL: `/tablets/app?TabletID=<id>&page=dbg` and `&dbg=<index>`.

Rendered by `mon_page/` (`EMonPage::Dbg`). The unit suite
`mon_page/mon_render_ut.cpp` already locks HTML structure: header, host
table, drill-down links.

| # | What to parse | Used by |
| --- | --- | --- |
| F5.1 | Per-host: node id, role (`Primary` / `HandOff`), `EHostState`, enabled bit, watermark, `ConsecutiveErrorCount`, `InflightByOperation` | F2.3–F2.5 (which nodes to stop), F3.1–F3.6, F3.19, F4.7 |

Add `NbsTestBase.parse_dbg_hosts(html) -> list[HostSnapshot]` rather than
ad-hoc regex in each test. Until the HTML is stable enough, start with
the fields `mon_render_ut` already expects.

---

## 3. Persistent Buffer page

URL: `/node/<nodeId>/actors/persistent_buffer?showTablets=1&pb=...`.
`fetch_pbuffer_page` already groups by node.

| # | What to parse | Used by |
| --- | --- | --- |
| F5.2 | Per-tablet LSN presence, free-space / occupancy if `describeFreeSpace=1` | F1.4 (already: tablet id gone after delete), F4.6, F4.8 |

---

## 4. Counters

These exist in code. They are not asserted by any functional test.

| # | Sensor | Path | Used by |
| --- | --- | --- | --- |
| F5.3 | `TVChunkCounters`: `Pending`, `MinLsn`, `ReplyOk` / `ReplyErr` for `Flush` / `Erase` / `EraseBelated` | volume / partition mon or Solomon dump | F2.9–F2.10 (phase detection), F4.5–F4.6 |
| F5.4 | `TVolumeRequestCounters`: `Requests`, `ReplyOk`, `ReplyErr`, `Bytes`, `Inflight`, `RequestTimeMs` for Read/Write/Zero | same | F1 load-actor cross-check; F2.5 (no silent SUCCESS) |
| F5.5 | DDisk `DirectIO.QueueSize`, `RunningCount`, `QueueTime`; `PersistentBuffer.PendingEventsQueueSize`; `DiskOperationsInflight` on `TEvGetPersistentBufferInfo` | `/actors` on the storage node | F4.8–F4.9 (approaching OVERFILL / OVERLOADED) |

Discover the exact HTML / JSON dump in the first implementation spike;
do not block the rest of F5 on a pretty parser.

---

## 5. Missing sensors

These would turn inferred assertions into direct ones. Until they exist,
the cases stay as written (infer from watermark / wall time / error
status).

| Sensor | Would unblock |
| --- | --- |
| Copy MB/s and `TakeVolumeCopyRangeBudget` wait | F4.13, tighter F4.1–F4.3 |
| Space buckets (`issued` / `spent` / `used`) | F4.8 without filling 4 GiB blindly |
| Barrier position vs `min(Inflight)` | S6 / F2.10 orphan detection |
| Per-class inflight (`UserCritical` vs `Repair`) | RFC 009, out of scope here |
| Distinct export of `OVERFILL` / `OVERLOADED` / `E_REJECTED` / blocked-generation | F2.1, F4.8–F4.10 without log scraping |

| # | Case | Assertion |
| --- | --- | --- |
| F5.7 | Inventory the sensors above against a running cluster | document which are already on a mon page or counter dump and which are absent. This is a one-off spike, not a regression test |

F5.7 findings (from `mon_page/mon_render.cpp` + the F5.1–F5.5 smoke cases):

| Sensor | Where it is today |
| --- | --- |
| Per-host State / Health / ConsecutiveErrorCount | DBG detail table (`page=dbg&dbg=N`). Parsed by `fixtures/mon.py:parse_dbg_hosts` |
| Node / PDisk / DDisk id | DBG Connections table. DDisk id is `node:pdisk:slot` |
| PBuffer / DDisk role, Enabled, Watermark | VChunk host-roles table (`page=vchunk&vchunk=N`) |
| PBuffer tablet LSN presence | `/node/<id>/actors/persistent_buffer?showTablets=1` — tablet id as text |
| `TVChunkCounters` Pending / MinLsn | **not** on the partition HTML pages as named fields. Overview / VChunk render dirty-map dumps and barriers, not Flush.Pending |
| `TVolumeRequestCounters` | **not** on the partition HTML pages. No Requests / ReplyOk / Inflight table |
| DDisk `DirectIO.QueueSize` | DDisk actor page `/node/<id>/actors/ddisks/ddisk_p..._s...` exists; named DirectIO fields are best-effort |
| Copy MB/s, leaky-bucket wait, space buckets, barrier vs min(Inflight), per-class inflight, distinct OVERFILL/OVERLOADED export | **absent** — still the missing-sensor list above |

---

## 6. Data oracle (reminder)

F5 does not replace the self-describing payload in [README.md](../README.md) §7.
Mon pages tell you **state**. They do not tell you the guest read the
right bytes. Every F2/F3 case that claims no data loss still needs a
read-back of known blocks.

---

## Suggested pytest layout

* Extend `common.py` with parsers; add `test_nbs_mon.py` for F5.1–F5.5
  smoke (pages render and parsers do not throw on a live tablet).
* F5.6 is a convention, not a test.
* F5.7 is a spike note in the first PR description.
