# F4 — Throttling and resource limits

Priority **P2**.

There is **no user-level IOPS or bandwidth throttle** on the NBS 2.0
datapath. `TVolumePerformanceProfile`, `PerformanceProfileMax*Iops/Bandwidth`
and `ThrottlerDelay` exist in protos and are unused.
`TVolumeActor` is config-only. RFC 009 device scheduler (`C_io`, `C_bgw`)
is design-only.

"Throttling" in this suite means:

1. handoff **copy-range** leaky bucket
2. flush / erase **pacing**
3. DDisk / PBuffer **rejection** when a hard limit is hit
4. hedging and request **timeouts** as implicit load control

---

## 1. Negative case — no user throttle

| # | Case | Assertion |
| --- | --- | --- |
| F4.12 | Drive load-actor well above any leftover `PerformanceProfile` numbers | requests are not delayed or rejected with a throttling status; `ThrottlerDelay` is never set on responses. This documents current behaviour so a future user throttle is a deliberate change |

---

## 2. Handoff copy throttling

Implemented. Knob: `CopyRangeBandwidthMbs` (`TStorageServiceConfig` field 33),
default **200**. `0` disables. Volume-wide `TSimpleLeakyBucket` via
`TDDiskDataCopier::StartCopyRange` → `TakeCopyRangeBudget` →
`TFastPathService::TakeVolumeCopyRangeBudget`. Rate and burst are both
`Mbs * 1_MB` bytes/s. Waits shorter than 5 µs are suppressed.

Copy itself is serial 1 MiB ranges per destination (F3.7). Several
copiers (hosts × vchunks) share **one** volume bucket.

Needs `nbs_config` override (README §6.1) and an Offline promotion to
start a copier (F3.3). There is **no** mon counter for leaky-bucket delay
or copy MB/s — infer rate from watermark progress (F4.13).

| # | Case | Assertion |
| --- | --- | --- |
| F4.1 | Copy a known volume of fresh data (one vchunk = 32 MiB on this harness, or 128 MiB on disk PDisks) at `CopyRangeBandwidthMbs=20` vs `200` | wall time to watermark `nullopt` scales roughly inversely (20 MB/s → ~1.6 s per 32 MiB; 200 MB/s → ~0.16 s). Accept a wide band; the point is that the knob has effect |
| F4.2 | `CopyRangeBandwidthMbs=0` | copy is not paced by the bucket; finishes no slower than the 200 default and typically faster |
| F4.3 | Two copiers on the same volume (promote two hosts, or two vchunks with a fresh DDisk) | combined copy rate respects the single volume cap |
| F4.4 | User IO latency during copy at 20 vs 200 MB/s | isolation today is **range locking**, not a shared budget. Copy and user IO still share DDisk queues. Assert: user writes still ack; record latency. Do not require that user p99 is independent of the copy rate — that would be RFC 009 |

---

## 3. Flush and erase pacing

| Knob / constant | Default | Effect |
| --- | --- | --- |
| `SyncRequestsBatchSize` | 10 | `MakeFlushHint` / `MakeEraseHint` return empty unless ready size ≥ batch. `force=true` uses 1 |
| `CleaningUp` period | 1 s, hardcoded | force-flush / erase only when `InflightFlushesCount == 0` and `InflightWritesCount == 0` |
| Flush cooldown | `ConsecutiveErrorCount * 10 ms`, cap 10 s | `TFlushRequestExecutor` delays `DoRun`. Erase has **no** cooldown |
| Concurrent flush / erase cap | **absent** | `DoFlush` starts one executor per route; `DoErase` one per host |

| # | Case | Assertion |
| --- | --- | --- |
| F4.5 | `SyncRequestsBatchSize=100` vs `1` | with a slow write trickle, flush pending grows to the batch size before DDisk sees data (`TVChunkCounters.Flush.Pending`). At 1, flush follows each write more closely |
| F4.6 | Idle after a burst of writes | within ~1 s of quiesce, `CleaningUp` force-flushes remaining ready LSNs; PBuffer occupancy drops |
| F4.7 | Degraded host (F2.8 `SIGSTOP`, or F2.15) | `ConsecutiveErrorCount` on that host rises; flush cooldown becomes visible as slower drain. Cap is 10 s |

No-concurrency-cap is a **risk note**, not a case: a test that floods
flush/erase and looks for unbounded inflight would be useful once
counters exist, but it is not a pass/fail product requirement today.

---

## 4. DDisk / PBuffer rejection

Implemented limits (DDisk / PBuffer, not the partition tablet):

| Identifier | Default | Status on overflow |
| --- | --- | --- |
| `PerTabletStorageLimit` | 4 GiB | `OVERFILL` |
| `MinFreeSectorsReserve` | 256 sectors | `OVERFILL` (room kept for barrier / fast erase) |
| `MaxChunks` | 256 | `OVERFILL` when full |
| `MaxPendingEventsQueueSize` | 1024 | `OVERLOADED` while PB not ready / batching |
| `MaxSectorsPerBufferRecord` | 128 × 4 KiB = **512 KiB** | `INCORRECT_REQUEST` |
| `TDDiskActor::MaxInFlight` | 256 | uring queue depth; `DirectIoQueue` itself is unbounded |

Reachability **without** a new hook: stop the DDisk-bearing hosts so
flush cannot drain, then write until PBuffer fills.

| # | Case | Assertion |
| --- | --- | --- |
| F4.8 | Fill one tablet's PBuffer past `PerTabletStorageLimit` (stop flush targets, then write) | further writes fail with `OVERFILL` (or a mapped partition error); no silent drop; after hosts return and flush/erase run, writes succeed again |
| F4.9 | Burst writes into a not-yet-ready or saturated PBuffer past 1024 pending | `OVERLOADED`; client sees a retriable error |
| F4.10 | Single write larger than 512 KiB through dstool / vhost | `INCORRECT_REQUEST` or a never-retriable argument error; no partial persist |

F4.8 needs enough RAM and time on `SIZE(LARGE)` if the limit is the full
4 GiB. A lower `per_tablet_storage_limit` via `nbs_config` override is
the practical path.

---

## 5. Hedging and timeouts

Defaults from `config.cpp`:

| Field | Default |
| --- | --- |
| `WriteHedgingDelay` / `ReadHedgingDelay` | 1 ms |
| `Write/Read/Flush/EraseRequestTimeout` | 10 s |
| `PBufferReplyTimeoutMicroseconds` | 50 ms |

Hedging is only useful when the hedge delay **exceeds** the PBuffer reply
timeout (otherwise the coordinator has already replied). The 1 ms vs 50 ms
defaults mean IndirectWrite hedge is aimed at a **hung** coordinator, not
a slow one. F2.8 (`SIGSTOP`) is the way to hang a replica.

| # | Case | Assertion |
| --- | --- | --- |
| F4.11 | `WriteHedgingDelay` set **above** `PBufferReplyTimeoutMicroseconds`, one replica `SIGSTOP` | write still completes via hedge / remaining hosts within `WriteRequestTimeout`. Invert the knobs and show that hedge does not fire before the reply timeout (timing assertion, wide band) |

---

## 6. Blocked sensor

| # | Case | Required hook |
| --- | --- | --- |
| F4.13 | Direct assertion of copy MB/s and leaky-bucket wait | a counter for `TakeVolumeCopyRangeBudget` delay and bytes granted. Until then F4.1–F4.3 infer from watermark vs wall time |

---

## Suggested pytest layout

* `test_nbs_copy_throttle.py` — F4.1–F4.4 (needs F3 promotion + config override)
* `test_nbs_flush_pacing.py` — F4.5–F4.7
* `test_nbs_limits.py` — F4.8–F4.12
