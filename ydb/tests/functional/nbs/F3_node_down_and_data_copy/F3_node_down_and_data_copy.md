# F3 — Node down and data copy

Priority **P1**. This is the functional counterpart of high-level suite S5,
written against **implemented** behaviour. RFC 001's per-block handoff
chunks and same-node handoff allocation are **not** what this code does.

Depends on harness prerequisites in [README.md](../README.md) §6:

* `nbs_config` override, or TemporaryOffline is unobservable
* `use_in_memory_pdisks=False` for any "host returns with its data" case

Every executing F3 case is `known_bug` (NOTRUN). Stopping a Primary DDisk
host and driving IO so `Think` can demote it segfaults the NBS slot
(`exit_code=-11`) while `direct_block_group_impl` applies
`Online -> TemporaryOffline` / `vchunk.cpp` ApplyConfig. The partition
tablet and its vhost endpoint disappear; later cases then time out on
`wait_host_offline` or fail `write_pattern` with `ConnectionRefused` /
`ConnectionReset`. Re-enable the markers when that crash is fixed.

---

## 1. Transition rules (assert these, do not re-derive them)

`TOracle::Think` runs every `ThinkingInterval` (default 1 s).

```
hasSuffering        = ConsecutiveErrorCount != 0
hasTemporaryOffline = hasSuffering && (
                        (errors >= MinErrors && FromFirstError > MaxDurationTemporaryOffline)
                        || errors >= ErrorsCountForGoingOffline
                        || UsedPBuffers.Size >= ErrorsTotalSizeForGoingOffline)
hasOffline          = hasTemporaryOffline && FromFirstError > MaxDurationOffline
```

Defaults: both durations **10 s**, `MinErrorsCountBeforeGoingOffline` **10**,
`ErrorsCountForGoingOffline` **1000**, `ErrorsTotalSizeForGoingOffline`
**100 MiB** (compared to current PBuffer occupancy, not error-bytes).

With both durations at 10 s, a host can become TemporaryOffline and
Offline in the **same tick**. To observe TemporaryOffline alone, set
something like 2 s / 20 s (unit tests use 2 s / 4 s).

`OnDDiskBroken` (from `IsDeviceBrokenError`) sets `Broken` + `Offline`
immediately. `Think` never un-breaks it.

`SetHostState` on the vchunk:

| New state | Config change |
| --- | --- |
| `Online` | `EnableHost` |
| `TemporaryOffline` | `DisableHost` only |
| `Offline` | `DisableHost` + `PromoteHostIfNeeded` |

`PromoteHostIfNeeded`: if enabled primary DDisks `< 3`, promote the first
enabled host that is not already a primary DDisk. `PromoteHost` sets
PBuffer + DDisk Primary and **watermark 0**.

`AddHost` (`AppendHost`): if `GetDDisks().Count() < 3` (including
**disabled** primaries) the new host is Primary with watermark 0;
otherwise it is HandOff / None, no copy. After a typical one-primary
Offline, an existing HandOff is promoted and the later AddHost is
HandOff / None.

---

## 2. TemporaryOffline vs Offline

This split is the core of the suite.

| Path | TemporaryOffline | Offline |
| --- | --- | --- |
| Writes | Host excluded from `GetDesiredPBuffers` / `GetSecondaryPBuffers` | Same. If primaries drop below 3, desired set is padded with enabled HandOff |
| Reads | `FilterLocations` drops disabled; empty mask **falls back to `DesiredDDisks` including disabled** (F3.17) | Same |
| Flush | Destinations = desired DDisks minus disabled; need ≥ 3 enabled or flush is skipped; missed flushes → `BehindField` | Same, plus the promoted host becomes a flush target |
| Erase | Disabled hosts: local `ConfirmErase`, no RPC | Same |
| Copy | No promotion → no new Fresh DDisk → no copier | If enabled DDisks `< 3`, promote HandOff, watermark 0, copier starts |
| AddHost | Host still **alive** (`GetAliveHostCount`). No replacement | Not alive. `QueryAddHost` every think tick until a host is appended |

`GetTemporaryOfflinePBuffers` is unused on IO paths. There is no hedge
specifically aimed at TemporaryOffline.

---

## 3. Cases — state machine

| # | Case | How to drive | Assertion |
| --- | --- | --- | --- |
| F3.1 | TemporaryOffline only | Override durations (2 s / 20 s). `SIGSTOP` or stop a host long enough to cross TemporaryOffline but not Offline. Inspect DBG mon | host disabled; **no** watermark 0 on a former HandOff; **no** `AddHost`; `GetDDisks()` still lists the host |
| F3.2 | TemporaryOffline recovery | F3.1, then `SIGCONT` / `node.start()` (needs on-disk PDisks if the process was stopped) | errors clear on success; next `Think` returns the host to `Online`; Behind ranges catch up or stay lagging without promotion |
| F3.3 | Offline promotes a HandOff | Default thresholds, or shorten Offline duration. Keep a primary down past Offline | an enabled HandOff becomes Primary DDisk with **watermark 0**; copier appears (F3.6) |
| F3.4 | Offline requests AddHost | Same as F3.3 | `QueryAddHost` repeats until a host is appended; new host is HandOff / None when 3 DDisks (including the disabled one) already exist |
| F3.5 | `OnDDiskBroken` | no functional fault exists: CMS `pdisk set --status BROKEN` is metadata only and never yields `TReplyStatus::BROKEN` / `IsDeviceBrokenError` | covered by `oracle_ut` `OnDDiskBrokenForcesHostOfflineAndRequestsReplacement` / `ThinkNeverBringsBrokenHostBackOnline`. The pytest case stays `known_bug` until a DDisk error hook exists |

---

## 4. Cases — data copy

Copier (`TDDiskDataCopier`) starts only in `TVChunk::ApplyConfig` when
`GetFreshRange(host) != nullopt` and the host is not disabled.

* Range size: `CopyRangeSize = 1_MB`, one range at a time per destination.
* `GetFreshRange`: first `BehindField` range, else
  `[OperationalBlockCount, TotalBlockCount)`.
* Fresh DDisk is a read source only for `range.End < OperationalBlockCount`
  and not Behind. `AheadField` is **not** used for reads (commented out in
  `CanReadFromDDisk`).
* Completion: no fresh range → `OnCopyComplete` → persist watermark
  `nullopt`.

| # | Case | Assertion |
| --- | --- | --- |
| F3.6 | Copier starts after Offline promotion | after F3.3, mon shows a watermark of 0 on the promoted host and copy progress (watermark / operational prefix advances) |
| F3.7 | Serial 1 MiB ranges | observed progress jumps in ~1 MiB steps; no overlapping copy ranges on one dest (infer from mon + logs `Will copy range`) |
| F3.8 | Fresh DDisk not a read source above the watermark | verified IO only to already-copied prefix; a read of a not-yet-copied range is served from another replica or PBuffer, never from garbage on the fresh DDisk |
| F3.9 | Copy complete | watermark becomes unset / `nullopt`; host appears in healthy/full DDisks; reads of the whole vchunk may use it |
| F3.10 | Original host returns during copy | exactly one survivor is used as a read source for a given range; the abandoned replica is not read. There is **no** automatic demote of the failed host — document the observed choice |
| F3.11 | Lag then return (TemporaryOffline or short Offline without promotion), `BehindField` drains | after the host is enabled again, missed ranges are copied first; Behind shrinks; no stale data returned |
| F3.12 | User writes during copy | writes go to PBuffer and ack; `MakeFlushHint` skips LSNs overlapping `InflightDDiskSyncMap`; after copy of a range, a concurrent write is not overwritten by the copier (unit: `ShouldCopyWithWrites`; this is the functional form) |
| F3.13 | Copier retriable error | inject via host bounce during copy, or F2.15 when it exists. Backoff 100 ms doubling to 10 s; copy continues |
| F3.14 | Copier never-retriable error (`E_IO`, `E_IO_SILENT`, `E_ARGUMENT`, `E_CANCELLED`) | copier stops; in-flight sync cleared. **Blocked** on F2.15 unless a real device error can be produced |

---

## 5. Implementation facts that look like defects

Treat these as cases. If they fail the "obviously correct" assertion, file
a ticket rather than weakening the test.

| # | Fact | Case |
| --- | --- | --- |
| F3.15 | Incremental watermark persist (NBS-7656). `OnCopyProgress` writes the operational prefix every 8 MiB | Kill the tablet after a promoted host is Fresh / has a rising watermark, or after the 32 MiB vchunk already finished. After recovery the promoted host's watermark is unchanged or further ahead, never rewound. Track only the promoted host |
| F3.16 | `DoStart` does not start copiers. They start only on the next `ApplyConfig` | After F3.15, several Think ticks must not advance the promoted host's watermark or `operational_block_count`. If the copy is already complete, it stays complete |
| F3.17 | `FilterLocations` falls back to `DesiredDDisks` including disabled when the enabled mask is empty | Construct a range whose only replica is the disabled host. The read must not return stale or torn data. If it reads the disabled replica, that must be explicit and safe |
| F3.18 | `AheadField` is not readable, but `GetFreshRange` still copies those offsets | A user write flushed to the fresh DDisk above the watermark is Ahead; the copier may rewrite it. Assert no user-visible mismatch |

---

## 6. What is absent (do not write tests that assume these)

| Item | Status |
| --- | --- |
| Move a vchunk to another DBG | absent |
| Offline → `EvacuateHost` + demote + rebalance | `EvacuateHost` is still unit-test only. `DemoteUnavailableHostsIfNeeded` **does** run after every config persist: once healthy DDisks are back to quorum, the disabled replica is demoted to DDisk None (F3.19) |
| RFC 001 per-block handoff chunks, drop handoff after sync | RFC only |
| Incremental watermark persist | present since NBS-7656 (F3.15 asserts it survives tablet kill) |
| Copier auto-resume on `DoStart` | absent (F3.16) |
| Hedge to TemporaryOffline | comment only |

"Permanently down" in this plan therefore means: disable + maybe promote
an existing HandOff + copy from watermark 0 + append a spare as HandOff,
then demote the disabled replica once a healthy quorum is back.

F3.19 is a documentation case: after Offline + AddHost, count DDisks and
roles on the mon page and assert they match the paragraph above.

---

## Suggested pytest layout

* `test_nbs_host_state.py` — F3.1–F3.5 (needs `nbs_config` override)
* `test_nbs_data_copy.py` — F3.6–F3.14, F3.19
* `test_nbs_data_copy_restart.py` — F3.15–F3.18 (`use_in_memory_pdisks` as
  required)
