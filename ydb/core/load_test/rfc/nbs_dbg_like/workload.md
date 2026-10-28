# Workload and Data Flow

This page describes algorithms specific to the NBS-like load actor. The [usage guide](../../../../docs/en/core/contributor/load-actors-nbs-dbg-like.md) owns parameter defaults and examples. The shared [DDisk](../../../../docs/en/core/contributor/distributed-storage/ddisk.md) and [PersistentBuffer](../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md) pages own wire and storage contracts.

## Address Space and Request Generation

The selected prefix of `M` DBGs exposes a flat byte address space:

```text
bytesPerDbg = TargetNumVChunks * VChunkSizeBytes
dbgIndex = address / bytesPerDbg
vChunkIndex = (address % bytesPerDbg) / VChunkSizeBytes
offsetInVChunk = address % VChunkSizeBytes
```

The proxy requires a fixed I/O size that is sector aligned, fits within a vChunk, and evenly divides its size. Load workers choose I/O-unit indices rather than arbitrary byte offsets, so generated requests cannot cross a vChunk boundary. Random workers get disjoint ranges: whole DBG ranges when there are enough DBGs, otherwise ranges of I/O units. Sequential load wraps through the flat space with one worker.

Local arrays use the DBG-local `vChunkIndex`, but every PB/DDisk wire selector uses a unique 64-bit index:

```text
wireVChunkIndex = ui64(DbgIndex) * TargetNumVChunks + localVChunkIndex
```

DDisk's physical map is keyed by `(TabletId, VChunkIndex)`, so this mapping keeps data separate when several DBGs of one tablet share a DDisk, including equal local offsets and multiple vChunks. Allocation dimensions stay fixed across workload configuration changes; the flat client address space is unchanged. Create fresh load-tablet allocations for this mapping. Existing data written with DBG-local wire indexes is not migrated.

Each worker fills its combined read/write concurrency budget. It issues a read while `ReadsIssued / WritesIssued < ReadRatio / 100`; otherwise it issues a write. This measures a ratio to writes, not the percentage of all operations. Reads and writes independently select addresses in the worker's range.

The worker builds one random payload rope of the configured size and reuses it for its writes. The generator records latency and status, but does not compare reads to an expected data image. Reading an unwritten address is possible. Workload behavior is not a substitute for the data-integrity integration tests.

Every request has a monotonically allocated cookie in a circular sparse in-flight queue. Replies recover the address, size, operation kind, start time, and trace span from this entry. Unknown replies are ignored; mismatched response types are treated as an internal error. Failed replies schedule a short error backoff instead of permanently aborting the run.

## Configuration and Backpressure

The proxy resolves geometry from `GetSummary` and sends one `TEvConfigureTablet` with a nonzero configuration ID. It waits for the matching acknowledgement before spawning workers. The tablet closes admission, drains old accepted work in all per-DBG actors (including inactive DBGs), and then installs the configuration. Only matching acknowledgements from all workers reopen admission and install routing. New I/O during reconfiguration or deletion receives `NBSIO_TABLET_NOT_READY`.

The drain admits every record that is already flush-ready or erase-ready, regardless of count, then finishes Sync and PB erasure while preserving read pins and operation ordering. Incomplete PB writes stay unadmitted. It preserves peer sessions during reconfiguration. Supersession replaces the pending configuration and rejects the old request; stale drain/configuration acknowledgements cannot install it. See [Architecture](architecture.md) for deletion and shutdown ordering.

The LSN budget is enforced independently per DBG. For a positive `MaxInflightLsns`, the cap is `max(1, MaxInflightLsns / NumDbgsTotal)`; `NumDbgsTotal` includes allocated DBGs excluded from the run. Zero rejects every write. An LSN occupies this budget until its outstanding operations and PB cleanup complete, including failed writes with confirmed copies. Client concurrency and the tracked-LSN budget are consequently different limits.

## Write and LSN State

Each per-DBG actor generates `lsn = sequence++ * NumDbgsTotal + DbgIndex + 1`. This preserves uniqueness across DBGs of one tablet generation, including DBGs that share PB instances. The sequence lives in the long-lived per-DBG actor and continues across runs; a tablet restart changes the generation and recreates it.

For each write, the actor validates configuration, payload, address, PB connection count, and the LSN cap. It records the accepted LSN in its address slot before sending I/O and stores a `TWriteInfo` with origin actor/cookie, selector, tracing state, coordinator, and requested/responded/confirmed peer masks. Accepted versions determine ordering; the latest acknowledged version determines read visibility. Out-of-order quorum replies cannot move visibility backwards, and a pending overwrite leaves the previous acknowledged PB version readable.

When checksums are enabled, the load worker calculates one checksum per 4 KiB block of its reusable payload once and includes them in every `TEvNbsWrite`. The per-DBG actor forwards those checksums unchanged in `TEvWritePersistentBuffers`. When checksums are disabled, both events omit them. The setting must match the DDisk/PersistentBuffer storage configuration.

The actor picks a coordinator from the primary PB peers and sends `TEvWritePersistentBuffers` with all three primary PB IDs. Normal writes always go to PB; they do not use DDisk `TEvWrite` and are not blocked by an overlapping flush. Additional configured peers are connected, but the normal load path does not implement production NBS handoff, replacement-host, or repair policy.

The actor accumulates per-peer confirmations from plural-write results. Three confirmations satisfy the normal write quorum; `ReplySent` prevents multiple client replies. A successful quorum replies to the client before background flush completes. The write is finalized for flush or failure cleanup only after all requested peers have unambiguous results. A lost quorum returns an error once and remains invisible. The actor keeps the LSN until outstanding results finish. A failed write does not enter the flush cohort. It is marked flushed so a newer version can proceed, and confirmed PB copies bypass the erase gate after every requested PB outcome is definite. Duplicate results cannot repeat accounting, and late confirmations still participate in cleanup. Missing results or ambiguous failures (including `UNKNOWN`, `ERROR`, and `SESSION_MISMATCH`) cannot authorize erasure or drain completion.

```text
PBufferIncompleteWrite
    → PBufferWritten       definite successful PB results; joins the unadmitted flush-ready set
    → PBufferFlushing      flush segments scheduled
    → PBufferFlushed       required destinations confirmed; joins the unadmitted erase-ready set
    → PBufferErasing       erase requests scheduled
    → PBufferErased        remove tracked LSN and its matching slot reference
```

With `DisableReplication`, only the chosen PB is targeted and one confirmation acknowledges the write. The state jumps directly from incomplete to flushed, skipping Sync, and then uses this same erase gate so PB erasure can reclaim space without DDisk synchronization. The actor rejects reads in this mode.

## Flush

A record joins the unadmitted flush-ready set only when it reaches `PBufferWritten`: the write quorum succeeded and every requested PB peer has a definite result. Incomplete PB writes are not members. `SyncRequestsBatchSize` is normalized to at least one. When that set reaches the threshold, exactly its current members are admitted and leave the set. Already admitted records do not count toward the next gate. A completion that arrives later, including one with a lower LSN than records already admitted, waits for a new cohort or idle cleanup.

Admission survives overlap waits, DDisk read pins, partial per-destination scheduling, and retries. Only the oldest unflushed version of a slot is eligible for Sync. Disjoint slots proceed. Overlapping versions therefore flush in acceptance order, even if their PB replies arrive in reverse order. A newer Sync waits until the older version has completed every required destination. Client DDisk reads hold reservations against overlapping Sync; PB reads do not reserve Sync, and neither kind of read reservation blocks a new PB write.

Sync requests group admitted records by destination and local vChunk, so every wire batch uses one wire vChunk. `FlushBatchSize` bounds the total LSNs scheduled per destination in one pump across those groups. Grouping preserves exact cohort membership. If that limit leaves admitted actionable work queued, one coalesced local continuation schedules the next batch. No continuation is scheduled when everything left is blocked.

Reconfigure, delete, and poison admit every record that is already flush-ready, regardless of count. They do not admit incomplete writes.

Each `TEvSync` goes to a DDisk destination and contains PB source segments added with `AddSegmentFromPB`. Source peer `k` supplies destination `k`; the descriptor includes the PB's ID, instance GUID, LSN, generation, and block selector. The shared DDisk sync contract describes how the destination reads PB data and writes its own storage.

An LSN enters flushing after its required destinations have been scheduled. Batch records retain the expected reply actor, destination, and corresponding LSNs, keyed by a separate batch cookie. `TEvSyncResult` updates per-segment confirmations only for a matching actor and cookie. Successful replies must contain every expected segment; malformed cardinality and unknown results retain the reservation. Explicit failures reopen the destination's work and retry the LSN. It stays admitted, and overlap ordering is retained until all destinations complete. A completed flush joins the unadmitted erase-ready set. The load actor does not implement the production partition's full recovery and repair policy.

## Erase

A record joins the unadmitted erase-ready set only after Sync completes (`PBufferFlushed`). `DisableReplication` skips Sync and then uses this same erase gate. The same threshold and current-member rule apply. In-progress Syncs are not erase-ready. Already admitted records do not count toward the next gate. A completion that arrives later, including one with a lower LSN than records already admitted, waits for a new cohort or idle cleanup.

Failed-write cleanup bypasses the erase gate. Reconfigure, delete, and poison admit currently erase-ready records regardless of count. They do not admit records that have not reached `PBufferFlushed`.

Only the oldest version of a slot can be erased, and only after it is flushed and has no PB read pin. DDisk reads do not block erase. A PB read pins its exact LSN before its request is sent. Erase waits until all matched terminal PB read replies release that pin. This wait does not block a newer Sync once the older Sync is complete. An older erase failure or retry holds newer erases of that slot until the older record fully retires. The older record stays admitted across that retry. Independent slots can still erase.

Per-PB batches stay bounded by `EraseBatchSize`. Erase targets are the PB peers that confirmed the write. If that limit leaves admitted actionable work queued, one coalesced local continuation schedules the next batch. No continuation is scheduled when everything left is blocked.

After all target erase requests are sent, the state becomes erasing. Matched completion releases the LSN budget and removes that version from the slot. Retiring an older version cannot clear the newer visible version.

The erase result handler applies the response's outer status to the batch. It does not interpret individual result statuses independently. Erase failures retry the admitted records; wrong-sender, stale, duplicate, and unknown replies cannot release tracked work.

## Idle Cleanup

Each per-DBG worker coalesces cleanup requests into one timer due one second after the first request. Ready work and PB/Sync completions request this timer. On firing, the worker selectively admits flush-ready and erase-ready records whose local vChunk is idle, even below the normal cohort threshold. A vChunk is idle when it has neither incomplete PB writes nor outstanding Sync destination segments. Reads and erases do not disqualify it; ordering and read pins still determine whether admitted work can execute. This also lets a late older write unblock already admitted successors when the LSN budget prevents another cohort from forming.

PB activity starts at write acceptance and ends only when the existing result handler finalizes that write. Sync activity counts destination segments when sent and retires them only when a validated reply is consumed. Each segment is accounted against its local vChunk, independently of other vChunk batches sent to the same destination. Explicit failures retire the attempt before a retry adds its destinations again; duplicate replies cannot retire them twice. Ambiguous writes and unconsumed malformed or unknown Sync replies keep the vChunk busy.

Both admission passes finish before either queue is pumped, so they use the same activity snapshot. Each pass scans its unadmitted vector once, retaining busy-vChunk entries in order. Already admitted blocked records are not rescanned. The timer is worker-wide: a record that becomes ready shortly before an existing timer fires may be admitted by that pass. There is no requirement for one continuous second of idleness.

Busy or pinned records alone do not schedule another timer. Later PB/Sync completions request another pass; admitted pinned records retain their targeted last-reader wakeups. Drain and poison invalidate ordinary cleanup timers using an internal generation independent of user configuration IDs. A stale event cannot act on a new configuration or clear its pending timer flag. Lifecycle drain still forces immediate admission of all ready records.

Normal admission walks only the vector prefix present at entry, then clears it while retaining capacity. Selective admission compacts retained entries in place and preserves capacity and membership order. Admission runs synchronously without callbacks that append ready records; later completions belong to a later pass. Slot keys pack `(VChunk, Index)` into a 64-bit value, casting before the shift, and hash that value with `absl::Hash<ui64>` to mix both parts.

## Wakeups

A write that becomes flush-ready evaluates flush admission and then sends admitted actionable Syncs. A fully completed Sync may make the next oldest unflushed version of that slot flushable, and it evaluates erase admission for records that just became erase-ready. Retiring an erased version may make the next oldest version of that slot erasable.

The last PB reader of a version wakes erase for that slot only. It does not open the normal erase gate and does not scan other slots. The last DDisk reader of a slot wakes flush for that slot only. It does not open the normal flush gate and does not scan other slots. A PB read completion does not run flush maintenance. A DDisk read completion does not run erase maintenance.

`SyncGateFlushBlocked` and `SyncGateEraseBlocked`, both the root counters and the per-DBG counters, increment only when a nonempty normal unadmitted ready set is below the threshold and that pass schedules no admitted work. They do not increment while admitted work is sent, during forced lifecycle or idle cleanup, or from a read-pin wakeup.

## Reads

Reads require at least three connected DDisk peers even when a PB route will be chosen. After address validation, the actor consults the slot's latest acknowledged LSN, independently of a newer accepted but incomplete overwrite:

| Latest Acknowledged Version | Route |
| --- | --- |
| `PBufferWritten` or `PBufferFlushing` | Read a randomly selected PB that confirmed this LSN |
| Flushed or erasing | DDisk, using the primary mask |
| No tracked acknowledged LSN | DDisk, using the primary mask |

The PB route uses `TEvReadPersistentBuffer` with the LSN and generation. It pins the PB version until the expected sender/cookie/type supplies a terminal reply, delaying Erase but allowing Sync. The DDisk route uses `TEvRead` with a block selector and reserves that slot against a later overlapping Sync. The actor releases a reservation only on a matched terminal reply; stale, duplicate, wrong-kind, wrong-sender, and unknown replies cannot release it.

Result handlers forward returned payloads and translate statuses into `TEvNbsReadResult`. They do not retry alternate peers after an I/O error. Reads of unwritten slots remain possible; the generator does not maintain an expected data image.

## Stopping and Measurement

Timing starts when a load worker's pipe connects. Warm-up occurs within `DurationSeconds`. Successful/error completions contribute to measured results only after warm-up and before draining; lifetime counters also include activity outside that window. Completion latency uses the original request timestamp, so a request issued before the warm-up boundary can contribute when it completes after the boundary.

A timer, stop request, or successful-write target enters draining. No new I/O is issued, and the worker waits up to 30 seconds for client requests to finish. Remaining trace spans are ended as abandoned when the worker finishes. The per-tablet proxy merges worker counters and histograms into one finish result.

This 30-second drain concerns only load-worker client requests. Per-DBG actors keep their own flush and erase pipeline, with its normal cohort thresholds still active. The client drain does not force background flush or erase. The one-second idle cleanup can finish short tails naturally after a run, including erase tails with replication disabled; client completion does not wait for this background maintenance. Reconfiguration, deletion, and poison close admission and admit every record that is already flush-ready or erase-ready, regardless of count, then drain that work through flush, read-pin release, and erasure. They do not admit incomplete writes. Delete and poison also wait for peer disconnect acknowledgements. Their worker drain has no timeout and can wait indefinitely for missing or ambiguous results.

Only allocation metadata is durable in the load tablet. There is no recovery marker or fence, and no reconstruction of unfinished LSN state after a crash. These normal I/O and orderly-drain rules do not establish full production NBS crash recovery.

The count target currently limits both issued writes and successful completions. A failed write can leave a worker below its success target with no more writes allowed. Use a duration bound as well, especially when exercising failure or backpressure behavior.

See [Testing and Observability](testing.md) for current coverage and diagnostic counters.
