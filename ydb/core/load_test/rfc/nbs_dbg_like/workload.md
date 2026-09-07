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

The wire selector uses the DBG-local `vChunkIndex` above. DDisk's physical map
is keyed by `(TabletId, VChunkIndex)`, without the DBG index. If two DBGs of
this tablet share a data DDisk, equal vChunk indexes and offsets address the
same DDisk data after flush. PB records remain separated by their DBG namespace.
Disjoint logical DBG ranges therefore do not guarantee independent DDisk data
in a shared-slot allocation. The shared-DDisk regression test deliberately
uses different offsets for the two DBGs.

Each worker fills its combined read/write concurrency budget. It issues a read while `ReadsIssued / WritesIssued < ReadRatio / 100`; otherwise it issues a write. This measures a ratio to writes, not the percentage of all operations. Reads and writes independently select addresses in the worker's range.

The worker builds one random payload rope of the configured size and reuses it for its writes. The generator records latency and status, but does not compare reads to an expected data image. Reading an unwritten address is possible. Workload behavior is not a substitute for the data-integrity integration tests.

Every request has a monotonically allocated cookie in a circular sparse in-flight queue. Replies recover the address, size, operation kind, start time, and trace span from this entry. Unknown replies are ignored; mismatched response types are treated as an internal error. Failed replies schedule a short error backoff instead of permanently aborting the run.

## Configuration and Backpressure

The proxy resolves geometry from `GetSummary` and sends one `TEvConfigureTablet` before spawning workers. The tablet installs routing parameters and forwards the configuration to all per-DBG actors, including inactive DBGs.

Each per-DBG actor invokes `BestEffortEraseAll` for state left from an earlier run before installing the new settings. This is an asynchronous cleanup attempt, not a barrier proving that prior requests are finished. It is one reason to avoid overlapping runs on the same tablet.

The LSN budget is enforced independently per DBG. For a positive `MaxInflightLsns`, the cap is `max(1, MaxInflightLsns / NumDbgsTotal)`; `NumDbgsTotal` includes allocated DBGs excluded from the run. Zero rejects every write. An LSN occupies this budget until it fails or completes PB erasure. Client concurrency and the tracked-LSN budget are consequently different limits.

## Write and LSN State

Each per-DBG actor generates `lsn = sequence++ * NumDbgsTotal + DbgIndex + 1`. This preserves uniqueness across DBGs of one tablet generation, including DBGs that share PB instances. The sequence lives in the long-lived per-DBG actor and continues across runs; a tablet restart changes the generation and recreates it.

For each write, the actor validates configuration, payload, address, PB connection count, and the LSN cap. It stores a `TWriteInfo` with origin actor/cookie, selector, tracing state, coordinator, and requested/confirmed peer masks. `InflightLsnAtSlot` points each address slot to its latest outstanding LSN; older LSN records may still be completing.

The actor picks a coordinator from the primary PB peers and sends `TEvWritePersistentBuffers` with all three primary PB IDs. `AddPayloadThenChecksum` attaches the rope and checksum. Additional configured peers are connected, but the normal load path does not implement production NBS handoff or replacement-host policy.

The actor accumulates per-peer confirmations from plural-write results. Three confirmations satisfy the normal write quorum; `ReplySent` prevents multiple client replies. A definitively lost quorum returns an error and drops the tracked LSN. A successful quorum replies to the client immediately and queues background flush work.

```text
PBufferIncompleteWrite
    → PBufferWritten       client write acknowledged; queued for flush
    → PBufferFlushing      flush segments scheduled
    → PBufferFlushed       required destinations confirmed; queued for erase
    → PBufferErasing       erase requests scheduled
    → PBufferErased        remove tracked LSN and its matching slot reference
```

With `DisableReplication`, only the chosen PB is targeted and one confirmation acknowledges the write. The state jumps directly from incomplete to flushed so PB erasure can reclaim space without DDisk synchronization. The actor rejects reads in this mode.

## Flush

`PendingFlush` is a FIFO of actionable LSNs. `DoFlush` opens only when the DBG has at least `SyncRequestsBatchSize` LSNs in `PBufferWritten`; the threshold is normalized to at least one. It then builds a separate batch for each primary destination, bounded by `FlushBatchSize`.

Each `TEvSync` goes to a DDisk destination and contains PB source segments added with `AddSegmentFromPB`. Source peer `k` supplies destination `k`; the descriptor includes the PB's ID, instance GUID, LSN, generation, and block selector. The shared DDisk sync contract describes how the destination reads PB data and writes its own storage.

An LSN enters flushing after its required destinations have been scheduled. Batch records retain the destination and corresponding LSNs, keyed by a separate batch cookie. `TEvSyncResult` updates per-segment confirmations. Failures reopen the destination's work and requeue the LSN; completed flushes queue PB erase work. The load actor does not implement the production partition's full recovery and repair policy.

## Erase

`DoErase` gates on the number of `PBufferFlushed` LSNs, counting work not yet scheduled rather than every LSN still awaiting an erase response. It consumes `PendingErase` and creates per-PB `TEvBatchErasePersistentBuffer` requests, bounded by `EraseBatchSize`. Erase targets are the PB peers that confirmed the write.

After all target erase requests are sent, the state becomes erasing. Completion clears matching `InflightLsnAtSlot` entries, releases the LSN budget, and removes the LSN. Removing a slot entry is conditional on it still referring to this LSN, so an older completion cannot erase a newer write's slot mapping.

The current erase result handler applies the response's outer status to the batch. It does not interpret individual result statuses independently. Erase failures requeue work. `BestEffortEraseAll` also uses batch erase, with its own cleanup bookkeeping; it is not a persistent guarantee that every PB record has been removed.

## Reads

Reads require at least three connected DDisk peers even when a PB route will be chosen. After address validation, the actor consults `InflightLsnAtSlot` and the referenced LSN's state:

| Current Slot State | Route |
| --- | --- |
| `PBufferWritten` or `PBufferFlushing` | Try a randomly selected PB that confirmed this LSN; otherwise fall through to DDisk |
| Incomplete write, flushed, erasing, or erased | DDisk; use confirmed flush destinations when present, otherwise the primary mask |
| No tracked LSN | DDisk, using the primary mask |

The PB route uses `TEvReadPersistentBuffer` with the LSN and generation. The DDisk route uses `TEvRead` with the block selector. Result handlers forward the returned payload and translate the status into `TEvNbsReadResult`. They do not implement a retry over alternate peers after an I/O error.

`FlushedSlots` is updated as bookkeeping, but current read routing does not consult it to prevent reads of unwritten slots. An incomplete write can therefore lead to reading the previous DDisk contents. This is narrower behavior than the production partition's dirty-map and read-consistency machinery.

## Stopping and Measurement

Timing starts when a load worker's pipe connects. Warm-up occurs within `DurationSeconds`. Successful/error completions contribute to measured results only after warm-up and before draining; lifetime counters also include activity outside that window. Completion latency uses the original request timestamp, so a request issued before the warm-up boundary can contribute when it completes after the boundary.

A timer, stop request, or successful-write target enters draining. No new I/O is issued, and the worker waits up to 30 seconds for client requests to finish. Remaining trace spans are ended as abandoned when the worker finishes. The per-tablet proxy merges worker counters and histograms into one finish result.

This drain concerns the load workers' requests. Per-DBG actors keep their own flush and erase pipeline, with its sync gates still active. There is no end-of-run command that bypasses those gates. Short tails may remain tracked until subsequent activity; a later Configure starts best-effort erase of prior state. Tablet shutdown disconnects/poisons actors without waiting for that pipeline to finish.

The count target currently limits both issued writes and successful completions. A failed write can leave a worker below its success target with no more writes allowed. Use a duration bound as well, especially when exercising failure or backpressure behavior.

See [Testing and Observability](testing.md) for current coverage and diagnostic counters.
