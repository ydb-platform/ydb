# NBS direct-partition data path

This page describes partition policy. Shared service identity, placement and
wire contracts are in [DirectBlockGroups](../../../../../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md),
[DDisk](../../../../../../../../docs/en/core/contributor/distributed-storage/ddisk.md),
and [PersistentBuffer](../../../../../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md).
Use the [component map](../README.md) to find the implementation and tests.

## Addressing and roles

`TFastPathService` owns regions; each `TRegion` owns vChunks; each `TVChunk`
uses one `IDirectBlockGroup`. Regions are 4 GiB. The configured vChunk size
must divide the region size; the current default is 128 MiB, giving 32 vChunks
per region. The default stripe size is 512 KiB. These defaults are defined in
[constants.h](../../../common/constants.h) and
[config.cpp](../../../../config/config.cpp).

[region_geometry.cpp](../model/region_geometry.cpp) translates a request
within a stripe as follows, using block counts:

```text
stripeIndex         = regionBlockOffset / blocksPerStripe
vChunksPerRegion    = RegionSize / vChunkSize
vChunkInRegion      = stripeIndex % vChunksPerRegion
stripeInVChunk      = stripeIndex / vChunksPerRegion
vChunkBlockOffset   = stripeInVChunk * blocksPerStripe
                      + regionBlockOffset % blocksPerStripe
globalVChunkIndex   = regionIndex * vChunksPerRegion + vChunkInRegion
dbgIndex            = globalVChunkIndex % numberOfDBGs
```

The internal request headers describe an already split range; geometry helpers
assert that its length does not exceed a stripe. A vChunk selector is not a
physical PDisk chunk ID. DDisk resolves it to its own allocation.

The initial NBS configuration uses five host positions and three primaries.
[TVChunkConfig::MakeDefault](../model/vchunk_config.cpp) independently builds
PB and DDisk roles with the same rotation:

```text
primary position i = (i + globalVChunkIndex) % hostCount, i in [0, 3)
```

| vChunk index modulo 5 | Initial primary positions | Other PB positions |
| --- | --- | --- |
| 0 | 0, 1, 2 | 3, 4 |
| 1 | 1, 2, 3 | 0, 4 |
| 2 | 2, 3, 4 | 0, 1 |
| 3 | 0, 3, 4 | 1, 2 |
| 4 | 0, 1, 4 | 2, 3 |

The remaining PBs have `HandOff` roles; the remaining DDisks initially have
`None` roles. A host index is an index into the DBG connection lists, not an
ordinal such as "the second primary" and not a promise of physical
co-location. PB and DDisk entries at one index are paired by the allocator,
whose placement contract is described in the shared DBG page.

Runtime configuration can promote, demote, evacuate and append hosts. Three
primaries is an initial value, not a maximum. Request code must use
`TVChunkConfig` masks rather than reconstructing the initial rotation.

## Record identity

[TFastPathService::GenerateLsn](../fast_path_service.cpp) increments an atomic
counter shared by this partition's DBGs. Each new vChunk write registers a
pending record before sending I/O. Its
[TPBufferKey](../../../common/pbuffer_key.h) is `(Generation, Lsn)`, ordered
lexicographically; the tablet generation separates new writes after restart
from restored records. Do not replace a restored key's generation with the
current tablet generation.

LSNs identify writes, not individual blocks, and are not ordered across
tablets. The PB wire/storage identity additionally includes the tablet and
DBG index; see the shared PB contract. A retry or replica of one write keeps
the same record key. Different overlapping writes have different keys, and
the greatest key determines the PB data visible for the overlap.

## Writes

[TVChunk::DoWriteBlocksLocal](../vchunk.cpp) waits for dirty-map readiness,
registers a pending write, and creates a
[TWriteRequestExecutor](../write_request.cpp). The executor snapshots the
vChunk configuration and obtains its write mode and timing from the oracle.
It requires at least three desired PB hosts before starting.

- `DirectWrite` sends individual writes from the partition to desired PBs.
- `IndirectWrite` selects a coordinator with `SelectBestPBufferHost` and
  sends one plural PB write containing the destination set. The coordinator
  supplies per-destination results, potentially in multiple replies.

[TOracle::SelectBestPBufferHost](../model/oracle.cpp) chooses from the supplied
candidate mask the host with the fewest in-flight requests of the requested
operation type (`EOperation::WriteToManyPBuffers` for indirect writes). It
breaks ties uniformly at random using reservoir sampling. The selection
does not weight latency, success rate, or PB free space.

Both modes acknowledge success after three PB positions have confirmed.
Those confirmations may include handoffs. They do not wait for PB-to-DDisk
flush. On hedging or insufficient indirect results, the executor issues
additional **direct** writes, first to eligible handoffs and then, when
needed, to desired hosts. Requested/completed/failed masks prevent counting
one destination twice. The request timeout bounds the client operation.

Late successful responses still matter after the client reply: they identify
extra PB copies that need cleanup. `ReplyOrNotifyBelated`,
`TVChunk::OnBelatedWriteBlocksResponse`, and the belated erase queue implement that
path. Changes to timeout handling must preserve it.

The proto enum's zero value is `IndirectWrite`; the C++ fallback for an absent
configuration field is `DirectWrite`. Inspect the effective config rather
than assuming that zero and absence select the same path. Historical test
names may still contain `PBufferReplication` or `DirectPBufferFilling`.

## Reads

[TBlocksDirtyMap::MakeReadHint](../dirty_map/dirty_map.cpp) splits the range
using overlapping record keys. Reads do not perform quorum reconciliation;
each hint succeeds on one usable replica. The current state mapping comes
from [TInflightInfo::ReadMask](../dirty_map/inflight_info.cpp):

| State of an overlapping record | Read source |
| --- | --- |
| `PBufferPendingWrite` | Does not introduce a PB source; the unacknowledged write is invisible |
| `PBufferIncompleteWrite` | Wait for its quorum-ready future and recompute hints |
| `PBufferWritten`, `PBufferFlushing` | A PB that confirmed the record, using its original key |
| `PBufferFlushed`, `PBufferErasing`, `PBufferErased` | DDisk |
| No visible PB overlap | DDisk |

Pending writes preserve the pre-write view: an older visible PB record can
still supply an overlap; otherwise the hint falls through to DDisk. Pending
writes and incomplete records found during recovery are distinct states.

DDisk masks normally contain desired, enabled hosts that can read the whole
range. [TDDiskState::CanReadFromDDisk](../dirty_map/ddisk_state.cpp) rejects
disabled DDisks, ranges beyond a fresh disk's watermark, and ranges marked
outdated in its behind map. The ahead map helps repair accounting but does
not currently make above-watermark ranges eligible for this read check.
PB masks start from confirmed replicas and also exclude disabled hosts.
If filtering empties the mask, `MakeReadRangeHint` currently falls back to
the desired DDisk host positions, keeping the hint's original PB/DDisk source
type. This is a best-effort retry path, not an extra quorum or reconciliation
step.

[TReadSingleLocationRequestExecutor](../read_request_single_location.cpp)
chooses the first not-yet-requested position in its hint mask. Errors and
hedging timers start the next candidate; the oracle provides hedging delays,
not a reordered read mask. Several attempts can be in flight. The first
success completes the hint; exhaustion or the request timeout fails it.
There is no fixed four-attempt limit in this implementation.

[TReadMultipleLocationRequestExecutor](../read_request_multiple_location.cpp)
splits the caller's scatter/gather list for independent hints and runs their
executors together. All hints must succeed for the whole request to succeed.

## Read/maintenance synchronization

Every hint carries a [TRangeLock](../dirty_map/range_locker.cpp). The read
executor arms it before starting and disarms it while completing the request;
it also closes its dependent scatter/gather list before publishing completion.

- A PB lock pins the record and prevents it becoming erase-ready while the
  read uses it.
- A DDisk range lock prevents overlapping PB-to-DDisk flush work. The dirty
  map also tracks range synchronization for repair and prevents conflicting
  flushes.

These are partition-local ordering mechanisms. They do not send lock messages
to PBs or DDisks and must survive retry/hedging changes. See
[lifecycle and recovery](lifecycle-and-recovery.md) for the other flush and
erase gates.
