#pragma once

#include "defs.h"

#include "ddisk_checksums.h"

#include <ydb/library/actors/util/rc_buf.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

#include <util/generic/bitmap.h>
#include <util/generic/intrlist.h>

#include <deque>
#include <memory>
#include <variant>
#include <vector>

namespace NKikimr::NDDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TIntegrityManager
//
// Pure-logic (no I/O) owner of IntegrityChunk / IntegrityExtent allocation and of the in-memory
// per-data-chunk integrity state: used-block bitmaps, data block checksums and per-TIntegrityBlock
// digests. It performs no I/O itself: every disk operation it needs is queued as a TAction
// (allocate an integrity chunk / read or write a buffer); TDDiskActor drains the queue with
// TakeActions(), executes the async I/O and feeds completions back via
// OnIntegrityChunkAllocated / OnIoCompleted / OnReadIoCompleted.
// This makes the whole state machine unit-testable without a DDisk.
//
// PDisk never restarts separately from DDisk, so a reserved chunk may be formatted immediately
// (as if committed). Formatting writes (chunk headers, extent image) run in parallel; the actor
// logs a single combined increment only after the extent is Ready, and does not reply to the
// originating write until that record is durable. A crash before the increment just loses the
// reserved chunks.
//
// Persistence scope: the DataChunk -> IntegrityExtent mapping (plus generations and the monotonic
// generation counter) is persisted in the DDisk chunk-map log by the actor and restored on boot
// via ApplyMappingSnapshot(). A durable increment always references a fully formatted extent (and,
// when it carries an IntegrityChunk, a fully formatted chunk), so every restored chunk is Ready.
// The used-block bitmaps and checksums still live in memory only; extent formatting writes valid
// TIntegrityBlock images with empty bitmaps, and TIntegrityBlocks are not rewritten on data
// writes. Restored extents therefore have unknown bitmaps: reads of them pass through unchanged
// until a later phase restores bitmaps from the extents on disk; new writes are tracked again.
//
// Memory: used-block bitmaps are small (1 bit per 4 KiB data block) and are kept per extent,
// never evicted - reads depend on them. Checksums and digests are kept sparsely, one
// TIntegrityBlockState (~4 KiB) per TIntegrityBlock actually written with checksums, bounded by a
// manager-wide LRU budget. Evicting a state loses its checksums and its digest together, which
// preserves the invariant "digest = XOR of contributions of the currently known checksums" - the
// same information loss a checksum-less overwrite already produces.
//
// On-disk layout of an integrity chunk (same size as a data chunk):
//   [0, IntegrityChunkHeaderRegionSize)  - TIntegrityChunkHeader replicas
//   then ExtentsPerChunk() extents, each occupying ExtentOnDiskSize() bytes: BlocksPerExtent()
//   ping-pong pairs of two adjacent 4 KiB TIntegrityBlock slots (A then B). Formatting writes both
//   slots with PairSequenceNumber 0 (A) and 1 (B), so slot B starts as the current one.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TIntegrityManager {
public:
    struct TDataChunkKey {
        ui64 TabletId = 0;
        ui64 VChunkIndex = 0;

        friend constexpr auto operator<=>(const TDataChunkKey&, const TDataChunkKey&) = default;

        template <typename H>
        friend H AbslHashValue(H h, const TDataChunkKey& key) {
            return H::combine(std::move(h), key.TabletId, key.VChunkIndex);
        }
    };

    struct TExtentRef {
        TChunkIdx IntegrityChunkIdx = 0;
        ui32 ExtentSlot = 0;
        ui64 VChunkGeneration = 0;
    };

    // ---- actions (outputs): drained by the actor after any mutating call ----

    // Actor must obtain a chunk (e.g. from its reserve) and call OnIntegrityChunkAllocated().
    struct TAllocateIntegrityChunk {};

    // Actor must write Data at the given chunk offset and call OnIoCompleted(IoId) on success.
    enum class EWriteIoKind {
        Pair,
        ChunkHeader,
        ExtentFormat,
    };

    struct TWriteIo {
        ui64 IoId = 0;
        TChunkIdx ChunkIdx = 0;
        ui32 OffsetInBytes = 0;
        TRcBuf Data; // page-aligned, ready for direct I/O
        EWriteIoKind Kind = EWriteIoKind::Pair;
    };

    // Actor must read Size bytes and call OnReadIoCompleted(IoId, Data) on success. Integrity pair
    // reads are always one adjacent A/B pair (8 KiB).
    struct TReadIo {
        ui64 IoId = 0;
        TChunkIdx ChunkIdx = 0;
        ui32 OffsetInBytes = 0;
        ui32 Size = 0;
    };

    using TAction = std::variant<TAllocateIntegrityChunk, TWriteIo, TReadIo>;

    enum class EOperationKind {
        Write,
        Read,
    };

    enum class EOperationStatus {
        Ok,
        Corrupted,
    };

    struct TOperationResult {
        ui64 OperationId = 0;
        EOperationKind Kind = EOperationKind::Write;
        EOperationStatus Status = EOperationStatus::Ok;
        TString ErrorReason;
        bool LostWriteDetected = false;

        // Read only. One pure checksum per requested 4 KiB block.
        std::vector<ui64> Checksums;
    };

    // ---- read plans ----

    struct TReadPlan {
        enum EKind {
            Passthrough, // read from disk as-is because every block of the range is used
            AllZero,     // no block of the range was ever written: reply zeros without disk I/O
            Mixed,       // read from disk, then zero the unused blocks according to UsedBlocks
        };

        EKind Kind = Passthrough;
        // Mixed only: bit i corresponds to the i-th IntegrityUnitSize block of the requested range;
        // set = keep disk data, unset = zero-fill.
        TDynBitMap UsedBlocks;
    };

    // ---- persistence hooks ----

    struct TMappingSnapshot {
        struct TIntegrityChunkEntry {
            TChunkIdx ChunkIdx = 0;
            ui64 Generation = 0;
        };

        struct TExtentEntry {
            TDataChunkKey Key;
            TChunkIdx DataChunkIdx = 0;
            TExtentRef Ref;
        };

        std::vector<TIntegrityChunkEntry> IntegrityChunks;
        std::vector<TExtentEntry> Extents;
        // Last generation value ever assigned (see AllocateGeneration); restore resumes past the
        // maximum of this watermark and every generation in the restored records.
        ui64 GenerationCounter = 0;
    };

public:
    // Approximate memory cost of one cached TIntegrityBlockState; the ctor's checksumCacheBytes
    // budget is converted to a state count with it (tests pass N * BlockStateApproxBytes).
    static constexpr size_t BlockStateApproxBytes =
        128 /* struct + hash map overhead */ + ChecksumsPerIntegrityBlock * sizeof(ui64)
        + ChecksumsPerIntegrityBlock / 8;

    static constexpr ui64 DefaultChecksumCacheBytes = 64ull << 20;

public:
    // Geometry is derived from the data chunk size so that unit tests can use small chunks.
    // ddiskId / pdiskGuid are stamped into TIntegrityChunkHeader. checksumCacheBytes bounds the
    // total memory spent on cached checksums/digests (see the memory note above).
    TIntegrityManager(ui64 dataChunkSizeBytes, ui64 ddiskId, ui64 pdiskGuid,
        ui64 checksumCacheBytes = DefaultChecksumCacheBytes);

    [[nodiscard]] std::vector<TAction> TakeActions();
    bool HasActions() const { return !Actions.empty(); }

    // Keys whose extents were assigned a slot (IntegrityChunk found) since the last take.
    // The actor uses this to open the data-write path in parallel with formatting.
    [[nodiscard]] std::vector<TDataChunkKey> TakePlacedKeys();

    // ---- data chunk lifecycle ----

    // A new data chunk was allocated: starts extent assignment (may queue TAllocateIntegrityChunk
    // and/or extent-format TWriteIo actions). The chunk is *placed* once it has an IntegrityChunk
    // (slot assigned) and *Ready* once its extent format I/O and the owning chunk's header writes
    // have both completed.
    void OnDataChunkAllocated(TDataChunkKey key, TChunkIdx dataChunkIdx);

    // Starts a durable tablet deletion. Matching extents stop participating in snapshots and
    // pending assignment, but their slots remain withheld until CommitTabletChunksDeletion():
    // reusing a slot before the deletion snapshot commits could overwrite an extent that recovery
    // would still map to the old data chunk.
    void PrepareTabletChunksDeletion(ui64 tabletId);

    // Completes a prepared deletion after its removal snapshot commits. The extents are erased and
    // their slots become available for pending allocations / integrity-chunk reclamation.
    void CommitTabletChunksDeletion(ui64 tabletId);

    // ---- integrity chunk / I/O completions ----

    ui64 GetGenerationCounter() const { return GenerationCounter; }

    // Fulfills one previously queued TAllocateIntegrityChunk. Draws the next IntegrityChunkGeneration
    // and queues all header replica writes in parallel. Extents may be assigned immediately
    // (placed) into this still-formatting chunk.
    void OnIntegrityChunkAllocated(TChunkIdx chunkIdx);

    // Cancels one queued-but-unfulfilled TAllocateIntegrityChunk when the remaining supply still
    // covers all pending extents (demand may vanish when a tablet deletion frees slots). Returns
    // false when the allocation is still needed and must proceed.
    bool CancelChunkAllocationIfExcess();

    // Removes and returns the integrity chunks that can be released back to PDisk: header writes
    // settled (Ready), every slot free (slots withheld by in-flight orphaned format writes do not
    // count as free, so no extent I/O targets these chunks) and no pending extent demand - pending
    // extents are assigned into free slots first, which may queue their format writes.
    std::vector<TChunkIdx> TakeReleasableIntegrityChunks();

    // Reports successful completion of a TWriteIo. Returns the data chunk keys whose extents
    // became Ready as a result (empty for chunk header writes that do not finish an extent).
    std::vector<TDataChunkKey> OnIoCompleted(ui64 ioId);
    void OnReadIoCompleted(ui64 ioId, TRope data);

    // True when the key needs no further integrity work before its mapping may be logged: its
    // extent is formatted and the owning chunk's headers are written.
    bool IsExtentReady(TDataChunkKey key) const;

    // ---- write path (called at data-write submission) ----

    // Starts persistence of the supplied pure data checksums. The range must be 4 KiB aligned and
    // carry exactly one checksum per block. Completion is reported by TakeCompletedOperations only
    // after every affected pair image is durable.
    ui64 BeginBlocksWrite(TDataChunkKey key, ui32 offsetInBytes, ui32 size,
        const std::vector<ui64>& checksums);

    // ---- read path ----

    TReadPlan MakeReadPlan(TDataChunkKey key, ui32 offsetInBytes, ui32 size) const;

    // Starts a metadata read and returns an operation id. The result is always delivered through
    // TakeCompletedOperations (possibly immediately, without queued I/O).
    ui64 BeginChecksumRead(TDataChunkKey key, ui32 offsetInBytes, ui32 size);
    [[nodiscard]] std::vector<TOperationResult> TakeCompletedOperations();

    // ---- persistence hooks ----

    // Captures the Ready part of the mapping (integrity chunks + key -> extent). In-flight
    // allocations are deliberately excluded: they will be redone after recovery.
    TMappingSnapshot SnapshotMapping() const;

    // Rebuilds the mapping from a snapshot; the manager must be freshly constructed. Bitmaps and
    // checksums are not part of the snapshot, so restored extents are marked BitmapUnknown: reads
    // of them pass through unchanged (a later phase restores bitmaps from the extents on disk).
    // Every restored chunk is Ready: a durable increment is only logged after formatting.
    void ApplyMappingSnapshot(const TMappingSnapshot& snapshot);

    // ---- geometry / introspection (for the actor and unit tests) ----

    ui32 DataBlocksInChunk() const { return DataBlocksPerChunkCount; }
    ui32 BlocksPerExtent() const { return BlocksPerExtentCount; }
    ui32 ExtentsPerChunk() const { return ExtentsPerChunkCount; }
    size_t ExtentOnDiskSize() const { return ExtentOnDiskSizeBytes; }

    static constexpr ui32 ChunkHeaderReplicaCount = 3;
    // Offset of the i-th TIntegrityChunkHeader replica within the chunk.
    ui32 ChunkHeaderReplicaOffset(ui32 replica) const;
    // Offset of the extent slot within the chunk.
    ui32 ExtentOffset(ui32 extentSlot) const;

    const TExtentRef* FindExtentRef(TDataChunkKey key) const;
    ui64 GetIntegrityChunkGeneration(TChunkIdx chunkIdx) const;
    // True once all header replicas of the chunk were written (State == Ready). False for chunks
    // the manager does not know yet.
    bool IsIntegrityChunkFormatted(TChunkIdx chunkIdx) const;
    // Digest of the given TIntegrityBlock (pair) of the key's extent; 0 when nothing was recorded
    // (or the state was evicted).
    ui64 GetIntegrityBlockDigest(TDataChunkKey key, ui32 integrityBlockIdx) const;
    // Recorded checksum of the given data block; returns false when the block has no known checksum.
    bool GetBlockChecksum(TDataChunkKey key, ui32 blockIdx, ui64* checksum) const;
    // Currently cached TIntegrityBlockState count and the cache capacity (for unit tests).
    size_t CachedBlockStates() const { return BlockStateCount; }
    size_t MaxCachedBlockStates() const { return MaxBlockStates; }
    bool HasInFlightOperationsForTablet(ui64 tabletId) const;

private:
    enum class EChunkState {
        Formatting, // TIntegrityChunkHeader replica writes are in flight
        Ready,
    };

    struct TIntegrityChunkInfo {
        EChunkState State = EChunkState::Formatting;
        ui64 Generation = 0;
        std::vector<ui32> FreeSlots; // kept descending, so the smallest slot is assigned first
        ui32 HeaderWritesRemaining = 0;
        // Extents whose format write has completed while this chunk is still Formatting.
        std::vector<TDataChunkKey> WaitingForChunkReady;
    };

    enum class EExtentState {
        Pending,    // waiting for an integrity chunk with a free slot
        Formatting, // extent-format write is in flight, or done and waiting for chunk headers
        Ready,
    };

    // Checksums and digest of one TIntegrityBlock (ChecksumsPerIntegrityBlock data blocks),
    // allocated lazily on the first checksummed write to its range and evictable via the
    // manager-wide LRU (checksums, Known and Digest live and die together).
    struct TIntegrityBlockState : TIntrusiveListItem<TIntegrityBlockState> {
        TDataChunkKey Key;   // owning extent, for eviction
        ui32 PairIdx = 0;    // TIntegrityBlock pair index within the extent
        ui64 PairSequenceNumber = 0;
        TDynBitMap Known;              // per checksum slot: Checksums[slot] is recorded
        std::vector<ui64> Checksums;   // ChecksumsPerIntegrityBlock entries
    };

    enum class EPairSlot : ui8 {
        Unknown,
        A,
        B,
    };

    // Small, pinned state used for lost-write detection. Checksum arrays remain evictable, but the
    // expected digest must survive their eviction.
    struct TPairMeta {
        ui64 Digest = 0;
        ui32 OperationPins = 0;
        EPairSlot CurrentSlot = EPairSlot::Unknown;
        bool DigestKnown : 1 = false;
        bool BitmapKnown : 1 = false;
        bool Resident : 1 = false;
        bool Corrupted : 1 = false;
    };

    static_assert(sizeof(TPairMeta) <= 16);

    // Sparse state only for pairs with queued/in-flight work (or a remembered corruption). It is
    // removed when a pair becomes idle, keeping the per-disk pinned footprint at TPairMeta size.
    struct TPairRuntime {
        ui64 MutationVersion = 0;
        ui64 DurableVersion = 0;
        ui64 ReadIoId = 0;
        ui64 WriteIoId = 0;
        ui64 WriteVersion = 0;
        bool Dirty = false;
        bool LostWriteCorruption = false;
        TString CorruptionReason;
        std::vector<ui64> LoadWaiters;
        std::vector<std::pair<ui64, ui64>> DurabilityWaiters; // operation id, required version
    };

    struct TExtentInfo {
        TExtentRef Ref; // valid once State >= Formatting
        EExtentState State = EExtentState::Pending;
        TChunkIdx DataChunkIdx = 0;
        ui64 FormatIoId = 0; // the in-flight extent-format write; valid while the write is in flight
        bool FormatComplete = false;
        // Set while the actor's tablet-removal snapshot is in flight. The extent is absent from
        // logical snapshots, but its physical slot is quarantined until that record is durable.
        bool DeletionPending = false;

        // Empty until the first write to the chunk; never evicted (reads depend on it).
        TDynBitMap UsedBlocks; // per data block of the chunk
        // One pinned entry per on-disk A/B pair.
        std::vector<TPairMeta> Pairs;
        absl::flat_hash_map<ui32, TPairRuntime> PairRuntime;
        // Sparse per-TIntegrityBlock checksum/digest states, keyed by TIntegrityBlock index.
        absl::flat_hash_map<ui32, std::unique_ptr<TIntegrityBlockState>> BlockStates;
    };

    struct THeaderWriteRef {
        TChunkIdx ChunkIdx = 0;
        ui32 Replica = 0;
    };

    // Format write of a live extent. FreeExtent rewrites the ref of a freed extent to
    // TOrphanedExtentFormatRef, so on completion this ref always identifies the extent whose
    // FormatIoId issued it - a stale completion can never complete a reused key.
    struct TExtentFormatRef {
        TDataChunkKey Key;
    };

    // Format write whose extent was freed while the write was in flight. The slot is withheld
    // from the free list until this completion: reusing it earlier could let the old write land
    // after the new one, leaving a stale-generation image on disk.
    struct TOrphanedExtentFormatRef {
        TChunkIdx ChunkIdx = 0;
        ui32 ExtentSlot = 0;
    };

    struct TPairReadRef {
        TDataChunkKey Key;
        ui32 PairIdx = 0;
    };

    struct TPairWriteRef {
        TDataChunkKey Key;
        ui32 PairIdx = 0;
        ui64 Version = 0;
        EPairSlot Slot = EPairSlot::Unknown;
    };

    using TIoRef = std::variant<THeaderWriteRef, TExtentFormatRef, TOrphanedExtentFormatRef,
        TPairReadRef, TPairWriteRef>;

    struct TPendingOperation {
        EOperationKind Kind = EOperationKind::Write;
        TDataChunkKey Key;
        ui32 OffsetInBytes = 0;
        ui32 Size = 0;
        std::vector<ui64> Checksums;
        ui32 PendingLoads = 0;
        ui32 PendingDurability = 0;
        bool Applied = false;
        bool PairsPinned = false;
    };

private:
    ui64 AllocateGeneration() { return ++GenerationCounter; }
    void EnsureChunkCapacity();
    void TryAssignExtents();
    void QueueChunkHeaderWrite(TChunkIdx chunkIdx, ui32 replica);
    void QueueExtentFormatWrite(TDataChunkKey key, TExtentInfo& extent);
    void FreeExtent(TDataChunkKey key, TExtentInfo& extent);
    void ReleaseSlot(TChunkIdx chunkIdx, ui32 extentSlot);
    void MaybeCompleteExtent(TDataChunkKey key, TExtentInfo& extent, std::vector<TDataChunkKey>& readyKeys);
    ui32 FirstPair(ui32 offsetInBytes) const;
    ui32 EndPair(ui32 offsetInBytes, ui32 size) const;
    void QueuePairRead(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx);
    void QueuePairWrite(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx);
    void ApplyWriteOperation(ui64 operationId, TPendingOperation& operation);
    void CompleteReadOperation(ui64 operationId, TPendingOperation& operation);
    void CompleteOperation(ui64 operationId, EOperationStatus status = EOperationStatus::Ok,
        TString errorReason = {}, bool lostWriteDetected = false);
    void NotifyPairLoaded(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx);
    void NotifyPairDurable(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx);
    bool LoadPairImage(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx, const TRope& data,
        TString* errorReason, bool* lostWriteDetected);
    TIntegrityBlockIdentity MakeBlockIdentity(TDataChunkKey key, const TExtentInfo& extent,
        ui32 pairIdx) const;
    bool PairHasAllUsedChecksums(const TExtentInfo& extent, ui32 pairIdx,
        const TIntegrityBlockState& state) const;
    TPairRuntime& GetPairRuntime(TExtentInfo& extent, ui32 pairIdx);
    void MaybeDropPairRuntime(TExtentInfo& extent, ui32 pairIdx);

    // Get-or-create the state of the given TIntegrityBlock, touching the LRU; creation may evict
    // the least recently used state (of any extent) when over budget.
    TIntegrityBlockState& GetOrCreateBlockState(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx);
    // Lookup without creation (still touches the LRU); nullptr when absent (never written/evicted).
    TIntegrityBlockState* FindBlockState(TExtentInfo& extent, ui32 pairIdx);
    void EvictBlockStatesOverBudget();
    void DropBlockStates(TExtentInfo& extent);

private:
    // Geometry, computed once in the ctor.
    const ui64 DataChunkSize;
    const ui32 DataBlocksPerChunkCount;
    const ui32 BlocksPerExtentCount;
    const size_t ExtentOnDiskSizeBytes;
    const ui32 ExtentsPerChunkCount;

    const ui64 DDiskId;
    const ui64 PDiskGuid;

    absl::flat_hash_map<TChunkIdx, TIntegrityChunkInfo> IntegrityChunks;
    absl::flat_hash_map<TDataChunkKey, TExtentInfo> Extents;

    // Monotonic source of every VChunkGeneration / IntegrityChunkGeneration; persisted as a
    // snapshot watermark, so reuse after free keeps bumping generations even across restarts
    // (lost-write protection).
    ui64 GenerationCounter = 0;

    std::deque<TDataChunkKey> PendingExtents;
    ui32 PendingChunkAllocations = 0; // TAllocateIntegrityChunk actions not yet fulfilled
    std::vector<TDataChunkKey> PlacedKeys;

    // LRU over all cached TIntegrityBlockStates: front is the eviction victim.
    TIntrusiveList<TIntegrityBlockState> BlockStateLru;
    size_t BlockStateCount = 0;
    const size_t MaxBlockStates;

    std::vector<TAction> Actions;
    absl::flat_hash_map<ui64, TIoRef> IosInFlight;
    ui64 NextIoId = 1;
    absl::flat_hash_map<ui64, TPendingOperation> PendingOperations;
    std::vector<TOperationResult> CompletedOperations;
    ui64 NextOperationId = 1;
};

} // namespace NKikimr::NDDisk
