#pragma once

#include "defs.h"

#include "ddisk_checksums.h"
#include "read_result.h"

#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/async/event.h>
#include <optional>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <contrib/restricted/abseil-cpp/absl/container/node_hash_map.h>
#include <contrib/restricted/abseil-cpp/absl/container/inlined_vector.h>

#include <util/generic/bitmap.h>
#include <util/generic/intrlist.h>
#include <util/generic/array_ref.h>

#include <deque>
#include <memory>
#include <vector>

namespace NKikimr::NDDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TIntegrityManager
//
// Owns integrity allocation, shared pair loads and exclusive metadata writer ownership.
// Pair preparation and completion are ordinary actor-local operations; DDisk submits
// their descriptors without launching read coroutines. Handles retain completed results.
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
// Extent formatting writes valid TIntegrityBlock images with empty bitmaps. Data writes persist
// updated bitmaps, checksums and digests in ping-pong TIntegrityBlock pairs. Restored extents start
// with unknown bitmaps and lazily load the pairs needed by reads or writes.
//
// Memory: used-block bitmaps are small (1 bit per 4 KiB data block) and are kept per extent,
// never evicted - reads depend on them. The expected digest and current slot are also pinned per
// pair so an acknowledged lost metadata write remains detectable after cache eviction. Checksum
// images are kept sparsely, one immutable 4 KiB image per resident metadata pair,
// bounded by a manager-wide LRU budget and loaded again from the slot pair after eviction.
//
// On-disk layout of an integrity chunk (same size as a data chunk):
//   [0, IntegrityChunkHeaderRegionSize)  - TIntegrityChunkHeader replicas
//   then ExtentsPerChunk() extents, each occupying ExtentOnDiskSize() bytes: BlocksPerExtent()
//   ping-pong pairs of two adjacent 4 KiB TIntegrityBlock slots (A then B). Formatting writes both
//   slots with PairSequenceNumber 0 (A) and 1 (B), so slot B starts as the current one.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TIntegrityManager {
    struct TCacheEntry;
    struct TWriterState;

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

    enum class EOperationStatus { Ok, Corrupted, Failed };

    struct TIoResult {
        bool Ok = false;
        TReadPayload Data;
    };

    // One contiguous metadata I/O. The descriptor ID is opaque to the submitter;
    // only the manager interprets the returned image and its integrity slots.
    struct TMetadataRead {
        ui64 Id = 0;
        TChunkIdx ChunkIdx = 0;
        ui32 OffsetInBytes = 0;
        ui32 Size = 0;
    };

    struct TMetadataReadResult {
        ui64 Id = 0;
        TIoResult Result;
    };

    // One preparation claims at most one read. A crossing range is still one I/O.
    using TMetadataReads = absl::InlinedVector<TMetadataRead, 1>;

    struct TWriteSubmission {
        ui64 Id;
        TChunkIdx ChunkIdx;
        ui32 OffsetInBytes;
        TRcBuf Data;
    };

    struct TWork {
        std::vector<ui64> Allocations;
        std::vector<TChunkIdx> ReturnedChunks;
        std::vector<TWriteSubmission> Writes;
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

    struct TOperationResult {
        EOperationStatus Status = EOperationStatus::Ok;
        TString ErrorReason;
        bool LostWriteDetected = false;
        TReadChecksums Checksums;
        TReadPlan ReadPlan;
    };

    struct TDependency;
    struct TExtentState;
    struct TDependency {
        NActors::TAsyncEvent Changed;
        bool NotificationQueued = false;
    };

    struct TOperationState : TDependency {
        // Notified only after Result is set; completion is retained for every waiter.
        std::optional<TOperationResult> Result;
    };

    class TOperation {
    public:
        TOperation() = default;

        explicit TOperation(std::shared_ptr<TOperationState> state) : State(std::move(state)) {
        }

        const TOperationResult* GetResult() const {
            return State && State->Result ? &*State->Result : nullptr;
        }

        // A terminal result owns its snapshot and has retired every accepted dependency.
        bool IsDone() const {
            return GetResult();
        }

        NActors::NDetail::TAsyncEventAwaiter WaitChanged() const {
            Y_ABORT_UNLESS(State);
            return State->Changed.Wait();
        }

    private:
        std::shared_ptr<TOperationState> State;
    };

    struct TReadPreparation {
        // Immediate snapshot, including a failure that needs no metadata wait.
        // Empty while metadata is still outstanding on Pending.
        std::optional<TOperationResult> Warm;
        TOperation Pending;
        // Only newly claimed loads. Existing loads are joined through Pending.
        TMetadataReads MetadataReads;
    };

    // Exclusive pair ownership. Waiting ownership and a selected resuming writer retain
    // stable entries; destruction cancels an unsubmitted operation and releases its claims.
    class TWriteOperation {
    public:
        TWriteOperation() = default;
        TWriteOperation(TWriteOperation&&) noexcept;
        TWriteOperation& operator=(TWriteOperation&&) noexcept;
        TWriteOperation(const TWriteOperation&) = delete;
        TWriteOperation& operator=(const TWriteOperation&) = delete;
        ~TWriteOperation();

        bool IsReady();
        const TOperationResult* GetResult() const;
        NActors::NDetail::TAsyncEventAwaiter WaitChanged() const;

        void Cancel();

    private:
        friend class TIntegrityManager;
        TIntegrityManager* Manager = nullptr;
        std::shared_ptr<TWriterState> State;
    };

    // All fields used by I/O callbacks are owned here. Transform is independent of
    // manager/cache/actor state and can run on the native router's completion thread.
    struct TMetadataWrite {
        struct TPair {
            TIntegrityBlockIdentity Identity;
            bool DigestKnown = false;
            ui64 ExpectedDigest = 0;
            ui32 CurrentSlot = 0;
            TRcBuf CurrentImage;
            TRcBuf UpdatedImage;
        };

        TChunkIdx ChunkIdx = 0;
        ui32 ReadOffset = 0;
        ui32 ReadSize = 0;
        ui32 WriteOffset = 0;
        TRcBuf WriteImage;
        ui64 DDiskId = 0;
        ui64 PDiskGuid = 0;
        ui32 Offset = 0;
        TReadChecksums Checksums;
        absl::InlinedVector<TPair, 2> Pairs;
        TOperationResult Result;

        bool Transform(const TReadPayload& data = {});

    private:
        friend class TIntegrityManager;
        std::shared_ptr<TWriterState> Owner;
    };

    struct TExtentState : TDependency {
        bool Placed = false;
        bool Ready = false;
        bool Failed = false;
    };
    class TExtent {
    public:
        explicit TExtent(std::shared_ptr<TExtentState> state) : State(std::move(state)) {
        }

        std::optional<bool> GetPlacedResult() const {
            if (State->Placed || State->Failed) {
                return State->Placed;
            }
            return std::nullopt;
        }

        std::optional<bool> GetReadyResult() const {
            if (State->Ready || State->Failed) {
                return State->Ready;
            }
            return std::nullopt;
        }

        auto WaitChanged() const {
            return State->Changed.Wait();
        }

    private:
        std::shared_ptr<TExtentState> State;
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
    // Approximate memory cost of one cached metadata image; the ctor's checksumCacheBytes
    // budget is converted to a state count with it (tests pass N * BlockStateApproxBytes).
    static constexpr size_t BlockStateApproxBytes =
        128 /* struct + hash map overhead */ + ChecksumsPerIntegrityBlock * sizeof(ui64)
        + ChecksumsPerIntegrityBlock / 8;

    static constexpr ui64 DefaultChecksumCacheBytes = 64ull << 20;

public:
    // Geometry is derived from the data chunk size so that unit tests can use small chunks.
    // ddiskId / pdiskGuid are stamped into TIntegrityChunkHeader. checksumCacheBytes bounds the
    // memory spent on evictable checksum arrays and their cache state (see the memory note above).
    TIntegrityManager(ui64 dataChunkSizeBytes, ui64 ddiskId, ui64 pdiskGuid,
        ui64 checksumCacheBytes = DefaultChecksumCacheBytes);

    [[nodiscard]] std::pair<TExtent, TWork> StartExtent(TDataChunkKey key, TChunkIdx dataChunkIdx);
    // Preparation claims and pins metadata but does not submit I/O. A cold range claims
    // one contiguous metadata read: one ping-pong pair, or the span from the first claimed
    // pair through the last when the range crosses a boundary. The caller batches that
    // read with its data read. Every claimed descriptor requires completion.
    // Warm carries the snapshot when it is already known.
    TReadPreparation PrepareRead(TDataChunkKey key, ui32 offsetInBytes, ui32 size);
    void CompleteMetadataReads(TConstArrayRef<TMetadataReadResult> results);
    [[nodiscard]] TWork CompleteAllocation(ui64 token, TChunkIdx chunkIdx);
    [[nodiscard]] TWork CompleteWrite(ui64 id, bool ok);
    TWriteOperation PrepareWrite(TDataChunkKey key, ui32 offsetInBytes, ui32 size);
    std::shared_ptr<TMetadataWrite> PrepareMetadataWrite(TWriteOperation& operation,
        TConstArrayRef<ui64> checksums);
    void CompleteMetadataWrite(const std::shared_ptr<TMetadataWrite>& context, bool ok);
    // State and immutable results are complete before targeted observers run.
    void NotifyCompleted();
    // Close admission. Operations retain their pins until accepted dependencies retire.
    void Stop();

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

    // Removes and returns the integrity chunks that can be released back to PDisk: header writes
    // settled (Ready), every slot free (slots withheld by in-flight orphaned format writes do not
    // count as free, so no extent I/O targets these chunks) and no pending extent demand - pending
    // extents are assigned into free slots first, which may queue their format writes.
    [[nodiscard]] TWork TakeReleasableIntegrityChunks();

    bool IsExtentReady(TDataChunkKey key) const;

    // ---- persistence hooks ----

    // Captures the Ready part of the mapping (integrity chunks + key -> extent). In-flight
    // allocations are deliberately excluded: they will be redone after recovery.
    TMappingSnapshot SnapshotMapping() const;

    // Rebuilds the mapping from a snapshot; the manager must be freshly constructed. Bitmaps and
    // checksums are not part of the snapshot, so restored extents are marked with unknown bitmaps and load the
    // required pairs before returning checksums and a matching read plan.
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
    // Includes chunks whose headers or extents are still being formatted.
    std::vector<TChunkIdx> GetIntegrityChunkIdxs() const;
    ui64 GetIntegrityChunkCount() const { return IntegrityChunks.size(); }
    // True once all header replicas of the chunk were written (State == Ready). False for chunks
    // the manager does not know yet.
    bool IsIntegrityChunkFormatted(TChunkIdx chunkIdx) const;
    // Digest of the given metadata pair; retained independently of cache eviction.
    ui64 GetIntegrityBlockDigest(TDataChunkKey key, ui32 integrityBlockIdx) const;
    // Recorded checksum of the given data block; returns false when the block has no known checksum.
    bool GetBlockChecksum(TDataChunkKey key, ui32 blockIdx, ui64* checksum) const;
    // Currently cached metadata image count and the cache capacity (for unit tests).
    size_t CachedBlockStates() const { return BlockStateCount; }

    // Logical reads still owed a checksum result, including those joined to loads started
    // by another reader.
    size_t PendingReadCount() const { return PendingReads.size(); }
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
        bool HeaderWriteFailed = false;
    };

    // Digest, current slot and bitmap knowledge survive eviction of checksum images.
    struct TPairMeta {
        ui64 Digest = 0;
        ui8 CurrentSlot = 0;
        bool Known = false;
    };

    struct TPendingRead;

    enum class ECacheState : ui8 {
        Missing,
        WaitingRead,
        Data,
        WaitingWrite,
    };

    struct TCacheEntry : TIntrusiveListItem<TCacheEntry> {
        TDataChunkKey Key;
        ui32 PairIdx = 0;
        ui64 VChunkGeneration = 0;
        ECacheState State = ECacheState::Missing;
        TRcBuf Image;
        std::optional<TOperationResult> Failure;
        std::weak_ptr<TWriterState> Writer;
        std::deque<std::weak_ptr<TWriterState>> Writers;
        std::vector<std::weak_ptr<TPendingRead>> Readers;
    };

    struct TPendingRead {
        ui64 Id = 0;
        ui32 Offset = 0;
        ui32 Size = 0;
        ui32 Remaining = 0;
        TOperationResult Result;
        std::shared_ptr<TOperationState> Completion;
    };

    struct TWriterState : TDependency {
        TDataChunkKey Key;
        ui32 Offset = 0;
        ui32 Size = 0;
        size_t Acquired = 0;
        bool Submitted = false;
        bool Queued = false;
        std::optional<TOperationResult> Result;
        std::shared_ptr<TExtentState> Extent;
        absl::InlinedVector<std::shared_ptr<TCacheEntry>, 2> Pairs;
    };

    struct TFormatWrite {
        enum class EKind { ChunkHeader, ExtentFormat };

        EKind Kind;
        TChunkIdx ChunkIdx;
        ui64 ChunkGeneration;
        TDataChunkKey Key;
        TExtentRef Ref;
        std::shared_ptr<TExtentState> Completion;
    };

    struct TExtentInfo {
        TExtentRef Ref; // valid once Completion->Placed
        TChunkIdx DataChunkIdx = 0;
        bool Formatting = false;
        std::shared_ptr<TExtentState> Completion = std::make_shared<TExtentState>();
        bool FormatComplete = false;
        // Set while the actor's tablet-removal snapshot is in flight. The extent is absent from
        // logical snapshots, but its physical slot is quarantined until that record is durable.
        bool DeletionPending = false;

        // Empty until the first write to the chunk; never evicted (reads depend on it).
        TDynBitMap UsedBlocks; // per data block of the chunk
        // One pinned entry per on-disk A/B pair.
        std::vector<TPairMeta> Pairs;
        // Node-based storage plus retained handles keep every active lookup stable.
        absl::node_hash_map<ui32, std::shared_ptr<TCacheEntry>> Cache;

    };

private:
    TWork TakeWork();
    ui64 AllocateGeneration() { return ++GenerationCounter; }
    void EnsureChunkCapacity();
    void TryAssignExtents();
    void FreeExtent(TDataChunkKey key, TExtentInfo& extent);
    void ReleaseSlot(TChunkIdx chunkIdx, ui32 extentSlot);
    bool CancelChunkAllocationIfExcess();
    void OnIntegrityChunkAllocated(TChunkIdx chunkIdx);
    void CompleteFormatWrite(TFormatWrite write, bool ok);
    void MaybeCompleteExtent(TDataChunkKey key, TExtentRef ref,
        const std::shared_ptr<TExtentState>& completion);
    TRcBuf MakeHeaderImage(TChunkIdx chunkIdx, ui64 generation) const;
    TRcBuf MakeExtentImage(TDataChunkKey key, TExtentRef ref, ui64 generation) const;
    ui32 FirstPair(ui32 offsetInBytes) const;
    ui32 EndPair(ui32 offsetInBytes, ui32 size) const;
    TMetadataRead ClaimMetadataRead(TExtentInfo& extent,
        TArrayRef<const std::shared_ptr<TCacheEntry>> entries);
    void CompleteLoadedPair(const std::shared_ptr<TCacheEntry>& entry,
        const TOperationResult* ioFailure, const void* pairImage);
    void QueueDependency(const std::shared_ptr<TDependency>& dependency);
    void ValidateOperationRange(ui32 offset, ui32 size) const;
    TIntegrityBlockIdentity MakeBlockIdentity(TDataChunkKey key, const TExtentInfo& extent,
        ui32 pairIdx) const;
    std::shared_ptr<TCacheEntry> GetEntry(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx);
    void PublishImage(TExtentInfo& extent, TCacheEntry& entry, TRcBuf image, ui32 currentSlot);
    void CapturePair(TExtentInfo& extent, const TCacheEntry& entry, TPendingRead& read);
    void CompleteReaders(TExtentInfo& extent, TCacheEntry& entry);
    void FinishPendingRead(const std::shared_ptr<TPendingRead>& read, bool notify = true);
    bool AdvanceWriter(const std::shared_ptr<TWriterState>& writer);
    void ReleaseWriter(const std::shared_ptr<TWriterState>& writer);
    void WakeWriter(TCacheEntry& entry);
    void FailEntry(TExtentInfo& extent, TCacheEntry& entry, TOperationResult result, bool resolveReaders = true);
    void EvictBlockStatesOverBudget();
    void DropBlockStates(TExtentInfo& extent);

private:
    bool Stopped = false;
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
    // One accepted metadata read. Entries are the claimed pairs inside
    // [FirstPairIdx, FirstPairIdx + PairCount). Unclaimed pairs in that span
    // are read with them and ignored.
    struct TMetadataLoad {
        ui32 FirstPairIdx = 0;
        ui32 PairCount = 0;
        absl::InlinedVector<std::shared_ptr<TCacheEntry>, 2> Entries;
    };

    absl::flat_hash_map<ui64, TMetadataLoad> PairLoads;
    absl::flat_hash_map<ui64, std::shared_ptr<TPendingRead>> PendingReads;
    ui64 NextPairWriteId = 1;
    absl::flat_hash_map<ui64, TFormatWrite> FormatWrites;
    std::deque<std::shared_ptr<TDependency>> CompletedDependencies;
    TWork Work;
    ui64 NextPairReadId = 1;
    ui64 NextPendingReadId = 1;

    // Monotonic source of every VChunkGeneration / IntegrityChunkGeneration; persisted as a
    // snapshot watermark, so reuse after free keeps bumping generations even across restarts
    // (lost-write protection).
    ui64 GenerationCounter = 0;

    std::deque<TDataChunkKey> PendingExtents;
    absl::flat_hash_set<ui64> PendingChunkAllocations;
    ui64 NextAllocationToken = 1;

    // LRU over resident immutable metadata images: front is the eviction victim.
    TIntrusiveList<TCacheEntry> BlockStateLru;
    size_t BlockStateCount = 0;
    const size_t MaxBlockStates;


};

} // namespace NKikimr::NDDisk
