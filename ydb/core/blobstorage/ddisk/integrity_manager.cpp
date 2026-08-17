#include "integrity_manager.h"

#include <util/generic/overloaded.h>

#include <algorithm>
#include <cstring>

namespace NKikimr::NDDisk {

TIntegrityManager::TIntegrityManager(ui64 dataChunkSizeBytes, ui64 ddiskId, ui64 pdiskGuid,
        ui64 checksumCacheBytes)
    : DataChunkSize(dataChunkSizeBytes)
    , DataBlocksPerChunkCount(dataChunkSizeBytes / IntegrityUnitSize)
    , BlocksPerExtentCount((DataBlocksPerChunkCount + ChecksumsPerIntegrityBlock - 1) / ChecksumsPerIntegrityBlock)
    , ExtentOnDiskSizeBytes(size_t(BlocksPerExtentCount) * IntegrityUnitSize * IntegrityPairSlots)
    , ExtentsPerChunkCount((dataChunkSizeBytes - IntegrityChunkHeaderRegionSize) / ExtentOnDiskSizeBytes)
    , DDiskId(ddiskId)
    , PDiskGuid(pdiskGuid)
    , MaxBlockStates(Max<size_t>(1, checksumCacheBytes / BlockStateApproxBytes))
{
    Y_ABORT_UNLESS(dataChunkSizeBytes % IntegrityUnitSize == 0);
    Y_ABORT_UNLESS(dataChunkSizeBytes > IntegrityChunkHeaderRegionSize);
    Y_ABORT_UNLESS(ExtentsPerChunkCount >= 1);
}

std::vector<TIntegrityManager::TAction> TIntegrityManager::TakeActions() {
    return std::exchange(Actions, {});
}

std::vector<TIntegrityManager::TDataChunkKey> TIntegrityManager::TakePlacedKeys() {
    return std::exchange(PlacedKeys, {});
}

ui32 TIntegrityManager::ChunkHeaderReplicaOffset(ui32 replica) const {
    Y_ABORT_UNLESS(replica < ChunkHeaderReplicaCount);
    // Replicas are spread evenly across the header region so that a single localized corruption
    // cannot take out all of them.
    const ui32 headerRegionBlocks = IntegrityChunkHeaderRegionSize / IntegrityUnitSize;
    return replica * (headerRegionBlocks / ChunkHeaderReplicaCount) * IntegrityUnitSize;
}

ui32 TIntegrityManager::ExtentOffset(ui32 extentSlot) const {
    Y_ABORT_UNLESS(extentSlot < ExtentsPerChunkCount);
    return IntegrityChunkHeaderRegionSize + extentSlot * ExtentOnDiskSizeBytes;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Data chunk lifecycle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TIntegrityManager::OnDataChunkAllocated(TDataChunkKey key, TChunkIdx dataChunkIdx) {
    const auto [it, inserted] = Extents.try_emplace(key);
    Y_ABORT_UNLESS(inserted, "data chunk already tracked, TabletId# %" PRIu64 " VChunkIndex# %" PRIu64,
        key.TabletId, key.VChunkIndex);

    TExtentInfo& extent = it->second;
    extent.DataChunkIdx = dataChunkIdx;
    extent.Ref.VChunkGeneration = AllocateGeneration();

    PendingExtents.push_back(key);
    EnsureChunkCapacity();
    TryAssignExtents();
}

void TIntegrityManager::PrepareTabletChunksDeletion(ui64 tabletId) {
    bool found = false;
    for (auto& [key, extent] : Extents) {
        if (key.TabletId != tabletId) {
            continue;
        }
        Y_ABORT_UNLESS(!extent.DeletionPending);
        extent.DeletionPending = true;
        found = true;

        // A pending extent has no durable mapping and no physical slot yet. Stop it from being
        // assigned while the deletion record is in flight; CommitTabletChunksDeletion will erase
        // the extent itself.
        if (extent.State == EExtentState::Pending) {
            std::erase(PendingExtents, key);
        }
    }
    Y_ABORT_UNLESS(found, "tablet deletion has no integrity extents, TabletId# %" PRIu64, tabletId);
}

void TIntegrityManager::CommitTabletChunksDeletion(ui64 tabletId) {
    bool found = false;
    for (auto it = Extents.begin(); it != Extents.end(); ) {
        if (it->first.TabletId == tabletId) {
            Y_ABORT_UNLESS(it->second.DeletionPending);
            found = true;
            FreeExtent(it->first, it->second);
            Extents.erase(it++);
        } else {
            ++it;
        }
    }
    Y_ABORT_UNLESS(found, "tablet deletion was not prepared, TabletId# %" PRIu64, tabletId);
}

void TIntegrityManager::FreeExtent(TDataChunkKey key, TExtentInfo& extent) {
    DropBlockStates(extent);
    switch (extent.State) {
        case EExtentState::Pending:
            // Still queued for assignment: drop it from the queue.
            std::erase(PendingExtents, key);
            break;

        case EExtentState::Formatting: {
            if (extent.FormatIoId) {
                // The format write is still in flight: the slot must not be reused until it
                // settles, otherwise the old write could land after the new extent's format write
                // and leave a stale-generation image on disk. Orphan the I/O; its completion
                // releases the slot.
                const auto ioIt = IosInFlight.find(extent.FormatIoId);
                Y_ABORT_UNLESS(ioIt != IosInFlight.end());
                ioIt->second = TOrphanedExtentFormatRef{extent.Ref.IntegrityChunkIdx, extent.Ref.ExtentSlot};
            } else {
                // Format write already landed; the extent was waiting for chunk headers.
                TIntegrityChunkInfo& chunk = IntegrityChunks.at(extent.Ref.IntegrityChunkIdx);
                std::erase(chunk.WaitingForChunkReady, key);
                ReleaseSlot(extent.Ref.IntegrityChunkIdx, extent.Ref.ExtentSlot);
            }
            break;
        }

        case EExtentState::Ready:
            // No I/O in flight: the slot is immediately reusable.
            ReleaseSlot(extent.Ref.IntegrityChunkIdx, extent.Ref.ExtentSlot);
            break;
    }
}

void TIntegrityManager::ReleaseSlot(TChunkIdx chunkIdx, ui32 extentSlot) {
    const auto chunkIt = IntegrityChunks.find(chunkIdx);
    Y_ABORT_UNLESS(chunkIt != IntegrityChunks.end());
    chunkIt->second.FreeSlots.push_back(extentSlot);
    std::sort(chunkIt->second.FreeSlots.begin(), chunkIt->second.FreeSlots.end(), std::greater<ui32>());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Integrity chunk allocation and formatting
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TIntegrityManager::EnsureChunkCapacity() {
    size_t supply = size_t(PendingChunkAllocations) * ExtentsPerChunkCount;
    for (const auto& [chunkIdx, chunk] : IntegrityChunks) {
        supply += chunk.FreeSlots.size();
    }
    while (supply < PendingExtents.size()) {
        Actions.emplace_back(TAllocateIntegrityChunk{});
        ++PendingChunkAllocations;
        supply += ExtentsPerChunkCount;
    }
}

void TIntegrityManager::OnIntegrityChunkAllocated(TChunkIdx chunkIdx) {
    Y_ABORT_UNLESS(PendingChunkAllocations > 0);
    --PendingChunkAllocations;

    const auto [it, inserted] = IntegrityChunks.try_emplace(chunkIdx);
    Y_ABORT_UNLESS(inserted, "integrity chunk already in use, ChunkIdx# %" PRIu32, chunkIdx);

    TIntegrityChunkInfo& chunk = it->second;
    chunk.Generation = AllocateGeneration();
    chunk.FreeSlots.reserve(ExtentsPerChunkCount);
    for (ui32 slot = ExtentsPerChunkCount; slot > 0; --slot) {
        chunk.FreeSlots.push_back(slot - 1);
    }

    chunk.HeaderWritesRemaining = ChunkHeaderReplicaCount;
    for (ui32 replica = 0; replica < ChunkHeaderReplicaCount; ++replica) {
        QueueChunkHeaderWrite(chunkIdx, replica);
    }
    TryAssignExtents();
}

bool TIntegrityManager::CancelChunkAllocationIfExcess() {
    Y_ABORT_UNLESS(PendingChunkAllocations > 0);
    size_t supply = size_t(PendingChunkAllocations - 1) * ExtentsPerChunkCount;
    for (const auto& [chunkIdx, chunk] : IntegrityChunks) {
        supply += chunk.FreeSlots.size();
    }
    if (supply < PendingExtents.size()) {
        return false;
    }
    --PendingChunkAllocations;
    return true;
}

void TIntegrityManager::QueueChunkHeaderWrite(TChunkIdx chunkIdx, ui32 replica) {
    const TIntegrityChunkInfo& chunk = IntegrityChunks.at(chunkIdx);
    Y_ABORT_UNLESS(chunk.State == EChunkState::Formatting && replica < ChunkHeaderReplicaCount);

    auto data = TRcBuf::UninitializedPageAligned(sizeof(TIntegrityChunkHeader));
    auto* header = reinterpret_cast<TIntegrityChunkHeader*>(data.GetDataMut());
    memset(header, 0, sizeof(*header));
    header->Magic = MagicIntegrityChunkHeader;
    header->FormatVersion = static_cast<ui32>(EIntegrityFormatVersion::BaseAwupf4KiB);
    header->HeaderSize = sizeof(TIntegrityChunkHeader);
    header->DDiskId = DDiskId;
    header->PDiskGuid = PDiskGuid;
    header->IntegrityChunkId = chunkIdx;
    header->IntegrityChunkGeneration = chunk.Generation;
    header->HeaderChecksum = CalculateRawChecksum(header, sizeof(*header));

    const ui64 ioId = NextIoId++;
    IosInFlight.emplace(ioId, THeaderWriteRef{chunkIdx, replica});
    Actions.emplace_back(TWriteIo{ioId, chunkIdx, ChunkHeaderReplicaOffset(replica), std::move(data)});
}

std::vector<TChunkIdx> TIntegrityManager::TakeReleasableIntegrityChunks() {
    // Freed slots first satisfy pending extents (queueing their format writes); a chunk that is
    // still fully free afterwards has no demand left for its slots.
    TryAssignExtents();

    std::vector<TChunkIdx> released;
    for (auto it = IntegrityChunks.begin(); it != IntegrityChunks.end(); ) {
        const TIntegrityChunkInfo& chunk = it->second;
        // Formatting chunks have a header write in flight: skip them, they become releasable
        // once headers settle. A slot withheld by an orphaned format write keeps FreeSlots
        // below capacity, so a fully free chunk has no extent I/O in flight either.
        if (chunk.State == EChunkState::Ready && chunk.FreeSlots.size() == ExtentsPerChunkCount) {
            released.push_back(it->first);
            IntegrityChunks.erase(it++);
        } else {
            ++it;
        }
    }
    return released;
}

void TIntegrityManager::TryAssignExtents() {
    while (!PendingExtents.empty()) {
        // Find a chunk with a free slot (Formatting or Ready; smallest chunk index first for
        // determinism). Extents may be formatted in parallel with the chunk's own headers.
        TChunkIdx chunkIdx = 0;
        TIntegrityChunkInfo* chunk = nullptr;
        for (auto& [idx, info] : IntegrityChunks) {
            if (!info.FreeSlots.empty() && (!chunk || idx < chunkIdx)) {
                chunkIdx = idx;
                chunk = &info;
            }
        }
        if (!chunk) {
            return; // waiting for a chunk allocation
        }

        const TDataChunkKey key = PendingExtents.front();
        PendingExtents.pop_front();

        const auto it = Extents.find(key);
        Y_ABORT_UNLESS(it != Extents.end() && it->second.State == EExtentState::Pending);
        TExtentInfo& extent = it->second;

        extent.Ref.IntegrityChunkIdx = chunkIdx;
        extent.Ref.ExtentSlot = chunk->FreeSlots.back();
        chunk->FreeSlots.pop_back();
        extent.State = EExtentState::Formatting;

        PlacedKeys.push_back(key);
        QueueExtentFormatWrite(key, extent);
    }
}

void TIntegrityManager::QueueExtentFormatWrite(TDataChunkKey key, TExtentInfo& extent) {
    const TIntegrityChunkInfo& chunk = IntegrityChunks.at(extent.Ref.IntegrityChunkIdx);

    auto data = TRcBuf::UninitializedPageAligned(ExtentOnDiskSizeBytes);
    auto* blocks = reinterpret_cast<TIntegrityBlock*>(data.GetDataMut());
    memset(blocks, 0, ExtentOnDiskSizeBytes);

    for (ui32 pair = 0; pair < BlocksPerExtentCount; ++pair) {
        for (ui32 slot = 0; slot < IntegrityPairSlots; ++slot) {
            TIntegrityBlock& block = blocks[pair * IntegrityPairSlots + slot];
            TIntegrityBlockHeader& header = block.Header;
            header.Magic = MagicIntegrityBlock;
            header.FormatVersion = static_cast<ui16>(EIntegrityFormatVersion::BaseAwupf4KiB);
            header.ChecksumBlockIdx = pair;
            header.OwnerId = key.TabletId;
            header.VChunkId = key.VChunkIndex;
            header.VChunkGeneration = extent.Ref.VChunkGeneration;
            header.IntegrityChunkId = extent.Ref.IntegrityChunkIdx;
            header.IntegrityExtentId = extent.Ref.ExtentSlot;
            header.IntegrityChunkGeneration = chunk.Generation;
            // Slot A gets sequence 0, slot B gets 1, so B starts as the current slot of each pair.
            header.PairSequenceNumber = slot;
            header.BlockChecksum = CalculateRawChecksum(&block, sizeof(block));
        }
    }

    const ui64 ioId = NextIoId++;
    extent.FormatIoId = ioId;
    IosInFlight.emplace(ioId, TExtentFormatRef{key});
    Actions.emplace_back(TWriteIo{ioId, extent.Ref.IntegrityChunkIdx, ExtentOffset(extent.Ref.ExtentSlot),
        std::move(data)});
}

void TIntegrityManager::MaybeCompleteExtent(TDataChunkKey key, TExtentInfo& extent,
        std::vector<TDataChunkKey>& readyKeys) {
    Y_ABORT_UNLESS(extent.State == EExtentState::Formatting);
    TIntegrityChunkInfo& chunk = IntegrityChunks.at(extent.Ref.IntegrityChunkIdx);
    if (!extent.FormatComplete || chunk.State != EChunkState::Ready) {
        if (extent.FormatComplete && chunk.State != EChunkState::Ready) {
            chunk.WaitingForChunkReady.push_back(key);
        }
        return;
    }
    extent.State = EExtentState::Ready;
    if (!extent.DeletionPending) {
        readyKeys.push_back(key);
    }
}

std::vector<TIntegrityManager::TDataChunkKey> TIntegrityManager::OnIoCompleted(ui64 ioId) {
    const auto ioIt = IosInFlight.find(ioId);
    Y_ABORT_UNLESS(ioIt != IosInFlight.end(), "unknown IoId# %" PRIu64, ioId);
    const TIoRef ref = ioIt->second;
    IosInFlight.erase(ioIt);

    std::vector<TDataChunkKey> readyKeys;

    std::visit(TOverloaded{
        [&](const THeaderWriteRef& headerRef) {
            TIntegrityChunkInfo& chunk = IntegrityChunks.at(headerRef.ChunkIdx);
            Y_ABORT_UNLESS(chunk.State == EChunkState::Formatting && chunk.HeaderWritesRemaining > 0);
            --chunk.HeaderWritesRemaining;
            if (chunk.HeaderWritesRemaining > 0) {
                return;
            }
            chunk.State = EChunkState::Ready;
            auto waiting = std::exchange(chunk.WaitingForChunkReady, {});
            for (const TDataChunkKey& key : waiting) {
                const auto it = Extents.find(key);
                if (it == Extents.end() || it->second.State != EExtentState::Formatting) {
                    continue;
                }
                MaybeCompleteExtent(key, it->second, readyKeys);
            }
            TryAssignExtents();
        },
        [&](const TExtentFormatRef& formatRef) {
            // FreeExtent rewrites the ref of a freed extent to TOrphanedExtentFormatRef, so this
            // completion always belongs to the live extent that issued exactly this write.
            const auto it = Extents.find(formatRef.Key);
            Y_ABORT_UNLESS(it != Extents.end() && it->second.State == EExtentState::Formatting
                && it->second.FormatIoId == ioId);
            it->second.FormatIoId = 0;
            it->second.FormatComplete = true;
            MaybeCompleteExtent(formatRef.Key, it->second, readyKeys);
        },
        [&](const TOrphanedExtentFormatRef& orphanRef) {
            // The format write of a freed extent settled: the slot is now safe to reuse; a pending
            // extent may have been waiting for it.
            ReleaseSlot(orphanRef.ChunkIdx, orphanRef.ExtentSlot);
            TryAssignExtents();
        },
    }, ref);

    return readyKeys;
}

bool TIntegrityManager::IsExtentReady(TDataChunkKey key) const {
    const auto it = Extents.find(key);
    return it != Extents.end() && !it->second.DeletionPending
        && it->second.State == EExtentState::Ready;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Write path
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TIntegrityManager::TIntegrityBlockState& TIntegrityManager::GetOrCreateBlockState(TDataChunkKey key,
        TExtentInfo& extent, ui32 blockIdx) {
    const auto [it, inserted] = extent.BlockStates.try_emplace(blockIdx);
    if (inserted) {
        it->second = std::make_unique<TIntegrityBlockState>();
        TIntegrityBlockState& state = *it->second;
        state.Key = key;
        state.BlockIdx = blockIdx;
        state.Known.Reserve(ChecksumsPerIntegrityBlock);
        state.Checksums.resize(ChecksumsPerIntegrityBlock, 0);
        BlockStateLru.PushBack(&state);
        ++BlockStateCount;
        EvictBlockStatesOverBudget();
        return state;
    }
    TIntegrityBlockState& state = *it->second;
    state.Unlink();
    BlockStateLru.PushBack(&state); // touch
    return state;
}

TIntegrityManager::TIntegrityBlockState* TIntegrityManager::FindBlockState(TExtentInfo& extent, ui32 blockIdx) {
    const auto it = extent.BlockStates.find(blockIdx);
    if (it == extent.BlockStates.end()) {
        return nullptr;
    }
    TIntegrityBlockState& state = *it->second;
    state.Unlink();
    BlockStateLru.PushBack(&state); // touch
    return &state;
}

void TIntegrityManager::EvictBlockStatesOverBudget() {
    while (BlockStateCount > MaxBlockStates) {
        TIntegrityBlockState* victim = BlockStateLru.Front();
        const auto extentIt = Extents.find(victim->Key);
        Y_ABORT_UNLESS(extentIt != Extents.end());
        const size_t numErased = extentIt->second.BlockStates.erase(victim->BlockIdx);
        Y_ABORT_UNLESS(numErased == 1); // unlinks from the LRU via ~TIntrusiveListItem
        --BlockStateCount;
    }
}

void TIntegrityManager::DropBlockStates(TExtentInfo& extent) {
    BlockStateCount -= extent.BlockStates.size();
    extent.BlockStates.clear(); // each state unlinks from the LRU via ~TIntrusiveListItem
}

void TIntegrityManager::OnBlocksWritten(TDataChunkKey key, ui32 offsetInBytes, ui32 size,
        const std::vector<ui64>& checksums) {
    const auto it = Extents.find(key);
    Y_ABORT_UNLESS(it != Extents.end(), "write to unknown data chunk, TabletId# %" PRIu64 " VChunkIndex# %" PRIu64,
        key.TabletId, key.VChunkIndex);
    TExtentInfo& extent = it->second;

    Y_ABORT_UNLESS(size > 0 && ui64(offsetInBytes) + size <= DataChunkSize);

    // Blocks partially covered by an unaligned write are still marked used: the read path must
    // return their (partially updated) disk contents, not zeros.
    const ui32 firstBlock = offsetInBytes / IntegrityUnitSize;
    const ui32 endBlock = (offsetInBytes + size + IntegrityUnitSize - 1) / IntegrityUnitSize;

    const bool aligned = offsetInBytes % IntegrityUnitSize == 0 && size % IntegrityUnitSize == 0;
    const bool withChecksums = !checksums.empty();
    if (withChecksums) {
        Y_ABORT_UNLESS(aligned && checksums.size() == endBlock - firstBlock,
            "checksums must be per-4KiB-block of an aligned range: offset# %" PRIu32 " size# %" PRIu32
            " checksums# %zu", offsetInBytes, size, checksums.size());
    }

    for (ui32 block = firstBlock; block < endBlock; ++block) {
        extent.UsedBlocks.Set(block);

        const ui32 blockStateIdx = block / ChecksumsPerIntegrityBlock;
        const ui32 slot = block % ChecksumsPerIntegrityBlock;
        const ui64 generation = extent.Ref.VChunkGeneration;

        if (withChecksums) {
            TIntegrityBlockState& state = GetOrCreateBlockState(key, extent, blockStateIdx);
            const ui64 newCsum = checksums[block - firstBlock];
            if (state.Known.Get(slot)) {
                UpdateRoot(state.Digest, generation, block, state.Checksums[slot], newCsum);
            } else {
                state.Digest ^= Contribution(generation, block, newCsum);
                state.Known.Set(slot);
            }
            state.Checksums[slot] = newCsum;
        } else if (TIntegrityBlockState* state = FindBlockState(extent, blockStateIdx)) {
            // The block was overwritten without a checksum: the recorded one is now stale.
            if (state->Known.Get(slot)) {
                state->Digest ^= Contribution(generation, block, state->Checksums[slot]);
                state->Known.Reset(slot);
                state->Checksums[slot] = 0;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Read path
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TIntegrityManager::TReadPlan TIntegrityManager::MakeReadPlan(TDataChunkKey key, ui32 offsetInBytes, ui32 size) const {
    TReadPlan plan;

    const auto it = Extents.find(key);
    if (it == Extents.end()) {
        // Unknown chunk: pass through unchanged (safe fallback).
        return plan;
    }
    const TExtentInfo& extent = it->second;

    if (extent.BitmapUnknown) {
        // Restored extent: the previous incarnation's bitmap is lost, so any block may hold data.
        return plan;
    }

    if (extent.UsedBlocks.Empty()) {
        // Nothing was ever written to this chunk.
        plan.Kind = TReadPlan::AllZero;
        return plan;
    }

    if (offsetInBytes % IntegrityUnitSize != 0 || size % IntegrityUnitSize != 0) {
        // Unaligned ranges cannot be safely zero-masked per block; fall back to passthrough.
        // (DDisk validates requests against a 4 KiB sector size, so this should not happen.)
        return plan;
    }

    Y_ABORT_UNLESS(size > 0 && ui64(offsetInBytes) + size <= DataChunkSize);

    const ui32 firstBlock = offsetInBytes / IntegrityUnitSize;
    const ui32 numBlocks = size / IntegrityUnitSize;

    ui32 usedCount = 0;
    plan.UsedBlocks.Reserve(numBlocks);
    for (ui32 i = 0; i < numBlocks; ++i) {
        if (extent.UsedBlocks.Get(firstBlock + i)) {
            plan.UsedBlocks.Set(i);
            ++usedCount;
        }
    }

    if (usedCount == 0) {
        plan.Kind = TReadPlan::AllZero;
        plan.UsedBlocks.Clear();
    } else if (usedCount == numBlocks) {
        plan.Kind = TReadPlan::Passthrough;
        plan.UsedBlocks.Clear();
    } else {
        plan.Kind = TReadPlan::Mixed;
    }
    return plan;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Mapping snapshot
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TIntegrityManager::TMappingSnapshot TIntegrityManager::SnapshotMapping() const {
    TMappingSnapshot snapshot;
    snapshot.GenerationCounter = GenerationCounter;
    for (const auto& [chunkIdx, chunk] : IntegrityChunks) {
        if (chunk.State == EChunkState::Ready) {
            snapshot.IntegrityChunks.push_back({chunkIdx, chunk.Generation});
        }
    }
    for (const auto& [key, extent] : Extents) {
        if (extent.State == EExtentState::Ready && !extent.DeletionPending) {
            snapshot.Extents.push_back({key, extent.DataChunkIdx, extent.Ref});
        }
    }
    return snapshot;
}

void TIntegrityManager::ApplyMappingSnapshot(const TMappingSnapshot& snapshot) {
    Y_ABORT_UNLESS(IntegrityChunks.empty() && Extents.empty() && PendingExtents.empty() && IosInFlight.empty(),
        "mapping snapshot must be applied to a fresh manager");

    // Resume the generation counter past everything ever persisted. Generations handed out after
    // the snapshot watermark was taken can only appear in records logged after it, so the max
    // over the watermark and the restored records covers all durable state. A generation reused
    // from an uncommitted (crash-lost) record is benign: the identity fields plus the
    // format-before-use ordering already disambiguate such extents.
    GenerationCounter = Max(GenerationCounter, snapshot.GenerationCounter);

    for (const auto& entry : snapshot.IntegrityChunks) {
        const auto [it, inserted] = IntegrityChunks.try_emplace(entry.ChunkIdx);
        Y_ABORT_UNLESS(inserted);
        it->second.Generation = entry.Generation;
        it->second.State = EChunkState::Ready;
        GenerationCounter = Max(GenerationCounter, entry.Generation);
    }

    absl::flat_hash_map<TChunkIdx, TDynBitMap> usedSlots;
    for (const auto& entry : snapshot.Extents) {
        const auto chunkIt = IntegrityChunks.find(entry.Ref.IntegrityChunkIdx);
        Y_ABORT_UNLESS(chunkIt != IntegrityChunks.end() && entry.Ref.ExtentSlot < ExtentsPerChunkCount);

        const auto [it, inserted] = Extents.try_emplace(entry.Key);
        Y_ABORT_UNLESS(inserted);
        TExtentInfo& extent = it->second;
        extent.Ref = entry.Ref;
        extent.State = EExtentState::Ready;
        extent.DataChunkIdx = entry.DataChunkIdx;
        // The previous incarnation's used-block bitmap is not recoverable yet: reads must pass
        // through unchanged until a later phase restores bitmaps from the extents on disk.
        extent.BitmapUnknown = true;

        GenerationCounter = Max(GenerationCounter, entry.Ref.VChunkGeneration);

        usedSlots[entry.Ref.IntegrityChunkIdx].Set(entry.Ref.ExtentSlot);
    }

    for (auto& [chunkIdx, chunk] : IntegrityChunks) {
        const auto usedIt = usedSlots.find(chunkIdx);
        chunk.FreeSlots.reserve(ExtentsPerChunkCount);
        for (ui32 slot = ExtentsPerChunkCount; slot > 0; --slot) {
            if (usedIt == usedSlots.end() || !usedIt->second.Get(slot - 1)) {
                chunk.FreeSlots.push_back(slot - 1);
            }
        }
    }

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Introspection
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TIntegrityManager::TExtentRef* TIntegrityManager::FindExtentRef(TDataChunkKey key) const {
    const auto it = Extents.find(key);
    if (it == Extents.end() || it->second.DeletionPending
            || it->second.State == EExtentState::Pending) {
        return nullptr;
    }
    return &it->second.Ref;
}

ui64 TIntegrityManager::GetIntegrityChunkGeneration(TChunkIdx chunkIdx) const {
    const auto it = IntegrityChunks.find(chunkIdx);
    return it != IntegrityChunks.end() ? it->second.Generation : 0;
}

bool TIntegrityManager::IsIntegrityChunkFormatted(TChunkIdx chunkIdx) const {
    const auto it = IntegrityChunks.find(chunkIdx);
    return it != IntegrityChunks.end() && it->second.State == EChunkState::Ready;
}

ui64 TIntegrityManager::GetIntegrityBlockDigest(TDataChunkKey key, ui32 integrityBlockIdx) const {
    const auto it = Extents.find(key);
    Y_ABORT_UNLESS(it != Extents.end() && integrityBlockIdx < BlocksPerExtentCount);
    const auto stateIt = it->second.BlockStates.find(integrityBlockIdx);
    return stateIt != it->second.BlockStates.end() ? stateIt->second->Digest : 0;
}

bool TIntegrityManager::GetBlockChecksum(TDataChunkKey key, ui32 blockIdx, ui64* checksum) const {
    const auto it = Extents.find(key);
    Y_ABORT_UNLESS(it != Extents.end() && blockIdx < DataBlocksPerChunkCount);
    const auto stateIt = it->second.BlockStates.find(blockIdx / ChecksumsPerIntegrityBlock);
    if (stateIt == it->second.BlockStates.end()) {
        return false;
    }
    const TIntegrityBlockState& state = *stateIt->second;
    const ui32 slot = blockIdx % ChecksumsPerIntegrityBlock;
    if (!state.Known.Get(slot)) {
        return false;
    }
    *checksum = state.Checksums[slot];
    return true;
}

} // namespace NKikimr::NDDisk
