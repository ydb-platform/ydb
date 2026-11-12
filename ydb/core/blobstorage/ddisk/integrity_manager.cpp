#include "integrity_manager.h"

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
    , MaxBlockStates(checksumCacheBytes ? Max<size_t>(1, checksumCacheBytes / BlockStateApproxBytes) : 0)
{
    Y_ABORT_UNLESS(dataChunkSizeBytes % IntegrityUnitSize == 0);
    Y_ABORT_UNLESS(dataChunkSizeBytes > IntegrityChunkHeaderRegionSize);
    Y_ABORT_UNLESS(ExtentsPerChunkCount >= 1);
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

std::pair<TIntegrityManager::TExtent, TIntegrityManager::TWork> TIntegrityManager::StartExtent(TDataChunkKey key, TChunkIdx dataChunkIdx) {
    const auto [it, inserted] = Extents.try_emplace(key);
    Y_ABORT_UNLESS(inserted, "data chunk already tracked, TabletId# %" PRIu64 " VChunkIndex# %" PRIu64,
        key.TabletId, key.VChunkIndex);

    TExtentInfo& extent = it->second;
    extent.DataChunkIdx = dataChunkIdx;
    extent.Ref.VChunkGeneration = AllocateGeneration();
    extent.Pairs.resize(BlocksPerExtentCount);
    for (TPairMeta& pair : extent.Pairs) {
        pair.Known = true;
        pair.CurrentSlot = 1;
    }

    PendingExtents.push_back(key);
    auto completion = extent.Completion;
    TryAssignExtents();
    EnsureChunkCapacity();
    return {TExtent(std::move(completion)), TakeWork()};
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
        if (!extent.Completion->Placed) {
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
    for (const auto& [_, entry] : extent.Cache) {
        Y_ABORT_UNLESS(entry.use_count() == 1 && entry->Writer.expired());
    }
    extent.Completion->Failed = true;
    DropBlockStates(extent);
    if (!extent.Completion->Placed) {
        std::erase(PendingExtents, key);
    } else if (!extent.Formatting) {
        ReleaseSlot(extent.Ref.IntegrityChunkIdx, extent.Ref.ExtentSlot);
    }
    // A submitted format retains the original slot until its result, including after key reuse.
    QueueDependency(extent.Completion);
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
    if (Stopped) {
        return;
    }
    size_t supply = PendingChunkAllocations.size() * ExtentsPerChunkCount;
    for (const auto& [chunkIdx, chunk] : IntegrityChunks) {
        supply += chunk.FreeSlots.size();
    }
    while (supply < PendingExtents.size()) {
        const ui64 token = NextAllocationToken++;
        PendingChunkAllocations.insert(token);
        supply += ExtentsPerChunkCount;
        Work.Allocations.push_back(token);
    }
}

void TIntegrityManager::OnIntegrityChunkAllocated(TChunkIdx chunkIdx) {
    const auto [it, inserted] = IntegrityChunks.try_emplace(chunkIdx);
    Y_ABORT_UNLESS(inserted, "integrity chunk already in use, ChunkIdx# %" PRIu32, chunkIdx);

    TIntegrityChunkInfo& chunk = it->second;
    chunk.Generation = AllocateGeneration();
    chunk.FreeSlots.reserve(ExtentsPerChunkCount);
    for (ui32 slot = ExtentsPerChunkCount; slot > 0; --slot) {
        chunk.FreeSlots.push_back(slot - 1);
    }

    chunk.HeaderWritesRemaining = ChunkHeaderReplicaCount;
    // Publish all descriptors before the first submission can complete inline.
    for (ui32 replica = 0; replica < ChunkHeaderReplicaCount; ++replica) {
        const ui64 id = NextPairWriteId++;
        FormatWrites.emplace(id, TFormatWrite{
            .Kind = TFormatWrite::EKind::ChunkHeader,
            .ChunkIdx = chunkIdx,
            .ChunkGeneration = chunk.Generation,
        });
        Work.Writes.push_back(TWriteSubmission{id, chunkIdx, ChunkHeaderReplicaOffset(replica),
            MakeHeaderImage(chunkIdx, chunk.Generation)});
    }
    TryAssignExtents();
}

bool TIntegrityManager::CancelChunkAllocationIfExcess() {
    size_t supply = PendingChunkAllocations.size() * ExtentsPerChunkCount;
    for (const auto& [chunkIdx, chunk] : IntegrityChunks) {
        supply += chunk.FreeSlots.size();
    }
    return supply >= PendingExtents.size();
}

TRcBuf TIntegrityManager::MakeHeaderImage(TChunkIdx chunkIdx, ui64 generation) const {
    auto data = TRcBuf::UninitializedPageAligned(sizeof(TIntegrityChunkHeader));
    auto* header = reinterpret_cast<TIntegrityChunkHeader*>(data.GetDataMut());
    memset(header, 0, sizeof(*header));
    header->Magic = MagicIntegrityChunkHeader;
    header->FormatVersion = static_cast<ui32>(EIntegrityFormatVersion::BaseAwupf4KiB);
    header->HeaderSize = sizeof(TIntegrityChunkHeader);
    header->DDiskId = DDiskId;
    header->PDiskGuid = PDiskGuid;
    header->IntegrityChunkId = chunkIdx;
    header->IntegrityChunkGeneration = generation;
    header->HeaderChecksum = CalculateRawChecksum(header, sizeof(*header));
    return data;
}

TIntegrityManager::TWork TIntegrityManager::TakeReleasableIntegrityChunks() {
    // Freed slots first satisfy pending extents (queueing their format writes); a chunk that is
    // still fully free afterwards has no demand left for its slots.
    TryAssignExtents();

    auto& released = Work.ReturnedChunks;
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
    return TakeWork();
}

void TIntegrityManager::TryAssignExtents() {
    while (!Stopped && !PendingExtents.empty()) {
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
            break; // waiting for a chunk allocation; still drain previously registered work
        }

        const TDataChunkKey key = PendingExtents.front();
        PendingExtents.pop_front();

        const auto it = Extents.find(key);
        Y_ABORT_UNLESS(it != Extents.end() && !it->second.Completion->Placed);
        TExtentInfo& extent = it->second;

        extent.Ref.IntegrityChunkIdx = chunkIdx;
        extent.Ref.ExtentSlot = chunk->FreeSlots.back();
        chunk->FreeSlots.pop_back();
        extent.Formatting = true;
        const auto ref = extent.Ref;
        auto completion = extent.Completion;
        const ui64 id = NextPairWriteId++;
        FormatWrites.emplace(id, TFormatWrite{TFormatWrite::EKind::ExtentFormat, chunkIdx,
            chunk->Generation, key, ref, completion});
        Work.Writes.push_back(TWriteSubmission{id, chunkIdx, ExtentOffset(ref.ExtentSlot),
            MakeExtentImage(key, ref, chunk->Generation)});
        extent.Completion->Placed = true;
        QueueDependency(completion);
    }
}

TRcBuf TIntegrityManager::MakeExtentImage(TDataChunkKey key, TExtentRef ref, ui64 generation) const {
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
            header.VChunkGeneration = ref.VChunkGeneration;
            header.IntegrityChunkId = ref.IntegrityChunkIdx;
            header.IntegrityExtentId = ref.ExtentSlot;
            header.IntegrityChunkGeneration = generation;
            // Slot A gets sequence 0, slot B gets 1, so B starts as the current slot of each pair.
            header.PairSequenceNumber = slot;
            header.BlockChecksum = CalculateRawChecksum(&block, sizeof(block));
        }
    }


    return data;
}

void TIntegrityManager::MaybeCompleteExtent(TDataChunkKey key, TExtentRef ref,
        const std::shared_ptr<TExtentState>& completion)
{
    const auto it = Extents.find(key);
    if (it == Extents.end() || it->second.Completion != completion
            || it->second.Ref.VChunkGeneration != ref.VChunkGeneration
            || it->second.Formatting || !it->second.Completion->Placed) {
        return;
    }
    const auto chunkIt = IntegrityChunks.find(ref.IntegrityChunkIdx);
    if (chunkIt == IntegrityChunks.end() || chunkIt->second.HeaderWritesRemaining) {
        return;
    }
    auto& extent = it->second;
    if (completion->Ready || completion->Failed) {
        return;
    }
    if (!Stopped && extent.FormatComplete && chunkIt->second.State == EChunkState::Ready
            && !extent.DeletionPending) {
        completion->Ready = true;
    } else {
        completion->Failed = true;
    }
    EvictBlockStatesOverBudget();
    QueueDependency(completion);
}

void TIntegrityManager::CompleteFormatWrite(TFormatWrite write, bool ok) {
    const auto chunkIt = IntegrityChunks.find(write.ChunkIdx);
    if (chunkIt == IntegrityChunks.end() || chunkIt->second.Generation != write.ChunkGeneration) {
        // The original owner has retired; the same physical index may now have a new
        // generation. Its free-slot set must never be touched by this stale completion.
        return;
    }
    if (write.Kind == TFormatWrite::EKind::ChunkHeader) {
        auto& chunk = chunkIt->second;
        if (!ok) {
            chunk.HeaderWriteFailed = true;
        }
        Y_ABORT_UNLESS(chunk.HeaderWritesRemaining);
        if (!--chunk.HeaderWritesRemaining) {
            if (!Stopped && !chunk.HeaderWriteFailed) {
                chunk.State = EChunkState::Ready;
            }
            for (const auto& [key, extent] : Extents) {
                if (extent.Ref.IntegrityChunkIdx == write.ChunkIdx
                        && extent.Completion->Placed && !extent.Completion->Ready) {
                    MaybeCompleteExtent(key, extent.Ref, extent.Completion);
                }
            }
        }
    } else {
        const auto it = Extents.find(write.Key);
        if (it == Extents.end() || it->second.Completion != write.Completion
                || it->second.Ref.VChunkGeneration != write.Ref.VChunkGeneration) {
            ReleaseSlot(write.Ref.IntegrityChunkIdx, write.Ref.ExtentSlot);
            TryAssignExtents();
            return;
        }
        it->second.Formatting = false;
        it->second.FormatComplete = ok;
        MaybeCompleteExtent(write.Key, write.Ref, write.Completion);
    }
}

bool TIntegrityManager::IsExtentReady(TDataChunkKey key) const {
    const auto it = Extents.find(key);
    return it != Extents.end() && !it->second.DeletionPending
        && it->second.Completion->Ready;
}

ui32 TIntegrityManager::FirstPair(ui32 offsetInBytes) const {
    return (offsetInBytes / IntegrityUnitSize) / ChecksumsPerIntegrityBlock;
}

ui32 TIntegrityManager::EndPair(ui32 offsetInBytes, ui32 size) const {
    const ui32 endBlock = (offsetInBytes + size) / IntegrityUnitSize;
    return (endBlock + ChecksumsPerIntegrityBlock - 1) / ChecksumsPerIntegrityBlock;
}

TIntegrityBlockIdentity TIntegrityManager::MakeBlockIdentity(TDataChunkKey key,
        const TExtentInfo& extent, ui32 pairIdx) const {
    return {
        .OwnerId = key.TabletId,
        .VChunkId = key.VChunkIndex,
        .VChunkGeneration = extent.Ref.VChunkGeneration,
        .IntegrityChunkId = extent.Ref.IntegrityChunkIdx,
        .IntegrityExtentId = extent.Ref.ExtentSlot,
        .IntegrityChunkGeneration = IntegrityChunks.at(extent.Ref.IntegrityChunkIdx).Generation,
        .ChecksumBlockIdx = pairIdx,
    };
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
        if (extent.Completion->Ready && !extent.DeletionPending) {
            snapshot.Extents.push_back({key, extent.DataChunkIdx, extent.Ref});
        }
    }
    return snapshot;
}

void TIntegrityManager::ApplyMappingSnapshot(const TMappingSnapshot& snapshot) {
    Y_ABORT_UNLESS(IntegrityChunks.empty() && Extents.empty() && PendingExtents.empty(),
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
        extent.Completion->Ready = extent.Completion->Placed = true;
        extent.DataChunkIdx = entry.DataChunkIdx;
        // Pinned pair state is reconstructed lazily by adjacent 8 KiB A/B reads.
        extent.Pairs.resize(BlocksPerExtentCount);

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
            || !it->second.Completion->Placed) {
        return nullptr;
    }
    return &it->second.Ref;
}

ui64 TIntegrityManager::GetIntegrityChunkGeneration(TChunkIdx chunkIdx) const {
    const auto it = IntegrityChunks.find(chunkIdx);
    return it != IntegrityChunks.end() ? it->second.Generation : 0;
}

std::vector<TChunkIdx> TIntegrityManager::GetIntegrityChunkIdxs() const {
    std::vector<TChunkIdx> chunks;
    chunks.reserve(IntegrityChunks.size());
    for (const auto& [chunkIdx, info] : IntegrityChunks) {
        Y_UNUSED(info);
        chunks.push_back(chunkIdx);
    }
    return chunks;
}

bool TIntegrityManager::IsIntegrityChunkFormatted(TChunkIdx chunkIdx) const {
    const auto it = IntegrityChunks.find(chunkIdx);
    return it != IntegrityChunks.end() && it->second.State == EChunkState::Ready;
}

ui64 TIntegrityManager::GetIntegrityBlockDigest(TDataChunkKey key, ui32 integrityBlockIdx) const {
    const auto it = Extents.find(key);
    Y_ABORT_UNLESS(it != Extents.end() && integrityBlockIdx < BlocksPerExtentCount);
    const TPairMeta& pair = it->second.Pairs.at(integrityBlockIdx);
    return pair.Known ? pair.Digest : 0;
}

// Explicit cache ownership and I/O transitions follow below.
TIntegrityManager::TWork TIntegrityManager::CompleteAllocation(ui64 token, TChunkIdx chunkIdx) {
    if (!PendingChunkAllocations.erase(token)) {
        return TakeWork();
    }
    if (!chunkIdx) {
        return TakeWork();
    }
    if (Stopped || CancelChunkAllocationIfExcess()) {
        Work.ReturnedChunks.push_back(chunkIdx);
    } else {
        OnIntegrityChunkAllocated(chunkIdx);
    }
    return TakeWork();
}

bool TIntegrityManager::GetBlockChecksum(TDataChunkKey key, ui32 blockIdx, ui64* checksum) const {
    const auto& extent = Extents.at(key);
    const auto it = extent.Cache.find(blockIdx / ChecksumsPerIntegrityBlock);
    if (it == extent.Cache.end() || !it->second->Image || !extent.UsedBlocks.Get(blockIdx)) {
        return false;
    }
    const auto& block = *reinterpret_cast<const TIntegrityBlock*>(it->second->Image.GetData());
    *checksum = UnsealBlockChecksum(block.Checksums[blockIdx % ChecksumsPerIntegrityBlock],
        DDiskId, PDiskGuid, key.TabletId, key.VChunkIndex, blockIdx);
    return true;
}

bool TIntegrityManager::HasInFlightOperationsForTablet(ui64 tabletId) const {
    for (const auto& [key, extent] : Extents) {
        if (key.TabletId == tabletId) {
            for (const auto& [_, entry] : extent.Cache) {
                if (entry.use_count() > 1 || entry->State == ECacheState::WaitingRead
                        || entry->State == ECacheState::WaitingWrite || !entry->Writers.empty()) {
                    return true;
                }
            }
        }
    }
    return false;
}

std::shared_ptr<TIntegrityManager::TCacheEntry> TIntegrityManager::GetEntry(
        TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx)
{
    auto [it, inserted] = extent.Cache.try_emplace(pairIdx);
    if (inserted) {
        it->second = std::make_shared<TCacheEntry>();
        auto& entry = *it->second;
        entry.Key = key;
        entry.PairIdx = pairIdx;
        entry.VChunkGeneration = extent.Ref.VChunkGeneration;
        const auto& meta = extent.Pairs.at(pairIdx);
        bool empty = meta.Known;
        for (ui32 block = pairIdx * ChecksumsPerIntegrityBlock;
                empty && block < Min(DataBlocksPerChunkCount, (pairIdx + 1) * ChecksumsPerIntegrityBlock); ++block) {
            empty = !extent.UsedBlocks.Get(block);
        }
        if (empty && meta.Digest == 0 && extent.Completion->Placed) {
            auto image = TRcBuf::UninitializedPageAligned(IntegrityUnitSize);
            auto* block = reinterpret_cast<TIntegrityBlock*>(image.GetDataMut());
            memset(block, 0, sizeof(*block));
            const auto identity = MakeBlockIdentity(key, extent, pairIdx);
            auto& header = block->Header;
            header.Magic = MagicIntegrityBlock;
            header.FormatVersion = static_cast<ui16>(EIntegrityFormatVersion::BaseAwupf4KiB);
            header.ChecksumBlockIdx = pairIdx;
            header.OwnerId = identity.OwnerId;
            header.VChunkId = identity.VChunkId;
            header.VChunkGeneration = identity.VChunkGeneration;
            header.IntegrityChunkId = identity.IntegrityChunkId;
            header.IntegrityExtentId = identity.IntegrityExtentId;
            header.IntegrityChunkGeneration = identity.IntegrityChunkGeneration;
            header.PairSequenceNumber = 1;
            header.BlockChecksum = CalculateRawChecksum(block, sizeof(*block));
            PublishImage(extent, entry, std::move(image), 1);
            entry.State = ECacheState::Data;
        }
    }
    if (it->second->Image) {
        it->second->Unlink();
        BlockStateLru.PushBack(it->second.get());
    }
    return it->second;
}

void TIntegrityManager::PublishImage(TExtentInfo& extent, TCacheEntry& entry,
        TRcBuf image, ui32 currentSlot)
{
    if (!entry.Image) {
        ++BlockStateCount;
    }
    entry.Image = std::move(image);
    entry.Unlink();
    BlockStateLru.PushBack(&entry);
    const auto& block = *reinterpret_cast<const TIntegrityBlock*>(entry.Image.GetData());
    auto& meta = extent.Pairs.at(entry.PairIdx);
    meta.Digest = block.Header.IntegrityBlockDigest;
    meta.Known = true;
    meta.CurrentSlot = currentSlot;
    const ui32 first = entry.PairIdx * ChecksumsPerIntegrityBlock;
    for (ui32 idx = first; idx < Min(DataBlocksPerChunkCount, first + ChecksumsPerIntegrityBlock); ++idx) {
        const ui32 slot = idx - first;
        if (block.Header.UsedBlocksBitmap[slot / 8] & ui8(1u << (slot % 8))) {
            extent.UsedBlocks.Set(idx);
        } else {
            extent.UsedBlocks.Reset(idx);
        }
    }
}

void TIntegrityManager::EvictBlockStatesOverBudget() {
    if (Stopped) {
        return;
    }
    size_t examined = 0;
    while (BlockStateCount > MaxBlockStates && examined++ < BlockStateCount) {
        auto* entry = BlockStateLru.Front();
        auto& extent = Extents.at(entry->Key);
        const auto it = extent.Cache.find(entry->PairIdx);
        Y_ABORT_UNLESS(it != extent.Cache.end());
        if (it->second.use_count() == 1 && entry->State == ECacheState::Data
                && entry->Writer.expired() && entry->Writers.empty() && entry->Readers.empty()) {
            entry->Unlink();
            if (entry->Failure) {
                // Keep terminal corruption independently of evictable checksum
                // images, including for subsequent reads of known holes.
                entry->Image = {};
                entry->State = ECacheState::Missing;
            } else {
                extent.Cache.erase(it);
            }
            --BlockStateCount;
            examined = 0;
        } else {
            entry->Unlink();
            BlockStateLru.PushBack(entry);
        }
    }
}

void TIntegrityManager::DropBlockStates(TExtentInfo& extent) {
    for (const auto& [_, entry] : extent.Cache) {
        if (entry->Image) {
            --BlockStateCount;
        }
    }
    extent.Cache.clear();
}

void TIntegrityManager::ValidateOperationRange(ui32 offset, ui32 size) const {
    Y_ABORT_UNLESS(size && offset % IntegrityUnitSize == 0 && size % IntegrityUnitSize == 0
        && ui64(offset) + size <= DataChunkSize);
}

TIntegrityManager::TMetadataRead TIntegrityManager::ClaimMetadataRead(
        TExtentInfo& extent, TArrayRef<const std::shared_ptr<TCacheEntry>> entries)
{
    Y_ABORT_UNLESS(!entries.empty());
    const ui32 first = entries.front()->PairIdx;
    ui32 previous = first;
    for (size_t idx = 0; idx < entries.size(); ++idx) {
        const auto& entry = entries[idx];
        Y_ABORT_UNLESS(entry->State == ECacheState::Missing);
        Y_ABORT_UNLESS(idx == 0 || entry->PairIdx > previous);
        previous = entry->PairIdx;
        entry->State = ECacheState::WaitingRead;
    }
    const ui32 last = previous;
    const ui32 pairCount = last - first + 1;
    const ui64 id = NextPairReadId++;
    TMetadataLoad load;
    load.FirstPairIdx = first;
    load.PairCount = pairCount;
    load.Entries.assign(entries.begin(), entries.end());
    PairLoads.emplace(id, std::move(load));
    const ui32 pairBytes = IntegrityPairSlots * IntegrityUnitSize;
    return {id, extent.Ref.IntegrityChunkIdx,
        static_cast<ui32>(ExtentOffset(extent.Ref.ExtentSlot) + ui64(first) * pairBytes),
        pairCount * pairBytes};
}

void TIntegrityManager::CapturePair(TExtentInfo& extent, const TCacheEntry& entry, TPendingRead& read) {
    if (entry.Failure) {
        if (read.Result.Status != EOperationStatus::Corrupted) {
            read.Result = *entry.Failure;
        }
        return;
    }
    if (read.Result.Status != EOperationStatus::Ok) {
        return;
    }
    Y_ABORT_UNLESS(entry.Image);
    const auto& image = *reinterpret_cast<const TIntegrityBlock*>(entry.Image.GetData());
    const ui32 first = read.Offset / IntegrityUnitSize;
    const ui32 begin = Max(first, entry.PairIdx * ChecksumsPerIntegrityBlock);
    const ui32 end = Min<ui32>((read.Offset + read.Size) / IntegrityUnitSize,
        (entry.PairIdx + 1) * ChecksumsPerIntegrityBlock);
    for (ui32 block = begin; block < end; ++block) {
        if (extent.UsedBlocks.Get(block)) {
            read.Result.ReadPlan.UsedBlocks.Set(block - first);
            read.Result.Checksums[block - first] = UnsealBlockChecksum(
                image.Checksums[block % ChecksumsPerIntegrityBlock], DDiskId, PDiskGuid,
                entry.Key.TabletId, entry.Key.VChunkIndex, block);
        }
    }
}

void TIntegrityManager::FinishPendingRead(const std::shared_ptr<TPendingRead>& read, bool notify) {
    Y_ABORT_UNLESS(!read->Remaining);
    if (read->Result.Status == EOperationStatus::Ok) {
        ui32 used = 0;
        const ui32 count = read->Size / IntegrityUnitSize;
        for (ui32 i = 0; i < count; ++i) {
            used += read->Result.ReadPlan.UsedBlocks.Get(i);
        }
        read->Result.ReadPlan.Kind = !used ? TReadPlan::AllZero
            : used == count ? TReadPlan::Passthrough : TReadPlan::Mixed;
        if (read->Result.ReadPlan.Kind != TReadPlan::Mixed) {
            read->Result.ReadPlan.UsedBlocks.Clear();
        }
    }
    if (!read->Completion->Result) {
        read->Completion->Result.emplace(std::move(read->Result));
        if (notify) {
            QueueDependency(read->Completion);
        }
    }
    PendingReads.erase(read->Id);
}

void TIntegrityManager::CompleteReaders(TExtentInfo& extent, TCacheEntry& entry) {
    auto readers = std::exchange(entry.Readers, {});
    for (const auto& weak : readers) {
        if (auto read = weak.lock()) {
            CapturePair(extent, entry, *read);
            Y_ABORT_UNLESS(read->Remaining);
            if (!--read->Remaining) {
                FinishPendingRead(read);
            }
        }
    }
}

TIntegrityManager::TReadPreparation TIntegrityManager::PrepareRead(TDataChunkKey key,
        ui32 offset, ui32 size)
{
    ValidateOperationRange(offset, size);
    TReadPreparation preparation;
    const auto it = Extents.find(key);
    if (Stopped || it == Extents.end() || it->second.DeletionPending) {
        auto& result = preparation.Warm.emplace();
        result.Status = Stopped ? EOperationStatus::Failed : EOperationStatus::Corrupted;
        result.ErrorReason = "integrity extent is unavailable";
        return preparation;
    }
    auto& extent = it->second;
    bool knownZero = true;
    for (ui32 idx = FirstPair(offset); idx < EndPair(offset, size); ++idx) {
        knownZero &= extent.Pairs.at(idx).Known;
    }
    for (ui32 block = offset / IntegrityUnitSize;
            knownZero && block < (offset + size) / IntegrityUnitSize; ++block) {
        knownZero = !extent.UsedBlocks.Get(block);
    }
    // Look up each entry once and retain its stable handle through preparation.
    // Known holes need no synthesized checksum image; an existing cold writer
    // still owns their metadata and readers join its final completion.
    absl::InlinedVector<std::shared_ptr<TCacheEntry>, 1> entries;
    for (ui32 idx = FirstPair(offset); idx < EndPair(offset, size); ++idx) {
        if (knownZero) {
            const auto cached = extent.Cache.find(idx);
            if (cached != extent.Cache.end()) {
                entries.push_back(cached->second);
            }
        } else {
            entries.push_back(GetEntry(key, extent, idx));
        }
    }
    auto read = std::make_shared<TPendingRead>();
    read->Id = NextPendingReadId++;
    read->Offset = offset;
    read->Size = size;
    read->Completion = std::make_shared<TOperationState>();
    read->Result.Checksums.assign(size / IntegrityUnitSize, GetZeroBlockChecksum());
    read->Result.ReadPlan.UsedBlocks.Reserve(size / IntegrityUnitSize);
    absl::InlinedVector<std::shared_ptr<TCacheEntry>, 2> claimed;
    for (const auto& entry : entries) {
        if (entry->Image || entry->Failure) {
            CapturePair(extent, *entry, *read);
        } else {
            ++read->Remaining;
            entry->Readers.emplace_back(read);
            if (entry->State == ECacheState::Missing) {
                claimed.push_back(entry);
            }
        }
    }
    if (!claimed.empty()) {
        preparation.MetadataReads.push_back(ClaimMetadataRead(extent, claimed));
    }
    if (!read->Remaining) {
        FinishPendingRead(read, false);
        preparation.Warm.emplace(std::move(*read->Completion->Result));
    } else {
        PendingReads.emplace(read->Id, read);
        preparation.Pending = TOperation(read->Completion);
    }
    entries.clear();
    EvictBlockStatesOverBudget();
    return preparation;
}

void TIntegrityManager::CompleteLoadedPair(const std::shared_ptr<TCacheEntry>& entry,
        const TOperationResult* ioFailure, const void* pairImage)
{
    const auto extentIt = Extents.find(entry->Key);
    if (extentIt == Extents.end() || extentIt->second.Ref.VChunkGeneration != entry->VChunkGeneration) {
        auto readers = std::exchange(entry->Readers, {});
        for (const auto& weak : readers) {
            if (auto read = weak.lock()) {
                read->Result.Status = EOperationStatus::Failed;
                read->Result.ErrorReason = "integrity extent replaced during metadata load";
                if (!--read->Remaining) {
                    FinishPendingRead(read);
                }
            }
        }
        return;
    }
    auto& extent = extentIt->second;
    TOperationResult outcome;
    if (ioFailure) {
        outcome = *ioFailure;
    } else {
        TIntegrityBlock slots[IntegrityPairSlots];
        memcpy(slots, pairImage, sizeof(slots));
        const auto winner = SelectIntegrityBlockWinner(slots, MakeBlockIdentity(entry->Key, extent, entry->PairIdx));
        const auto& meta = extent.Pairs.at(entry->PairIdx);
        if (winner < 0) {
            outcome.Status = EOperationStatus::Corrupted;
            outcome.ErrorReason = TStringBuilder() << "both integrity slots are invalid for pair " << entry->PairIdx;
        } else if (meta.Known && meta.Digest != slots[winner].Header.IntegrityBlockDigest) {
            outcome.Status = EOperationStatus::Corrupted;
            outcome.ErrorReason = TStringBuilder() << "integrity digest mismatch for pair " << entry->PairIdx;
            outcome.LostWriteDetected = true;
        } else {
            auto image = TRcBuf::UninitializedPageAligned(IntegrityUnitSize);
            memcpy(image.GetDataMut(), &slots[winner], IntegrityUnitSize);
            PublishImage(extent, *entry, std::move(image), winner);
        }
    }
    entry->State = entry->Image ? ECacheState::Data : ECacheState::Missing;
    if (outcome.Status != EOperationStatus::Ok) {
        FailEntry(extent, *entry, std::move(outcome));
    } else {
        CompleteReaders(extent, *entry);
        WakeWriter(*entry);
    }
}

void TIntegrityManager::CompleteMetadataReads(TConstArrayRef<TMetadataReadResult> results) {
    for (const auto& result : results) {
        const auto it = PairLoads.find(result.Id);
        if (it == PairLoads.end()) {
            continue;
        }
        auto load = std::move(it->second);
        PairLoads.erase(it);
        const ui32 pairBytes = IntegrityPairSlots * IntegrityUnitSize;
        const bool ioOk = result.Result.Ok && !Stopped
            && result.Result.Data.size() == ui64(load.PairCount) * pairBytes;
        TOperationResult ioFailure;
        TRcBuf image;
        if (!ioOk) {
            if (!result.Result.Ok || Stopped) {
                ioFailure.Status = EOperationStatus::Failed;
                ioFailure.ErrorReason = "integrity metadata read failed";
            } else {
                ioFailure.Status = EOperationStatus::Corrupted;
                ioFailure.ErrorReason = "short integrity metadata read";
            }
        } else {
            image = TRcBuf::Uninitialized(load.PairCount * pairBytes);
            result.Result.Data.CopyTo(image.GetDataMut(), image.size());
        }
        for (const auto& entry : load.Entries) {
            const ui32 offset = (entry->PairIdx - load.FirstPairIdx) * pairBytes;
            CompleteLoadedPair(entry, ioOk ? nullptr : &ioFailure,
                ioOk ? image.GetData() + offset : nullptr);
        }
    }
    EvictBlockStatesOverBudget();
}

void TIntegrityManager::FailEntry(TExtentInfo& extent, TCacheEntry& entry, TOperationResult result, bool resolveReaders) {
    entry.Failure.emplace(std::move(result));
    if (resolveReaders) {
        CompleteReaders(extent, entry);
    }
    auto waiters = std::exchange(entry.Writers, {});
    for (const auto& weak : waiters) {
        if (auto writer = weak.lock()) {
            writer->Queued = false;
            writer->Result = entry.Failure;
            ReleaseWriter(writer);
            QueueDependency(writer);
        }
    }
}

bool TIntegrityManager::AdvanceWriter(const std::shared_ptr<TWriterState>& writer) {
    if (writer->Result) {
        return true;
    }
    if (Stopped || writer->Extent->Failed) {
        writer->Result.emplace().Status = EOperationStatus::Failed;
        writer->Result->ErrorReason = "integrity write canceled";
        ReleaseWriter(writer);
        return true;
    }
    if (!writer->Extent->Ready) {
        return false;
    }
    while (writer->Acquired < writer->Pairs.size()) {
        auto& entry = *writer->Pairs[writer->Acquired];
        if (entry.Failure) {
            writer->Result = entry.Failure;
            ReleaseWriter(writer);
            return true;
        }
        const auto owner = entry.Writer.lock();
        if (owner == writer) {
            writer->Queued = false;
            ++writer->Acquired;
            continue;
        }
        if (!owner && entry.State != ECacheState::WaitingRead) {
            entry.Writer = writer;
            entry.State = ECacheState::WaitingWrite;
            ++writer->Acquired;
            continue;
        }
        if (!writer->Queued) {
            writer->Queued = true;
            entry.Writers.emplace_back(writer);
        }
        return false;
    }
    return true;
}

void TIntegrityManager::WakeWriter(TCacheEntry& entry) {
    if (!entry.Writer.expired() || entry.State == ECacheState::WaitingRead) {
        return;
    }
    while (!entry.Writers.empty()) {
        auto writer = entry.Writers.front().lock();
        entry.Writers.pop_front();
        if (writer && !writer->Result) {
            // Reserve ownership before notification; new arrivals cannot steal it.
            entry.Writer = writer;
            entry.State = ECacheState::WaitingWrite;
            writer->Queued = false;
            QueueDependency(writer);
            break;
        }
    }
}

void TIntegrityManager::ReleaseWriter(const std::shared_ptr<TWriterState>& writer) {
    for (auto& held : writer->Pairs) {
        if (held->Writer.lock() == writer) {
            if (!writer->Submitted && !held->Image && !held->Readers.empty()) {
                const auto savedFailure = held->Failure;
                held->Failure.emplace().Status = EOperationStatus::Failed;
                held->Failure->ErrorReason = "metadata owner canceled before submission";
                CompleteReaders(Extents.at(writer->Key), *held);
                held->Failure = savedFailure;
            }
            held->Writer.reset();
            held->State = held->Image ? ECacheState::Data : ECacheState::Missing;
            WakeWriter(*held);
        }
        std::erase_if(held->Writers, [&](const auto& weak) {
            const auto queued = weak.lock();
            return !queued || queued == writer;
        });
    }
    writer->Pairs.clear();
    writer->Acquired = 0;
    writer->Queued = false;
    EvictBlockStatesOverBudget();
}

TIntegrityManager::TWriteOperation::TWriteOperation(TWriteOperation&& other) noexcept {
    *this = std::move(other);
}

TIntegrityManager::TWriteOperation& TIntegrityManager::TWriteOperation::operator=(TWriteOperation&& other) noexcept {
    if (this != &other) {
        Cancel();
        Manager = std::exchange(other.Manager, nullptr);
        State = std::move(other.State);
    }
    return *this;
}

TIntegrityManager::TWriteOperation::~TWriteOperation() {
    if (Manager && State && !State->Submitted && !State->Pairs.empty()) {
        Manager->ReleaseWriter(State);
    }
}

void TIntegrityManager::TWriteOperation::Cancel() {
    if (Manager && State && !State->Submitted && !State->Pairs.empty()) {
        if (!State->Result) {
            State->Result.emplace().Status = EOperationStatus::Failed;
            State->Result->ErrorReason = "integrity write canceled";
        }
        Manager->ReleaseWriter(State);
        Manager->NotifyCompleted();
    }
}

bool TIntegrityManager::TWriteOperation::IsReady() {
    return State && Manager->AdvanceWriter(State);
}

const TIntegrityManager::TOperationResult* TIntegrityManager::TWriteOperation::GetResult() const {
    return State && State->Result ? &*State->Result : nullptr;
}

NActors::NDetail::TAsyncEventAwaiter TIntegrityManager::TWriteOperation::WaitChanged() const {
    Y_ABORT_UNLESS(State);
    return !State->Extent->Ready && !State->Extent->Failed
        ? State->Extent->Changed.Wait() : State->Changed.Wait();
}

TIntegrityManager::TWriteOperation TIntegrityManager::PrepareWrite(TDataChunkKey key, ui32 offset, ui32 size) {
    ValidateOperationRange(offset, size);
    Y_ABORT_UNLESS(EndPair(offset, size) - FirstPair(offset) <= 2);
    TWriteOperation operation;
    operation.Manager = this;
    operation.State = std::make_shared<TWriterState>();
    auto& writer = *operation.State;
    writer.Key = key;
    writer.Offset = offset;
    writer.Size = size;
    const auto it = Extents.find(key);
    if (Stopped || it == Extents.end() || it->second.DeletionPending) {
        writer.Result.emplace().Status = Stopped ? EOperationStatus::Failed : EOperationStatus::Corrupted;
        writer.Result->ErrorReason = "integrity extent is unavailable";
        return operation;
    }
    auto& extent = it->second;
    writer.Extent = extent.Completion;
    for (ui32 idx = FirstPair(offset); idx < EndPair(offset, size); ++idx) {
        writer.Pairs.push_back(GetEntry(key, extent, idx));
    }
    AdvanceWriter(operation.State);
    return operation;
}

std::shared_ptr<TIntegrityManager::TMetadataWrite> TIntegrityManager::PrepareMetadataWrite(
        TWriteOperation& operation, TConstArrayRef<ui64> checksums)
{
    Y_ABORT_UNLESS(operation.Manager == this && operation.IsReady() && !operation.GetResult());
    auto& writer = *operation.State;
    Y_ABORT_UNLESS(!writer.Submitted && checksums.size() == writer.Size / IntegrityUnitSize);
    writer.Submitted = true;
    auto context = std::make_shared<TMetadataWrite>();
    context->Owner = operation.State;
    auto& extent = Extents.at(writer.Key);
    context->ChunkIdx = extent.Ref.IntegrityChunkIdx;
    context->ReadOffset = ExtentOffset(extent.Ref.ExtentSlot)
        + writer.Pairs.front()->PairIdx * IntegrityPairSlots * IntegrityUnitSize;
    context->Offset = writer.Offset;
    context->Checksums.assign(checksums.begin(), checksums.end());
    context->DDiskId = DDiskId;
    context->PDiskGuid = PDiskGuid;
    bool cold = false;
    for (const auto& entry : writer.Pairs) {
        auto& pair = context->Pairs.emplace_back();
        pair.Identity = MakeBlockIdentity(writer.Key, extent, entry->PairIdx);
        const auto& meta = extent.Pairs.at(entry->PairIdx);
        pair.DigestKnown = meta.Known;
        pair.ExpectedDigest = meta.Digest;
        pair.CurrentSlot = meta.CurrentSlot;
        pair.CurrentImage = entry->Image;
        cold |= !pair.CurrentImage;
    }
    if (cold) {
        context->ReadSize = context->Pairs.size() * IntegrityPairSlots * IntegrityUnitSize;
    } else {
        context->Transform();
    }
    return context;
}

bool TIntegrityManager::TMetadataWrite::Transform(const TReadPayload& data) {
    if (ReadSize) {
        if (data.size() != ReadSize) {
            Result.Status = EOperationStatus::Corrupted;
            Result.ErrorReason = "short integrity RMW read";
            return false;
        }
        TIntegrityBlock blocks[4];
        data.CopyTo(blocks, ReadSize);
        for (size_t idx = 0; idx < Pairs.size(); ++idx) {
            auto& pair = Pairs[idx];
            TIntegrityBlock slots[IntegrityPairSlots];
            memcpy(slots, blocks + idx * IntegrityPairSlots, sizeof(slots));
            const i32 winner = SelectIntegrityBlockWinner(slots, pair.Identity);
            if (winner < 0 || (pair.DigestKnown && pair.ExpectedDigest != slots[winner].Header.IntegrityBlockDigest)) {
                Result.Status = EOperationStatus::Corrupted;
                Result.LostWriteDetected = winner >= 0;
                Result.ErrorReason = TStringBuilder() << (winner < 0 ? "both integrity slots are invalid for pair "
                    : "integrity digest mismatch for pair ") << pair.Identity.ChecksumBlockIdx;
                return false;
            }
            pair.CurrentSlot = winner;
            pair.CurrentImage = TRcBuf::UninitializedPageAligned(IntegrityUnitSize);
            memcpy(pair.CurrentImage.GetDataMut(), &slots[winner], IntegrityUnitSize);
        }
    }
    const ui32 first = Offset / IntegrityUnitSize;
    const ui32 end = first + Checksums.size();
    for (auto& pair : Pairs) {
        pair.UpdatedImage = TRcBuf::UninitializedPageAligned(IntegrityUnitSize);
        memcpy(pair.UpdatedImage.GetDataMut(), pair.CurrentImage.GetData(), IntegrityUnitSize);
        auto& block = *reinterpret_cast<TIntegrityBlock*>(pair.UpdatedImage.GetDataMut());
        auto& header = block.Header;
        const ui32 base = pair.Identity.ChecksumBlockIdx * ChecksumsPerIntegrityBlock;
        for (ui32 idx = Max(first, base); idx < Min(end, base + ChecksumsPerIntegrityBlock); ++idx) {
            const ui32 slot = idx - base;
            const ui64 checksum = Checksums[idx - first];
            if (header.UsedBlocksBitmap[slot / 8] & ui8(1u << (slot % 8))) {
                const auto old = UnsealBlockChecksum(block.Checksums[slot], DDiskId, PDiskGuid,
                    pair.Identity.OwnerId, pair.Identity.VChunkId, idx);
                UpdateRoot(header.IntegrityBlockDigest, pair.Identity.VChunkGeneration, idx, old, checksum);
            } else {
                header.UsedBlocksBitmap[slot / 8] |= ui8(1u << (slot % 8));
                header.IntegrityBlockDigest ^= Contribution(pair.Identity.VChunkGeneration, idx, checksum);
            }
            block.Checksums[slot] = SealBlockChecksum(checksum, DDiskId, PDiskGuid,
                pair.Identity.OwnerId, pair.Identity.VChunkId, idx);
        }
        ++header.PairSequenceNumber;
        header.BlockChecksum = 0;
        header.BlockChecksum = CalculateRawChecksum(&block, sizeof(block));
    }
    if (Pairs.size() == 1) {
        WriteOffset = ReadOffset + (1 - Pairs[0].CurrentSlot) * IntegrityUnitSize;
        WriteImage = Pairs[0].UpdatedImage;
    } else if (Pairs[0].CurrentSlot == 0 && Pairs[1].CurrentSlot == 1) {
        WriteOffset = ReadOffset + IntegrityUnitSize;
        WriteImage = TRcBuf::UninitializedPageAligned(2 * IntegrityUnitSize);
        memcpy(WriteImage.GetDataMut(), Pairs[0].UpdatedImage.GetData(), IntegrityUnitSize);
        memcpy(WriteImage.GetDataMut() + IntegrityUnitSize, Pairs[1].UpdatedImage.GetData(), IntegrityUnitSize);
    } else {
        WriteOffset = ReadOffset;
        WriteImage = TRcBuf::UninitializedPageAligned(4 * IntegrityUnitSize);
        for (size_t idx = 0; idx < Pairs.size(); ++idx) {
            char* slots = WriteImage.GetDataMut() + idx * IntegrityPairSlots * IntegrityUnitSize;
            memcpy(slots + Pairs[idx].CurrentSlot * IntegrityUnitSize, Pairs[idx].CurrentImage.GetData(), IntegrityUnitSize);
            memcpy(slots + (1 - Pairs[idx].CurrentSlot) * IntegrityUnitSize, Pairs[idx].UpdatedImage.GetData(), IntegrityUnitSize);
        }
    }
    return true;
}

void TIntegrityManager::CompleteMetadataWrite(const std::shared_ptr<TMetadataWrite>& context, bool ok) {
    auto writer = context->Owner;
    Y_ABORT_UNLESS(writer && writer->Submitted && !writer->Result);
    auto& extent = Extents.at(writer->Key);
    writer->Result = context->Result;
    if (writer->Result->Status == EOperationStatus::Ok && !ok) {
        writer->Result->Status = EOperationStatus::Failed;
        writer->Result->ErrorReason = "integrity metadata write failed";
    }
    for (size_t idx = 0; idx < writer->Pairs.size(); ++idx) {
        auto& entry = *writer->Pairs[idx];
        if (writer->Result->Status == EOperationStatus::Ok) {
            entry.Failure.reset();
            PublishImage(extent, entry, context->Pairs[idx].UpdatedImage, 1 - context->Pairs[idx].CurrentSlot);
            CompleteReaders(extent, entry);
        } else {
            FailEntry(extent, entry, *writer->Result);
        }
    }
    ReleaseWriter(writer);
    QueueDependency(writer);
    context->Owner.reset();
}

TIntegrityManager::TWork TIntegrityManager::CompleteWrite(ui64 id, bool ok) {
    const auto it = FormatWrites.find(id);
    if (it != FormatWrites.end()) {
        auto write = std::move(it->second);
        FormatWrites.erase(it);
        CompleteFormatWrite(std::move(write), ok);
    }
    return TakeWork();
}

void TIntegrityManager::Stop() {
    Stopped = true;
    for (auto& [_, extent] : Extents) {
        extent.Completion->Failed = true;
        QueueDependency(extent.Completion);
        for (auto& [_, entry] : extent.Cache) {
            TOperationResult result;
            result.Status = EOperationStatus::Failed;
            result.ErrorReason = "integrity manager stopped";
            FailEntry(extent, *entry, std::move(result),
                entry->State != ECacheState::WaitingRead && entry->State != ECacheState::WaitingWrite);
            if (auto writer = entry->Writer.lock(); writer && !writer->Submitted) {
                writer->Result = entry->Failure;
                ReleaseWriter(writer);
                QueueDependency(writer);
            }
        }
    }
}

TIntegrityManager::TWork TIntegrityManager::TakeWork() {
    return std::exchange(Work, {});
}

void TIntegrityManager::NotifyCompleted() {
    while (!CompletedDependencies.empty()) {
        auto dependency = std::move(CompletedDependencies.front());
        CompletedDependencies.pop_front();
        dependency->NotificationQueued = false;
        dependency->Changed.NotifyAll();
    }
}

void TIntegrityManager::QueueDependency(const std::shared_ptr<TDependency>& dependency) {
    if (!dependency->NotificationQueued) {
        dependency->NotificationQueued = true;
        CompletedDependencies.push_back(dependency);
    }
}

} // namespace NKikimr::NDDisk
