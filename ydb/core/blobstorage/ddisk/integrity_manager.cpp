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
    extent.Pairs.resize(BlocksPerExtentCount);
    for (TPairMeta& pair : extent.Pairs) {
        pair.DigestKnown = true;
        pair.BitmapKnown = true;
        pair.Resident = true; // a freshly formatted pair is known to contain no used blocks
        pair.CurrentSlot = EPairSlot::B;
    }

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
    std::vector<ui64> operationIds;
    for (const auto& [operationId, operation] : PendingOperations) {
        if (operation.Key == key) {
            operationIds.push_back(operationId);
        }
    }
    for (const ui64 operationId : operationIds) {
        CompleteOperation(operationId, EOperationStatus::Corrupted, "integrity extent was deleted");
    }

    auto cancelQueuedIo = [&](ui64 ioId) {
        if (!ioId) {
            return;
        }
        const size_t oldSize = Actions.size();
        std::erase_if(Actions, [ioId](const TAction& action) {
            return std::visit(TOverloaded{
                [](const TAllocateIntegrityChunk&) { return false; },
                [ioId](const TWriteIo& io) { return io.IoId == ioId; },
                [ioId](const TReadIo& io) { return io.IoId == ioId; },
            }, action);
        });
        Y_ABORT_UNLESS(Actions.size() + 1 == oldSize,
            "attempt to delete an extent with submitted pair I/O, IoId# %" PRIu64, ioId);
        Y_ABORT_UNLESS(IosInFlight.erase(ioId) == 1);
    };
    for (const auto& [pairIdx, runtime] : extent.PairRuntime) {
        Y_UNUSED(pairIdx);
        cancelQueuedIo(runtime.ReadIoId);
        cancelQueuedIo(runtime.WriteIoId);
    }
    extent.PairRuntime.clear();

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
    Actions.emplace_back(TWriteIo{
        .IoId = ioId,
        .ChunkIdx = chunkIdx,
        .OffsetInBytes = ChunkHeaderReplicaOffset(replica),
        .Data = std::move(data),
        .Kind = EWriteIoKind::ChunkHeader,
    });
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
    Actions.emplace_back(TWriteIo{
        .IoId = ioId,
        .ChunkIdx = extent.Ref.IntegrityChunkIdx,
        .OffsetInBytes = ExtentOffset(extent.Ref.ExtentSlot),
        .Data = std::move(data),
        .Kind = EWriteIoKind::ExtentFormat,
    });
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
    for (ui32 pairIdx = 0; pairIdx < extent.Pairs.size(); ++pairIdx) {
        const auto runtimeIt = extent.PairRuntime.find(pairIdx);
        if (runtimeIt != extent.PairRuntime.end() && runtimeIt->second.Dirty) {
            QueuePairWrite(key, extent, pairIdx);
        }
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
        [&](const TPairReadRef&) {
            Y_ABORT("read integrity I/O completed through the write completion path");
        },
        [&](const TPairWriteRef& writeRef) {
            const auto it = Extents.find(writeRef.Key);
            if (it == Extents.end()) {
                return;
            }
            TExtentInfo& extent = it->second;
            TPairMeta& pair = extent.Pairs.at(writeRef.PairIdx);
            TPairRuntime& runtime = extent.PairRuntime.at(writeRef.PairIdx);
            Y_ABORT_UNLESS(runtime.WriteIoId == ioId);
            runtime.WriteIoId = 0;
            pair.CurrentSlot = writeRef.Slot;
            TIntegrityBlockState* state = FindBlockState(extent, writeRef.PairIdx);
            Y_ABORT_UNLESS(state);
            ++state->PairSequenceNumber;
            runtime.DurableVersion = Max(runtime.DurableVersion, writeRef.Version);
            NotifyPairDurable(writeRef.Key, extent, writeRef.PairIdx);
            if (const auto runtimeIt = extent.PairRuntime.find(writeRef.PairIdx);
                    runtimeIt != extent.PairRuntime.end() && runtimeIt->second.Dirty) {
                QueuePairWrite(writeRef.Key, extent, writeRef.PairIdx);
            }
            MaybeDropPairRuntime(extent, writeRef.PairIdx);
            EvictBlockStatesOverBudget();
        },
    }, ref);

    return readyKeys;
}

void TIntegrityManager::NotifyPairDurable(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx) {
    Y_UNUSED(key);
    TPairRuntime& runtime = extent.PairRuntime.at(pairIdx);
    std::vector<std::pair<ui64, ui64>> remaining;
    std::vector<ui64> durableOperations;
    for (const auto& [operationId, requiredVersion] : runtime.DurabilityWaiters) {
        if (requiredVersion > runtime.DurableVersion) {
            remaining.emplace_back(operationId, requiredVersion);
            continue;
        }
        durableOperations.push_back(operationId);
    }
    runtime.DurabilityWaiters = std::move(remaining);

    for (const ui64 operationId : durableOperations) {
        const auto operationIt = PendingOperations.find(operationId);
        if (operationIt == PendingOperations.end()) {
            continue;
        }
        TPendingOperation& operation = operationIt->second;
        Y_ABORT_UNLESS(operation.PendingDurability > 0);
        if (--operation.PendingDurability == 0) {
            CompleteOperation(operationId);
        }
    }
}

void TIntegrityManager::CompleteOperation(ui64 operationId, EOperationStatus status,
        TString errorReason, bool lostWriteDetected) {
    const auto it = PendingOperations.find(operationId);
    if (it == PendingOperations.end()) {
        return;
    }

    TPendingOperation& operation = it->second;
    TOperationResult result{
        .OperationId = operationId,
        .Kind = operation.Kind,
        .Status = status,
        .ErrorReason = std::move(errorReason),
        .LostWriteDetected = lostWriteDetected,
    };

    if (status == EOperationStatus::Ok && operation.Kind == EOperationKind::Read) {
        const ui32 firstBlock = operation.OffsetInBytes / IntegrityUnitSize;
        const ui32 numBlocks = operation.Size / IntegrityUnitSize;
        result.Checksums.reserve(numBlocks);
        const auto extentIt = Extents.find(operation.Key);
        if (extentIt == Extents.end()) {
            result.Status = EOperationStatus::Corrupted;
            result.ErrorReason = "integrity extent is missing for an allocated data chunk";
            CompletedOperations.push_back(std::move(result));
            PendingOperations.erase(it);
            return;
        }
        const TExtentInfo& extent = extentIt->second;
        for (ui32 blockIdx = firstBlock; blockIdx < firstBlock + numBlocks; ++blockIdx) {
            const ui32 pairIdx = blockIdx / ChecksumsPerIntegrityBlock;
            const ui32 slot = blockIdx % ChecksumsPerIntegrityBlock;
            if (!extent.UsedBlocks.Get(blockIdx)) {
                result.Checksums.push_back(GetZeroBlockChecksum());
                continue;
            }
            const auto stateIt = extent.BlockStates.find(pairIdx);
            if (stateIt == extent.BlockStates.end() || !stateIt->second->Known.Get(slot)) {
                result.Status = EOperationStatus::Corrupted;
                result.ErrorReason = TStringBuilder()
                    << "checksum is missing for used data block " << blockIdx;
                result.Checksums.clear();
                break;
            }
            result.Checksums.push_back(stateIt->second->Checksums[slot]);
        }
    }

    if (operation.PairsPinned) {
        const auto extentIt = Extents.find(operation.Key);
        Y_ABORT_UNLESS(extentIt != Extents.end());
        TExtentInfo& extent = extentIt->second;
        for (ui32 pairIdx = FirstPair(operation.OffsetInBytes);
                pairIdx < EndPair(operation.OffsetInBytes, operation.Size); ++pairIdx) {
            TPairMeta& pair = extent.Pairs.at(pairIdx);
            Y_ABORT_UNLESS(pair.OperationPins > 0);
            --pair.OperationPins;
            MaybeDropPairRuntime(extent, pairIdx);
        }
        operation.PairsPinned = false;
    }

    CompletedOperations.push_back(std::move(result));
    PendingOperations.erase(it);
}

void TIntegrityManager::CompleteReadOperation(ui64 operationId, TPendingOperation& operation) {
    Y_ABORT_UNLESS(operation.Kind == EOperationKind::Read && operation.PendingLoads == 0);
    CompleteOperation(operationId);
}

void TIntegrityManager::NotifyPairLoaded(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx) {
    TPairMeta& pair = extent.Pairs.at(pairIdx);
    TPairRuntime& runtime = extent.PairRuntime.at(pairIdx);
    auto waiters = std::exchange(runtime.LoadWaiters, {});
    const TString corruptionReason = runtime.CorruptionReason;
    const bool lostWriteCorruption = runtime.LostWriteCorruption;
    for (const ui64 operationId : waiters) {
        const auto operationIt = PendingOperations.find(operationId);
        if (operationIt == PendingOperations.end()) {
            continue;
        }
        TPendingOperation& operation = operationIt->second;
        if (pair.Corrupted) {
            CompleteOperation(operationId, EOperationStatus::Corrupted, corruptionReason,
                lostWriteCorruption);
            continue;
        }
        Y_ABORT_UNLESS(operation.PendingLoads > 0);
        if (--operation.PendingLoads != 0) {
            continue;
        }
        if (operation.Kind == EOperationKind::Write) {
            ApplyWriteOperation(operationId, operation);
        } else {
            CompleteReadOperation(operationId, operation);
        }
    }
    Y_UNUSED(key);
}

bool TIntegrityManager::LoadPairImage(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx,
        const TRope& data, TString* errorReason, bool* lostWriteDetected) {
    *lostWriteDetected = false;
    if (data.size() != IntegrityPairSlots * sizeof(TIntegrityBlock)) {
        *errorReason = TStringBuilder() << "short integrity pair read: expected "
            << IntegrityPairSlots * sizeof(TIntegrityBlock) << " bytes, got " << data.size();
        return false;
    }

    TIntegrityBlock slots[IntegrityPairSlots];
    data.Begin().ExtractPlainDataAndAdvance(slots, sizeof(slots));
    const i32 winner = SelectIntegrityBlockWinner(slots, MakeBlockIdentity(key, extent, pairIdx));
    if (winner < 0) {
        *errorReason = TStringBuilder() << "both integrity slots are invalid for pair " << pairIdx;
        return false;
    }

    const TIntegrityBlock& block = slots[winner];
    TPairMeta& pair = extent.Pairs.at(pairIdx);
    if (pair.DigestKnown && pair.Digest != block.Header.IntegrityBlockDigest) {
        *errorReason = TStringBuilder() << "integrity digest mismatch for pair " << pairIdx
            << ": expected " << pair.Digest << ", got " << block.Header.IntegrityBlockDigest;
        *lostWriteDetected = true;
        return false;
    }

    pair.Digest = block.Header.IntegrityBlockDigest;
    pair.DigestKnown = true;
    pair.BitmapKnown = true;
    pair.Resident = true;
    pair.CurrentSlot = winner == 0 ? EPairSlot::A : EPairSlot::B;

    TIntegrityBlockState& state = GetOrCreateBlockState(key, extent, pairIdx);
    state.PairSequenceNumber = block.Header.PairSequenceNumber;
    state.Known.Clear();
    state.Known.Reserve(ChecksumsPerIntegrityBlock);
    std::fill(state.Checksums.begin(), state.Checksums.end(), 0);

    const ui32 firstBlock = pairIdx * ChecksumsPerIntegrityBlock;
    const ui32 endBlock = Min(DataBlocksPerChunkCount, firstBlock + ChecksumsPerIntegrityBlock);
    for (ui32 blockIdx = firstBlock; blockIdx < endBlock; ++blockIdx) {
        const ui32 slot = blockIdx - firstBlock;
        const bool used = block.Header.UsedBlocksBitmap[slot / 8] & ui8(1u << (slot % 8));
        if (!used) {
            continue;
        }
        extent.UsedBlocks.Set(blockIdx);
        state.Known.Set(slot);
        state.Checksums[slot] = UnsealBlockChecksum(block.Checksums[slot], DDiskId,
            PDiskGuid, key.TabletId, key.VChunkIndex, blockIdx);
    }
    return true;
}

void TIntegrityManager::OnReadIoCompleted(ui64 ioId, TRope data) {
    const auto ioIt = IosInFlight.find(ioId);
    Y_ABORT_UNLESS(ioIt != IosInFlight.end(), "unknown read IoId# %" PRIu64, ioId);
    const auto* readRef = std::get_if<TPairReadRef>(&ioIt->second);
    Y_ABORT_UNLESS(readRef);
    const TPairReadRef ref = *readRef;
    IosInFlight.erase(ioIt);

    const auto extentIt = Extents.find(ref.Key);
    if (extentIt == Extents.end()) {
        return;
    }
    TExtentInfo& extent = extentIt->second;
    TPairMeta& pair = extent.Pairs.at(ref.PairIdx);
    {
        TPairRuntime& runtime = extent.PairRuntime.at(ref.PairIdx);
        Y_ABORT_UNLESS(runtime.ReadIoId == ioId);
        runtime.ReadIoId = 0;

        TString errorReason;
        bool lostWriteDetected = false;
        if (!LoadPairImage(ref.Key, extent, ref.PairIdx, data, &errorReason, &lostWriteDetected)) {
            pair.Corrupted = true;
            runtime.LostWriteCorruption = lostWriteDetected;
            runtime.CorruptionReason = std::move(errorReason);
        }
    }
    NotifyPairLoaded(ref.Key, extent, ref.PairIdx);
    if (!pair.Corrupted) {
        if (const auto runtimeIt = extent.PairRuntime.find(ref.PairIdx);
                runtimeIt != extent.PairRuntime.end() && runtimeIt->second.Dirty) {
            QueuePairWrite(ref.Key, extent, ref.PairIdx);
        }
    }
    MaybeDropPairRuntime(extent, ref.PairIdx);
    EvictBlockStatesOverBudget();
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
        TExtentInfo& extent, ui32 pairIdx) {
    const auto [it, inserted] = extent.BlockStates.try_emplace(pairIdx);
    if (inserted) {
        it->second = std::make_unique<TIntegrityBlockState>();
        TIntegrityBlockState& state = *it->second;
        state.Key = key;
        state.PairIdx = pairIdx;
        state.PairSequenceNumber = extent.Pairs.at(pairIdx).CurrentSlot == EPairSlot::B ? 1 : 0;
        state.Known.Reserve(ChecksumsPerIntegrityBlock);
        state.Checksums.resize(ChecksumsPerIntegrityBlock, 0);
        BlockStateLru.PushBack(&state);
        ++BlockStateCount;
        return state;
    }
    TIntegrityBlockState& state = *it->second;
    state.Unlink();
    BlockStateLru.PushBack(&state); // touch
    return state;
}

TIntegrityManager::TIntegrityBlockState* TIntegrityManager::FindBlockState(TExtentInfo& extent, ui32 pairIdx) {
    const auto it = extent.BlockStates.find(pairIdx);
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
        TIntegrityBlockState* victim = nullptr;
        size_t examined = 0;
        while (examined++ < BlockStateCount) {
            TIntegrityBlockState* candidate = BlockStateLru.Front();
            TExtentInfo& extent = Extents.at(candidate->Key);
            const auto runtimeIt = extent.PairRuntime.find(candidate->PairIdx);
            if (extent.Pairs.at(candidate->PairIdx).OperationPins == 0
                    && (runtimeIt == extent.PairRuntime.end()
                    || (!runtimeIt->second.ReadIoId && !runtimeIt->second.WriteIoId
                        && !runtimeIt->second.Dirty && runtimeIt->second.LoadWaiters.empty()
                        && runtimeIt->second.DurabilityWaiters.empty()))) {
                victim = candidate;
                break;
            }
            candidate->Unlink();
            BlockStateLru.PushBack(candidate);
        }
        if (!victim) {
            // The budget is soft while all resident states are needed by in-flight operations.
            return;
        }
        const auto extentIt = Extents.find(victim->Key);
        Y_ABORT_UNLESS(extentIt != Extents.end());
        extentIt->second.Pairs.at(victim->PairIdx).Resident = false;
        const size_t numErased = extentIt->second.BlockStates.erase(victim->PairIdx);
        Y_ABORT_UNLESS(numErased == 1); // unlinks from the LRU via ~TIntrusiveListItem
        --BlockStateCount;
    }
}

void TIntegrityManager::DropBlockStates(TExtentInfo& extent) {
    BlockStateCount -= extent.BlockStates.size();
    extent.BlockStates.clear(); // each state unlinks from the LRU via ~TIntrusiveListItem
}

ui32 TIntegrityManager::FirstPair(ui32 offsetInBytes) const {
    return (offsetInBytes / IntegrityUnitSize) / ChecksumsPerIntegrityBlock;
}

ui32 TIntegrityManager::EndPair(ui32 offsetInBytes, ui32 size) const {
    const ui32 endBlock = (offsetInBytes + size + IntegrityUnitSize - 1) / IntegrityUnitSize;
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

TIntegrityManager::TPairRuntime& TIntegrityManager::GetPairRuntime(
        TExtentInfo& extent, ui32 pairIdx) {
    return extent.PairRuntime[pairIdx];
}

void TIntegrityManager::MaybeDropPairRuntime(TExtentInfo& extent, ui32 pairIdx) {
    const auto it = extent.PairRuntime.find(pairIdx);
    if (it == extent.PairRuntime.end()) {
        return;
    }
    const TPairRuntime& runtime = it->second;
    const TPairMeta& pair = extent.Pairs.at(pairIdx);
    if (!pair.Corrupted && pair.OperationPins == 0 && !runtime.ReadIoId && !runtime.WriteIoId
            && !runtime.Dirty && runtime.LoadWaiters.empty()
            && runtime.DurabilityWaiters.empty()) {
        extent.PairRuntime.erase(it);
    }
}

void TIntegrityManager::QueuePairRead(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx) {
    TPairMeta& pair = extent.Pairs.at(pairIdx);
    TPairRuntime& runtime = GetPairRuntime(extent, pairIdx);
    if (runtime.ReadIoId || pair.Resident || pair.Corrupted) {
        return;
    }

    const ui64 ioId = NextIoId++;
    runtime.ReadIoId = ioId;
    IosInFlight.emplace(ioId, TPairReadRef{key, pairIdx});
    Actions.emplace_back(TReadIo{
        .IoId = ioId,
        .ChunkIdx = extent.Ref.IntegrityChunkIdx,
        .OffsetInBytes = ExtentOffset(extent.Ref.ExtentSlot)
            + pairIdx * IntegrityPairSlots * static_cast<ui32>(IntegrityUnitSize),
        .Size = IntegrityPairSlots * static_cast<ui32>(IntegrityUnitSize),
    });
}

bool TIntegrityManager::PairHasAllUsedChecksums(const TExtentInfo& extent, ui32 pairIdx,
        const TIntegrityBlockState& state) const {
    const ui32 firstBlock = pairIdx * ChecksumsPerIntegrityBlock;
    const ui32 endBlock = Min(DataBlocksPerChunkCount, firstBlock + ChecksumsPerIntegrityBlock);
    for (ui32 block = firstBlock; block < endBlock; ++block) {
        if (extent.UsedBlocks.Get(block) && !state.Known.Get(block - firstBlock)) {
            return false;
        }
    }
    return true;
}

void TIntegrityManager::QueuePairWrite(TDataChunkKey key, TExtentInfo& extent, ui32 pairIdx) {
    TPairMeta& pair = extent.Pairs.at(pairIdx);
    TPairRuntime& runtime = GetPairRuntime(extent, pairIdx);
    if (extent.State != EExtentState::Ready || runtime.ReadIoId || runtime.WriteIoId
            || !pair.Resident || pair.Corrupted || !runtime.Dirty) {
        return;
    }

    TIntegrityBlockState& state = GetOrCreateBlockState(key, extent, pairIdx);
    Y_ABORT_UNLESS(PairHasAllUsedChecksums(extent, pairIdx, state),
        "used data block without a checksum in pair# %" PRIu32, pairIdx);

    auto data = TRcBuf::UninitializedPageAligned(sizeof(TIntegrityBlock));
    auto* block = reinterpret_cast<TIntegrityBlock*>(data.GetDataMut());
    memset(block, 0, sizeof(*block));

    TIntegrityBlockHeader& header = block->Header;
    header.Magic = MagicIntegrityBlock;
    header.FormatVersion = static_cast<ui16>(EIntegrityFormatVersion::BaseAwupf4KiB);
    header.ChecksumBlockIdx = pairIdx;
    header.OwnerId = key.TabletId;
    header.VChunkId = key.VChunkIndex;
    header.VChunkGeneration = extent.Ref.VChunkGeneration;
    header.IntegrityChunkId = extent.Ref.IntegrityChunkIdx;
    header.IntegrityExtentId = extent.Ref.ExtentSlot;
    header.IntegrityChunkGeneration = IntegrityChunks.at(extent.Ref.IntegrityChunkIdx).Generation;
    header.IntegrityBlockDigest = pair.Digest;
    header.PairSequenceNumber = state.PairSequenceNumber + 1;

    const ui32 firstBlock = pairIdx * ChecksumsPerIntegrityBlock;
    const ui32 endBlock = Min(DataBlocksPerChunkCount, firstBlock + ChecksumsPerIntegrityBlock);
    for (ui32 blockIdx = firstBlock; blockIdx < endBlock; ++blockIdx) {
        const ui32 slot = blockIdx - firstBlock;
        if (!extent.UsedBlocks.Get(blockIdx)) {
            continue;
        }
        header.UsedBlocksBitmap[slot / 8] |= ui8(1u << (slot % 8));
        Y_ABORT_UNLESS(state.Known.Get(slot));
        block->Checksums[slot] = SealBlockChecksum(state.Checksums[slot], DDiskId,
            PDiskGuid, key.TabletId, key.VChunkIndex, blockIdx);
    }
    header.BlockChecksum = CalculateRawChecksum(block, sizeof(*block));

    const EPairSlot targetSlot = pair.CurrentSlot == EPairSlot::A ? EPairSlot::B : EPairSlot::A;
    const ui32 targetSlotIdx = targetSlot == EPairSlot::A ? 0 : 1;
    const ui64 ioId = NextIoId++;
    runtime.WriteIoId = ioId;
    runtime.WriteVersion = runtime.MutationVersion;
    runtime.Dirty = false;
    IosInFlight.emplace(ioId, TPairWriteRef{key, pairIdx, runtime.WriteVersion, targetSlot});
    Actions.emplace_back(TWriteIo{
        .IoId = ioId,
        .ChunkIdx = extent.Ref.IntegrityChunkIdx,
        .OffsetInBytes = ExtentOffset(extent.Ref.ExtentSlot)
            + (pairIdx * IntegrityPairSlots + targetSlotIdx) * static_cast<ui32>(IntegrityUnitSize),
        .Data = std::move(data),
        .Kind = EWriteIoKind::Pair,
    });
}

ui64 TIntegrityManager::BeginBlocksWrite(TDataChunkKey key, ui32 offsetInBytes, ui32 size,
        const std::vector<ui64>& checksums) {
    const auto it = Extents.find(key);
    Y_ABORT_UNLESS(it != Extents.end(), "write to unknown data chunk, TabletId# %" PRIu64 " VChunkIndex# %" PRIu64,
        key.TabletId, key.VChunkIndex);
    TExtentInfo& extent = it->second;

    Y_ABORT_UNLESS(size > 0 && ui64(offsetInBytes) + size <= DataChunkSize);

    const ui32 firstBlock = offsetInBytes / IntegrityUnitSize;
    const ui32 endBlock = (offsetInBytes + size) / IntegrityUnitSize;
    Y_ABORT_UNLESS(offsetInBytes % IntegrityUnitSize == 0 && size % IntegrityUnitSize == 0
            && checksums.size() == endBlock - firstBlock,
        "checksums must be per-4KiB-block of an aligned range: offset# %" PRIu32 " size# %" PRIu32
        " checksums# %zu", offsetInBytes, size, checksums.size());

    const ui64 operationId = NextOperationId++;
    auto [operationIt, inserted] = PendingOperations.emplace(operationId, TPendingOperation{
        .Kind = EOperationKind::Write,
        .Key = key,
        .OffsetInBytes = offsetInBytes,
        .Size = size,
        .Checksums = checksums,
    });
    Y_ABORT_UNLESS(inserted);
    TPendingOperation& operation = operationIt->second;

    for (ui32 pairIdx = FirstPair(offsetInBytes); pairIdx < EndPair(offsetInBytes, size); ++pairIdx) {
        const TPairMeta& pair = extent.Pairs.at(pairIdx);
        if (pair.Corrupted) {
            CompleteOperation(operationId, EOperationStatus::Corrupted,
                extent.PairRuntime.at(pairIdx).CorruptionReason,
                extent.PairRuntime.at(pairIdx).LostWriteCorruption);
            return operationId;
        }
    }

    operation.PairsPinned = true;
    for (ui32 pairIdx = FirstPair(offsetInBytes); pairIdx < EndPair(offsetInBytes, size); ++pairIdx) {
        TPairMeta& pair = extent.Pairs.at(pairIdx);
        ++pair.OperationPins;
        if (!pair.Resident) {
            GetPairRuntime(extent, pairIdx).LoadWaiters.push_back(operationId);
            ++operation.PendingLoads;
            QueuePairRead(key, extent, pairIdx);
        }
    }
    if (!operation.PendingLoads) {
        ApplyWriteOperation(operationId, operation);
    }
    return operationId;
}

void TIntegrityManager::ApplyWriteOperation(ui64 operationId, TPendingOperation& operation) {
    Y_ABORT_UNLESS(operation.Kind == EOperationKind::Write && !operation.Applied
        && operation.PendingLoads == 0);
    TExtentInfo& extent = Extents.at(operation.Key);
    const ui32 firstBlock = operation.OffsetInBytes / IntegrityUnitSize;
    const ui32 endBlock = (operation.OffsetInBytes + operation.Size) / IntegrityUnitSize;

    operation.Applied = true;
    for (ui32 block = firstBlock; block < endBlock; ++block) {
        extent.UsedBlocks.Set(block);

        const ui32 blockStateIdx = block / ChecksumsPerIntegrityBlock;
        const ui32 slot = block % ChecksumsPerIntegrityBlock;
        const ui64 generation = extent.Ref.VChunkGeneration;
        TPairMeta& pair = extent.Pairs.at(blockStateIdx);

        TIntegrityBlockState& state = GetOrCreateBlockState(operation.Key, extent, blockStateIdx);
        const ui64 newCsum = operation.Checksums[block - firstBlock];
        if (state.Known.Get(slot)) {
            UpdateRoot(pair.Digest, generation, block, state.Checksums[slot], newCsum);
        } else {
            pair.Digest ^= Contribution(generation, block, newCsum);
            state.Known.Set(slot);
        }
        state.Checksums[slot] = newCsum;
        pair.DigestKnown = true;
    }

    for (ui32 pairIdx = FirstPair(operation.OffsetInBytes);
            pairIdx < EndPair(operation.OffsetInBytes, operation.Size); ++pairIdx) {
        TPairRuntime& runtime = GetPairRuntime(extent, pairIdx);
        ++runtime.MutationVersion;
        runtime.Dirty = true;
        runtime.DurabilityWaiters.emplace_back(operationId, runtime.MutationVersion);
        ++operation.PendingDurability;
        QueuePairWrite(operation.Key, extent, pairIdx);
    }
    if (!operation.PendingDurability) {
        CompleteOperation(operationId);
    }
    EvictBlockStatesOverBudget();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Read path
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

ui64 TIntegrityManager::BeginChecksumRead(TDataChunkKey key, ui32 offsetInBytes, ui32 size) {
    Y_ABORT_UNLESS(size > 0 && offsetInBytes % IntegrityUnitSize == 0
        && size % IntegrityUnitSize == 0 && ui64(offsetInBytes) + size <= DataChunkSize);

    const ui64 operationId = NextOperationId++;
    auto [operationIt, inserted] = PendingOperations.emplace(operationId, TPendingOperation{
        .Kind = EOperationKind::Read,
        .Key = key,
        .OffsetInBytes = offsetInBytes,
        .Size = size,
    });
    Y_ABORT_UNLESS(inserted);
    TPendingOperation& operation = operationIt->second;

    const auto extentIt = Extents.find(key);
    if (extentIt == Extents.end()) {
        CompleteOperation(operationId, EOperationStatus::Corrupted,
            "integrity extent is missing for an allocated data chunk");
        return operationId;
    }
    TExtentInfo& extent = extentIt->second;

    for (ui32 pairIdx = FirstPair(offsetInBytes); pairIdx < EndPair(offsetInBytes, size); ++pairIdx) {
        const TPairMeta& pair = extent.Pairs.at(pairIdx);
        if (pair.Corrupted) {
            CompleteOperation(operationId, EOperationStatus::Corrupted,
                extent.PairRuntime.at(pairIdx).CorruptionReason,
                extent.PairRuntime.at(pairIdx).LostWriteCorruption);
            return operationId;
        }
    }

    operation.PairsPinned = true;
    for (ui32 pairIdx = FirstPair(offsetInBytes); pairIdx < EndPair(offsetInBytes, size); ++pairIdx) {
        TPairMeta& pair = extent.Pairs.at(pairIdx);
        ++pair.OperationPins;
        if (!pair.Resident) {
            GetPairRuntime(extent, pairIdx).LoadWaiters.push_back(operationId);
            ++operation.PendingLoads;
            QueuePairRead(key, extent, pairIdx);
        }
    }
    if (!operation.PendingLoads) {
        CompleteReadOperation(operationId, operation);
    }
    return operationId;
}

std::vector<TIntegrityManager::TOperationResult> TIntegrityManager::TakeCompletedOperations() {
    return std::exchange(CompletedOperations, {});
}

TIntegrityManager::TReadPlan TIntegrityManager::MakeReadPlan(TDataChunkKey key, ui32 offsetInBytes, ui32 size) const {
    TReadPlan plan;

    const auto it = Extents.find(key);
    if (it == Extents.end()) {
        // BeginChecksumRead rejects allocated chunks without an integrity extent, so this fallback
        // is unreachable through the actor read path.
        return plan;
    }
    const TExtentInfo& extent = it->second;

    if (offsetInBytes % IntegrityUnitSize == 0 && size % IntegrityUnitSize == 0 && size > 0
            && ui64(offsetInBytes) + size <= DataChunkSize) {
        for (ui32 pairIdx = FirstPair(offsetInBytes); pairIdx < EndPair(offsetInBytes, size); ++pairIdx) {
            if (!extent.Pairs.at(pairIdx).BitmapKnown) {
                // A restored pair has not been read yet; pass through until BeginChecksumRead
                // restores its exact bitmap.
                return plan;
            }
        }
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
    const TPairMeta& pair = it->second.Pairs.at(integrityBlockIdx);
    return pair.DigestKnown ? pair.Digest : 0;
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

bool TIntegrityManager::HasInFlightOperationsForTablet(ui64 tabletId) const {
    for (const auto& [operationId, operation] : PendingOperations) {
        Y_UNUSED(operationId);
        if (operation.Key.TabletId == tabletId) {
            return true;
        }
    }
    for (const auto& [ioId, ref] : IosInFlight) {
        Y_UNUSED(ioId);
        if (const auto* read = std::get_if<TPairReadRef>(&ref);
                read && read->Key.TabletId == tabletId) {
            return true;
        }
        if (const auto* write = std::get_if<TPairWriteRef>(&ref);
                write && write->Key.TabletId == tabletId) {
            return true;
        }
    }
    return false;
}

} // namespace NKikimr::NDDisk
