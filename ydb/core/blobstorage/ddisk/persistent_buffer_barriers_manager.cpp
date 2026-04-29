#include "ddisk.h"
#include "persistent_buffer_barriers_manager.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    bool TPersistentBufferBarriersManager::CanMoveBarrier(ui64 tabletId, ui32 barriersLimit) {
        return !PersistentBufferBarrierHoles.empty()
            || PersistentBufferBarriersLocation.find(tabletId) != PersistentBufferBarriersLocation.end()
            || FreeBarrierPosition < TPersistentBufferHeader::MaxBarriersPerHeader // all barriers sectors are full
            || PersistentBufferBarriers.size() < barriersLimit; // max barrier sectors limit reached
    }

    std::unordered_map<ui64, ui64> TPersistentBufferBarriersManager::GetBarriers() {
        std::unordered_map<ui64, ui64> res;
        for (auto& b : PersistentBufferBarriers) {
            for (auto& h : b.Header.Barrier.Barriers) {
                if (h.TabletId != 0) {
                    res.insert({h.TabletId, h.Lsn});
                }
            }
        }
        return res;
    }

    std::tuple<ui32, ui32, TEraseBarrier&> TPersistentBufferBarriersManager::MoveBarrier(ui64 tabletId, ui64 lsn, const TPersistentBufferSectorInfo& newSector) {
        auto it = PersistentBufferBarriersLocation.find(tabletId);
        ui32 barrierIdx = 0;
        ui32 pos = 0;
        if (it == PersistentBufferBarriersLocation.end()) {
            if (!PersistentBufferBarrierHoles.empty()) {
                barrierIdx = std::get<0>(PersistentBufferBarrierHoles.back());
                pos = std::get<1>(PersistentBufferBarrierHoles.back());
                PersistentBufferBarrierHoles.pop_back();
                PersistentBufferBarriersLocation[tabletId] = {barrierIdx, pos};
            } else {
                if (FreeBarrierPosition >= TPersistentBufferHeader::MaxBarriersPerHeader || PersistentBufferBarriers.empty()) {
                    FreeBarrierPosition = 0;

                    TPersistentBufferHeader header;
                    memset(&header, 0, sizeof(TPersistentBufferHeader));
                    memcpy(header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
                    header.Flags = TPersistentBufferHeader::IS_BARRIER;
                    header.Barrier.BarrierLsn = 0;
                    header.Barrier.BarrierIdx = PersistentBufferBarriers.size();
                    PersistentBufferBarriers.push_back({Max<ui32>(), Max<ui32>(), std::move(header)});
                }
                barrierIdx = PersistentBufferBarriers.size() - 1;
                pos = FreeBarrierPosition;
                PersistentBufferBarriersLocation[tabletId] = {barrierIdx, pos};
                FreeBarrierPosition++;
            }
        } else {
            barrierIdx = std::get<0>(it->second);
            pos = std::get<1>(it->second);
        }
        auto& barrier = PersistentBufferBarriers[barrierIdx];

        ui32 oldChunkIdx = barrier.ChunkIdx;
        ui32 oldSectorIdx = barrier.SectorIdx;

        barrier.ChunkIdx = newSector.ChunkIdx;
        barrier.SectorIdx = newSector.SectorIdx;

        if (barrier.Header.Barrier.Barriers[pos].Lsn >= lsn) {
            STLOG(PRI_ERROR, BS_DDISK, BSDD29, "TPersistentBufferBarriersManager::RestoreBarriers tablet new barrier lsn is not bigger than previous", (TabletId, tabletId), (Lsn, lsn), (PrevLsn, barrier.Header.Barrier.Barriers[pos].Lsn));
        }
        barrier.Header.Barrier.Barriers[pos] = {tabletId, lsn};
        barrier.Header.Barrier.BarrierLsn++;

        return {oldChunkIdx, oldSectorIdx, barrier};
    }

    void TPersistentBufferBarriersManager::RestoreBarriers(std::map<std::tuple<ui64, ui32>, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator) {
        for (ui32 pos = 0; pos < PersistentBufferBarriers.size(); pos++) {
            auto& b = PersistentBufferBarriers[pos];
            allocator.MarkOccupied({{.ChunkIdx = b.ChunkIdx, .SectorIdx = b.SectorIdx}});
            for (FreeBarrierPosition = 0; FreeBarrierPosition < TPersistentBufferHeader::MaxBarriersPerHeader && b.Header.Barrier.Barriers[FreeBarrierPosition].TabletId > 0; FreeBarrierPosition++) {
                auto& barrier = b.Header.Barrier.Barriers[FreeBarrierPosition];
                auto it = persistentBuffers.lower_bound({barrier.TabletId, 0});
                if (it == persistentBuffers.end() || std::get<0>(it->first) != barrier.TabletId) {
                    STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::RestoreBarriers tablet records not found, erase barrier marked as free", (TabletId, barrier.TabletId), (Lsn, barrier.Lsn));
                    PersistentBufferBarrierHoles.push_back({pos, FreeBarrierPosition});
                } else {
                    auto it = PersistentBufferBarriersLocation.find(barrier.TabletId);
                    if (it == PersistentBufferBarriersLocation.end()) {
                        PersistentBufferBarriersLocation[barrier.TabletId] = {pos, FreeBarrierPosition};
                    } else {
                        auto oldBarrierLocation = PersistentBufferBarriersLocation[barrier.TabletId];
                        if (barrier.Lsn > PersistentBufferBarriers[std::get<0>(oldBarrierLocation)].Header.Barrier.Barriers[std::get<1>(oldBarrierLocation)].Lsn) {
                            STLOG(PRI_DEBUG, BS_DDISK, BSDD41, "TPersistentBufferBarriersManager::RestoreBarriers duplicated barrier erase record found, bigger lsn used", (TabletId, barrier.TabletId), (Lsn, barrier.Lsn));
                            PersistentBufferBarrierHoles.push_back(it->second);
                            it->second = {pos, FreeBarrierPosition};
                        } else {
                            STLOG(PRI_DEBUG, BS_DDISK, BSDD38, "TPersistentBufferBarriersManager::RestoreBarriers duplicated barrier erase record found, bigger lsn used", (TabletId, barrier.TabletId), (Lsn, barrier.Lsn));
                            PersistentBufferBarrierHoles.push_back({pos, FreeBarrierPosition});
                        }
                    }
                }
                for (; it != persistentBuffers.end() &&
                        std::get<0>(it->first) == barrier.TabletId;) {
                    TPersistentBuffer& buffer = it->second;
                    auto recordIt = buffer.Records.begin();
                    while (recordIt != buffer.Records.end() && recordIt->first <= barrier.Lsn) {
                        auto eraseIt = recordIt++;
                        buffer.Records.erase(eraseIt);
                    }
                    if (buffer.Records.empty()) {
                        auto eraseIt = it++;
                        persistentBuffers.erase(eraseIt);
                    } else {
                        ++it;
                    }
                }
            }
        }
    }

    bool TPersistentBufferBarriersManager::AddBarrier(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx) {
        if (header->Flags & TPersistentBufferHeader::IS_BARRIER == 0) {
            return false;
        }
        auto idx = header->Barrier.BarrierIdx;
        if (idx >= PersistentBufferBarriers.size()) {
            PersistentBufferBarriers.resize(idx + 1);
        }

        if (PersistentBufferBarriers[idx].Header.Barrier.BarrierLsn < header->Barrier.BarrierLsn) {
            PersistentBufferBarriers[idx] = {chunkIdx, sectorIdx, *header};
        }
        return true;
    }
}
