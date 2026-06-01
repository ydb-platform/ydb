#include "ddisk.h"
#include "persistent_buffer_barriers_manager.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    void TPersistentBufferBarriersManager::Initialize(ui64 uniqueId, ui32 nodeId, ui32 pdiskId, ui32 slotId) {
        PersistentBufferUniqueId = uniqueId;
        NodeId = nodeId;
        PDiskId = pdiskId;
        SlotId = slotId;
    }

    ui64 TPersistentBufferBarriersManager::GetBarrier(ui64 tabletId) const {
        auto it = PersistentBufferBarriersLocation.find(tabletId);
        if (it == PersistentBufferBarriersLocation.end()) {
            return 0;
        }
        const auto pos = std::get<0>(it->second);
        const auto hpos = std::get<1>(it->second);
        Y_ABORT_UNLESS(pos < PersistentBufferBarriers.size());
        Y_ABORT_UNLESS(hpos < TPersistentBufferHeader::MaxBarriersPerHeader);
        return PersistentBufferBarriers[pos].Header.Barrier.Barriers[hpos].Lsn;
    }

    bool TPersistentBufferBarriersManager::CanMoveBarrier(ui64 tabletId, ui32 barriersLimit) {
        return !PersistentBufferBarrierHoles.empty()
            || PersistentBufferBarriersLocation.find(tabletId) != PersistentBufferBarriersLocation.end()
            || FreeBarrierPosition < TPersistentBufferHeader::MaxBarriersPerHeader
            || PersistentBufferBarriers.size() < barriersLimit;
    }

    std::unordered_map<ui64, ui64> TPersistentBufferBarriersManager::GetBarriers() const {
        std::unordered_map<ui64, ui64> res;
        for (auto& b : PersistentBufferBarriers) {
            for (auto& h : b.Header.Barrier.Barriers) {
                if (h.TabletId != 0) {
                    res[h.TabletId] = Max(res[h.TabletId], h.Lsn);
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
                    header.RecordLsn = 0;
                    header.PersistentBufferUniqueId = PersistentBufferUniqueId;
                    header.NodeId = NodeId;
                    header.PDiskId = PDiskId;
                    header.SlotId = SlotId;
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
            STLOG(PRI_ERROR, BS_DDISK, BSDD29, "TPersistentBufferBarriersManager::MoveBarrier tablet new barrier lsn is not bigger than previous", (TabletId, tabletId), (Lsn, lsn), (PrevLsn, barrier.Header.Barrier.Barriers[pos].Lsn));
        }
        barrier.Header.Barrier.Barriers[pos] = {tabletId, lsn};
        barrier.Header.RecordLsn++;

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
                        ui64 oldLsn = PersistentBufferBarriers[std::get<0>(oldBarrierLocation)].Header.Barrier.Barriers[std::get<1>(oldBarrierLocation)].Lsn;
                        STLOG(PRI_DEBUG, BS_DDISK, BSDD38, "TPersistentBufferBarriersManager::RestoreBarriers duplicated barrier erase record found, bigger lsn used", (TabletId, barrier.TabletId), (Lsn, barrier.Lsn), (oldLsn, oldLsn));
                        if (barrier.Lsn > oldLsn) {
                            PersistentBufferBarrierHoles.push_back(it->second);
                            it->second = {pos, FreeBarrierPosition};
                        } else {
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
        if ((header->Flags & TPersistentBufferHeader::IS_BARRIER) == 0) {
            return false;
        }
        auto idx = header->Barrier.BarrierIdx;
        if (idx >= PersistentBufferBarriers.size()) {
            PersistentBufferBarriers.resize(idx + 1);
        }

        if (PersistentBufferBarriers[idx].Header.RecordLsn < header->RecordLsn) {
            PersistentBufferBarriers[idx] = {chunkIdx, sectorIdx, *header};
        }
        return true;
    }

    bool TPersistentBufferBarriersManager::Compact(const std::set<ui64>& lsns, char* compactLsns) {
        ui32 resPos = 0;
        if (lsns.empty()) {
            return false;
        }

        auto it = lsns.begin();
        ui64 first = *it;

        // Write first LSN as 8 raw bytes (little-endian)
        memcpy(compactLsns, &first, 8);
        resPos += 8;
        ui64 prev = first;
        ++it;
        for (; it != lsns.end(); ++it) {
            ui64 delta = *it - prev;
            prev = *it;
            // Encode delta as variable-length (LEB128-style)
            while (delta >= 0x80) {
                if (resPos >= TPersistentBufferHeader::ErasesBufferSize) {
                    return false;
                }
                compactLsns[resPos++] = static_cast<char>((delta & 0x7F) | 0x80);
                delta >>= 7;
            }
            if (resPos >= TPersistentBufferHeader::ErasesBufferSize) {
                return false;
            }
            compactLsns[resPos++] = static_cast<char>(delta & 0x7F);
        }

        return true;
    }

    std::set<ui64> TPersistentBufferBarriersManager::Uncompact(const char* data) {
        std::set<ui64> res;
        ui64 first = 0;
        memcpy(&first, data, 8);
        if (first == 0) {
            return res;
        }
        res.insert(first);

        ui64 prev = first;
        size_t pos = 8;
        while (pos < TPersistentBufferHeader::ErasesBufferSize) {
            ui64 delta = 0;
            int shift = 0;
            while (pos < TPersistentBufferHeader::ErasesBufferSize) {
                ui8 byte = static_cast<ui8>(data[pos++]);
                delta |= static_cast<ui64>(byte & 0x7F) << shift;
                shift += 7;
                if ((byte & 0x80) == 0) {
                    break;
                }
            }
            if (delta == 0) {
                return res;
            }
            prev += delta;
            res.insert(prev);
        }

        return res;
    }

    std::optional<TFastErase> TPersistentBufferBarriersManager::Erase(ui64 tabletId, const std::set<ui64>& lsns,
        TPersistentBufferSpaceAllocator& allocator) {
        if (allocator.GetFreeSpace() < 2 || lsns.size() < 2) {
            return std::nullopt;
        }
        auto& erase = Erases[tabletId];
        auto newLsns = erase.Lsns;
        auto barrier = GetBarrier(tabletId);
        auto it = newLsns.upper_bound(barrier);
        newLsns.erase(newLsns.begin(), it);
        newLsns.insert(lsns.begin(), lsns.end());

        TPersistentBufferHeader header;
        memset(&header, 0, sizeof(TPersistentBufferHeader));
        if (!Compact(newLsns, header.Erase.CompactLsns)) {
            return std::nullopt;
        }
        memcpy(header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
        erase.Lsns = std::move(newLsns);
        auto oldChunkIdx = erase.ChunkIdx;
        auto oldSectorIdx = erase.SectorIdx;
        auto space = allocator.Occupy(1);
        Y_ABORT_UNLESS(space.size() == 1);
        erase.ChunkIdx = space[0].ChunkIdx;
        erase.SectorIdx = space[0].SectorIdx;

        header.Flags = TPersistentBufferHeader::IS_ERASE;
        header.RecordLsn = ++erase.HeaderLsn;
        header.PersistentBufferUniqueId = PersistentBufferUniqueId;
        header.NodeId = NodeId;
        header.PDiskId = PDiskId;
        header.SlotId = SlotId;
        header.Erase.TabletId = tabletId;
        header.Erase.EraseIdx = 0;
        return std::make_optional(TFastErase{oldChunkIdx, oldSectorIdx, erase.ChunkIdx, erase.SectorIdx, std::move(header)});
    }

    bool TPersistentBufferBarriersManager::AddErase(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx) {
        if ((header->Flags & TPersistentBufferHeader::IS_ERASE) == 0) {
            return false;
        }
        auto tabletId = header->Erase.TabletId;
        auto& erase = Erases[tabletId];
        if (erase.HeaderLsn > header->RecordLsn) {
            STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::AddErase deprecated HeaderLsn found ", (TabletId, tabletId), (erase.HeaderLsn, erase.HeaderLsn), (header->RecordLsn, header->RecordLsn));
            return false;
        }
        erase.ChunkIdx = chunkIdx;
        erase.SectorIdx = sectorIdx;
        erase.HeaderLsn = header->RecordLsn;
        erase.Lsns = Uncompact(header->Erase.CompactLsns);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::AddErase", (TabletId, tabletId), (HeaderLsn, header->RecordLsn));
        return true;
    }

    void TPersistentBufferBarriersManager::RestoreErases(std::map<std::tuple<ui64, ui32>, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator) {
        for (auto it = Erases.begin(); it != Erases.end();) {
            auto& [tid, erase] = *it;

            auto pbIt = persistentBuffers.lower_bound({tid, 0});
            bool hasRecords = pbIt != persistentBuffers.end() && std::get<0>(pbIt->first) == tid;
            if (!hasRecords) {
                it = Erases.erase(it);
                continue;
            }

            allocator.MarkOccupied({{.ChunkIdx = erase.ChunkIdx, .SectorIdx = erase.SectorIdx}});

            while (pbIt != persistentBuffers.end() && std::get<0>(pbIt->first) == tid) {
                TPersistentBuffer& buffer = pbIt->second;
                for (ui64 lsn : erase.Lsns) {
                    STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::RestoreErases tablet erase record found", (TabletId, tid), (Lsn, lsn));
                    buffer.Records.erase(lsn);
                }
                if (buffer.Records.empty()) {
                    auto eraseIt = pbIt++;
                    persistentBuffers.erase(eraseIt);
                } else {
                    ++pbIt;
                }
            }
            ++it;
        }
    }

    ui32 TPersistentBufferBarriersManager::GetErasesCount(ui64 tabletId) {
        const auto& it = Erases.find(tabletId);
        if (it == Erases.end()) {
            return 0;
        }
        return it->second.Lsns.size();
    }
}
