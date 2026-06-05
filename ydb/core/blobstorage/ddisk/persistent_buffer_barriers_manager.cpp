#include "ddisk.h"
#include "persistent_buffer_barriers_manager.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    // Returns a new sorted vector containing all unique elements from sorted a and sorted b.
    static std::vector<ui64> MergeUnique(const std::vector<ui64>& a, const std::vector<ui64>& b) {
        std::vector<ui64> result;
        result.reserve(a.size() + b.size());
        auto ai = a.begin(), ae = a.end();
        auto bi = b.begin(), be = b.end();
        while (ai != ae && bi != be) {
            if (*ai < *bi) {
                if (result.empty() || result.back() != *ai) {
                    result.push_back(*ai);
                }
                ++ai;
            } else if (*bi < *ai) {
                if (result.empty() || result.back() != *bi) {
                    result.push_back(*bi);
                }
                ++bi;
            } else {
                if (result.empty() || result.back() != *ai) {
                    result.push_back(*ai);
                }
                ++ai;
                ++bi;
            }
        }
        for (; ai != ae; ++ai) {
            if (result.empty() || result.back() != *ai) {
                result.push_back(*ai);
            }
        }
        for (; bi != be; ++bi) {
            if (result.empty() || result.back() != *bi) {
                result.push_back(*bi);
            }
        }
        return result;
    }

    void TPersistentBufferBarriersManager::Initialize(ui64 uniqueId, ui32 nodeId, ui32 pdiskId, ui32 slotId) {
        PersistentBufferUniqueId = uniqueId;
        NodeId = nodeId;
        PDiskId = pdiskId;
        SlotId = slotId;
    }

    TPersistentBufferBarrierRecord TPersistentBufferBarriersManager::GetBarrier(ui64 tabletId) const {
        auto it = PersistentBufferBarriersLocation.find(tabletId);
        if (it == PersistentBufferBarriersLocation.end()) {
            return {tabletId, 0, 0};
        }
        const auto barrierIdx = it->second.BarrierIdx;
        const auto hpos = it->second.Position;
        Y_ABORT_UNLESS(barrierIdx < PersistentBufferBarriers.size());
        Y_ABORT_UNLESS(hpos < TPersistentBufferBarriers::MaxBarriersPerHeader);
        return PersistentBufferBarriers[barrierIdx].Header.Barriers[hpos];
    }

    bool TPersistentBufferBarriersManager::CanMoveBarrier(ui64 tabletId, ui32 barriersLimit) {
        return !PersistentBufferBarrierHoles.empty()
            || PersistentBufferBarriersLocation.find(tabletId) != PersistentBufferBarriersLocation.end()
            || FreeBarrierPosition < TPersistentBufferBarriers::MaxBarriersPerHeader
            || PersistentBufferBarriers.size() < barriersLimit;
    }

    std::unordered_map<ui64, ui64> TPersistentBufferBarriersManager::GetBarriers() const {
        std::unordered_map<ui64, ui64> res;
        for (auto& b : PersistentBufferBarriers) {
            for (auto& h : b.Header.Barriers) {
                if (h.TabletId != 0) {
                    res[h.TabletId] = Max(res[h.TabletId], h.Lsn);
                }
            }
        }
        return res;
    }

    std::tuple<ui32, ui32, TEraseBarrier&> TPersistentBufferBarriersManager::MoveBarrier(ui64 tabletId, ui32 generation, ui64 lsn, const TPersistentBufferSectorInfo& newSector) {
        auto it = PersistentBufferBarriersLocation.find(tabletId);
        ui32 barrierIdx = 0;
        ui32 pos = 0;
        if (it == PersistentBufferBarriersLocation.end()) {
            if (!PersistentBufferBarrierHoles.empty()) {
                barrierIdx = PersistentBufferBarrierHoles.back().BarrierIdx;
                pos = PersistentBufferBarrierHoles.back().Position;
                PersistentBufferBarrierHoles.pop_back();
                PersistentBufferBarriersLocation[tabletId] = {barrierIdx, pos};
            } else {
                if (FreeBarrierPosition >= TPersistentBufferBarriers::MaxBarriersPerHeader || PersistentBufferBarriers.empty()) {
                    FreeBarrierPosition = 0;

                    TPersistentBufferBarriers header;
                    memset(&header, 0, sizeof(TPersistentBufferBarriers));
                    memcpy(header.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
                    header.Header.Flags = TPersistentBufferHeader::IS_BARRIER;
                    header.Header.RecordLsn = 0;
                    header.Header.PersistentBufferUniqueId = PersistentBufferUniqueId;
                    header.Header.NodeId = NodeId;
                    header.Header.PDiskId = PDiskId;
                    header.Header.SlotId = SlotId;
                    header.Header.RecordIdx = PersistentBufferBarriers.size();
                    header.Header.Version = 0;
                    PersistentBufferBarriers.push_back({Max<ui32>(), Max<ui32>(), std::move(header)});
                }
                barrierIdx = PersistentBufferBarriers.size() - 1;
                pos = FreeBarrierPosition;
                PersistentBufferBarriersLocation[tabletId] = {barrierIdx, pos};
                FreeBarrierPosition++;
            }
        } else {
            barrierIdx = it->second.BarrierIdx;
            pos = it->second.Position;
        }
        auto& barrier = PersistentBufferBarriers[barrierIdx];

        ui32 oldChunkIdx = barrier.ChunkIdx;
        ui32 oldSectorIdx = barrier.SectorIdx;

        barrier.ChunkIdx = newSector.ChunkIdx;
        barrier.SectorIdx = newSector.SectorIdx;

        if (barrier.Header.Barriers[pos].Generation > generation
            || (barrier.Header.Barriers[pos].Generation == generation && barrier.Header.Barriers[pos].Lsn >= lsn)) {
            STLOG(PRI_ERROR, BS_DDISK, BSDD29, "TPersistentBufferBarriersManager::MoveBarrier tablet new barrier lsn is not bigger than previous", (TabletId, tabletId), (Lsn, lsn), (PrevLsn, barrier.Header.Barriers[pos].Lsn));
        }
        barrier.Header.Barriers[pos] = {tabletId, generation, lsn};
        barrier.Header.Header.RecordLsn++;

        auto erasesIt = Erases.find(tabletId);
        if (erasesIt != Erases.end()) {
            if (erasesIt->second.Generation < generation) {
                erasesIt->second.Lsns.clear();
                erasesIt->second.Generation = generation;
            } else {
                auto it = std::upper_bound(erasesIt->second.Lsns.begin(), erasesIt->second.Lsns.end(), lsn);
                erasesIt->second.Lsns = std::vector<ui64>(it, erasesIt->second.Lsns.end());
            }
        }
        return {oldChunkIdx, oldSectorIdx, barrier};
    }

    void TPersistentBufferBarriersManager::RestoreBarriers(std::map<TPersistentBufferId, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator) {
        for (ui32 pos = 0; pos < PersistentBufferBarriers.size(); pos++) {
            auto& b = PersistentBufferBarriers[pos];
            allocator.MarkOccupied({{.ChunkIdx = b.ChunkIdx, .SectorIdx = b.SectorIdx}});
            for (FreeBarrierPosition = 0; FreeBarrierPosition < TPersistentBufferBarriers::MaxBarriersPerHeader && b.Header.Barriers[FreeBarrierPosition].TabletId > 0; FreeBarrierPosition++) {
                auto& barrier = b.Header.Barriers[FreeBarrierPosition];
                auto it = persistentBuffers.lower_bound({barrier.TabletId, 0});
                if (it == persistentBuffers.end() || it->first.TabletId != barrier.TabletId) {
                    STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::RestoreBarriers tablet records not found, erase barrier marked as free", (TabletId, barrier.TabletId), (Lsn, barrier.Lsn));
                    PersistentBufferBarrierHoles.push_back({pos, FreeBarrierPosition});
                } else {
                    auto locationIt = PersistentBufferBarriersLocation.find(barrier.TabletId);
                    if (locationIt == PersistentBufferBarriersLocation.end()) {
                        PersistentBufferBarriersLocation[barrier.TabletId] = {pos, FreeBarrierPosition};
                    } else {
                        auto oldBarrierLocation = PersistentBufferBarriersLocation[barrier.TabletId];
                        auto oldBarrier = PersistentBufferBarriers[oldBarrierLocation.BarrierIdx].Header.Barriers[oldBarrierLocation.Position];
                        STLOG(PRI_DEBUG, BS_DDISK, BSDD38, "TPersistentBufferBarriersManager::RestoreBarriers duplicated barrier erase record found, bigger lsn used",
                            (TabletId, barrier.TabletId),
                            (barrier.Generation, barrier.Generation),
                            (oldBarrier.Generation, oldBarrier.Generation),
                            (barrier.Lsn, barrier.Lsn),
                            (oldBarrier.Lsn, oldBarrier.Lsn),
                        );
                        if (barrier.Generation > oldBarrier.Generation
                            || (barrier.Generation == oldBarrier.Generation && barrier.Lsn > oldBarrier.Lsn)) {
                            PersistentBufferBarrierHoles.push_back(locationIt->second);
                            locationIt->second = {pos, FreeBarrierPosition};
                        } else {
                            PersistentBufferBarrierHoles.push_back({pos, FreeBarrierPosition});
                        }
                    }
                }
                while (it != persistentBuffers.end() && it->first.TabletId == barrier.TabletId) {
                    if (it->first.Generation < barrier.Generation) {
                        it = persistentBuffers.erase(it);
                        continue;
                    }
                    TPersistentBuffer& buffer = it->second;
                    auto recordIt = buffer.Records.begin();
                    while (recordIt != buffer.Records.end() && recordIt->first <= barrier.Lsn) {
                        auto eraseIt = recordIt++;
                        buffer.Records.erase(eraseIt);
                    }
                    if (buffer.Records.empty()) {
                        it = persistentBuffers.erase(it);
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
        TPersistentBufferBarriers* barriersHeader = (TPersistentBufferBarriers*)header;
        auto idx = header->RecordIdx;
        if (idx >= PersistentBufferBarriers.size()) {
            PersistentBufferBarriers.resize(idx + 1);
        }

        if (PersistentBufferBarriers[idx].Header.Header.RecordLsn < header->RecordLsn) {
            PersistentBufferBarriers[idx] = {chunkIdx, sectorIdx, *barriersHeader};
        }
        return true;
    }

    bool TPersistentBufferBarriersManager::Compact(std::vector<ui64>& oldLsns, std::vector<ui64>& newLsns, TPersistentBufferFastErases& header) {
        ui32 resPos = 0;
        ui32 cnt = oldLsns.size() + newLsns.size();
        if (cnt == 0 || cnt > TPersistentBufferFastErases::ErasesBufferLsnCount) {
            return false;
        }
        std::sort(newLsns.begin(), newLsns.end());

        if (cnt * sizeof(oldLsns[0]) <= TPersistentBufferFastErases::ErasesBufferSize) {
            oldLsns = MergeUnique(oldLsns, newLsns);
            memcpy(header.CompactLsns, oldLsns.data(), oldLsns.size() * sizeof(oldLsns[0]));
            return true;
        }

        std::vector<ui64> lsns = MergeUnique(oldLsns, newLsns);

        auto it = lsns.begin();
        ui64 first = *it;

        memcpy(header.CompactLsns, &first, sizeof(first));
        resPos += sizeof(lsns[0]);
        ui64 prev = first;
        ++it;
        for (; it != lsns.end(); ++it) {
            ui64 delta = *it - prev;
            if (delta == 0) {
                continue;
            }
            prev = *it;
            // Encode delta as variable-length (LEB128-style)
            while (delta >= 0x80) {
                if (resPos >= TPersistentBufferFastErases::ErasesBufferSize) {
                    return false;
                }
                header.CompactLsns[resPos++] = static_cast<char>((delta & 0x7F) | 0x80);
                delta >>= 7;
            }
            if (resPos >= TPersistentBufferFastErases::ErasesBufferSize) {
                return false;
            }
            header.CompactLsns[resPos++] = static_cast<char>(delta & 0x7F);
        }
        oldLsns = std::move(lsns);
        header.Header.Flags |= TPersistentBufferHeader::IS_ERASE_COMPACT;
        return true;
    }

    std::vector<ui64> TPersistentBufferBarriersManager::Uncompact(const ui8* data, bool isCompact) {
        std::vector<ui64> res;
        ui64 first = 0;
        memcpy(&first, data, sizeof(res[0]));
        if (first == 0) {
            return res;
        }
        res.push_back(first);

        if (!isCompact) {
            size_t pos = sizeof(res[0]);
            while (pos < TPersistentBufferFastErases::ErasesBufferSize) {
                ui64 v = 0;
                memcpy(&v, data + pos, sizeof(res[0]));
                if (v == 0) {
                    return res;
                }
                res.push_back(v);
                pos += sizeof(res[0]);
            }
            return res;
        }
        ui64 prev = first;
        size_t pos = sizeof(res[0]);
        while (pos < TPersistentBufferFastErases::ErasesBufferSize) {
            ui64 delta = 0;
            int shift = 0;
            while (pos < TPersistentBufferFastErases::ErasesBufferSize) {
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
            res.push_back(prev);
        }

        return res;
    }

    std::optional<TFastErase> TPersistentBufferBarriersManager::Erase(ui64 tabletId, ui32 generation, std::vector<ui64>& lsns,
        TPersistentBufferSpaceAllocator& allocator) {
        if (allocator.GetFreeSpace() < 2 || lsns.size() < 2) {
            return std::nullopt;
        }
        auto& erase = Erases[tabletId];

        if (erase.Generation < generation) {
            erase.Lsns.clear();
            erase.Generation = generation;
        }
        TPersistentBufferFastErases header;
        memset(&header, 0, sizeof(TPersistentBufferFastErases));
        if (!Compact(erase.Lsns, lsns, header)) {
            return std::nullopt;
        }
        header.Generation = generation;
        memcpy(header.Header.Signature, TPersistentBufferHeader::PersistentBufferHeaderSignature, 16);
        auto oldChunkIdx = erase.ChunkIdx;
        auto oldSectorIdx = erase.SectorIdx;
        auto space = allocator.Occupy(1);
        Y_ABORT_UNLESS(space.size() == 1);
        erase.ChunkIdx = space[0].ChunkIdx;
        erase.SectorIdx = space[0].SectorIdx;

        header.Header.Flags |= TPersistentBufferHeader::IS_ERASE;
        header.Header.RecordLsn = ++erase.HeaderLsn;
        header.Header.PersistentBufferUniqueId = PersistentBufferUniqueId;
        header.Header.NodeId = NodeId;
        header.Header.PDiskId = PDiskId;
        header.Header.SlotId = SlotId;
        header.TabletId = tabletId;
        header.Header.RecordIdx = 0;
        header.Header.Version = 0;

        return std::make_optional(TFastErase{oldChunkIdx, oldSectorIdx, erase.ChunkIdx, erase.SectorIdx, std::move(header)});
    }

    bool TPersistentBufferBarriersManager::AddErase(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx) {
        if ((header->Flags & TPersistentBufferHeader::IS_ERASE) == 0) {
            return false;
        }
        TPersistentBufferFastErases* erasesHeader = (TPersistentBufferFastErases*)header;
        auto tabletId = erasesHeader->TabletId;
        auto& erase = Erases[tabletId];
        if (erase.HeaderLsn > header->RecordLsn) {
            STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::AddErase deprecated HeaderLsn found ", (TabletId, tabletId), (erase.HeaderLsn, erase.HeaderLsn), (header->RecordLsn, header->RecordLsn));
            return false;
        }
        erase.ChunkIdx = chunkIdx;
        erase.SectorIdx = sectorIdx;
        erase.HeaderLsn = header->RecordLsn;
        erase.Generation = erasesHeader->Generation;
        erase.Lsns = Uncompact(erasesHeader->CompactLsns, header->Flags & TPersistentBufferHeader::IS_ERASE_COMPACT);
        STLOG(PRI_DEBUG, BS_DDISK, BSDD30, "TPersistentBufferBarriersManager::AddErase", (TabletId, tabletId), (HeaderLsn, header->RecordLsn));
        return true;
    }

    void TPersistentBufferBarriersManager::RestoreErases(std::map<TPersistentBufferId, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator) {
        for (auto it = Erases.begin(); it != Erases.end();) {
            auto& [tid, erase] = *it;

            const auto barrier = GetBarrier(tid);
            auto itErase = std::upper_bound(erase.Lsns.begin(), erase.Lsns.end(), barrier.Lsn);
            erase.Lsns = std::vector<ui64>(itErase, erase.Lsns.end());

            auto pbIt = persistentBuffers.find({tid, erase.Generation});
            if (pbIt == persistentBuffers.end()) {
                it = Erases.erase(it);
                continue;
            }

            allocator.MarkOccupied({{.ChunkIdx = erase.ChunkIdx, .SectorIdx = erase.SectorIdx}});

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

    bool TPersistentBufferBarriersManager::CanFastErase(ui64 tabletId, ui32 generation) {
        const auto barrier = GetBarrier(tabletId);
        return barrier.Generation == generation;
    }

}
