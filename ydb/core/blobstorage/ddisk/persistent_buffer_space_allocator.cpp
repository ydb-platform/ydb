#include "persistent_buffer_space_allocator.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::TChunkSpaceOccupation(ui32 chunkIdx, ui32 first, ui32 last)
        : ChunkIdx(chunkIdx) {
        Y_ABORT_UNLESS(last >= first);
        FreeSectors.insert({first, last});
        FreeSpace = last - first + 1;
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result) {
        Y_ABORT_UNLESS(sectorsCount > result.size());

        for (auto it : FreeSectors) {
            if (it.Size() >= sectorsCount - result.size()) {
                TSpaceRange freeRange = it;
                FreeSectors.erase(it);
                while(result.size() < sectorsCount) {
                    result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.First, 0, 0});
                    Y_ABORT_UNLESS(FreeSpace > 0);
                    FreeSpace--;
                    freeRange.First++;
                }
                if (freeRange.First <= freeRange.Last) {
                    FreeSectors.insert(freeRange);
                }
                return;
            }
        }

        while (result.size() < sectorsCount && FreeSectors.size() > 0) {
            auto it = FreeSectors.begin();
            TSpaceRange freeRange = *it;
            FreeSectors.erase(it);
            while(result.size() < sectorsCount && freeRange.First <= freeRange.Last) {
                result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.First, 0, 0});
                Y_ABORT_UNLESS(FreeSpace > 0);
                FreeSpace--;
                freeRange.First++;
            }
            if (freeRange.First <= freeRange.Last) {
                Y_ABORT_UNLESS(result.size() == sectorsCount);
                FreeSectors.insert(freeRange);
            }
        }
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Free(ui32 fromSectorIdx, ui32 toSectorIdx) {
        Y_ABORT_UNLESS(toSectorIdx >= fromSectorIdx);
        auto [it, inserted] = FreeSectors.insert({fromSectorIdx, toSectorIdx});
        Y_ABORT_UNLESS(inserted);
        FreeSpace += toSectorIdx - fromSectorIdx + 1;
        if (it != FreeSectors.begin()) {
            auto prev = std::prev(it);
            Y_ABORT_UNLESS(prev->Last < it->First);
            if (prev->Last + 1 == it->First) {
                TSpaceRange newRange{prev->First, it->Last};
                FreeSectors.erase(it);
                FreeSectors.erase(prev);
                auto [newIt, inserted] = FreeSectors.insert(newRange);
                Y_ABORT_UNLESS(inserted);
                it = newIt;
            }
        }
        if (std::next(it) != FreeSectors.end()) {
            auto next = std::next(it);
            Y_ABORT_UNLESS(it->Last < next->First);
            if (it->Last + 1 == next->First) {
                TSpaceRange newRange{it->First, next->Last};
                FreeSectors.erase(it);
                FreeSectors.erase(next);
                auto [_, inserted] = FreeSectors.insert(newRange);
                Y_ABORT_UNLESS(inserted);
            }
        }
    }

    TPersistentBufferSpaceAllocator::TPersistentBufferSpaceAllocator(ui32 sectorsInChunk, ui32 maxChunks)
        : SectorsInChunk(sectorsInChunk)
        , MaxChunks(maxChunks)
    {}

    std::vector<TPersistentBufferSectorInfo> TPersistentBufferSpaceAllocator::Occupy(ui32 sectorsCount) {
        std::vector<TPersistentBufferSectorInfo> result;
        if (FreeSpace < sectorsCount) {
            return result;
        }
        for (ui32 i = 0; i < OwnedChunks.size(); i++) {
            ui32 selectedChunkPos = (i + OccupyChunkSeed) % OwnedChunks.size();
            auto it = FreeSpaceMap.find(OwnedChunks[selectedChunkPos]);
            Y_ABORT_UNLESS(it != FreeSpaceMap.end());
            it->second.Occupy(sectorsCount, result);
            if (result.size() == sectorsCount) {
                OccupyChunkSeed++;
                break;
            }
        }
        Y_ABORT_UNLESS(result.size() == sectorsCount);
        FreeSpace -= sectorsCount;
        return result;
    }

    void TPersistentBufferSpaceAllocator::Free(const std::vector<TPersistentBufferSectorInfo>& locations) {
        ui32 startLoc = 0;
        for (ui32 i = 1; i < locations.size(); i++) {
            if (locations[i].ChunkIdx != locations[startLoc].ChunkIdx
                || locations[i].SectorIdx != locations[startLoc].SectorIdx + i - startLoc) {
                    const auto& it = FreeSpaceMap.find(locations[startLoc].ChunkIdx);
                    Y_ABORT_UNLESS(it != FreeSpaceMap.end());
                    it->second.Free(locations[startLoc].SectorIdx, locations[i - 1].SectorIdx);
                    FreeSpace += locations[i - 1].SectorIdx - locations[startLoc].SectorIdx + 1;
                    startLoc = i;
                }
        }
        const auto& it = FreeSpaceMap.find(locations[startLoc].ChunkIdx);
        Y_ABORT_UNLESS(it != FreeSpaceMap.end());
        it->second.Free(locations[startLoc].SectorIdx, locations.back().SectorIdx);
        FreeSpace += locations.back().SectorIdx - locations[startLoc].SectorIdx + 1;
    }

    void TPersistentBufferSpaceAllocator::AddNewChunk(ui32 chunkIdx) {
        FreeSpaceMap.insert({chunkIdx, TChunkSpaceOccupation{chunkIdx, 0, SectorsInChunk - 1}});
        OwnedChunks.push_back(chunkIdx);
        FreeSpace += SectorsInChunk;
    }


} // NKikimr::NDDisk
