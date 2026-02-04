#include "persistent_buffer_space_allocator.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::TChunkSpaceOccupation(ui32 chunkIdx, ui32 first, ui32 last)
        : ChunkIdx(chunkIdx) {
        Y_ABORT_UNLESS(last >= first);
        FreeSectors.insert({first, last});
        FreeSectorsQueue.insert({first, last});
        FreeSpace = last - first + 1;
    }

    TString TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::ToString() const {
        TStringBuilder sb;
        sb << "ChunkIdx: " << ChunkIdx << ", FreeSpace: " << FreeSpace << ", FreeSectors: [";
        for (auto s : FreeSectors) {
            sb << s.ToString() << " ";
        }
        sb << "], FreeSectorsQueue: [";
        for (auto s : FreeSectorsQueue) {
            sb << s.ToString() << " ";
        }
        sb << "]";
        return sb;
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result) {
        Y_ABORT_UNLESS(sectorsCount > result.size());

        ui32 needSpace = sectorsCount - result.size();
        TSpaceRange searchSpace{0, needSpace};
        //std::lower_bound(FreeSectorsQueue.begin(), FreeSectorsQueue.end(), searchSpace, TBestChoiceComparator{})
        if (auto it = FreeSectorsQueue.lower_bound(searchSpace); it != FreeSectorsQueue.end() && it->Size() == needSpace) {
            TSpaceRange freeRange = *it;
            FreeSectors.erase(*it);
            FreeSectorsQueue.erase(it);
            while(result.size() < sectorsCount) {
                result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.First, 0, 0});
                Y_ABORT_UNLESS(FreeSpace > 0);
                FreeSpace--;
                freeRange.First++;
            }
            Y_ABORT_UNLESS(result.size() == sectorsCount);

            return;
        }

        while (result.size() < sectorsCount && FreeSectors.size() > 0) {
            auto it = FreeSectorsQueue.begin();
            TSpaceRange freeRange = *it;
            FreeSectorsQueue.erase(it);
            FreeSectors.erase(*it);
            while(result.size() < sectorsCount && freeRange.First <= freeRange.Last) {
                result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.First, 0, 0});
                Y_ABORT_UNLESS(FreeSpace > 0);
                FreeSpace--;
                freeRange.First++;
            }
            if (freeRange.First <= freeRange.Last) {
                Y_ABORT_UNLESS(result.size() == sectorsCount);
                FreeSectors.insert(freeRange);
                FreeSectorsQueue.insert(freeRange);
            }
        }
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Free(ui32 fromSectorIdx, ui32 toSectorIdx) {
        Y_ABORT_UNLESS(toSectorIdx >= fromSectorIdx);
        auto [it, inserted] = FreeSectors.insert({fromSectorIdx, toSectorIdx});
        auto [_, inserted2] = FreeSectorsQueue.insert({fromSectorIdx, toSectorIdx});
        Y_ABORT_UNLESS(inserted && inserted2);
        FreeSpace += toSectorIdx - fromSectorIdx + 1;
        auto replace = [this](TSpaceRange newRange, auto it1, auto it2) {
            FreeSectors.erase(it1);
            FreeSectors.erase(it2);
            FreeSectorsQueue.erase(*it1);
            FreeSectorsQueue.erase(*it2);
            auto [newIt, inserted] = FreeSectors.insert(newRange);
            auto [_, inserted2] = FreeSectorsQueue.insert(newRange);
            Y_ABORT_UNLESS(inserted && inserted2);
            return newIt;
        };
        if (it != FreeSectors.begin()) {
            auto prev = std::prev(it);
            Y_ABORT_UNLESS(prev->Last < it->First);
            if (prev->Last + 1 == it->First) {
                it = replace({prev->First, it->Last}, it, prev);
            }
        }
        if (std::next(it) != FreeSectors.end()) {
            auto next = std::next(it);
            Y_ABORT_UNLESS(it->Last < next->First);
            if (it->Last + 1 == next->First) {
                replace({it->First, next->Last}, it, next);
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
