#include "persistent_buffer_space_allocator.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::TChunkSpaceOccupation(TPersistentBufferSpaceAllocator::TChunksQueue& ownerChunksQueue, ui32 chunkIdx, ui32 first, ui32 last)
        : ChunkIdx(chunkIdx)
        , OwnerChunksQueue(ownerChunksQueue) {
        Y_ABORT_UNLESS(last >= first);
        FreeSectors.insert({first, last});
        FreeSectorsPriorityQueue.insert({first, last});
        FreeSpace = last - first + 1;
        OwnerChunksQueue.insert(GetRank());

    }

    TString TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::ToString() const {
        TStringBuilder sb;
        sb << "ChunkIdx: " << ChunkIdx << ", FreeSpace: " << FreeSpace << ", FreeSectors: [";
        for (auto s : FreeSectors) {
            sb << s.ToString() << " ";
        }
        sb << "], FreeSectorsPriorityQueue: [";
        for (auto s : FreeSectorsPriorityQueue) {
            sb << s.ToString() << " ";
        }
        sb << "]";
        return sb;
    }

    TString TPersistentBufferSpaceAllocator::ToString() const {
        TStringBuilder sb;
        sb << "FreeSpace: " << FreeSpace << ", ChunksPriority: [";
        for (auto s : ChunksPriorityQueue) {
            sb << s.ChunkIdx << ":" << s.FreeSpace << " ";
        }
        sb << "], Chunks: [\n";
        for (auto [i, s] : FreeSpaceMap) {
            sb << s.ToString() << "\n";
        }
        sb << "\n]";
        return sb;
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result) {
        Y_ABORT_UNLESS(sectorsCount > result.size());
        OwnerChunksQueue.erase(GetRank());

        auto process = [&](auto it) {
            TSpaceRange freeRange = *it;
            FreeSectors.erase(*it);
            FreeSectorsPriorityQueue.erase(it);
            while(result.size() < sectorsCount && freeRange.First <= freeRange.Last) {
                result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.First, 0, 0, 0});
                Y_ABORT_UNLESS(FreeSpace > 0);
                FreeSpace--;
                freeRange.First++;
            }
            if (freeRange.First <= freeRange.Last) {
                Y_ABORT_UNLESS(result.size() == sectorsCount);
                FreeSectors.insert(freeRange);
                FreeSectorsPriorityQueue.insert(freeRange);
            }
        };

        ui32 needSpace = sectorsCount - result.size();
        TSpaceRange searchSpace{0, needSpace};
        if (auto it = FreeSectorsPriorityQueue.lower_bound(searchSpace); it != FreeSectorsPriorityQueue.end() && it->Size() == needSpace) {
            process(it);
            Y_ABORT_UNLESS(result.size() == sectorsCount);
        } else {
            while (result.size() < sectorsCount && FreeSectors.size() > 0) {
                process(FreeSectorsPriorityQueue.begin());
            }
        }
        OwnerChunksQueue.insert(GetRank());
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Free(ui32 fromSectorIdx, ui32 toSectorIdx) {
        Y_ABORT_UNLESS(toSectorIdx >= fromSectorIdx);
        OwnerChunksQueue.erase(GetRank());

        auto [it, inserted] = FreeSectors.insert({fromSectorIdx, toSectorIdx});
        auto [_, inserted2] = FreeSectorsPriorityQueue.insert({fromSectorIdx, toSectorIdx});
        Y_ABORT_UNLESS(inserted && inserted2);
        FreeSpace += toSectorIdx - fromSectorIdx + 1;
        auto replace = [this](TSpaceRange newRange, auto it1, auto it2) {
            FreeSectorsPriorityQueue.erase(*it1);
            FreeSectorsPriorityQueue.erase(*it2);
            FreeSectors.erase(it1);
            FreeSectors.erase(it2);
            auto [newIt, inserted] = FreeSectors.insert(newRange);
            auto [_, inserted2] = FreeSectorsPriorityQueue.insert(newRange);
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
        OwnerChunksQueue.insert(GetRank());
    }

    TPersistentBufferSpaceAllocator::TPersistentBufferSpaceAllocator(ui32 sectorsInChunk, ui32 maxChunks)
        : SectorsInChunk(sectorsInChunk)
        , MaxChunks(maxChunks)
    {}

    std::vector<TPersistentBufferSectorInfo> TPersistentBufferSpaceAllocator::Occupy(ui32 sectorsCount) {
        std::vector<TPersistentBufferSectorInfo> result;
        result.reserve(sectorsCount);
        if (FreeSpace < sectorsCount) {
            return result;
        }
        while (result.size() != sectorsCount) {
            Y_ABORT_UNLESS(!ChunksPriorityQueue.empty());
            ui32 chunkIdx = ChunksPriorityQueue.begin()->ChunkIdx;
            auto it = FreeSpaceMap.find(chunkIdx);
            Y_ABORT_UNLESS(it != FreeSpaceMap.end());
            it->second.Occupy(sectorsCount, result);
        }
        Y_ABORT_UNLESS(result.size() == sectorsCount);
        FreeSpace -= sectorsCount;
        return result;
    }


    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::MarkOccupied(ui32 fromSectorIdx, ui32 toSectorIdx) {
        OwnerChunksQueue.erase(GetRank());
        Y_ABORT_UNLESS(FreeSpace >= toSectorIdx - fromSectorIdx + 1);
        FreeSpace -= toSectorIdx - fromSectorIdx + 1;
        TSpaceRange searchSpace{fromSectorIdx, toSectorIdx};
        auto it = FreeSectors.lower_bound(searchSpace);
        if (it == FreeSectors.end() || it->First > fromSectorIdx) {
            it--;
        }
        Y_ABORT_UNLESS(it != FreeSectors.end() && it->First <= fromSectorIdx && it->Last >= toSectorIdx);
        ui32 iFirst = it->First;
        ui32 iLast = it->Last;
        FreeSectors.erase(it);
        FreeSectorsPriorityQueue.erase(*it);
        if (iFirst != fromSectorIdx) {
            auto [it2, inserted] = FreeSectors.insert({iFirst, fromSectorIdx - 1});
            Y_ABORT_UNLESS(inserted);
            auto [_, inserted2] = FreeSectorsPriorityQueue.insert(*it2);
            Y_ABORT_UNLESS(inserted2);
        }
        if (iLast != toSectorIdx) {
            auto [it2, inserted] = FreeSectors.insert({toSectorIdx + 1, iLast});
            Y_ABORT_UNLESS(inserted);
            auto [_, inserted2] = FreeSectorsPriorityQueue.insert(*it2);
            Y_ABORT_UNLESS(inserted2);
        }
        OwnerChunksQueue.insert(GetRank());
    }

    void TPersistentBufferSpaceAllocator::Free(const std::vector<TPersistentBufferSectorInfo>& locations) {
        for (ui32 i = 1, startLoc = 0; i <= locations.size(); i++) {
            if (i == locations.size()
                || locations[i].ChunkIdx != locations[startLoc].ChunkIdx
                || locations[i].SectorIdx != locations[startLoc].SectorIdx + i - startLoc) {
                const auto& it = FreeSpaceMap.find(locations[startLoc].ChunkIdx);
                Y_ABORT_UNLESS(it != FreeSpaceMap.end());
                it->second.Free(locations[startLoc].SectorIdx, locations[i - 1].SectorIdx);
                FreeSpace += locations[i - 1].SectorIdx - locations[startLoc].SectorIdx + 1;
                startLoc = i;
            }
        }
    }

    void TPersistentBufferSpaceAllocator::AddNewChunk(ui32 chunkIdx) {
        auto [it, inserted] = FreeSpaceMap.insert({chunkIdx, TChunkSpaceOccupation{ChunksPriorityQueue, chunkIdx, 0, SectorsInChunk - 1}});
        Y_ABORT_UNLESS(inserted);
        OwnedChunks.push_back(chunkIdx);
        FreeSpace += SectorsInChunk;
    }

    void TPersistentBufferSpaceAllocator::MarkOccupied(const std::vector<TPersistentBufferSectorInfo>& locations) {
        for (ui32 i = 1, startLoc = 0; i <= locations.size(); i++) {
            if (i == locations.size()
                || locations[i].ChunkIdx != locations[startLoc].ChunkIdx
                || locations[i].SectorIdx != locations[startLoc].SectorIdx + i - startLoc) {
                const auto& it = FreeSpaceMap.find(locations[startLoc].ChunkIdx);
                Y_ABORT_UNLESS(it != FreeSpaceMap.end());
                it->second.MarkOccupied(locations[startLoc].SectorIdx, locations[i - 1].SectorIdx);
                FreeSpace -= locations[i - 1].SectorIdx - locations[startLoc].SectorIdx + 1;
                startLoc = i;
            }
        }
    }


} // NKikimr::NDDisk
