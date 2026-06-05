#include "persistent_buffer_space_allocator.h"

#include <ydb/core/util/stlog.h>

#include <algorithm>

namespace NKikimr::NDDisk {

    bool TPersistentBufferSpaceAllocator::ChunksByFreeSpaceLess(const TChunkInfo& a, const TChunkInfo& b) {
        return a.FreeSpace < b.FreeSpace
            || (a.FreeSpace == b.FreeSpace && a.ChunkIdx > b.ChunkIdx);
    }

    void TPersistentBufferSpaceAllocator::UpdateChunkInSortedQueue(ui32 chunkIdx, ui32 freeSpace, ui32 oldFreeSpace) {
        // Search backwards:
        //  * N is relatively small (at most 256 chunks per PB and TChunkInfo is just 8 bytes);
        //  * in the Occupy() path this is O(1) since the chunk was just taken from back().
        //  * in case of Free() might be any item, but for this size both linear and binary search
        //  should be almost equal.

        const auto isOccupy = freeSpace < oldFreeSpace;

        TChunksByFreeSpace::iterator chunkIter;
        if (isOccupy || ChunksByFreeSpace.size() < 64) {
            auto rit = std::find_if(ChunksByFreeSpace.rbegin(), ChunksByFreeSpace.rend(),
                [chunkIdx](const TChunkInfo& c) { return c.ChunkIdx == chunkIdx; });
            Y_ABORT_UNLESS(rit != ChunksByFreeSpace.rend());
            chunkIter = std::prev(rit.base());
        } else {
            // Binary search by the old key; {chunkIdx, oldFreeSpace} is unique in the vector.
            chunkIter = std::lower_bound(ChunksByFreeSpace.begin(), ChunksByFreeSpace.end(),
                TChunkInfo{chunkIdx, oldFreeSpace}, ChunksByFreeSpaceLess);
            Y_ABORT_UNLESS(chunkIter != ChunksByFreeSpace.end()
                && chunkIter->ChunkIdx == chunkIdx && chunkIter->FreeSpace == oldFreeSpace);
        }

        const TChunkInfo updatedInfo{chunkIdx, freeSpace};

        const size_t pos = chunkIter - ChunksByFreeSpace.begin();
        const bool okBefore = pos == 0
            || ChunksByFreeSpaceLess(ChunksByFreeSpace[pos - 1], updatedInfo);
        const bool okAfter = pos + 1 == ChunksByFreeSpace.size()
            || ChunksByFreeSpaceLess(updatedInfo, ChunksByFreeSpace[pos + 1]);

        if (okBefore && okAfter) {
            *chunkIter = updatedInfo;
            return;
        }

        if (!okBefore) {
            // Free space decreased; chunk moves left.
            auto newIt = std::upper_bound(ChunksByFreeSpace.begin(), chunkIter, updatedInfo, ChunksByFreeSpaceLess);
            *chunkIter = updatedInfo;
            if (newIt != chunkIter) {
                std::rotate(newIt, chunkIter, chunkIter + 1);
            }
        } else {
            // Free space increased; chunk moves right.
            Y_ABORT_UNLESS(!okAfter);
            auto newIt = std::upper_bound(chunkIter + 1, ChunksByFreeSpace.end(), updatedInfo, ChunksByFreeSpaceLess);
            *chunkIter = updatedInfo;
            if (newIt != chunkIter + 1) {
                std::rotate(chunkIter, chunkIter + 1, newIt);
            }
        }
    }

    void TPersistentBufferSpaceAllocator::AddChunkToSortedQueue(ui32 chunkIdx, ui32 freeSpace) {
        TChunkInfo info{chunkIdx, freeSpace};
        if (ChunksByFreeSpace.empty() || freeSpace > ChunksByFreeSpace.back().FreeSpace) {
            ChunksByFreeSpace.push_back(info);
        } else {
            auto insertIt = std::upper_bound(ChunksByFreeSpace.begin(), ChunksByFreeSpace.end(), info, ChunksByFreeSpaceLess);
            ChunksByFreeSpace.insert(insertIt, info);
        }
    }

    TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::TChunkSpaceOccupation(TPersistentBufferSpaceAllocator& owner, ui32 chunkIdx, ui32 first, ui32 last)
        : ChunkIdx(chunkIdx)
        , Owner(owner) {
        Y_ABORT_UNLESS(last >= first);
        FreeSectors.insert({first, last});
        FreeSectorsPriorityQueue.insert({first, last});
        FreeSpace = last - first + 1;
    }

    std::vector<std::vector<std::tuple<ui32, ui32>>> TPersistentBufferSpaceAllocator::DescribeFreeSpace() {
        std::vector<std::vector<std::tuple<ui32, ui32>>> res;
        res.resize(FreeSpaceMap.size());
        ui32 resIdx = 0;
        std::vector<ui32> chunkIdx;
        chunkIdx.reserve(FreeSpaceMap.size());
        for (auto& [k, _] : FreeSpaceMap) {
            chunkIdx.push_back(k);
        }
        std::sort(chunkIdx.begin(), chunkIdx.end());

        for (ui32 i : chunkIdx) {
            auto& ch = FreeSpaceMap.at(i);
            res[resIdx].resize(ch.FreeSectors.size());
            ui32 fsIdx = 0;
            for (auto& fs : ch.FreeSectors) {
                res[resIdx][fsIdx++] = {fs.First, fs.Last};
            }
            resIdx++;
        }
        Y_DEBUG_ABORT_UNLESS(FreeSpace == VerifyFreeSpace());
        return res;
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
        sb << "FreeSpace: " << FreeSpace << ", ChunksByFreeSpace: [";
        for (auto s : ChunksByFreeSpace) {
            sb << s.ChunkIdx << ":" << s.FreeSpace << " ";
        }
        sb << "], Chunks: [\n";
        for (auto& [i, s] : FreeSpaceMap) {
            sb << s.ToString() << "\n";
        }
        sb << "\n]";
        return sb;
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result) {
        Y_ABORT_UNLESS(sectorsCount > result.size());

        auto oldFreeSpace = FreeSpace;

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
        // Chunk was just taken from ChunksByFreeSpace.back(), so backward lookup is O(1).
        Owner.UpdateChunkInSortedQueue(ChunkIdx, FreeSpace, oldFreeSpace);
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Free(ui32 fromSectorIdx, ui32 toSectorIdx) {
        Y_ABORT_UNLESS(toSectorIdx >= fromSectorIdx);

        auto oldFreeSpace = FreeSpace;

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
        Owner.UpdateChunkInSortedQueue(ChunkIdx, FreeSpace, oldFreeSpace);
    }

    TPersistentBufferSpaceAllocator::TPersistentBufferSpaceAllocator(ui32 sectorsInChunk)
        : SectorsInChunk(sectorsInChunk) {
        ChunksByFreeSpace.reserve(64);
    }

    std::vector<TPersistentBufferSectorInfo> TPersistentBufferSpaceAllocator::Occupy(ui32 sectorsCount) {
        std::vector<TPersistentBufferSectorInfo> result;
        result.reserve(sectorsCount);
        if (FreeSpace < sectorsCount) {
            return result;
        }
        while (result.size() != sectorsCount) {
            Y_ABORT_UNLESS(!ChunksByFreeSpace.empty());
            ui32 chunkIdx = ChunksByFreeSpace.back().ChunkIdx;
            auto it = FreeSpaceMap.find(chunkIdx);
            Y_ABORT_UNLESS(it != FreeSpaceMap.end());
            it->second.Occupy(sectorsCount, result);
        }
        Y_ABORT_UNLESS(result.size() == sectorsCount);
        FreeSpace -= sectorsCount;
        Y_DEBUG_ABORT_UNLESS(FreeSpace == VerifyFreeSpace());
        return result;
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::MarkOccupied(ui32 fromSectorIdx, ui32 toSectorIdx) {
        Y_ABORT_UNLESS(FreeSpace >= toSectorIdx - fromSectorIdx + 1);
        auto oldFreeSpace = FreeSpace;
        FreeSpace -= toSectorIdx - fromSectorIdx + 1;
        TSpaceRange searchSpace{fromSectorIdx, toSectorIdx};
        auto it = FreeSectors.lower_bound(searchSpace);
        if (it == FreeSectors.end() || it->First > fromSectorIdx) {
            it--;
        }
        Y_ABORT_UNLESS(it != FreeSectors.end() && it->First <= fromSectorIdx && it->Last >= toSectorIdx);
        ui32 iFirst = it->First;
        ui32 iLast = it->Last;
        FreeSectorsPriorityQueue.erase(*it);
        FreeSectors.erase(it);
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
        Owner.UpdateChunkInSortedQueue(ChunkIdx, FreeSpace, oldFreeSpace);
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
        Y_DEBUG_ABORT_UNLESS(FreeSpace == VerifyFreeSpace());
    }

    void TPersistentBufferSpaceAllocator::AddNewChunk(ui32 chunkIdx) {
        auto [it, inserted] = FreeSpaceMap.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(chunkIdx),
            std::forward_as_tuple(*this, chunkIdx, 0, SectorsInChunk - 1));
        Y_ABORT_UNLESS(inserted);
        AddChunkToSortedQueue(chunkIdx, SectorsInChunk);
        OwnedChunks.push_back(chunkIdx);
        FreeSpace += SectorsInChunk;
        Y_DEBUG_ABORT_UNLESS(FreeSpace == VerifyFreeSpace());
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
        Y_DEBUG_ABORT_UNLESS(FreeSpace == VerifyFreeSpace());
    }

    ui32 TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::VerifyFreeSpace() {
        ui32 fs = 0;
        for (auto& v : FreeSectors) {
            fs += v.Last - v.First + 1;
        }
        Y_ABORT_UNLESS(fs == FreeSpace);
        return fs;
    }

    ui32 TPersistentBufferSpaceAllocator::VerifyFreeSpace() {
        ui32 fs = 0;
        for (auto& [k, v] : FreeSpaceMap) {
            fs += v.VerifyFreeSpace();
        }
        return fs;
    }


} // NKikimr::NDDisk
