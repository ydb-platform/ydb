#include "persistent_buffer_space_allocator.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::TChunkSpaceOccupation(ui32 chunkIdx, ui32 start, ui32 end)
        : ChunkIdx(chunkIdx) {
        FreeSectors.insert({start, end});
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result) {
        Y_ABORT_UNLESS(sectorsCount > result.size());

        for (auto it : FreeSectors) {
            if (it.Size() >= sectorsCount - result.size()) {
                TSpaceRange freeRange = it;
                FreeSectors.erase(it);
                while(result.size() < sectorsCount) {
                    result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.Start, 0, 0});
                    freeRange.Start++;
                }
                if (freeRange.Start <= freeRange.End) {
                    FreeSectors.insert(freeRange);
                }
                return;
            }
        }

        while (result.size() < sectorsCount && FreeSectors.size() > 0) {
            auto it = FreeSectors.begin();
            TSpaceRange freeRange = *it;
            FreeSectors.erase(it);
            while(result.size() < sectorsCount && freeRange.Start <= freeRange.End) {
                result.emplace_back(TPersistentBufferSectorInfo{ChunkIdx, freeRange.Start, 0, 0});
                freeRange.Start++;
            }
            if (freeRange.Start <= freeRange.End) {
                Y_ABORT_UNLESS(result.size() == sectorsCount);
                FreeSectors.insert(freeRange);
            }
        }
    }

    void TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::Free(ui32 sectorIdx) {
        auto [it, inserted] = FreeSectors.insert({sectorIdx, sectorIdx});
        Y_ABORT_UNLESS(inserted);
        if (it != FreeSectors.begin()) {
            auto prev = std::prev(it);
            Y_ABORT_UNLESS(prev->End < it->Start);
            if (prev->End + 1 == it->Start) {
                TSpaceRange newRange{prev->Start, it->End};
                FreeSectors.erase(it);
                FreeSectors.erase(prev);
                auto [newIt, inserted] = FreeSectors.insert(newRange);
                Y_ABORT_UNLESS(inserted);
                it = newIt;
            }
        }
        if (std::next(it) != FreeSectors.end()) {
            auto next = std::next(it);
            Y_ABORT_UNLESS(it->End < next->Start);
            if (it->End + 1 == next->Start) {
                TSpaceRange newRange{it->Start, next->End};
                FreeSectors.erase(it);
                FreeSectors.erase(next);
                auto [_, inserted] = FreeSectors.insert(newRange);
                Y_ABORT_UNLESS(inserted);
            }
        }
    }

    ui32 TPersistentBufferSpaceAllocator::TChunkSpaceOccupation::FreeSpace() const {
        ui32 result = 0;
        for (const auto& range : FreeSectors) {
            result += range.Size();
        }
        return result;
    }

    ui32 TPersistentBufferSpaceAllocator::FreeSpace() const {
        ui32 result = 0;
        for (const auto& [idx, chunk] : FreeSpaceMap) {
            result += chunk.FreeSpace();
        }
        return result;
    }

    TPersistentBufferSpaceAllocator::TPersistentBufferSpaceAllocator(ui32 sectorsInChunk, ui32 maxChunks)
        : SectorsInChunk(sectorsInChunk)
        , MaxChunks(maxChunks)
    {}

    std::vector<TPersistentBufferSectorInfo> TPersistentBufferSpaceAllocator::Occupy(ui32 sectorsCount) {
        std::vector<TPersistentBufferSectorInfo> result;
        if (FreeSpace() < sectorsCount) {
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
        return result;
    }

    void TPersistentBufferSpaceAllocator::Free(const std::vector<TPersistentBufferSectorInfo>& locations) {
        for (const auto& location : locations) {
            const auto& it = FreeSpaceMap.find(location.ChunkIdx);
            Y_ABORT_UNLESS(it != FreeSpaceMap.end());
            it->second.Free(location.SectorIdx);
        }
    }

    void TPersistentBufferSpaceAllocator::AddNewChunk(ui32 chunkIdx) {
        FreeSpaceMap.insert({chunkIdx, TChunkSpaceOccupation{chunkIdx, 0, SectorsInChunk - 1}});
        OwnedChunks.push_back(chunkIdx);
    }


} // NKikimr::NDDisk
