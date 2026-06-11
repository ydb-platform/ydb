#pragma once

#include "defs.h"
#include "persistent_buffer_header.h"

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

namespace NKikimr::NDDisk {

    class TPersistentBufferSpaceAllocator {
    protected:

        struct TChunkInfo {
            ui32 ChunkIdx;
            ui32 FreeSpace;
        };

        using TChunksByFreeSpace = std::vector<TChunkInfo>;

        struct TChunkSpaceOccupation {
            struct TSpaceRange {
                ui32 First;
                ui32 Last;

                ui32 Size() const { return Last - First + 1; }
                TString ToString() const {return TStringBuilder() << First << "-" << Last;}
            };

            struct TSequentialComparator {
                bool operator()(const TSpaceRange& a, const TSpaceRange& b) const {
                    return a.First < b.First;
                }
            };

            struct TBestChoiceComparator {
                bool operator()(const TSpaceRange& a, const TSpaceRange& b) const {
                    return a.Size() > b.Size() || (a.Size() == b.Size() && a.First < b.First);
                }
            };

            ui32 ChunkIdx;
            ui32 FreeSpace;
            std::set<TSpaceRange, TSequentialComparator> FreeSectors;
            std::set<TSpaceRange, TBestChoiceComparator> FreeSectorsPriorityQueue;
            TPersistentBufferSpaceAllocator& Owner;

            void Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result);
            void Free(ui32 fromSectorIdx, ui32 toSectorIdx);
            void MarkOccupied(ui32 fromSectorIdx, ui32 toSectorIdx);

            TChunkSpaceOccupation(TPersistentBufferSpaceAllocator& owner, ui32 chunkIdx, ui32 first, ui32 last);

            TString ToString() const;
            ui32 VerifyFreeSpace();
        };

        ui32 SectorsInChunk;
        absl::flat_hash_map<ui32, TChunkSpaceOccupation> FreeSpaceMap;
        TChunksByFreeSpace ChunksByFreeSpace;
        ui32 FreeSpace = 0;

        static bool ChunksByFreeSpaceLess(const TChunkInfo& a, const TChunkInfo& b);
        void UpdateChunkInSortedQueue(ui32 chunkIdx, ui32 freeSpace, ui32 oldFreeSpace);
        void AddChunkToSortedQueue(ui32 chunkIdx, ui32 freeSpace);
        ui32 VerifyFreeSpace();

    public:
        std::vector<ui32> OwnedChunks;

        TPersistentBufferSpaceAllocator(ui32 sectorsInChunk = 32768);

        std::vector<TPersistentBufferSectorInfo> Occupy(ui32 sectorsCount);
        void Free(const std::vector<TPersistentBufferSectorInfo>& locations);
        void MarkOccupied(const std::vector<TPersistentBufferSectorInfo>& locations);
        void AddNewChunk(ui32 chunkIdx);
        ui32 GetFreeSpace() const {
            return FreeSpace;
        }
        std::vector<std::vector<std::tuple<ui32, ui32>>> DescribeFreeSpace();
        TString ToString() const;

    };
}
