#pragma once

#include "defs.h"

#include <queue>

namespace NKikimr::NDDisk {

    struct TPersistentBufferSectorInfo {
        ui64 ChunkIdx : 32;
        ui64 SectorIdx : 16;
        ui64 HasSignatureCorrection : 1;
        ui64 Checksum : 64;
    };

    class TPersistentBufferSpaceAllocator {
        protected:

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
            std::set<TSpaceRange, TBestChoiceComparator> FreeSectorsQueue;

            void Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result);
            void Free(ui32 fromSectorIdx, ui32 toSectorIdx);

            TChunkSpaceOccupation(ui32 chunkIdx, ui32 first, ui32 last);

            TString ToString() const;
        };

        ui32 SectorsInChunk;
        ui32 MaxChunks;
        std::unordered_map<ui32, TChunkSpaceOccupation> FreeSpaceMap;
        ui32 OccupyChunkSeed = 0;
        ui32 FreeSpace = 0;

        public:
        std::vector<ui32> OwnedChunks;

        TPersistentBufferSpaceAllocator(ui32 sectorsInChunk = 32768, ui32 maxChunks = 128);

        std::vector<TPersistentBufferSectorInfo> Occupy(ui32 sectorsCount);
        void Free(const std::vector<TPersistentBufferSectorInfo>& locations);
        void AddNewChunk(ui32 chunkIdx);
        ui32 GetFreeSpace() const {
            return FreeSpace;
        }
    };
}
