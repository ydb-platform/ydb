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
                ui32 Start;
                ui32 End;

                ui32 Size() const { return End - Start + 1; }

                bool operator<(const TSpaceRange& other) const {
                    return Start < other.Start;
                }
            };

            ui32 ChunkIdx;
            std::set<TSpaceRange> FreeSectors;

            void Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result);
            void Free(ui32 sectorIdx);
            ui32 FreeSpace() const;

            TChunkSpaceOccupation(ui32 chunkIdx, ui32 start, ui32 end);
        };

        ui32 SectorsInChunk;
        ui32 MaxChunks;
        std::unordered_map<ui32, TChunkSpaceOccupation> FreeSpaceMap;
        ui32 OccupyChunkSeed = 0;

        public:
        std::vector<ui32> OwnedChunks;

        TPersistentBufferSpaceAllocator(ui32 sectorsInChunk = 32768, ui32 maxChunks = 128);

        std::vector<TPersistentBufferSectorInfo> Occupy(ui32 sectorsCount);
        void Free(const std::vector<TPersistentBufferSectorInfo>& locations);
        void AddNewChunk(ui32 chunkIdx);
        ui32 FreeSpace() const;

    };
}
