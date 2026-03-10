#pragma once

#include "defs.h"

#include <queue>

namespace NKikimr::NDDisk {

    struct TPersistentBufferSectorInfo {
        ui64 ChunkIdx : 32;
        ui64 SectorIdx : 16;
        ui64 HasSignatureCorrection : 1;
        ui64 Reserved : 15;
        ui64 Checksum : 64;
    };

    class TPersistentBufferSpaceAllocator {
    protected:

        struct TChunkRank {
            ui32 ChunkIdx;
            ui32 FreeSpace;

            bool operator==(const TChunkRank& other) const {
                return ChunkIdx == other.ChunkIdx;
            }
        };

        struct TBestChunkChoiceComparator {
            bool operator()(const TChunkRank& a, const TChunkRank& b) const {
                return a.FreeSpace > b.FreeSpace || (a.FreeSpace == b.FreeSpace && a.ChunkIdx < b.ChunkIdx);
            }
        };

        using TChunksQueue = std::set<TChunkRank, TBestChunkChoiceComparator>;

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
            TChunksQueue& OwnerChunksQueue;

            void Occupy(ui32 sectorsCount, std::vector<TPersistentBufferSectorInfo>& result);
            void Free(ui32 fromSectorIdx, ui32 toSectorIdx);
            void MarkOccupied(ui32 fromSectorIdx, ui32 toSectorIdx);
            TChunkRank GetRank() const { return {ChunkIdx, FreeSpace}; }

            TChunkSpaceOccupation(TChunksQueue& ownerChunksQueue, ui32 chunkIdx, ui32 first, ui32 last);

            TString ToString() const;
        };

        ui32 SectorsInChunk;
        std::unordered_map<ui32, TChunkSpaceOccupation> FreeSpaceMap;
        TChunksQueue ChunksPriorityQueue;
        ui32 FreeSpace = 0;

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
        TString ToString() const;

    };
}
