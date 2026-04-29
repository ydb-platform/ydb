#pragma once

#include "defs.h"
#include "persistent_buffer.h"
#include "persistent_buffer_header.h"
#include "persistent_buffer_space_allocator.h"

namespace NKikimr::NDDisk {

    struct TEraseBarrier {
        ui32 ChunkIdx;
        ui32 SectorIdx;
        TPersistentBufferHeader Header;
    };

    struct TPersistentBufferBarriersManager {
        std::vector<TEraseBarrier> PersistentBufferBarriers;
        std::unordered_map<ui64, std::tuple<ui32, ui32>> PersistentBufferBarriersLocation;
        std::vector<std::tuple<ui32, ui32>> PersistentBufferBarrierHoles;
        ui32 FreeBarrierPosition = 0;

        bool CanMoveBarrier(ui64 tabletId, ui32 barriersLimit);
        std::unordered_map<ui64, ui64> GetBarriers();
        std::tuple<ui32, ui32, TEraseBarrier&> MoveBarrier(ui64 tabletId, ui64 lsn, const TPersistentBufferSectorInfo& newSector);
        void RestoreBarriers(std::map<std::tuple<ui64, ui32>, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator);
        bool AddBarrier(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx);
    };
}
