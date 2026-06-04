#pragma once

#include "defs.h"
#include "persistent_buffer.h"
#include "persistent_buffer_header.h"
#include "persistent_buffer_space_allocator.h"

namespace NKikimr::NDDisk {

    struct TEraseBarrier {
        ui32 ChunkIdx;
        ui32 SectorIdx;
        TPersistentBufferBarriers Header;
    };

    struct TFastErase {
        ui32 OldChunkIdx;
        ui32 OldSectorIdx;
        ui32 ChunkIdx;
        ui32 SectorIdx;
        TPersistentBufferFastErases Header;
    };

    struct TPersistentBufferBarriersManager {
        struct TErase {
            std::vector<ui64> Lsns;
            ui32 ChunkIdx = Max<ui32>();
            ui32 SectorIdx = Max<ui32>();
            ui64 HeaderLsn = 0;
        };

        ui64 PersistentBufferUniqueId;
        ui32 NodeId;
        ui32 PDiskId;
        ui32 SlotId;

        std::unordered_map<ui64, TErase> Erases;

        std::vector<TEraseBarrier> PersistentBufferBarriers;
        std::unordered_map<ui64, std::tuple<ui32, ui32>> PersistentBufferBarriersLocation;
        std::vector<std::tuple<ui32, ui32>> PersistentBufferBarrierHoles;
        ui32 FreeBarrierPosition = 0;

        void Initialize(ui64 uniqueId, ui32 nodeId, ui32 pdiskId, ui32 slotId);
        bool CanMoveBarrier(ui64 tabletId, ui32 barriersLimit);
        ui64 GetBarrier(ui64 tabletId) const;
        std::unordered_map<ui64, ui64> GetBarriers() const;
        std::tuple<ui32, ui32, TEraseBarrier&> MoveBarrier(ui64 tabletId, ui64 lsn, const TPersistentBufferSectorInfo& newSector);
        void RestoreBarriers(std::map<std::tuple<ui64, ui32>, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator);
        bool AddBarrier(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx);

        bool Compact(std::vector<ui64>& oldLsns, std::vector<ui64>& newLsns, TPersistentBufferFastErases& header);
        std::vector<ui64> Uncompact(const ui8* data, bool isCompact);
        ui32 GetErasesCount(ui64 tabletId);
        std::optional<TFastErase> Erase(ui64 tabletId, std::vector<ui64>& lsns, TPersistentBufferSpaceAllocator& allocator);
        bool AddErase(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx);
        void RestoreErases(std::map<std::tuple<ui64, ui32>, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator);
    };
}
