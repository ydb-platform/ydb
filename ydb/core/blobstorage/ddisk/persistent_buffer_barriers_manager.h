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

    // The persistent buffer key is (TabletId, DirectBlockGroupIndex): a direct block group number
    // that fits in a single byte (0-255). TPersistentBufferTabletKey packs that pair as the lookup
    // key for the barriers manager's Erases / PersistentBufferBarriersLocation maps.
    // DirectBlockGroupIndex defaults to 0 everywhere so existing single-argument (tabletId-only)
    // call sites keep addressing the same namespace as before this change. Declared at namespace
    // scope (rather than nested inside TPersistentBufferBarriersManager) so the std::hash
    // specialization below is visible before any unordered_map<TPersistentBufferTabletKey, ...>
    // member is instantiated.
    struct TPersistentBufferTabletKey {
        ui64 TabletId;
        ui8 DirectBlockGroupIndex = 0;

        friend constexpr std::strong_ordering operator <=>(const TPersistentBufferTabletKey& x, const TPersistentBufferTabletKey& y) = default;
    };
}

namespace std {
    template <>
    struct hash<NKikimr::NDDisk::TPersistentBufferTabletKey> {
        inline size_t operator()(const NKikimr::NDDisk::TPersistentBufferTabletKey& k) const {
            return MultiHash(k.TabletId, k.DirectBlockGroupIndex);
        }
    };
}

namespace NKikimr::NDDisk {

    struct TPersistentBufferBarriersManager {
        using TTabletKey = TPersistentBufferTabletKey;

        struct TErase {
            std::vector<ui64> Lsns;
            ui32 ChunkIdx = Max<ui32>();
            ui32 SectorIdx = Max<ui32>();
            ui64 HeaderLsn = 0;
            ui32 Generation = 0;
        };

        struct TBarrierLocation {
            ui32 BarrierIdx;
            ui32 Position;
        };

        ui64 PersistentBufferUniqueId;
        ui32 NodeId;
        ui32 PDiskId;
        ui32 SlotId;

        std::unordered_map<TTabletKey, TErase> Erases;

        std::vector<TEraseBarrier> PersistentBufferBarriers;
        std::unordered_map<TTabletKey, TBarrierLocation> PersistentBufferBarriersLocation;

        std::vector<TBarrierLocation> PersistentBufferBarrierHoles;
        ui32 FreeBarrierPosition = 0;

        void Initialize(ui64 uniqueId, ui32 nodeId, ui32 pdiskId, ui32 slotId);
        bool CanMoveBarrier(ui64 tabletId, ui32 barriersLimit, ui8 directBlockGroupIndex = 0);
        TPersistentBufferBarrierRecord GetBarrier(ui64 tabletId, ui8 directBlockGroupIndex = 0) const;
        // Keyed by (TabletId, DirectBlockGroupIndex): a plain TabletId key would collapse barriers
        // belonging to different direct block groups of the same tablet into a single entry.
        std::map<std::pair<ui64, ui8>, ui64> GetBarriers() const;
        std::tuple<ui32, ui32, TEraseBarrier&> MoveBarrier(ui64 tabletId, ui32 generation, ui64 lsn, const TPersistentBufferSectorInfo& newSector, ui8 directBlockGroupIndex = 0);
        void RestoreBarriers(std::map<TPersistentBufferId, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator);
        bool AddBarrier(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx);

        bool Compact(std::vector<ui64>& oldLsns, std::vector<ui64>& newLsns, TPersistentBufferFastErases& header);
        std::vector<ui64> Uncompact(const ui8* data, bool isCompact);
        bool CanFastErase(ui64 tabletId, ui32 generation, ui8 directBlockGroupIndex = 0);
        ui32 GetErasesCount(ui64 tabletId, ui8 directBlockGroupIndex = 0);
        std::optional<TFastErase> Erase(ui64 tabletId, ui32 generation, std::vector<ui64>& lsns, TPersistentBufferSpaceAllocator& allocator, ui8 directBlockGroupIndex = 0);
        bool AddErase(const TPersistentBufferHeader* header, ui32 chunkIdx, ui32 sectorIdx);
        void RestoreErases(std::map<TPersistentBufferId, TPersistentBuffer> &persistentBuffers, TPersistentBufferSpaceAllocator& allocator);
    };
}
