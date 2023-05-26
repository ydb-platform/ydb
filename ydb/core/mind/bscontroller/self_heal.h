#pragma once

#include "defs.h"

#include "types.h"

namespace NKikimr::NBsController {

    class TGroupGeometryInfo;

    struct TEvControllerUpdateSelfHealInfo : TEventLocal<TEvControllerUpdateSelfHealInfo, TEvBlobStorage::EvControllerUpdateSelfHealInfo> {
        struct TGroupContent {
            struct TVDiskInfo {
                TVSlotId Location;
                bool Faulty;
                bool Bad;
                bool Decommitted;
                bool OnlyPhantomsRemain;
                bool IsReady;
                NKikimrBlobStorage::EVDiskStatus VDiskStatus;
            };
            ui32 Generation;
            TBlobStorageGroupType Type;
            TMap<TVDiskID, TVDiskInfo> VDisks;
            std::shared_ptr<TGroupGeometryInfo> Geometry;
        };

        THashMap<TGroupId, std::optional<TGroupContent>> GroupsToUpdate; // groups with faulty groups that are changed or got faulty PDisks for the first time
        TVector<std::tuple<TVDiskID, NKikimrBlobStorage::EVDiskStatus, bool>> VDiskStatusUpdate;
        TVector<std::pair<TVDiskID, bool>> VDiskIsReadyUpdate;
        std::optional<bool> GroupLayoutSanitizerEnabled;

        ui64 ConfigTxSeqNo = 0;
    };

} // NKikimr::NBsController
