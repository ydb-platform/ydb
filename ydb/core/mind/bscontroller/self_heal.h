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
                bool IsSelfHealReasonDecommit;
                bool OnlyPhantomsRemain;
                bool IsReady;
                TMonotonic ReadySince;
                NKikimrBlobStorage::EVDiskStatus VDiskStatus;
            };
            ui32 Generation;
            TBlobStorageGroupType Type;
            TMap<TVDiskID, TVDiskInfo> VDisks;
            std::shared_ptr<TGroupGeometryInfo> Geometry;
        };
        struct TVDiskStatusUpdate {
            TVDiskID VDiskId;
            std::optional<bool> OnlyPhantomsRemain;
            std::optional<bool> IsReady;
            std::optional<TMonotonic> ReadySince;
            std::optional<NKikimrBlobStorage::EVDiskStatus> VDiskStatus;
        };

        THashMap<TGroupId, std::optional<TGroupContent>> GroupsToUpdate; // groups with faulty groups that are changed or got faulty PDisks for the first time
        std::vector<TVDiskStatusUpdate> VDiskStatusUpdate;
        std::optional<bool> GroupLayoutSanitizerEnabled;
        std::optional<bool> AllowMultipleRealmsOccupation;
        std::optional<bool> DonorMode;

        ui64 ConfigTxSeqNo = 0;

        TEvControllerUpdateSelfHealInfo() = default;

        TEvControllerUpdateSelfHealInfo(std::vector<TVDiskStatusUpdate>&& updates)
            : VDiskStatusUpdate(std::move(updates))
        {}
    };

} // NKikimr::NBsController
