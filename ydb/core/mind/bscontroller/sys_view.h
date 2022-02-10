#pragma once

#include "impl.h"

namespace NKikimr::NBsController {

struct TControllerSystemViewsState {
    std::unordered_map<TPDiskId, NKikimrSysView::TPDiskInfo, THash<TPDiskId>> PDisks;
    std::unordered_map<TVSlotId, NKikimrSysView::TVSlotInfo, THash<TVSlotId>> VSlots;
    std::unordered_map<TGroupId, NKikimrSysView::TGroupInfo, THash<TGroupId>> Groups;
    std::unordered_map<TBlobStorageController::TBoxStoragePoolId, NKikimrSysView::TStoragePoolInfo,
        THash<TBlobStorageController::TBoxStoragePoolId>> StoragePools;
};

struct TEvControllerUpdateSystemViews :
    TEventLocal<TEvControllerUpdateSystemViews, TEvBlobStorage::EvControllerUpdateSystemViews>
{
    TControllerSystemViewsState State;
    std::unordered_set<TPDiskId, THash<TPDiskId>> DeletedPDisks;
    std::unordered_set<TVSlotId, THash<TVSlotId>> DeletedVSlots;
    std::unordered_set<TGroupId, THash<TGroupId>> DeletedGroups;
    std::unordered_set<TBlobStorageController::TBoxStoragePoolId, THash<TBlobStorageController::TBoxStoragePoolId>> DeletedStoragePools;
    TBlobStorageController::THostRecordMap HostRecords;
    ui32 GroupReserveMin;
    ui32 GroupReservePart;
};

} // NKikimr::NBsController

