#pragma once

#include "impl.h"

#include <ydb/core/protos/blobstorage_disk.pb.h>
#include <ydb/core/sys_view/common/events.h>

#include <util/system/types.h>

#include <vector>

namespace NKikimr::NBsController {

struct TControllerSystemViewsState {
    std::unordered_map<TPDiskId, NKikimrSysView::TPDiskInfo, THash<TPDiskId>> PDisks;
    std::unordered_map<TVSlotId, NKikimrSysView::TVSlotInfo, THash<TVSlotId>> VSlots;
    std::unordered_map<TGroupId, NKikimrSysView::TGroupInfo, THash<TGroupId>> Groups;
    std::unordered_map<TBoxStoragePoolId, NKikimrSysView::TStoragePoolInfo, THash<TBoxStoragePoolId>> StoragePools;
};

struct TEvControllerUpdateSystemViews :
    TEventLocal<TEvControllerUpdateSystemViews, TEvBlobStorage::EvControllerUpdateSystemViews>
{
    TControllerSystemViewsState State;
    std::unordered_set<TPDiskId, THash<TPDiskId>> DeletedPDisks;
    std::unordered_set<TVSlotId, THash<TVSlotId>> DeletedVSlots;
    std::unordered_set<TGroupId, THash<TGroupId>> DeletedGroups;
    std::unordered_set<TBoxStoragePoolId, THash<TBoxStoragePoolId>> DeletedStoragePools;
    TBlobStorageController::THostRecordMap HostRecords;
    ui32 GroupReserveMin;
    ui32 GroupReservePart;
};

struct TEvCalculateStorageStatsRequest :
    TEventLocal<TEvCalculateStorageStatsRequest, NSysView::TEvSysView::EvCalculateStorageStatsRequest>
{
};

struct TEvCalculateStorageStatsResponse :
    TEventLocal<TEvCalculateStorageStatsResponse, NSysView::TEvSysView::EvCalculateStorageStatsResponse>
{
    template <typename T>
    TEvCalculateStorageStatsResponse(T&& t)
        : StorageStats(std::forward<T>(t))
    {}

    std::vector<NKikimrSysView::TStorageStatsEntry> StorageStats;
};

struct TGroupDiskInfo {
    const NKikimrBlobStorage::TPDiskMetrics *PDiskMetrics;
    const NKikimrBlobStorage::TVDiskMetrics *VDiskMetrics;
    ui32 ExpectedSlotCount;
};

void CalculateGroupUsageStats(NKikimrSysView::TGroupInfo *info, const std::vector<TGroupDiskInfo>& disks, TBlobStorageGroupType type);

} // NKikimr::NBsController
