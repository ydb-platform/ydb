#pragma once

#include "hive.h"
#include "leader_tablet_info.h"

namespace NKikimr {
namespace NHive {

using namespace NKikimrBlobStorage;

struct TStoragePoolInfo;

struct TGroupFilter {
    NKikimrBlobStorage::TEvControllerSelectGroups::TGroupParameters GroupParameters;
    bool PhysicalGroupsOnly = false;
};

struct TStorageGroupInfo {
    const TStoragePoolInfo& StoragePool;
    TStorageGroupId Id;
    std::unordered_set<std::pair<const TLeaderTabletInfo*, ui32>> Units; // Tablet + Channel
    double AcquiredIOPS = 0;
    ui64 AcquiredThroughput = 0;
    ui64 AcquiredSize = 0;
    double MaximumIOPS = 0;
    ui64 MaximumThroughput = 0;
    ui64 MaximumSize = 0;
    NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters GroupParameters;

    TStorageGroupInfo(const TStoragePoolInfo& storagePool, TStorageGroupId id);
    TStorageGroupInfo(const TStorageGroupInfo&) = delete;
    TStorageGroupInfo(TStorageGroupInfo&&) = delete;
    TStorageGroupInfo& operator =(const TStorageGroupInfo&) = delete;
    TStorageGroupInfo& operator =(TStorageGroupInfo&&) = delete;
    bool AcquireAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel);
    bool ReleaseAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel);
    void UpdateStorageGroup(const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters);
    bool IsMatchesParameters(const TGroupFilter& filter) const;
    double GetUsage() const;
    double GetMaximumIOPS() const;
    ui64 GetMaximumThroughput() const;
    ui64 GetMaximumSize() const;
    bool IsBalanceByIOPS() const;
    bool IsBalanceByThroughput() const;
    bool IsBalanceBySize() const;
};

}
}
