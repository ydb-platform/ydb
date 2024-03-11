#pragma once

#include "hive.h"
#include "storage_group_info.h"

namespace NKikimr {
namespace NHive {

using namespace NKikimrBlobStorage;

class THive;

struct TStoragePoolInfo {
    struct TStats {
        double MinUsage;
        TStorageGroupId MinUsageGroupId;
        double MaxUsage;
        TStorageGroupId MaxUsageGroupId;
        double Scatter = 0;
    };

    THiveSharedSettings* Settings;

    NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy GetBalanceStrategy() const {
        return Settings->GetStorageBalanceStrategy();
    }

    NKikimrConfig::THiveConfig::EHiveStorageSelectStrategy GetSelectStrategy() const {
        return Settings->GetStorageSelectStrategy();
    }

    bool GetSafeMode() const {
        return Settings->GetStorageSafeMode();
    }

    double GetOvercommit() const {
        return Settings->GetStorageOvercommit();
    }

    double GetOvercommitIOPS() const {
        return GetOvercommit();
    }

    double GetOvercommitThroughput() const {
        return GetOvercommit();
    }

    double GetOvercommitSize() const {
        return GetOvercommit();
    }


    TString Name;
    THashMap<TStorageGroupId, TStorageGroupInfo> Groups;
    TInstant LastUpdate;
    TVector<TTabletId> TabletsWaiting;
    ui32 ConfigurationGeneration = 0; // used to discard cache across configuration changes
    ui32 RefreshRequestInFlight = 0;

    TStoragePoolInfo(const TString& name, THiveSharedSettings* hive);
    TStoragePoolInfo(const TStoragePoolInfo&) = delete;
    TStoragePoolInfo(TStoragePoolInfo&&) = delete;
    TStoragePoolInfo& operator =(const TStoragePoolInfo&) = delete;
    TStoragePoolInfo& operator =(TStoragePoolInfo&&) = delete;
    bool AcquireAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel, TStorageGroupId groupId);
    bool ReleaseAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel, TStorageGroupId groupId);
    TStorageGroupInfo& GetStorageGroup(TStorageGroupId groupId);
    void UpdateStorageGroup(TStorageGroupId groupId, const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters);
    void DeleteStorageGroup(TStorageGroupId groupId);
    // Guarantees to first call filter on all groups, then call calculateUsage on ones that passed the filter
    const TEvControllerSelectGroupsResult::TGroupParameters* FindFreeAllocationUnit(std::function<bool(const TStorageGroupInfo&)> filter,
                                                                                    std::function<double(const TStorageGroupInfo*)> calculateUsage = [](const TStorageGroupInfo* group) {
                                                                                        return group->GetUsage();
                                                                                    });
    bool IsBalanceByIOPS() const;
    bool IsBalanceByThroughput() const;
    bool IsBalanceBySize() const;
    bool IsFresh() const;
    void SetAsFresh();
    void Invalidate();
    THolder<TEvControllerSelectGroups::TGroupParameters> BuildRefreshRequest() const;
    bool AddTabletToWait(TTabletId tabletId);
    TVector<TTabletId> PullWaitingTablets();
    template <NKikimrConfig::THiveConfig::EHiveStorageSelectStrategy Strategy>
    size_t SelectGroup(const TVector<double>& groupCandidateUsages);
    TStats GetStats() const;

private:
    size_t RoundRobinPos = 0;
};

}
}
