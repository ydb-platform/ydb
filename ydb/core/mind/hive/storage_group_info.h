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

struct TStorageResources {
    double IOPS = 0;
    ui64 Throughput = 0;
    ui64 Size = 0;

    void Add(const TLeaderTabletInfo::TChannel& channel) {
        IOPS += channel.ChannelInfo->GetIOPS();
        Throughput += channel.ChannelInfo->GetIOPS();
        Size += channel.ChannelInfo->GetSize();
    }

    void Subtract(const TLeaderTabletInfo::TChannel& channel) {
        IOPS -= channel.ChannelInfo->GetIOPS();
        Throughput -= channel.ChannelInfo->GetIOPS();
        Size -= channel.ChannelInfo->GetSize();
    }
};

struct TChannelHash {
    size_t operator ()(const TLeaderTabletInfo::TChannel& channel) const {
        return hash_combiner::hash_val(channel.TabletId, channel.ChannelId);
    }
};

struct TStorageGroupInfo {
    const TStoragePoolInfo& StoragePool;
    TStorageGroupId Id;
    std::unordered_set<TLeaderTabletInfo::TChannel, TChannelHash> Units;
    TStorageResources AcquiredResources;
    TStorageResources MaximumResources;
    NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters GroupParameters;

    TStorageGroupInfo(const TStoragePoolInfo& storagePool, TStorageGroupId id);
    TStorageGroupInfo(const TStorageGroupInfo&) = delete;
    TStorageGroupInfo(TStorageGroupInfo&&) = delete;
    TStorageGroupInfo& operator =(const TStorageGroupInfo&) = delete;
    TStorageGroupInfo& operator =(TStorageGroupInfo&&) = delete;
    bool AcquireAllocationUnit(const TLeaderTabletInfo::TChannel& channel);
    bool ReleaseAllocationUnit(const TLeaderTabletInfo::TChannel& channel);
    void UpdateStorageGroup(const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters);
    bool IsMatchesParameters(const TGroupFilter& filter) const;
    double GetUsage(const TStorageResources& resources) const;
    double GetUsage() const;
    double GetUsageForChannel(const TLeaderTabletInfo::TChannel& channel) const; // usage if the channel is reassigned to this group
    double GetMaximumIOPS() const;
    ui64 GetMaximumThroughput() const;
    ui64 GetMaximumSize() const;
    bool IsBalanceByIOPS() const;
    bool IsBalanceByThroughput() const;
    bool IsBalanceBySize() const;
};

}
}
