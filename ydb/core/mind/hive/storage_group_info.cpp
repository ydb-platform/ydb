#include "storage_group_info.h"
#include "storage_pool_info.h"

namespace NKikimr {
namespace NHive {

TStorageGroupInfo::TStorageGroupInfo(const TStoragePoolInfo& storagePool, TStorageGroupId id)
    : StoragePool(storagePool)
    , Id(id)
{}

bool TStorageGroupInfo::AcquireAllocationUnit(const TLeaderTabletInfo::TChannel& channel) {
    Y_ABORT_UNLESS(channel.ChannelInfo);
    bool acquired = Units.insert(channel).second;
    if (acquired) {
        AcquiredResources.Add(channel);
    }
    return acquired;
}

bool TStorageGroupInfo::ReleaseAllocationUnit(const TLeaderTabletInfo::TChannel& channel) {
    Y_ABORT_UNLESS(channel.ChannelInfo);
    bool released = Units.erase(channel) != 0;
    if (released) {
        AcquiredResources.Subtract(channel);
    }
    return released;
}

void TStorageGroupInfo::UpdateStorageGroup(const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters) {
    if (groupParameters.GetAssuredResources().HasIOPS()) {
        MaximumResources.IOPS = groupParameters.GetAssuredResources().GetIOPS();
    }
    if (groupParameters.GetAssuredResources().HasReadThroughput() || groupParameters.GetAssuredResources().HasWriteThroughput()) {
        MaximumResources.Throughput = groupParameters.GetAssuredResources().GetReadThroughput() + groupParameters.GetAssuredResources().GetWriteThroughput();
    }
    if (groupParameters.GetAssuredResources().HasSpace()) {
        MaximumResources.Size = groupParameters.GetAssuredResources().GetSpace();
    }
    GroupParameters.CopyFrom(groupParameters);
}

bool TStorageGroupInfo::IsMatchesParameters(const TGroupFilter& filter) const {
    const auto& groupParameters = filter.GroupParameters;
    if (groupParameters.GetStoragePoolSpecifier().GetName() != GroupParameters.GetStoragePoolName()) {
        return false;
    }
    if (filter.PhysicalGroupsOnly && GroupParameters.HasPhysicalGroup() && !GroupParameters.GetPhysicalGroup()) {
        return false;
    }
    if (GroupParameters.GetDecommitted()) {
        return false;
    }
    if (StoragePool.GetSafeMode()) {
        return true;
    }
    if (IsBalanceByIOPS() && groupParameters.HasRequiredIOPS() && groupParameters.GetRequiredIOPS() + AcquiredResources.IOPS > GetMaximumIOPS()) {
        return false;
    }
    if (IsBalanceByThroughput() && groupParameters.HasRequiredThroughput() && groupParameters.GetRequiredThroughput() + AcquiredResources.Throughput > GetMaximumThroughput()) {
        return false;
    }
    if (IsBalanceBySize() && groupParameters.HasRequiredDataSize() && groupParameters.GetRequiredDataSize() + AcquiredResources.Size > GetMaximumSize()) {
        return false;
    }
    return true;
}

double TStorageGroupInfo::GetUsage(const TStorageResources& resources) const {
    double usage = 0;
    int countUsage = 0;
    if (IsBalanceByIOPS() && MaximumResources.IOPS > 0) {
        usage = std::max<double>(usage, static_cast<double>(resources.IOPS) / GetMaximumIOPS());
        ++countUsage;
    }
    if (IsBalanceByThroughput() && MaximumResources.Throughput > 0) {
        usage = std::max<double>(usage, static_cast<double>(resources.Throughput) / GetMaximumThroughput());
        ++countUsage;
    }
    if (IsBalanceBySize() && MaximumResources.Size > 0) {
        usage = std::max<double>(usage, static_cast<double>(resources.Size) / GetMaximumSize());
        ++countUsage;
    }
    if (countUsage > 0) {
        return usage;
    } else {
        return 1.0;
    }
}

double TStorageGroupInfo::GetUsage() const {
    return GetUsage(AcquiredResources);
}

double TStorageGroupInfo::GetUsageForChannel(const TLeaderTabletInfo::TChannel& channel) const {
    TStorageResources resources = AcquiredResources;
    if (!Units.contains(channel)) {
        resources.Add(channel);
    }
    return GetUsage(resources);
}

double TStorageGroupInfo::GetMaximumIOPS() const {
    return MaximumResources.IOPS * StoragePool.GetOvercommitIOPS();
}

ui64 TStorageGroupInfo::GetMaximumThroughput() const {
    return MaximumResources.Throughput * StoragePool.GetOvercommitThroughput();
}

ui64 TStorageGroupInfo::GetMaximumSize() const {
    return MaximumResources.Size * StoragePool.GetOvercommitSize();
}

bool TStorageGroupInfo::IsBalanceByIOPS() const {
    return StoragePool.IsBalanceByIOPS();
}

bool TStorageGroupInfo::IsBalanceByThroughput() const {
    return StoragePool.IsBalanceByThroughput();
}

bool TStorageGroupInfo::IsBalanceBySize() const {
    return StoragePool.IsBalanceBySize();
}

}
}
