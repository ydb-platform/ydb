#include "storage_group_info.h"
#include "storage_pool_info.h"

namespace NKikimr {
namespace NHive {

TStorageGroupInfo::TStorageGroupInfo(const TStoragePoolInfo& storagePool, TStorageGroupId id)
    : StoragePool(storagePool)
    , Id(id)
{}

bool TStorageGroupInfo::AcquireAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel) {
    Y_VERIFY(tablet->BoundChannels.size() > channel);
    bool acquired = Units.insert({tablet, channel}).second;
    if (acquired) {
        AcquiredIOPS += tablet->BoundChannels[channel].GetIOPS();
        AcquiredThroughput += tablet->BoundChannels[channel].GetThroughput();
        AcquiredSize += tablet->BoundChannels[channel].GetSize();
    }
    return acquired;
}

bool TStorageGroupInfo::ReleaseAllocationUnit(const TLeaderTabletInfo* tablet, ui32 channel) {
    Y_VERIFY(tablet->BoundChannels.size() > channel);
    bool released = Units.erase({tablet, channel}) != 0;
    if (released) {
        AcquiredIOPS -= tablet->BoundChannels[channel].GetIOPS();
        AcquiredThroughput -= tablet->BoundChannels[channel].GetThroughput();
        AcquiredSize -= tablet->BoundChannels[channel].GetSize();
    }
    return released;
}

void TStorageGroupInfo::UpdateStorageGroup(const TEvControllerSelectGroupsResult::TGroupParameters& groupParameters) {
    if (groupParameters.GetAssuredResources().HasIOPS()) {
        MaximumIOPS = groupParameters.GetAssuredResources().GetIOPS();
    }
    if (groupParameters.GetAssuredResources().HasReadThroughput() || groupParameters.GetAssuredResources().HasWriteThroughput()) {
        MaximumThroughput = groupParameters.GetAssuredResources().GetReadThroughput() + groupParameters.GetAssuredResources().GetWriteThroughput();
    }
    if (groupParameters.GetAssuredResources().HasSpace()) {
        MaximumSize = groupParameters.GetAssuredResources().GetSpace();
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
    if (StoragePool.GetSafeMode()) {
        return true;
    }
    if (IsBalanceByIOPS() && groupParameters.HasRequiredIOPS() && groupParameters.GetRequiredIOPS() + AcquiredIOPS > GetMaximumIOPS()) {
        return false;
    }
    if (IsBalanceByThroughput() && groupParameters.HasRequiredThroughput() && groupParameters.GetRequiredThroughput() + AcquiredThroughput > GetMaximumThroughput()) {
        return false;
    }
    if (IsBalanceBySize() && groupParameters.HasRequiredDataSize() && groupParameters.GetRequiredDataSize() + AcquiredSize > GetMaximumSize()) {
        return false;
    }
    return true;
}

double TStorageGroupInfo::GetUsage() const {
    double usage = 0;
    int countUsage = 0;
    if (IsBalanceByIOPS() && MaximumIOPS > 0) {
        usage = std::max<double>(usage, static_cast<double>(AcquiredIOPS) / GetMaximumIOPS());
        ++countUsage;
    }
    if (IsBalanceByThroughput() && MaximumThroughput > 0) {
        usage = std::max<double>(usage, static_cast<double>(AcquiredThroughput) / GetMaximumThroughput());
        ++countUsage;
    }
    if (IsBalanceBySize() && MaximumSize > 0) {
        usage = std::max<double>(usage, static_cast<double>(AcquiredSize) / GetMaximumSize());
        ++countUsage;
    }
    if (countUsage > 0) {
        return usage;
    } else {
        return 1.0;
    }
}

double TStorageGroupInfo::GetMaximumIOPS() const {
    return MaximumIOPS * StoragePool.GetOvercommitIOPS();
}

ui64 TStorageGroupInfo::GetMaximumThroughput() const {
    return MaximumThroughput * StoragePool.GetOvercommitThroughput();
}

ui64 TStorageGroupInfo::GetMaximumSize() const {
    return MaximumSize * StoragePool.GetOvercommitSize();
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
