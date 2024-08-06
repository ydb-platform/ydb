#include "manager.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

std::vector<std::shared_ptr<TManager::TAllocationInfo>> TManager::TGrouppedAllocations::AllocatePossible(TManager& manager, const bool force) {
    std::vector<std::shared_ptr<TManager::TAllocationInfo>> result;
    while (Allocations.size()) {
        const auto& allocation = Allocations.begin()->second;
        if (allocation->GetAllocatedVolume() + manager.ResourceUsage < manager.Config.GetMemoryLimit() || force) {
            if (!allocation->IsAllocated()) {
                allocation->Allocate(manager.OwnerActorId);
                manager.ResourceUsage += allocation->GetAllocatedVolume();
            }
            result.emplace_back(allocation);
        } else {
            return result;
        }
        Allocations.erase(Allocations.begin());
    }
    return result;
}

ui64 TManager::BuildInternalGroupId(const ui64 externalGroupId) {
    auto it = ExternalGroupIntoInternalGroup.find(externalGroupId);
    if (it == ExternalGroupIntoInternalGroup.end()) {
        it = ExternalGroupIntoInternalGroup.emplace(externalGroupId, ++CurrentInternalGroupId).first;
    }
    return it->second;
}

ui64 TManager::GetInternalGroupIdVerified(const ui64 externalGroupId) const {
    auto it = ExternalGroupIntoInternalGroup.find(externalGroupId);
    AFL_VERIFY(it != ExternalGroupIntoInternalGroup.end());
    return it->second;
}

const std::shared_ptr<TManager::TAllocationInfo>& TManager::RegisterAllocationImpl(const std::shared_ptr<IAllocation>& task) {
    auto it = AllocationInfo.find(task->GetIdentifier());
    if (it == AllocationInfo.end()) {
        it = AllocationInfo.emplace(task->GetIdentifier(), std::make_shared<TAllocationInfo>(task)).first;
    }
    return it->second;
}

std::optional<ui64> TManager::GetMinInternalGroupIdOptional() const {
    if (WaitAllocations.size()) {
        if (ReadyAllocations.size()) {
            return std::min(WaitAllocations.begin()->first, ReadyAllocations.begin()->first);
        } else {
            return WaitAllocations.begin()->first;
        }
    } else {
        if (ReadyAllocations.size()) {
            return ReadyAllocations.begin()->first;
        } else {
            return std::nullopt;
        }
    }
}

void TManager::RegisterAllocation(const std::shared_ptr<IAllocation>& task, const ui64 externalGroupId) {
    AFL_VERIFY(task);
    const ui64 internalGroupId = BuildInternalGroupId(externalGroupId);
    auto allocationInfo = RegisterAllocationImpl(task);
    allocationInfo->AddGroupId(internalGroupId);
    if (task->IsAllocated()) {
        ReadyAllocations[internalGroupId].AddAllocation(allocationInfo);
    } else if (WaitAllocations.size() && WaitAllocations.begin()->first < externalGroupId) {
        WaitAllocations[internalGroupId].AddAllocation(allocationInfo);
    } else if (!ResourceUsage || ResourceUsage + allocationInfo->GetAllocatedVolume() <= Config.GetMemoryLimit() ||
               externalGroupId == GetMinInternalGroupIdOptional().value_or(externalGroupId)) {
        allocationInfo->Allocate(OwnerActorId);
        ResourceUsage += allocationInfo->GetAllocatedVolume();
        ReadyAllocations[internalGroupId].AddAllocation(allocationInfo);
    } else {
        WaitAllocations[internalGroupId].AddAllocation(allocationInfo);
    }
}

void TManager::UpdateAllocation(const ui64 allocationId, const ui64 volume) {
    auto& info = GetAllocationInfoVerified(allocationId);
    AFL_VERIFY(ResourceUsage >= info.GetAllocatedVolume());
    ResourceUsage -= info.GetAllocatedVolume();
    ResourceUsage += volume;
    info.SetAllocatedVolume(volume);
    TryAllocateWaiting();
}

void TManager::TryAllocateWaiting() {
    for (auto it = WaitAllocations.begin(); it != WaitAllocations.end();) {
        auto internalGroupId = it->first;
        auto allocated = it->second.AllocatePossible(*this, internalGroupId == GetMinInternalGroupIdVerified());
        auto& readyAllocations = ReadyAllocations[internalGroupId];
        for (auto&& i : allocated) {
            readyAllocations.AddAllocation(i);
        }
        if (it->second.IsEmpty()) {
            it = WaitAllocations.erase(it);
        } else {
            break;
        }
    }
}

void TManager::UnregisterAllocation(const ui64 allocationId) {
    ui64 memoryAllocated = 0;
    {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        for (auto&& usageGroupId : it->second->GetGroupIds()) {
            {
                auto groupIt = WaitAllocations.find(usageGroupId);
                if (groupIt != WaitAllocations.end()) {
                    groupIt->second.Remove(it->second);
                    if (groupIt->second.IsEmpty()) {
                        WaitAllocations.erase(groupIt);
                    }
                }
            }
            {
                auto groupIt = ReadyAllocations.find(usageGroupId);
                if (groupIt != ReadyAllocations.end()) {
                    groupIt->second.Remove(it->second);
                    if (groupIt->second.IsEmpty()) {
                        ReadyAllocations.erase(groupIt);
                    }
                }
            }
        }
        memoryAllocated = it->second->GetAllocatedVolume();
        AllocationInfo.erase(it);
    }
    if (memoryAllocated) {
        AFL_VERIFY(memoryAllocated <= ResourceUsage);
        ResourceUsage -= memoryAllocated;
        TryAllocateWaiting();
    }
}

void TManager::UnregisterGroup(const ui64 externalGroupId) {
    const ui64 usageGroupId = GetInternalGroupIdVerified(externalGroupId);
    ExternalGroupIntoInternalGroup.erase(externalGroupId);
    {
        auto it = WaitAllocations.find(usageGroupId);
        if (it != WaitAllocations.end()) {
            for (auto&& [_, allocation] : it->second.GetAllocations()) {
                GetAllocationInfoVerified(allocation->GetIdentifier()).RemoveGroup(usageGroupId);
            }
            WaitAllocations.erase(it);
        }
    }
    {
        auto it = ReadyAllocations.find(usageGroupId);
        if (it != ReadyAllocations.end()) {
            for (auto&& [_, allocation] : it->second.GetAllocations()) {
                GetAllocationInfoVerified(allocation->GetIdentifier()).RemoveGroup(usageGroupId);
            }
            ReadyAllocations.erase(it);
        }
    }
}

ui64 TManager::GetMinInternalGroupIdVerified() const {
    return *TValidator::CheckNotNull(GetMinInternalGroupIdOptional());
}

}
