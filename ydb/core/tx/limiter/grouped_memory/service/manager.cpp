#include "manager.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

std::vector<std::shared_ptr<TManager::TAllocationInfo>> TManager::TGrouppedAllocations::AllocatePossible(
    const bool force, const ui64 freeMemory) {
    std::vector<std::shared_ptr<TManager::TAllocationInfo>> result;
    ui64 allocationMemory = 0;
    for (auto&& [_, allocation] : Allocations) {
        if (allocation->GetAllocatedVolume() + allocationMemory < freeMemory || force) {
            allocationMemory += allocation->GetAllocatedVolume();
            result.emplace_back(allocation);
        }
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
        it = AllocationInfo.emplace(task->GetIdentifier(), std::make_shared<TAllocationInfo>(task, &Allocated, &Waiting)).first;
    }
    return it->second;
}

std::optional<ui64> TManager::GetMinInternalGroupIdOptional() const {
    auto waitMinGroupId = WaitAllocations.GetMinGroupId();
    auto readyMinGroupId = ReadyAllocations.GetMinGroupId();
    if (waitMinGroupId) {
        if (readyMinGroupId) {
            return std::min(*readyMinGroupId, *waitMinGroupId);
        } else {
            return *waitMinGroupId;
        }
    } else {
        if (readyMinGroupId) {
            return *readyMinGroupId;
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
        ReadyAllocations.AddAllocation(internalGroupId, allocationInfo);
    } else if (WaitAllocations.GetMinGroupId().value_or(externalGroupId) < externalGroupId) {
        WaitAllocations.AddAllocation(internalGroupId, allocationInfo);
    } else if (!Allocated.Val() || Allocated.Val() + allocationInfo->GetAllocatedVolume() <= Config.GetMemoryLimit() ||
               externalGroupId == GetMinInternalGroupIdOptional().value_or(externalGroupId)) {
        allocationInfo->Allocate(OwnerActorId);
        ReadyAllocations.AddAllocation(internalGroupId, allocationInfo);
    } else {
        WaitAllocations.AddAllocation(internalGroupId, allocationInfo);
    }
    RefreshSignals();
}

void TManager::UpdateAllocation(const ui64 allocationId, const ui64 volume) {
    auto& info = GetAllocationInfoVerified(allocationId);
    info.SetAllocatedVolume(volume);
    TryAllocateWaiting();
    RefreshSignals();
}

void TManager::TryAllocateWaiting() {
    WaitAllocations.AllocateTo(*this, ReadyAllocations);
    RefreshSignals();
}

void TManager::UnregisterAllocation(const ui64 allocationId) {
    ui64 memoryAllocated = 0;
    {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        for (auto&& usageGroupId : it->second->GetGroupIds()) {
            const bool waitFlag = WaitAllocations.RemoveAllocation(usageGroupId, it->second);
            const bool readyFlag = ReadyAllocations.RemoveAllocation(usageGroupId, it->second);
            AFL_VERIFY(waitFlag ^ readyFlag);
        }
        memoryAllocated = it->second->GetAllocatedVolume();
        AllocationInfo.erase(it);
    }
    if (memoryAllocated) {
        TryAllocateWaiting();
    }
    RefreshSignals();
}

void TManager::UnregisterGroup(const ui64 externalGroupId) {
    const ui64 usageGroupId = GetInternalGroupIdVerified(externalGroupId);
    ExternalGroupIntoInternalGroup.erase(externalGroupId);
    auto minGroupId = GetMinInternalGroupIdOptional();
    if (auto data = WaitAllocations.ExtractGroup(usageGroupId)) {
        for (auto&& [_, allocation] : *data) {
            GetAllocationInfoVerified(allocation->GetIdentifier()).RemoveGroup(usageGroupId);
        }
    }
    if (auto data = ReadyAllocations.ExtractGroup(usageGroupId)) {
        for (auto&& [_, allocation] : *data) {
            GetAllocationInfoVerified(allocation->GetIdentifier()).RemoveGroup(usageGroupId);
        }
    }
    if (minGroupId && *minGroupId == externalGroupId) {
        TryAllocateWaiting();
    }
    RefreshSignals();
}

ui64 TManager::GetMinInternalGroupIdVerified() const {
    return *TValidator::CheckNotNull(GetMinInternalGroupIdOptional());
}

}
