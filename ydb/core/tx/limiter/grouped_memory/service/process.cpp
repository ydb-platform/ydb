#include "process.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TProcessMemory::RegisterAllocation(
    const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task, const std::optional<ui32>& stageIdx) {
    AFL_VERIFY(task);
    std::shared_ptr<TStageFeatures> stage;
    if (Stages.empty()) {
        AFL_VERIFY(!stageIdx);
        stage = DefaultStage;
    } else {
        AFL_VERIFY(stageIdx);
        AFL_VERIFY(*stageIdx < Stages.size());
        stage = Stages[*stageIdx];
    }
    AFL_VERIFY(stage);
    const std::optional<ui64> internalGroupIdOptional = GroupIds.GetInternalIdOptional(externalGroupId);
    if (!internalGroupIdOptional) {
        AFL_VERIFY(!task->OnAllocated(std::make_shared<TAllocationGuard>(ExternalProcessId, task->GetIdentifier(), OwnerActorId, task->GetMemory()), task))("ext_group",
                                                                      externalGroupId)("min_group", GroupIds.GetMinInternalIdOptional())(
                                                                      "stage", stage->GetName());
        AFL_VERIFY(!AllocationInfo.contains(task->GetIdentifier()));
    } else {
        const ui64 internalGroupId = *internalGroupIdOptional;
        auto allocationInfo = RegisterAllocationImpl(internalGroupId, task, stage);

        if (allocationInfo->GetAllocationStatus() != EAllocationStatus::Waiting) {
        } else if (WaitAllocations.GetMinGroupId().value_or(internalGroupId) < internalGroupId) {
            WaitAllocations.AddAllocation(internalGroupId, allocationInfo);
        } else if (allocationInfo->IsAllocatable(0) || (IsPriorityProcess() && internalGroupId == GroupIds.GetMinInternalIdVerified())) {
            Y_UNUSED(WaitAllocations.RemoveAllocation(internalGroupId, allocationInfo));
            if (!allocationInfo->Allocate(OwnerActorId)) {
                UnregisterAllocation(allocationInfo->GetIdentifier());
            }
        } else {
            WaitAllocations.AddAllocation(internalGroupId, allocationInfo);
        }
    }
}

const std::shared_ptr<NKikimr::NOlap::NGroupedMemoryManager::TAllocationInfo>& TProcessMemory::RegisterAllocationImpl(
    const ui64 internalGroupId, const std::shared_ptr<IAllocation>& task, const std::shared_ptr<TStageFeatures>& stage) {
    auto it = AllocationInfo.find(task->GetIdentifier());
    if (it == AllocationInfo.end()) {
        it = AllocationInfo.emplace(task->GetIdentifier(), std::make_shared<TAllocationInfo>(ExternalProcessId, internalGroupId, task, stage)).first;
    }
    return it->second;
}

bool TProcessMemory::UnregisterAllocation(const ui64 allocationId) {
    ui64 memoryAllocated = 0;
    auto it = AllocationInfo.find(allocationId);
    AFL_VERIFY(it != AllocationInfo.end());
    bool waitFlag = false;
    const ui64 internalGroupId = it->second->GetAllocationInternalGroupId();
    switch (it->second->GetAllocationStatus()) {
        case EAllocationStatus::Allocated:
        case EAllocationStatus::Failed:
            AFL_VERIFY(!WaitAllocations.RemoveAllocation(internalGroupId, it->second));
            break;
        case EAllocationStatus::Waiting:
            AFL_VERIFY(WaitAllocations.RemoveAllocation(internalGroupId, it->second));
            waitFlag = true;
            break;
    }
    AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocation_unregister")("allocation_id", allocationId)("wait", waitFlag)(
        "internal_group_id", internalGroupId)("allocation_status", it->second->GetAllocationStatus());
    memoryAllocated = it->second->GetAllocatedVolume();
    AllocationInfo.erase(it);
    return !!memoryAllocated;
}

void TProcessMemory::UnregisterGroupImpl(const ui64 internalGroupId) {
    auto data = WaitAllocations.ExtractGroup(internalGroupId);
    for (auto&& allocation : data) {
        AFL_VERIFY(!allocation->Allocate(OwnerActorId));
    }
}

void TProcessMemory::UnregisterGroup(const ui64 externalGroupId) {
    const ui64 internalGroupId = GroupIds.ExtractInternalIdVerified(externalGroupId);
    AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "remove_group")("external_group_id", externalGroupId)(
        "internal_group_id", internalGroupId);
    UnregisterGroupImpl(internalGroupId);
    if (IsPriorityProcess() && (internalGroupId < GroupIds.GetMinInternalIdDef(internalGroupId))) {
        Y_UNUSED(TryAllocateWaiting(0));
    }
}

void TProcessMemory::RegisterGroup(const ui64 externalGroupId) {
    Y_UNUSED(GroupIds.RegisterExternalId(externalGroupId));
}

void TProcessMemory::Unregister() {
    for (auto&& i : GroupIds.GetInternalIds()) {
        UnregisterGroupImpl(i);
    }
    GroupIds.Clear();
    AllocationInfo.clear();
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
