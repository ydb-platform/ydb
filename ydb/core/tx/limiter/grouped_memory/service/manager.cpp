#include "manager.h"

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

void TManager::RegisterGroup(const ui64 externalProcessId, const ui64 externalGroupId) {
    TProcessMemory& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    process.RegisterGroup(externalGroupId);
    RefreshSignals();
}

void TManager::UnregisterGroup(const ui64 externalProcessId, const ui64 externalGroupId) {
    TProcessMemory& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    process.UnregisterGroup(externalGroupId);
    RefreshSignals();
}

void TManager::UpdateAllocation(const ui64 externalProcessId, const ui64 allocationId, const ui64 volume) {
    TProcessMemory& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    if (process.UpdateAllocation(allocationId, volume)) {
        TryAllocateWaiting();
    }

    RefreshSignals();
}

void TManager::TryAllocateWaiting() {
    if (Processes.size()) {
        auto it = Processes.find(ProcessIds.GetMinInternalIdVerified());
        AFL_VERIFY(it != Processes.end());
        AFL_VERIFY(it->second.IsPriorityProcess());
        it->second.TryAllocateWaiting(0);
    }
    while (true) {
        bool found = false;
        for (auto&& i : Processes) {
            if (i.second.TryAllocateWaiting(1)) {
                found = true;
            }
        }
        if (!found) {
            break;
        }
    }
    RefreshSignals();
}

void TManager::UnregisterAllocation(const ui64 externalProcessId, const ui64 allocationId) {
    auto* process = GetProcessMemoryOptional(ProcessIds.GetInternalIdVerified(externalProcessId));
    if (!process) {
        return;
    }
    if (process->UnregisterAllocation(allocationId)) {
        TryAllocateWaiting();
    }
    RefreshSignals();
}

void TManager::RegisterAllocation(
    const ui64 externalProcessId, const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task, const std::optional<ui32>& stageIdx) {
    auto* process = GetProcessMemoryOptional(ProcessIds.GetInternalIdVerified(externalProcessId));
    if (!process) {
        AFL_VERIFY(!task->OnAllocated(std::make_shared<TAllocationGuard>(externalProcessId, task->GetIdentifier(), OwnerActorId, task->GetMemory()), task))(
                                                                                  "ext_group", externalGroupId)("stage_idx", stageIdx);
    } else {
        process->RegisterAllocation(externalGroupId, task, stageIdx);
    }
    RefreshSignals();
}

void TManager::RegisterProcess(const ui64 externalProcessId, const std::vector<std::shared_ptr<TStageFeatures>>& stages) {
    auto internalId = ProcessIds.GetInternalIdOptional(externalProcessId);
    if (!internalId) {
        const ui64 internalProcessId = ProcessIds.RegisterExternalIdOrGet(externalProcessId);
        AFL_VERIFY(Processes.emplace(internalProcessId, TProcessMemory(externalProcessId, OwnerActorId, Processes.empty(), stages, DefaultStage)).second);
    } else {
        ++Processes.find(*internalId)->second.MutableLinksCount();
    }
    RefreshSignals();
}

void TManager::UnregisterProcess(const ui64 externalProcessId) {
    const ui64 internalProcessId = ProcessIds.GetInternalIdVerified(externalProcessId);
    auto it = Processes.find(internalProcessId);
    AFL_VERIFY(it != Processes.end());
    if (--it->second.MutableLinksCount()) {
        return;
    }
    Y_UNUSED(ProcessIds.ExtractInternalIdVerified(externalProcessId));
    it->second.Unregister();
    Processes.erase(it);
    const ui64 nextInternalProcessId = ProcessIds.GetMinInternalIdDef(internalProcessId);
    if (internalProcessId < nextInternalProcessId) {
        GetProcessMemoryVerified(nextInternalProcessId).SetPriorityProcess();
        TryAllocateWaiting();
    }
    RefreshSignals();
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
