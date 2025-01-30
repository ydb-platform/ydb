#include "manager.h"

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TProcessMemory* TManager::GetProcessMemoryByExternalIdOptional(const ui64 externalProcessId) {
    auto internalId = ProcessIds.GetInternalIdOptional(externalProcessId);
    if (!internalId) {
        return nullptr;
    }
    return GetProcessMemoryOptional(*internalId);
}

void TManager::RegisterGroup(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId) {
    AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "register_group")("external_process_id", externalProcessId)(
        "external_group_id", externalGroupId)("size", ProcessIds.GetSize())("external_scope_id", externalScopeId);
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        process->RegisterGroup(externalScopeId, externalGroupId);
    }
    RefreshSignals();
}

void TManager::UnregisterGroup(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId) {
    AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "unregister_group")("external_process_id", externalProcessId)(
        "external_group_id", externalGroupId)("size", ProcessIds.GetSize());
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        process->UnregisterGroup(externalScopeId, externalGroupId);
    }
    RefreshSignals();
}

void TManager::UpdateAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId, const ui64 volume) {
    TProcessMemory& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    if (process.UpdateAllocation(externalScopeId, allocationId, volume)) {
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

void TManager::UnregisterAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId) {
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        if (process->UnregisterAllocation(externalScopeId, allocationId)) {
            TryAllocateWaiting();
        }
    }
    RefreshSignals();
}

void TManager::RegisterAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId,
    const std::shared_ptr<IAllocation>& task, const std::optional<ui32>& stageIdx) {
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        process->RegisterAllocation(externalScopeId, externalGroupId, task, stageIdx);
    } else {
        AFL_VERIFY(!task->OnAllocated(std::make_shared<TAllocationGuard>(externalProcessId, externalScopeId, task->GetIdentifier(), OwnerActorId, task->GetMemory()), task))(
                                                                                  "ext_group", externalGroupId)("stage_idx", stageIdx);
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

void TManager::RegisterProcessScope(const ui64 externalProcessId, const ui64 externalProcessScopeId) {
    GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId)).RegisterScope(externalProcessScopeId);
    RefreshSignals();
}

void TManager::UnregisterProcessScope(const ui64 externalProcessId, const ui64 externalProcessScopeId) {
    GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId)).UnregisterScope(externalProcessScopeId);
    RefreshSignals();
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
