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
        auto g = BuildProcessOrderGuard(*process);
        process->UnregisterGroup(externalScopeId, externalGroupId);
    }
    RefreshSignals();
}

void TManager::UpdateAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId, const ui64 volume) {
    TProcessMemory& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    bool updated = false;
    {
        auto g = BuildProcessOrderGuard(process);
        updated = process.UpdateAllocation(externalScopeId, allocationId, volume);
        if (!updated) {
            g.Release();
        }
    }

    if (updated) {
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
    for (auto it = ProcessesOrdered.begin(); it != ProcessesOrdered.end();) {
        if (it->second->TryAllocateWaiting(1)) {
            TProcessMemory* process = it->second;
            it = ProcessesOrdered.erase(it);
            auto info = ProcessesOrdered.emplace(process->BuildUsageAddress(), process);
            AFL_VERIFY(info.second);
            auto itNew = info.first;
            if (it == ProcessesOrdered.end() || itNew->first < it->first) {
                it = itNew;
            }
        } else {
            ++it;
        }
    }
    RefreshSignals();
}

void TManager::UnregisterAllocation(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId) {
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        bool unregistered = false;
        {
            auto g = BuildProcessOrderGuard(*process);
            unregistered = process->UnregisterAllocation(externalScopeId, allocationId);
            if (!unregistered) {
                g.Release();
            }
        }
        if (unregistered) {
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
        auto info = Processes.emplace(
            internalProcessId, TProcessMemory(externalProcessId, internalProcessId, OwnerActorId, Processes.empty(), stages, DefaultStage));
        AFL_VERIFY(info.second);
        ProcessesOrdered.emplace(info.first->second.BuildUsageAddress(), &info.first->second);
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
    AFL_VERIFY(ProcessesOrdered.erase(it->second.BuildUsageAddress()));
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
    auto& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    auto g = BuildProcessOrderGuard(process);
    process.RegisterScope(externalProcessScopeId);
    RefreshSignals();
}

void TManager::UnregisterProcessScope(const ui64 externalProcessId, const ui64 externalProcessScopeId) {
    auto& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    auto g = BuildProcessOrderGuard(process);
    process.UnregisterScope(externalProcessScopeId);
    RefreshSignals();
}

void TManager::UpdateMemoryLimits(const ui64 limit, const ui64 hardLimit) {
    if (!DefaultStage) {
        return;
    }

    bool isLimitIncreased = false;
    DefaultStage->UpdateMemoryLimits(limit, hardLimit, isLimitIncreased);
    if (isLimitIncreased) {
        TryAllocateWaiting();
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
