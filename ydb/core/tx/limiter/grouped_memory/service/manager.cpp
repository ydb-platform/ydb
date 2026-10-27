#include "manager.h"

#include <ydb/library/accessor/validator.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GROUPED_MEMORY_LIMITER

namespace NKikimr::NOlap::NGroupedMemoryManager {

TProcessMemory* TManager::GetProcessMemoryByExternalIdOptional(const ui64 externalProcessId) {
    auto internalId = ProcessIds.GetInternalIdOptional(externalProcessId);
    if (!internalId) {
        return nullptr;
    }
    return GetProcessMemoryOptional(*internalId);
}

void TManager::RegisterGroup(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId) {
    YDB_LOG_DEBUG("",
        {"event", "register_group"},
        {"externalProcessId", externalProcessId},
        {"externalGroupId", externalGroupId},
        {"size", ProcessIds.GetSize()},
        {"externalScopeId", externalScopeId});
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        process->RegisterGroup(externalScopeId, externalGroupId);
        UpdateWaitingProcesses(process);
    }
    RefreshSignals();
}

void TManager::UnregisterGroup(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 externalGroupId) {
    YDB_LOG_DEBUG("",
        {"event", "unregister_group"},
        {"externalProcessId", externalProcessId},
        {"externalGroupId", externalGroupId},
        {"size", ProcessIds.GetSize()});
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        auto g = BuildProcessOrderGuard(*process);
        process->UnregisterGroup(externalScopeId, externalGroupId);
    }
    if (Config.IsUnrestrictedEnabled()) {
        TryAllocateWaiting();
    }
    RefreshSignals();
}

void TManager::AllocationUpdated(const ui64 externalProcessId, const ui64 externalScopeId, const ui64 allocationId, const ui64 volume) {
    TProcessMemory& process = GetProcessMemoryVerified(ProcessIds.GetInternalIdVerified(externalProcessId));
    bool updated = false;
    {
        auto g = BuildProcessOrderGuard(process);
        updated = process.AllocationUpdated(externalScopeId, allocationId, volume);
        if (!updated) {
            g.Release();
        }
    }

    if (updated) {
        TryAllocateWaiting();
    }

    RefreshSignals();
}

void TManager::RelinkProcess(TProcessMemory& process, const TProcessMemoryUsage& oldAddress) {
    AFL_VERIFY(ProcessesOrdered.erase(oldAddress));
    AFL_VERIFY(ProcessesOrdered.emplace(process.BuildUsageAddress(), &process).second);
    WaitingProcesses.erase(oldAddress);
    if (process.HasWaitingAllocations()) {
        WaitingProcesses.emplace(process.BuildUsageAddress());
    }
}

bool TManager::ScheduleOneUnrestricted() {
    struct TCandidate {
        bool HasAdmission = false;
        ui64 InternalId = 0;
        ui64 ScopeId = 0;
        TProcessMemory* Process = nullptr;
    };

    auto isBetter = [](const TCandidate& left, const TCandidate& right) {
        if (left.HasAdmission != right.HasAdmission) {
            return !left.HasAdmission;
        }
        if (left.InternalId != right.InternalId) {
            return left.InternalId < right.InternalId;
        }
        return left.ScopeId < right.ScopeId;
    };

    std::optional<TCandidate> best;
    for (auto& [internalId, process] : Processes) {
        for (const ui64 scopeId : process.GetWaitingScopeIds()) {
            TProcessMemoryScope& scope = process.MutableScope(scopeId);
            if (!scope.CanScheduleUnrestricted()) {
                continue;
            }
            TCandidate candidate{scope.HasAdmission(), internalId, scopeId, &process};
            if (!best || isBetter(candidate, *best)) {
                best = candidate;
            }
        }
    }
    if (!best) {
        return false;
    }

    const auto oldAddress = best->Process->BuildUsageAddress();
    const auto step = best->Process->ScheduleOneUnrestricted(best->ScopeId);
    RelinkProcess(*best->Process, oldAddress);
    return step != EUnrestrictedScheduleResult::Idle;
}

void TManager::TryAllocateWaiting() {
    if (!Config.IsUnrestrictedEnabled() && Processes.size()) {
        auto it = Processes.find(ProcessIds.GetMinInternalIdVerified());
        AFL_VERIFY(it != Processes.end());
        TProcessMemory& process = it->second;
        AFL_VERIFY(process.IsPriorityProcess());
        process.TryAllocateWaiting(0);
        UpdateWaitingProcesses(&process);
    }
    for (auto waitingIt = WaitingProcesses.begin(); waitingIt != WaitingProcesses.end();) {
        // Check root availability
        if (!DefaultStage->IsAllocatable(1, 0)) {
            break;
        }
        auto it = ProcessesOrdered.find(*waitingIt);
        AFL_VERIFY(it != ProcessesOrdered.end());
        TProcessMemory* process = it->second;
        if (!process->TryAllocateWaiting(1)) {
            ++waitingIt;
            continue;
        }

        ProcessesOrdered.erase(it);
        auto [_, emplaced] = ProcessesOrdered.emplace(process->BuildUsageAddress(), process);
        AFL_VERIFY(emplaced);

        waitingIt = WaitingProcesses.erase(waitingIt);

        if (!process->HasWaitingAllocations()) {
            continue;
        }

        auto [waitingItNew, emplacedWaiting] = WaitingProcesses.emplace(process->BuildUsageAddress());
        AFL_VERIFY(emplacedWaiting);
        if (waitingIt == WaitingProcesses.end() || *waitingItNew < *waitingIt) {
            waitingIt = waitingItNew;
        }
    }

    if (Config.IsUnrestrictedEnabled()) {
        while (ScheduleOneUnrestricted()) {
        }
        // Keep forcing until some holder has all its requests served (it will release memory) or nothing is left to force.
        // Each step takes one waiting request, so the loop is finite.
        while (ForceOneOnDeadlock()) {
        }
    }

    RefreshSignals();
}

bool TManager::ForceOneOnDeadlock() {
    if (!Config.IsUnrestrictedEnabled() || WaitingProcesses.empty() || !DefaultStage->GetUnrestrictedSoft()) {
        return false;
    }
    // Memory must be the blocker: every holder waits and no waiting request fits the band.
    // A request that fits but is held back by a slot waits for that slot instead.
    for (const auto& [_, process] : Processes) {
        if (!process.AllHoldersWait() || process.HasWaitingThatFits()) {
            return false;
        }
    }
    for (const auto& address : WaitingProcesses) {
        auto it = ProcessesOrdered.find(address);
        AFL_VERIFY(it != ProcessesOrdered.end());
        TProcessMemory* process = it->second;
        const auto step = process->ForceOneUnrestricted();
        if (step == EUnrestrictedScheduleResult::Idle) {
            continue;
        }
        RelinkProcess(*process, address);
        return true;
    }
    return false;
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
    const std::shared_ptr<IAllocation>& allocation, const std::optional<ui32>& stageIdx) {
    if (auto* process = GetProcessMemoryByExternalIdOptional(externalProcessId)) {
        process->RegisterAllocation(externalScopeId, externalGroupId, allocation, stageIdx);
        UpdateWaitingProcesses(process);
        if (Config.IsUnrestrictedEnabled()) {
            TryAllocateWaiting();
        }
    } else {
        LWPROBE(Allocated, "on_register", allocation->GetIdentifier(), "", std::numeric_limits<ui64>::max(), std::numeric_limits<ui64>::max(), 0, 0, TDuration::Zero(), false, false);
        AFL_VERIFY(!allocation->OnAllocated(std::make_shared<TAllocationGuard>(externalProcessId, externalScopeId, allocation->GetIdentifier(), OwnerActorId, allocation->GetMemory(), nullptr), allocation))(
                                                               "process", externalProcessId)("scope", externalScopeId)(
                                                               "ext_group", externalGroupId)("stage_idx", stageIdx);
    }
    RefreshSignals();
}

void TManager::RegisterProcess(const ui64 externalProcessId, const std::vector<std::shared_ptr<TStageFeatures>>& stages) {
    auto internalId = ProcessIds.GetInternalIdOptional(externalProcessId);
    if (!internalId) {
        const ui64 internalProcessId = ProcessIds.RegisterExternalIdOrGet(externalProcessId);
        auto info = Processes.emplace(
            internalProcessId, TProcessMemory(externalProcessId, internalProcessId, OwnerActorId, Processes.empty(), stages, DefaultStage,
                Config.IsUnrestrictedEnabled(), Config.GetMaxUnrestrictedGroupsPerScope()));
        AFL_VERIFY(info.second);
        ProcessesOrdered.emplace(info.first->second.BuildUsageAddress(), &info.first->second);
        UpdateWaitingProcesses(&info.first->second);
    } else {
        auto& process = Processes.find(*internalId)->second;
        AFL_VERIFY(process.GetStages().size() == stages.size())("external_process_id", externalProcessId)(
            "registered_stages", process.GetStages().size())("new_stages", stages.size())(
            "reason", "process_id_collision: externalProcessId reused for a different set of stages");
        ++process.MutableLinksCount();
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
    auto processUsageAddress = it->second.BuildUsageAddress();
    AFL_VERIFY(ProcessesOrdered.erase(processUsageAddress));
    WaitingProcesses.erase(processUsageAddress);
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

void TManager::SetMemoryConsumptionUpdateFunction(std::function<void(ui64)> func) {
    AFL_ENSURE(DefaultStage);

    DefaultStage->SetMemoryConsumptionUpdateFunction(std::move(func));
}

void TManager::UpdateMemoryLimits(const ui64 limit, const std::optional<ui64>& hardLimit, const std::optional<ui64>& unrestrictedSoft) {
    AFL_ENSURE(DefaultStage);
    bool isLimitIncreased = false;
    DefaultStage->UpdateMemoryLimits(limit, hardLimit, isLimitIncreased, unrestrictedSoft);
    if (Config.IsUnrestrictedEnabled()) {
        for (auto& [_, process] : Processes) {
            auto g = BuildProcessOrderGuard(process);
            process.FailNeverFittingWaiting();
        }
    }
    if (isLimitIncreased) {
        TryAllocateWaiting();
    }
    RefreshSignals();
}

void TManager::UpdateWaitingProcesses(TProcessMemory* process) {
    bool hasWaitingAllocations = process->HasWaitingAllocations();
    const auto processUsageAddress = process->BuildUsageAddress();
    if (hasWaitingAllocations) {
        WaitingProcesses.insert(processUsageAddress);
        return;
    }
    WaitingProcesses.erase(processUsageAddress);
}

TString TManager::DebugString() const {
    TStringBuilder sb;
    sb << "TManager{" << Endl
       << "  Name=" << Name << Endl
       << "  OwnerActorId=" << OwnerActorId.ToString() << Endl
       << "  Config=" << Config.DebugString() << Endl
       << "  ProcessesCount=" << Processes.size() << Endl
       << "  ProcessesOrderedCount=" << ProcessesOrdered.size() << Endl
       << "  WaitingProcessesCount=" << WaitingProcesses.size() << Endl
       << "  DefaultStage=" << (DefaultStage ? DefaultStage->DebugString() : "null") << Endl
       << "  ProcessIds={" << Endl
       << "    Size=" << ProcessIds.GetSize() << Endl
       << "    MinInternalId=" << (ProcessIds.GetMinInternalIdOptional().has_value()
                                  ? ToString(ProcessIds.GetMinInternalIdOptional().value())
                                  : "null") << Endl
       << "    MinExternalId=" << (ProcessIds.GetMinExternalIdOptional().has_value()
                                 ? ToString(ProcessIds.GetMinExternalIdOptional().value())
                                 : "null") << Endl
       << "  }" << Endl
       << "  Processes=[" << Endl;
    
    bool first = true;
    for (const auto& [internalId, process] : Processes) {
        if (!first) {
            sb << "," << Endl;
        }
        first = false;
        sb << "    {InternalId=" << internalId << ";Process=" << process.DebugString() << "}";
    }
    
    sb << Endl << "  ]" << Endl
       << "  ProcessesOrdered=[" << Endl;
    
    first = true;
    for (const auto& [usage, processPtr] : ProcessesOrdered) {
        if (!first) {
            sb << "," << Endl;
        }
        first = false;
        sb << "    {Usage=" << usage.DebugString() << ";ProcessPtr=" << (processPtr ? "exists" : "null") << "}";
    }
    
    sb << Endl << "  ]" << Endl
       << "  WaitingProcesses=[" << Endl;
    
    first = true;
    for (const auto& usage : WaitingProcesses) {
        if (!first) {
            sb << "," << Endl;
        }
        first = false;
        sb << "    " << usage.DebugString();
    }
    
    sb << Endl << "  ]" << Endl << "}";
    return sb;
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
