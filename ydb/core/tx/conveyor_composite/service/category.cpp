#include "category.h"

namespace NKikimr::NConveyorComposite {

bool TProcessCategory::HasTasks() const {
    return ProcessesWithTasks.size();
}

void TProcessCategory::DoQuant(const TMonotonic newStart) {
    CPUUsage->Cut(newStart);
    for (auto&& i : Processes) {
        i.second->DoQuant(newStart);
    }
}

std::optional<TWorkerTask> TProcessCategory::ExtractTaskWithPrediction(const std::shared_ptr<TWPCategorySignals>& counters, THashSet<TString>& scopeIds) {
    std::shared_ptr<TProcess> pMin;
    TDuration dMin;
    for (auto&& [_, p] : ProcessesWithTasks) {
        if (!p->GetScope()->CheckToRun()) {
            continue;
        }
        const TDuration d = p->GetCPUUsage()->CalcWeight(p->GetWeight());
        if (!pMin || d < dMin) {
            dMin = d;
            pMin = p;
        }
    }
    if (!pMin) {
        return std::nullopt;
    }
    AFL_VERIFY(pMin);
    auto result = pMin->ExtractTaskWithPrediction(counters);
    if (pMin->GetTasksCount() == 0) {
        AFL_VERIFY(ProcessesWithTasks.erase(pMin->GetProcessId()));
    } 
    if (scopeIds.emplace(pMin->GetScope()->GetScopeId()).second) {
        pMin->GetScope()->IncInFlight();
    }
    Counters->WaitingQueueSize->Set(WaitingTasksCount->Val());
    return result;
}

TProcessScope& TProcessCategory::MutableProcessScope(const TString& scopeName) {
    auto it = Scopes.find(scopeName);
    AFL_VERIFY(it != Scopes.end())("cat", GetCategory())("scope", scopeName);
    return *it->second;
}

std::shared_ptr<TProcessScope> TProcessCategory::GetProcessScopePtrVerified(const TString& scopeName) const {
    auto it = Scopes.find(scopeName);
    AFL_VERIFY(it != Scopes.end());
    return it->second;
}

TProcessScope* TProcessCategory::MutableProcessScopeOptional(const TString& scopeName) {
    auto it = Scopes.find(scopeName);
    if (it != Scopes.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

std::shared_ptr<TProcessScope> TProcessCategory::RegisterScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    TCPUGroup::TPtr cpuGroup = std::make_shared<TCPUGroup>(processCpuLimits.GetCPUGroupThreadsLimitDef(256));
    auto info = Scopes.emplace(scopeId, std::make_shared<TProcessScope>(scopeId, std::move(cpuGroup), CPUUsage));
    AFL_VERIFY(info.second);
    return info.first->second;
}

std::shared_ptr<TProcessScope> TProcessCategory::UpdateScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    auto scope = GetProcessScopePtrVerified(scopeId);
    scope->UpdateLimits(processCpuLimits);
    return scope;
}

std::shared_ptr<TProcessScope> TProcessCategory::UpsertScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    if (Scopes.contains(scopeId)) {
        return UpdateScope(scopeId, processCpuLimits);
    } else {
        return RegisterScope(scopeId, processCpuLimits);
    }
}

void TProcessCategory::UnregisterScope(const TString& name) {
    auto it = Scopes.find(name);
    AFL_VERIFY(it != Scopes.end());
    Scopes.erase(it);
}

void TProcessCategory::PutTaskResult(TWorkerTaskResult&& result, THashSet<TString>& scopeIds) {
    const ui64 internalProcessId = result.GetProcessId();
    auto it = Processes.find(internalProcessId);
    if (scopeIds.emplace(result.GetScope()->GetScopeId()).second) {
        result.GetScope()->DecInFlight();
    }
    if (it == Processes.end()) {
        return;
    }
    it->second->PutTaskResult(std::move(result));
}

}   // namespace NKikimr::NConveyorComposite
