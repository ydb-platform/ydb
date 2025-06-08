#include "category.h"

namespace NKikimr::NConveyorComposite {

bool TProcessCategory::HasTasks() const {
    for (auto&& i : Processes) {
        if (i.second->HasTasks()) {
            return true;
        }
    }
    return false;
}

void TProcessCategory::DoQuant(const TMonotonic newStart) {
    CPUUsage->Cut(newStart);
    for (auto&& i : Processes) {
        i.second->DoQuant(newStart);
    }
}

TWorkerTask TProcessCategory::ExtractTaskWithPrediction() {
    std::shared_ptr<TProcess> pMin;
    TDuration dMin;
    for (auto&& [_, p] : Processes) {
        if (!p->HasTasks()) {
            continue;
        }
        const TDuration d = p->GetCPUUsage()->CalcWeight(p->GetWeight());
        if (!pMin || d < dMin) {
            dMin = d;
            pMin = p;
        }
    }
    AFL_VERIFY(pMin);
    auto result = pMin->ExtractTaskWithPrediction();
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
    auto info = Scopes.emplace(scopeId, std::make_shared<TProcessScope>(scopeId, std::move(cpuGroup), std::move(CPUUsage)));
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

}   // namespace NKikimr::NConveyorComposite
