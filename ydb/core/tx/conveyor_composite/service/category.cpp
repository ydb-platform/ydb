#include "category.h"

namespace NKikimr::NConveyorComposite {

bool TProcessCategory::HasTasks() const {
    for (auto&& i : Scopes) {
        if (i.second->HasTasks()) {
            return true;
        }
    }
    return false;
}

void TProcessCategory::DoQuant(const TMonotonic newStart) {
    CPUUsage->Cut(newStart);
    for (auto&& i : Scopes) {
        i.second->DoQuant(newStart);
    }
}

TWorkerTask TProcessCategory::ExtractTaskWithPrediction() {
    std::shared_ptr<TProcessScope> scopeMin;
    TDuration dMin;
    for (auto&& [_, scope] : Scopes) {
        if (!scope->HasTasks()) {
            continue;
        }
        const TDuration d = scope->GetCPUUsage()->CalcWeight(scope->GetWeight());
        if (!scopeMin || d < dMin) {
            dMin = d;
            scopeMin = scope;
        }
    }
    AFL_VERIFY(scopeMin);
    return scopeMin->ExtractTaskWithPrediction();
}

TProcessScope& TProcessCategory::MutableProcessScope(const TString& scopeName) {
    auto it = Scopes.find(scopeName);
    AFL_VERIFY(it != Scopes.end())("cat", GetCategory())("scope", scopeName);
    return *it->second;
}

TProcessScope* TProcessCategory::MutableProcessScopeOptional(const TString& scopeName) {
    auto it = Scopes.find(scopeName);
    if (it != Scopes.end()) {
        return it->second.get();
    } else {
        return nullptr;
    }
}

TProcessScope& TProcessCategory::RegisterScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    TCPUGroup::TPtr cpuGroup = std::make_shared<TCPUGroup>(processCpuLimits.GetCPUGroupThreadsLimitDef(256));
    auto info = Scopes.emplace(scopeId, std::make_shared<TProcessScope>(std::move(cpuGroup), CPUUsage));
    AFL_VERIFY(info.second);
    return *info.first->second;
}

TProcessScope& TProcessCategory::UpdateScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    auto& scope = MutableProcessScope(scopeId);
    scope.UpdateLimits(processCpuLimits);
    return scope;
}

TProcessScope& TProcessCategory::UpsertScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits) {
    if (Scopes.contains(scopeId)) {
        return UpdateScope(scopeId, processCpuLimits);
    } else {
        return RegisterScope(scopeId, processCpuLimits);
    }
}

void TProcessCategory::UnregisterScope(const TString& name) {
    auto it = Scopes.find(name);
    AFL_VERIFY(it != Scopes.end());
    AFL_VERIFY(!it->second->GetProcessesCount());
    Scopes.erase(it);
}

}
