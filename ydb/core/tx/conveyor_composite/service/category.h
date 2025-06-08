#pragma once
#include "common.h"
#include "counters.h"
#include "scope.h"
#include "worker.h"

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NConveyorComposite {

class TProcessCategory: public TNonCopyable {
private:
    const ESpecialTaskCategory Category;
    std::shared_ptr<TCPUUsage> CPUUsage = std::make_shared<TCPUUsage>(nullptr);
    std::shared_ptr<TPositiveControlInteger> WaitingTasksCount = std::make_shared<TPositiveControlInteger>();
    YDB_READONLY_DEF(std::shared_ptr<TCategorySignals>, Counters);
    THashMap<TString, std::shared_ptr<TProcessScope>> Scopes;
    THashMap<ui64, std::shared_ptr<TProcess>> Processes;
    const NConfig::TCategory Config;

public:
    ui32 GetWaitingQueueSize() const {
        return WaitingTasksCount->Val();
    }
    TProcessCategory(const NConfig::TCategory& config, TCounters& counters)
        : Category(config.GetCategory())
        , Config(config) {
        Counters = counters.GetCategorySignals(Category);
        RegisterScope("DEFAULT", TCPULimitsConfig(1000, 1000)).RegisterProcess(0);
        Counters->WaitingQueueSizeLimit->Set(config.GetQueueSizeLimit());
    }

    ~TProcessCategory() {
        MutableProcessScope("DEFAULT").UnregisterProcess(0);
        UnregisterScope("DEFAULT");
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        const ui64 id = result.GetProcessId();
        if (auto* process = MutableProcessOptional(id)) {
            process->PutTaskResult(std::move(result));
        }
    }

    TProcess& MutableProcessVerified(const ui64 processId) {
        auto it = Processes.find(processId);
        AFL_VERIFY(it != Processes.end());
        return *it->second;
    }

    TProcess* MutableProcessOptional(const ui64 processId) {
        auto it = Processes.find(processId);
        if (it != Processes.end()) {
            return it->second.get();
        } else {
            return nullptr;
        }
    }

    void RegisterProcess(const ui64 internalProcessId, std::shared_ptr<TProcessScope>&& scope) {
        AFL_VERIFY(Processes.emplace(internalProcessId, std::make_shared<TProcess>(internalProcessId, std::move(scope), WaitingTasksCount)).second);
    }

    void UnregisterProcess(const ui64 processId) {
        auto it = Processes.find(processId);
        AFL_VERIFY(it != Processes.end());
        if (it->second->GetScope().use_count() == 2) {
            AFL_VERIFY(Scopes.erase(it->second->GetScope()->GetScopeId()));
        }
        Processes.erase(it);
    }

    ESpecialTaskCategory GetCategory() const {
        return Category;
    }

    void PutTaskResult(TWorkerTaskResult&& result) {
        const TString id = result.GetScopeId();
        if (TProcessScope* scope = MutableProcessScopeOptional(id)) {
            scope->PutTaskResult(std::move(result));
        }
    }

    bool HasTasks() const;
    void DoQuant(const TMonotonic newStart);
    TWorkerTask ExtractTaskWithPrediction();
    TProcessScope& MutableProcessScope(const TString& scopeName);
    TProcessScope* MutableProcessScopeOptional(const TString& scopeName);
    TProcessScope& RegisterScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits);
    TProcessScope& UpsertScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits);

    TProcessScope& UpdateScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits);
    void UnregisterScope(const TString& name);
};

}   // namespace NKikimr::NConveyorComposite
