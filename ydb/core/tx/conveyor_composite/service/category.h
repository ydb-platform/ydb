#pragma once
#include "common.h"
#include "counters.h"
#include "process.h"
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
    std::map<TDuration, std::deque<std::shared_ptr<TProcess>>> WeightedProcesses;
    const NConfig::TCategory Config;

    [[nodiscard]] bool RemoveWeightedProcess(const std::shared_ptr<TProcess>& process);

public:
    ui32 GetWaitingQueueSize() const {
        return WaitingTasksCount->Val();
    }
    TProcessCategory(const NConfig::TCategory& config, TCounters& counters)
        : Category(config.GetCategory())
        , Config(config) {
        Counters = counters.GetCategorySignals(Category);
        RegisterProcess(0, RegisterScope("DEFAULT", TCPULimitsConfig(1000, 1000)));
        Counters->WaitingQueueSizeLimit->Set(config.GetQueueSizeLimit());
    }

    ~TProcessCategory() {
        UnregisterProcess(0);
    }

    void RegisterTask(const ui64 internalProcessId, std::shared_ptr<ITask>&& task) {
        auto it = Processes.find(internalProcessId);
        AFL_VERIFY(it != Processes.end())("process_id", internalProcessId);
        if (!it->second->GetTasks().size()) {
            if (!WeightedProcesses.empty() && !it->second->GetInProgressTasksCount()) {
                it->second->SetBaseWeight(WeightedProcesses.begin()->first);
            }
            WeightedProcesses[it->second->GetWeightedUsage()].emplace_back(it->second);
        }
        it->second->RegisterTask(std::move(task), Category);
    }

    void PutTaskResult(TWorkerTaskResult&& result, THashSet<TString>& scopeIds);

    void RegisterProcess(const ui64 internalProcessId, std::shared_ptr<TProcessScope>&& scope) {
        scope->IncProcesses();
        AFL_VERIFY(Processes.emplace(internalProcessId, std::make_shared<TProcess>(internalProcessId, std::move(scope), WaitingTasksCount)).second);
    }

    void UnregisterProcess(const ui64 processId) {
        auto it = Processes.find(processId);
        AFL_VERIFY(it != Processes.end());
        Y_UNUSED(RemoveWeightedProcess(it->second));
        if (it->second->GetScope()->DecProcesses()) {
            AFL_VERIFY(Scopes.erase(it->second->GetScope()->GetScopeId()));
        }
        Processes.erase(it);
    }

    ESpecialTaskCategory GetCategory() const {
        return Category;
    }

    bool HasTasks() const;
    std::optional<TWorkerTask> ExtractTaskWithPrediction(const std::shared_ptr<TWPCategorySignals>& counters, THashSet<TString>& scopeIds);
    TProcessScope& MutableProcessScope(const TString& scopeName);
    TProcessScope* MutableProcessScopeOptional(const TString& scopeName);
    std::shared_ptr<TProcessScope> GetProcessScopePtrVerified(const TString& scopeName) const;
    std::shared_ptr<TProcessScope> RegisterScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits);
    std::shared_ptr<TProcessScope> UpsertScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits);

    std::shared_ptr<TProcessScope> UpdateScope(const TString& scopeId, const TCPULimitsConfig& processCpuLimits);
    void UnregisterScope(const TString& name);
};

}   // namespace NKikimr::NConveyorComposite
