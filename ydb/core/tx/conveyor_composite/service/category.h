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
    TPositiveControlInteger WaitingTasksCount;
    YDB_READONLY_DEF(std::shared_ptr<TCategorySignals>, Counters);
    THashMap<TString, std::shared_ptr<TProcessScope>> Scopes;
    const NConfig::TCategory Config;

public:
    TProcessCategory(const NConfig::TCategory& config, TCounters& counters)
        : Category(config.GetCategory())
        , Config(config) {
        Counters = counters.GetCategorySignals(Category);
        RegisterScope("DEFAULT", TCPULimitsConfig(1000, 1000)).RegisterProcess(0);
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
