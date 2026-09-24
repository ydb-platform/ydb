#pragma once
#include "category.h"
#include "query.h"
#include "workers_pool.h"

#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <ranges>

namespace NKikimr::NConveyorComposite {
class TTasksManager {
private:
    TQueryRegistry QueryRegistry;
    std::vector<std::shared_ptr<TWorkersPool>> WorkerPools;
    THashMap<TString, ui64> WorkerPoolNameToIndex;
    std::vector<std::shared_ptr<TProcessCategory>> Categories;
    const NActors::TActorId DistributorId;

    auto BuildWorkerPools() const {
        return WorkerPools | std::views::filter([](const auto& pool) { return pool != nullptr; });
    }

    ui64 FindFreeWorkerPoolsPosition();
    ui64 AddWorkerPool(const NConfig::TWorkersPool& poolConfig,
        const NActors::TActorId& distributorActorId, TCounters& counters);
    ui64 CalculateParallelUpperBound(const TSchedulerQueryIdentity& identity) const;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        for (const auto& pool : BuildWorkerPools()) {
            sb << pool->GetMaxWorkerThreads() << ",";
        }
        sb << ";";
        sb << "}";
        return sb;
    }

    TTasksManager(const TString& convName, const NConfig::TConfig& config, NActors::TActorId distributorActorId, TCounters& counters);

    TWorkersPool& MutableWorkersPool(const ui64 workersPoolId) {
        Y_ENSURE(workersPoolId < WorkerPools.size(), "worker pool index is out of range: " << workersPoolId);
        Y_ENSURE(WorkerPools[workersPoolId], "worker pool is not active: " << workersPoolId);
        return *WorkerPools[workersPoolId];
    }

    [[nodiscard]] bool DrainTasks();

    TProcessCategory& MutableCategoryVerified(const ESpecialTaskCategory category) {
        AFL_VERIFY((ui64)category < Categories.size());
        AFL_VERIFY(!!Categories[(ui64)category]);
        return *Categories[(ui64)category];
    }

    void PrepareConfigUpdate(const NConfig::TConfig& config);
    bool IsReadyForUpdate() const;
    void ApplyConfigUpdate(const NConfig::TConfig& config,
        const NActors::TActorId& distributorActorId, TCounters& counters);

    bool RegisterProcess(const ESpecialTaskCategory category, const TString& scopeId, const ui64 internalProcessId,
        const TCPULimitsConfig& cpuLimits, const TSchedulerQueryIdentity& identity);
    TSchedulerQueryIdentity UnregisterProcess(ESpecialTaskCategory category, ui64 internalProcessId);
    bool SetQuery(const TSchedulerQueryIdentity& identity, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query);
    void MovePendingQueryToService(const TSchedulerQueryIdentity& identity);
    void ApplyPreparedQueryCapacity(const TSchedulerQueryIdentity& identity);
    bool TryReleaseQuery(const TSchedulerQueryIdentity& identity);
};

}   // namespace NKikimr::NConveyorComposite
