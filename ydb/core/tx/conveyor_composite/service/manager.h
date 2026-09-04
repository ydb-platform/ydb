#pragma once
#include "category.h"
#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

#include <algorithm>
#include <ranges>

namespace NKikimr::NConveyorComposite {
class TTasksManager {
private:
    struct TWorkloadManagerQuery {
        ui64 RegistrationsCount = 0;
        NYql::NDq::IDqSchedulerContextPtr SchedulerContext;
    };

    std::vector<std::shared_ptr<TWorkersPool>> WorkerPools;
    THashMap<TString, ui64> WorkerPoolNameToIndex;
    std::vector<std::shared_ptr<TProcessCategory>> Categories;
    THashMap<TWorkloadManagerQueryIdentity, TWorkloadManagerQuery, TWorkloadManagerQueryIdentity::THash> WorkloadManagerQueries;
    NConfig::TConfig Config;

    std::vector<std::shared_ptr<TWorkersPool>> BuildWorkerPools() const;
    ui64 FindFreeWorkerPoolsPosition();
    ui64 AddWorkerPool(const NConfig::TWorkersPool& poolConfig,
        const NActors::TActorId& distributorActorId, TCounters& counters);
    void TryFinalizeRemoval(const ui64 workersPoolId);

public:
    bool RegisterWorkloadManagerQuery(const TWorkloadManagerQueryIdentity& identity) {
        auto [it, inserted] = WorkloadManagerQueries.emplace(identity, TWorkloadManagerQuery());
        ++it->second.RegistrationsCount;
        return inserted;
    }

    bool SetWorkloadManagerQueryContext(
        const TWorkloadManagerQueryIdentity& identity, NYql::NDq::IDqSchedulerContextPtr schedulerContext) {
        auto queryIt = WorkloadManagerQueries.find(identity);
        if (queryIt == WorkloadManagerQueries.end()) {
            return false;
        }
        queryIt->second.SchedulerContext = std::move(schedulerContext);
        return true;
    }

    bool UnregisterWorkloadManagerQuery(const TWorkloadManagerQueryIdentity& identity) {
        auto queryIt = WorkloadManagerQueries.find(identity);
        Y_ENSURE(queryIt != WorkloadManagerQueries.end(), "unknown workload manager query " << identity.GetQueryId());
        Y_ENSURE(queryIt->second.RegistrationsCount > 0);
        if (--queryIt->second.RegistrationsCount != 0) {
            return false;
        }
        WorkloadManagerQueries.erase(queryIt);
        return true;
    }

    NYql::NDq::IDqSchedulerContextPtr GetWorkloadManagerQueryContext(
        const TWorkloadManagerQueryIdentity& identity) const {
        const auto it = WorkloadManagerQueries.find(identity);
        return it == WorkloadManagerQueries.end() ? nullptr : it->second.SchedulerContext;
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        for (const auto& pool : BuildWorkerPools()) {
            sb << pool->GetMaxWorkerThreads() << ",";
        }
        sb << ";";
        sb << "queries=" << WorkloadManagerQueries.size() << ";";
        sb << "}";
        return sb;
    }

    TTasksManager(const TString& /*convName*/, const NConfig::TConfig& config, const NActors::TActorId distributorActorId, TCounters& counters)
        : Config(config)
    {
        for (auto&& i : GetEnumAllValues<ESpecialTaskCategory>()) {
            Categories.emplace_back(std::make_shared<TProcessCategory>(Config.GetCategoryConfig(i), counters));
        }
        for (const auto& poolConfig : Config.GetWorkerPools()) {
            AddWorkerPool(poolConfig, distributorActorId, counters);
        }
    }

    TWorkersPool& MutableWorkersPool(const ui64 workersPoolId) {
        Y_ENSURE(workersPoolId < WorkerPools.size(), "worker pool index is out of range: " << workersPoolId);
        Y_ENSURE(WorkerPools[workersPoolId], "worker pool is not active: " << workersPoolId);
        return *WorkerPools[workersPoolId];
    }

    [[nodiscard]] bool DrainTasks() {
        bool result = false;
        for (const auto& pool : BuildWorkerPools()) {
            if (pool->DrainTasks()) {
                result = true;
            }
        }
        return result;
    }

    TProcessCategory& MutableCategoryVerified(const ESpecialTaskCategory category) {
        AFL_VERIFY((ui64)category < Categories.size());
        AFL_VERIFY(!!Categories[(ui64)category]);
        return *Categories[(ui64)category];
    }

    bool IsCurrentConfig(const NConfig::TConfig& config) const;

    bool StartConfigUpdate(const NConfig::TConfig& config,
        const NActors::TActorId& distributorActorId, TCounters& counters);
    bool OnTaskProcessedResult(const ui64 workersPoolId, const ui64 workerIdx);
    bool HasWorkersUpdateInProgress() const;
};

}   // namespace NKikimr::NConveyorComposite
