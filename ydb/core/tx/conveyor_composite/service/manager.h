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

    auto BuildWorkerPools() const {
        return WorkerPools | std::views::filter([](const auto& value) {
            return value != nullptr;
        });
    }

    ui64 FindFreeWorkerPoolsPosition() {
        const auto it = std::find(WorkerPools.begin(), WorkerPools.end(), nullptr);
        if (it != WorkerPools.end()) {
            return std::distance(WorkerPools.begin(), it);
        }
        WorkerPools.resize(WorkerPools.size() + 1);
        return WorkerPools.size() - 1;
    }

    ui64 AddWorkerPool(const NConfig::TWorkersPool& poolConfig,
        const NActors::TActorId& distributorActorId, TCounters& counters) {
        const ui64 workersPoolId = FindFreeWorkerPoolsPosition();
        Y_ENSURE(WorkerPoolNameToIndex.emplace(poolConfig.GetName(), workersPoolId).second,
            "duplicate worker pool name: " << poolConfig.GetName());
        WorkerPools[workersPoolId] = std::make_shared<TWorkersPool>(poolConfig.GetName(), workersPoolId, distributorActorId, poolConfig,
            counters.GetWorkersPoolSignals(poolConfig.GetName()), Categories);
        return workersPoolId;
    }

    void TryFinalizeRemoval(const ui64 workersPoolId) {
        auto& pool = MutableWorkersPool(workersPoolId);
        if (!WorkerPoolNameToIndex.contains(pool.GetPoolName()) && !pool.HasWorkersUpdateInProgress()) {
            WorkerPools[workersPoolId].reset();
        }
    }

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
        for (const auto& pool : BuildWorkerPools()) {
            pool->RemovePendingSchedulableWork(identity);
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
        const TSchedulerContextGetter schedulerContextGetter = [&](const TWorkloadManagerQueryIdentity& identity) {
            return GetWorkloadManagerQueryContext(identity);
        };
        for (const auto& pool : BuildWorkerPools()) {
            if (pool->DrainTasks(schedulerContextGetter)) {
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

    bool IsCurrentConfig(const NConfig::TConfig& config) const {
        return Config == config;
    }

    bool StartConfigUpdate(const NConfig::TConfig& config,
        const NActors::TActorId& distributorActorId, TCounters& counters) {
        Y_ENSURE(config.IsEnabled() == Config.IsEnabled(), "runtime Enabled update is not supported yet");

        THashSet<TString> desiredPoolNames;
        desiredPoolNames.reserve(config.GetWorkerPools().size());
        for (const auto& poolConfig : config.GetWorkerPools()) {
            desiredPoolNames.emplace(poolConfig.GetName());
        }

        std::vector<TString> removedPoolNames;
        for (const auto& [poolName, poolIdx] : WorkerPoolNameToIndex) {
            Y_UNUSED(poolIdx);
            if (!desiredPoolNames.contains(poolName)) {
                removedPoolNames.emplace_back(poolName);
            }
        }
        for (const auto& poolName : removedPoolNames) {
            const ui64 poolIdx = WorkerPoolNameToIndex.at(poolName);
            WorkerPoolNameToIndex.erase(poolName);
            auto& pool = MutableWorkersPool(poolIdx);
            pool.ClearTopology();
            if (pool.StartWorkersRetirement()) {
                TryFinalizeRemoval(poolIdx);
            }
        }

        for (const auto& poolConfig : config.GetWorkerPools()) {
            if (!WorkerPoolNameToIndex.contains(poolConfig.GetName())) {
                AddWorkerPool(poolConfig, distributorActorId, counters);
            }
        }

        // topology updates
        for (const auto& poolConfig : config.GetWorkerPools()) {
            auto& pool = MutableWorkersPool(WorkerPoolNameToIndex.at(poolConfig.GetName()));
            pool.UpdateMaxBatchSize(poolConfig.GetMaxBatchSize());
            pool.ApplyTopologyUpdate(poolConfig, Categories);
        }
        for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
            MutableCategoryVerified(category).ApplyConfig(config.GetCategoryConfig(category));
        }
        Config = config;

        // CPU usage updates
        const ui64 totalThreadsCount = NKqp::TStagePredictor::GetPossibleMaxLimitThreads();
        for (const auto& poolConfig : config.GetWorkerPools()) {
            const ui64 poolIdx = WorkerPoolNameToIndex.at(poolConfig.GetName());
            const ui64 workersCount = poolConfig.GetWorkersCount(totalThreadsCount);
            std::vector<double> desiredCPULimits;
            desiredCPULimits.reserve(workersCount);
            for (ui64 workerIdx = 0; workerIdx < workersCount; ++workerIdx) {
                desiredCPULimits.emplace_back(poolConfig.GetWorkerCPUUsage(workerIdx, totalThreadsCount));
            }
            MutableWorkersPool(poolIdx).StartWorkersUpdate(desiredCPULimits);
        }
        return !HasWorkersUpdateInProgress();
    }

    bool OnTaskProcessedResult(const ui64 workersPoolId, const ui64 workerIdx) {
        if (!MutableWorkersPool(workersPoolId).ReleaseWorker(workerIdx)) {
            return false;
        }
        TryFinalizeRemoval(workersPoolId);
        return !HasWorkersUpdateInProgress();
    }

    bool HasWorkersUpdateInProgress() const {
        for (const auto& pool : BuildWorkerPools()) {
            if (pool->HasWorkersUpdateInProgress()) {
                return true;
            }
        }
        return false;
    }
};

}   // namespace NKikimr::NConveyorComposite
