#pragma once
#include "category.h"
#include "workers_pool.h"

#include <ydb/core/tx/conveyor_composite/usage/common.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <ranges>

namespace NKikimr::NConveyorComposite {
class TTasksManager {
private:
    struct TQueryProcesses {
        ui64 ProcessesCount = 0;
        std::shared_ptr<TWorkloadManagerQuery> Query;
    };

    std::vector<std::shared_ptr<TWorkersPool>> WorkerPools;
    THashMap<TString, ui64> WorkerPoolNameToIndex;
    std::vector<std::shared_ptr<TProcessCategory>> Categories;
    THashMap<TWorkloadManagerQueryIdentity, TQueryProcesses, TWorkloadManagerQueryIdentity::THash> WorkloadManagerQueries;

    auto BuildWorkerPools() const {
        return WorkerPools | std::views::filter([](const auto& pool) { return pool != nullptr; });
    }

    ui64 FindFreeWorkerPoolsPosition();
    ui64 AddWorkerPool(const NConfig::TWorkersPool& poolConfig,
        const NActors::TActorId& distributorActorId, TCounters& counters);

public:
    // Return the identity only when the first process requires scheduler registration.
    [[nodiscard]] std::optional<TWorkloadManagerQueryIdentity> RegisterProcess(const ESpecialTaskCategory category, const ui64 processId,
        std::shared_ptr<TProcessScope>&& scope, const std::optional<TWorkloadManagerQueryIdentity>& identity = std::nullopt) {
        std::shared_ptr<TWorkloadManagerQuery> query;
        std::optional<TWorkloadManagerQueryIdentity> queryToRegister = std::nullopt;
        if (identity) {
            auto [it, inserted] = WorkloadManagerQueries.emplace(*identity, TQueryProcesses());
            if (inserted) {
                it->second.Query = std::make_shared<TWorkloadManagerQuery>(*identity);
                queryToRegister = identity;
            }
            ++it->second.ProcessesCount;
            query = it->second.Query;
        }
        MutableCategoryVerified(category).RegisterProcess(processId, std::move(scope), std::move(query));
        return queryToRegister;
    }

    bool SetWorkloadManagerQueryContext(
        const TWorkloadManagerQueryIdentity& identity, NYql::NDq::IDqSchedulableWorkFactoryPtr schedulerContext) {
        auto queryIt = WorkloadManagerQueries.find(identity);
        if (queryIt == WorkloadManagerQueries.end()) {
            return false;
        }
        queryIt->second.Query->SetSchedulerContext(std::move(schedulerContext));
        return true;
    }

    // Return the identity only when the last process releases the scheduler query.
    [[nodiscard]] std::optional<TWorkloadManagerQueryIdentity> UnregisterProcess(const ESpecialTaskCategory category, const ui64 processId) {
        auto& processCategory = MutableCategoryVerified(category);
        const auto query = processCategory.GetProcessVerified(processId).GetWorkloadManagerQuery();
        processCategory.UnregisterProcess(processId);
        if (!query) {
            return std::nullopt;
        }
        auto queryIt = WorkloadManagerQueries.find(query->GetIdentity());
        AFL_VERIFY(queryIt != WorkloadManagerQueries.end());
        AFL_VERIFY(queryIt->second.ProcessesCount > 0);
        if (--queryIt->second.ProcessesCount != 0) {
            return std::nullopt;
        }
        WorkloadManagerQueries.erase(queryIt);
        return query->GetIdentity();
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
    {
        for (auto&& i : GetEnumAllValues<ESpecialTaskCategory>()) {
            Categories.emplace_back(std::make_shared<TProcessCategory>(config.GetCategoryConfig(i), counters));
        }
        for (const auto& poolConfig : config.GetWorkerPools()) {
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

    void PrepareConfigUpdate(const NConfig::TConfig& config);
    bool IsReadyForUpdate() const;
    void ApplyConfigUpdate(const NConfig::TConfig& config,
        const NActors::TActorId& distributorActorId, TCounters& counters);
};

}   // namespace NKikimr::NConveyorComposite
