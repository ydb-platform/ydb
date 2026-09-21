#include "manager.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

#include <algorithm>
#include <numeric>

namespace NKikimr::NConveyorComposite {

TTasksManager::TTasksManager(
    const TString& /*convName*/, const NConfig::TConfig& config, const NActors::TActorId distributorActorId, TCounters& counters)
    : DistributorId(distributorActorId) {
    const TSchedulerQueryIdentity defaultIdentity = {};
    for (auto&& i : GetEnumAllValues<ESpecialTaskCategory>()) {
        Categories.emplace_back(std::make_shared<TProcessCategory>(config.GetCategoryConfig(i), counters));
        QueryRegistry.RegisterProcess(defaultIdentity);
    }
    for (const auto& poolConfig : config.GetWorkerPools()) {
        AddWorkerPool(poolConfig, distributorActorId, counters);
    }
    QueryRegistry.UpdateWorkCapacity(defaultIdentity, CalculateParallelUpperBound(defaultIdentity));
}

bool TTasksManager::DrainTasks() {
    const TMonotonic now = TMonotonic::Now();
    TDrainContext context{
        .Now = now,
        .AverageWakeUpDeadline = QueryRegistry.GetAverageWakeUpDeadline(now),
    };
    bool result = false;
    for (const auto& pool : BuildWorkerPools()) {
        if (pool->DrainTasks(context)) {
            result = true;
        }
    }
    if (const auto deadline = QueryRegistry.GetMinWakeUpDeadline()) {
        TActivationContext::Schedule(
            *deadline, new NActors::IEventHandle(DistributorId, {}, new NActors::TEvents::TEvWakeup()));
    }
    return result;
}

ui64 TTasksManager::FindFreeWorkerPoolsPosition() {
    const auto it = std::find(WorkerPools.begin(), WorkerPools.end(), nullptr);
    if (it != WorkerPools.end()) {
        return std::distance(WorkerPools.begin(), it);
    }
    WorkerPools.resize(WorkerPools.size() + 1);
    return WorkerPools.size() - 1;
}

ui64 TTasksManager::AddWorkerPool(const NConfig::TWorkersPool& poolConfig,
    const NActors::TActorId& distributorActorId, TCounters& counters) {
    const ui64 workersPoolId = FindFreeWorkerPoolsPosition();
    Y_ENSURE(WorkerPoolNameToIndex.emplace(poolConfig.GetName(), workersPoolId).second,
        "duplicate worker pool name: " << poolConfig.GetName());
    WorkerPools[workersPoolId] = std::make_shared<TWorkersPool>(poolConfig.GetName(), workersPoolId, distributorActorId, poolConfig,
        counters.GetWorkersPoolSignals(poolConfig.GetName()), Categories, &QueryRegistry);
    return workersPoolId;
}

ui64 TTasksManager::CalculateParallelUpperBound(const TSchedulerQueryIdentity& identity) const {
    auto workersCounts = BuildWorkerPools()
        | std::views::filter([&](const auto& pool) { return pool->HasProcesses(identity); })
        | std::views::transform(&TWorkersPool::GetWorkersCount);
    return std::accumulate(workersCounts.begin(), workersCounts.end(), ui64{0});
}

bool TTasksManager::RegisterProcess(const ESpecialTaskCategory category, const TString& scopeId, const ui64 internalProcessId,
    const TCPULimitsConfig& cpuLimits, const TSchedulerQueryIdentity& identity) {
    const bool isNewIdentity = QueryRegistry.RegisterProcess(identity);
    auto& processCategory = MutableCategoryVerified(category);
    auto scope = processCategory.UpsertScope(scopeId, cpuLimits);
    processCategory.RegisterProcess(internalProcessId, std::move(scope), identity);
    QueryRegistry.UpdateWorkCapacity(identity, CalculateParallelUpperBound(identity));
    return isNewIdentity;
}

void TTasksManager::UnregisterProcess(const ESpecialTaskCategory category, const ui64 internalProcessId) {
    const auto identity = MutableCategoryVerified(category).UnregisterProcess(internalProcessId);
    const bool removed = QueryRegistry.UnregisterProcess(identity);
    if (!removed) {
        QueryRegistry.UpdateWorkCapacity(identity, CalculateParallelUpperBound(identity));
    }
}

bool TTasksManager::SetQuery(
    const TSchedulerQueryIdentity& identity, NKqp::NScheduler::NHdrf::NDynamic::TQueryPtr query) {
    if (!QueryRegistry.SetQuery(identity, std::move(query))) {
        return false;
    }
    QueryRegistry.UpdateWorkCapacity(identity, CalculateParallelUpperBound(identity));
    return true;
}

void TTasksManager::PrepareConfigUpdate(const NConfig::TConfig& config) {
    THashMap<TString, const NConfig::TWorkersPool*> targets;
    for (const auto& poolConfig : config.GetWorkerPools()) {
        targets.emplace(poolConfig.GetName(), &poolConfig);
    }
    for (const auto& pool : WorkerPools) {
        if (pool) {
            const auto it = targets.find(pool->GetPoolName());
            pool->PrepareConfigUpdate(it == targets.end() ? nullptr : it->second);
        }
    }
}

bool TTasksManager::IsReadyForUpdate() const {
    for (const auto& pool : WorkerPools) {
        if (pool && !pool->IsReadyForUpdate()) {
            return false;
        }
    }
    return true;
}

void TTasksManager::ApplyConfigUpdate(const NConfig::TConfig& config,
    const NActors::TActorId& distributorActorId, TCounters& counters) {
    Y_ENSURE(IsReadyForUpdate(), "conveyor is not prepared for config update");

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
        pool.ApplyWorkersUpdate({});
        pool.ClearTopology();
        WorkerPools[poolIdx].reset();
    }

    for (const auto& poolConfig : config.GetWorkerPools()) {
        if (!WorkerPoolNameToIndex.contains(poolConfig.GetName())) {
            AddWorkerPool(poolConfig, distributorActorId, counters);
        }
    }

    const ui64 totalThreadsCount = NKqp::TStagePredictor::GetPossibleMaxLimitThreads();
    for (const auto& poolConfig : config.GetWorkerPools()) {
        const ui64 poolIdx = WorkerPoolNameToIndex.at(poolConfig.GetName());
        const ui64 workersCount = poolConfig.GetWorkersCount(totalThreadsCount);
        std::vector<double> desiredCPULimits;
        desiredCPULimits.reserve(workersCount);
        for (ui64 workerIdx = 0; workerIdx < workersCount; ++workerIdx) {
            desiredCPULimits.emplace_back(poolConfig.GetWorkerCPUUsage(workerIdx, totalThreadsCount));
        }
        auto& pool = MutableWorkersPool(poolIdx);
        pool.ApplyWorkersUpdate(desiredCPULimits);
        pool.UpdateMaxBatchSize(poolConfig.GetMaxBatchSize());
        pool.ApplyTopologyUpdate(poolConfig, Categories);
    }
    for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
        MutableCategoryVerified(category).ApplyConfig(config.GetCategoryConfig(category));
    }
    for (const auto& identity : QueryRegistry.GetIdentitiesView()) {
        QueryRegistry.UpdateWorkCapacity(identity, CalculateParallelUpperBound(identity));
    }
}

}
