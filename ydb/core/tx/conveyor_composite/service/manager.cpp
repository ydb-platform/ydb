#include "manager.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

#include <algorithm>

namespace NKikimr::NConveyorComposite {

std::vector<std::shared_ptr<TWorkersPool>> TTasksManager::BuildWorkerPools() const {
    std::vector<std::shared_ptr<TWorkersPool>> result;
    result.reserve(WorkerPools.size());
    for (const auto& pool : WorkerPools) {
        if (pool) {
            result.emplace_back(pool);
        }
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
        counters.GetWorkersPoolSignals(poolConfig.GetName()), Categories);
    return workersPoolId;
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
}

}
