#pragma once
#include "category.h"
#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>

namespace NKikimr::NConveyorComposite {
class TTasksManager {
private:
    using TWorkersPools = THashMap<TString, std::shared_ptr<TWorkersPool>>;

    TWorkersPools WorkerPools;
    std::vector<std::shared_ptr<TProcessCategory>> Categories;
    NConfig::TConfig Config;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        for (auto&& [id, wp] : WorkerPools) {
            Y_UNUSED(id);
            sb << wp->GetMaxWorkerThreads() << ",";
        }
        sb << ";";
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
            const auto& poolId = poolConfig.GetName();
            AFL_VERIFY(WorkerPools.emplace(poolId, std::make_shared<TWorkersPool>(
                poolId, distributorActorId, poolConfig, counters.GetWorkersPoolSignals(poolId), Categories)).second);
        }
    }

    TWorkersPool& MutableWorkersPool(const TString& workersPoolId) {
        auto it = WorkerPools.find(workersPoolId);
        AFL_VERIFY(it != WorkerPools.end())("workers_pool_id", workersPoolId);
        return *it->second;
    }

    [[nodiscard]] bool DrainTasks() {
        bool result = false;
        for (auto&& [id, pool] : WorkerPools) {
            Y_UNUSED(id);
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

    bool HasFreeWorkerForCategory(const ESpecialTaskCategory category) const {
        for (const auto& [id, pool] : WorkerPools) {
            Y_UNUSED(id);
            if (pool->CanExecuteCategory(category)) {
                return true;
            }
        }
        return false;
    }

    TConclusionStatus ValidateConfigUpdate(const NConfig::TConfig& config) const {
        if (config.IsEnabled() != Config.IsEnabled()) {
            return TConclusionStatus::Fail("runtime Enabled update is not supported yet");
        }
        if (config.GetWorkerPools().size() != WorkerPools.size()) {
            return TConclusionStatus::Fail("runtime worker pool add/remove is not supported yet");
        }
        for (const auto& desiredPool : config.GetWorkerPools()) {
            const auto& poolId = desiredPool.GetName();
            if (!WorkerPools.contains(poolId)) {
                return TConclusionStatus::Fail("runtime worker pool add/remove/rename is not supported yet: '" + poolId + "'");
            }
        }
        return TConclusionStatus::Success();
    }

    bool StartConfigUpdate(const NConfig::TConfig& config) {
        AFL_VERIFY(!ValidateConfigUpdate(config).IsFail());

        // topology updates
        for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
            Categories[(ui64)category]->UpdateConfig(config.GetCategoryConfig(category));
        }
        for (const auto& poolConfig : config.GetWorkerPools()) {
            const auto& poolId = poolConfig.GetName();
            auto& pool = MutableWorkersPool(poolId);
            pool.UpdateMaxBatchSize(poolConfig.GetMaxBatchSize());
            pool.ApplyTopologyUpdate(poolConfig, Categories);
        }
        Config = config;

        // CPU usage updates
        const ui64 totalThreadsCount = NKqp::TStagePredictor::GetPossibleMaxLimitThreads();
        for (const auto& poolConfig : config.GetWorkerPools()) {
            const auto& poolId = poolConfig.GetName();
            const ui64 workersCount = poolConfig.GetWorkersCount(totalThreadsCount);
            std::vector<double> desiredCPULimits;
            desiredCPULimits.reserve(workersCount);
            for (ui64 workerIdx = 0; workerIdx < workersCount; ++workerIdx) {
                desiredCPULimits.emplace_back(poolConfig.GetWorkerCPUUsage(workerIdx, totalThreadsCount));
            }
            MutableWorkersPool(poolId).StartWorkersUpdate(desiredCPULimits);
        }
        return !HasWorkersUpdateInProgress();
    }

    bool OnWorkerCPULimitUpdated(const TEvInternal::TEvWorkerCPULimitUpdated& ev) {
        MutableWorkersPool(ev.WorkersPoolId).OnWorkerCPULimitUpdated(ev);
        return !HasWorkersUpdateInProgress();
    }

    bool OnWorkerStopped(const TEvInternal::TEvWorkerStopped& ev) {
        MutableWorkersPool(ev.WorkersPoolId).OnWorkerStopped(ev);
        return !HasWorkersUpdateInProgress();
    }

    bool HasWorkersUpdateInProgress() const {
        for (const auto& [id, pool] : WorkerPools) {
            Y_UNUSED(id);
            if (pool->HasWorkersUpdateInProgress()) {
                return true;
            }
        }
        return false;
    }
};

}   // namespace NKikimr::NConveyorComposite
