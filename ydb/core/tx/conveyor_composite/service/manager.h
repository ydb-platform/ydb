#pragma once
#include "category.h"
#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>

namespace NKikimr::NConveyorComposite {
class TTasksManager {
private:
    std::vector<std::shared_ptr<TWorkersPool>> WorkerPools;
    std::vector<std::shared_ptr<TProcessCategory>> Categories;
    NConfig::TConfig Config;

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        for (auto&& wp : WorkerPools) {
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
        for (auto&& i : Config.GetWorkerPools()) {
            WorkerPools.emplace_back(std::make_shared<TWorkersPool>(
                i.GetName(), distributorActorId, i, counters.GetWorkersPoolSignals(i.GetName()), Categories));
        }
    }

    TWorkersPool& MutableWorkersPool(const ui32 workersPoolId) {
        AFL_VERIFY(workersPoolId < WorkerPools.size());
        return *WorkerPools[workersPoolId];
    }

    [[nodiscard]] bool DrainTasks() {
        bool result = false;
        for (auto&& i : WorkerPools) {
            if (i->DrainTasks()) {
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

    TConclusionStatus ValidateConfigUpdate(const NConfig::TConfig& config) const {
        if (config.IsEnabled() != Config.IsEnabled()) {
            return TConclusionStatus::Fail("runtime Enabled update is not supported yet");
        }
        if (config.GetWorkerPools().size() != WorkerPools.size()) {
            return TConclusionStatus::Fail("runtime worker pool add/remove is not supported yet");
        }
        for (ui32 poolIdx = 0; poolIdx < WorkerPools.size(); ++poolIdx) {
            const auto& currentPool = *WorkerPools[poolIdx];
            const auto& desiredPool = config.GetWorkerPools()[poolIdx];
            if (currentPool.GetPoolName() != desiredPool.GetName()) {
                return TConclusionStatus::Fail("runtime worker pool reorder/rename is not supported yet");
            }
            if (currentPool.GetMaxBatchSize() != desiredPool.GetMaxBatchSize()) {
                return TConclusionStatus::Fail("runtime MaxBatchSize update is not supported yet");
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
        for (ui32 poolIdx = 0; poolIdx < WorkerPools.size(); ++poolIdx) {
            WorkerPools[poolIdx]->ApplyTopologyUpdate(config.GetWorkerPools()[poolIdx], Categories);
        }
        Config = config;

        // CPU usage updates
        const ui32 totalThreadsCount = NKqp::TStagePredictor::GetPossibleMaxLimitThreads();
        for (ui32 poolIdx = 0; poolIdx < WorkerPools.size(); ++poolIdx) {
            const auto& poolConfig = config.GetWorkerPools()[poolIdx];
            const ui32 workersCount = poolConfig.GetWorkersCount(totalThreadsCount);
            std::vector<double> desiredCPULimits;
            desiredCPULimits.reserve(workersCount);
            for (ui32 workerIdx = 0; workerIdx < workersCount; ++workerIdx) {
                desiredCPULimits.emplace_back(poolConfig.GetWorkerCPUUsage(workerIdx, totalThreadsCount));
            }
            WorkerPools[poolIdx]->StartWorkersUpdate(desiredCPULimits);
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
        for (const auto& pool : WorkerPools) {
            if (pool->HasWorkersUpdateInProgress()) {
                return true;
            }
        }
        return false;
    }
};

}   // namespace NKikimr::NConveyorComposite
