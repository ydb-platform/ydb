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
    const NActors::TActorId DistributorActorId;
    const NConfig::TConfig Config;
    TCounters& Counters;

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
        : DistributorActorId(distributorActorId)
        , Config(config)
        , Counters(counters)
    {
        for (auto&& i : GetEnumAllValues<ESpecialTaskCategory>()) {
            Categories.emplace_back(std::make_shared<TProcessCategory>(Config.GetCategoryConfig(i), Counters));
        }
        for (auto&& i : Config.GetWorkerPools()) {
            WorkerPools.emplace_back(std::make_shared<TWorkersPool>(
                i.GetName(), distributorActorId, i, Counters.GetWorkersPoolSignals(i.GetName()), Categories));
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

    const TProcessCategory& GetCategoryVerified(const ESpecialTaskCategory category) const {
        AFL_VERIFY((ui64)category < Categories.size());
        AFL_VERIFY(!!Categories[(ui64)category]);
        return *Categories[(ui64)category];
    }

    bool StartWorkersUpdate(const NConfig::TConfig& config) {
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
