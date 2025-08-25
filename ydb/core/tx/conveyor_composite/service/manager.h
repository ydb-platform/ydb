#pragma once
#include "category.h"
#include "workers_pool.h"

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
};

}   // namespace NKikimr::NConveyorComposite
