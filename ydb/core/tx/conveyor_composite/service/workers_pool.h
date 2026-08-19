#pragma once
#include "category.h"
#include "common.h"
#include "worker.h"

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NConveyorComposite {

class TWeightedCategory {
private:
    YDB_READONLY(std::shared_ptr<TCPUUsage>, CPUUsage, std::make_shared<TCPUUsage>(nullptr));
    YDB_READONLY_DEF(std::shared_ptr<TProcessCategory>, Category);
    YDB_READONLY_DEF(std::shared_ptr<TWPCategorySignals>, Counters);
    double Weight = 1;

public:
    TWeightedCategory(const double weight, const std::shared_ptr<TProcessCategory>& cat, const std::shared_ptr<TWPCategorySignals>& counters)
        : Category(cat)
        , Counters(counters)
    {
        AFL_VERIFY(Counters);
        AFL_VERIFY(cat);
        SetWeight(weight);
    }

    double GetWeight() const {
        return Weight;
    }

    void SetWeight(const double weight) {
        AFL_VERIFY(weight > 0);
        Weight = weight;
        Counters->ValueWeight->Set(weight);
    }

};

class TWorkersPool {
private:
    class TTaskCompletionContext {
        YDB_READONLY_DEF(std::shared_ptr<TCPUUsage>, CPUUsage);
        YDB_READONLY_DEF(std::shared_ptr<TProcessCategory>, Category);
        YDB_READONLY_DEF(std::shared_ptr<TWPCategorySignals>, Counters);

    public:
        explicit TTaskCompletionContext(const TWeightedCategory& link)
            : CPUUsage(link.GetCPUUsage())
            , Category(link.GetCategory())
            , Counters(link.GetCounters()) {
        }
    };

    using TTaskCompletionContexts = THashMap<ESpecialTaskCategory, TTaskCompletionContext>;

    class TWorkerInfo {
        YDB_READONLY(bool, RunningTask, false);
        YDB_READONLY_DEF(NActors::TActorId, WorkerId);
        YDB_READONLY(double, CPULimit, 1);
        TTaskCompletionContexts CompletionContexts;

    public:
        TWorkerInfo(std::unique_ptr<TWorker>&& worker, const double cpuLimit)
            : WorkerId(TActivationContext::Register(worker.release()))
            , CPULimit(cpuLimit) {
        }

        void SetCPULimit(const double value) {
            CPULimit = value;
        }

        void OnStartTask(TTaskCompletionContexts&& completionContexts) {
            AFL_VERIFY(!RunningTask);
            AFL_VERIFY(!completionContexts.empty());
            RunningTask = true;
            CompletionContexts = std::move(completionContexts);
        }

        void OnStopTask() {
            AFL_VERIFY(RunningTask);
            RunningTask = false;
            CompletionContexts.clear();
        }

        const TTaskCompletionContext& GetCompletionContext(const ESpecialTaskCategory category) const {
            const auto it = CompletionContexts.find(category);
            AFL_VERIFY(it != CompletionContexts.end())("category", category)("contexts_count", CompletionContexts.size());
            return it->second;
        }
    };

    struct TWorkersUpdateState {
        ui64 DesiredWorkersCount = 0;
        THashSet<ui64> WorkersWaitingForLimitUpdate;
        THashSet<ui64> WorkersWaitingForStop;

        bool IsFinished() const {
            return WorkersWaitingForLimitUpdate.empty() && WorkersWaitingForStop.empty();
        }
    };

    ui64 WorkersCount = 0;
    YDB_READONLY(double, MaxWorkerThreads, 0);
    std::vector<TWeightedCategory> Processes;
    std::vector<TWorkerInfo> Workers;
    std::vector<ui64> ActiveWorkersIdx;
    std::shared_ptr<TWorkersPoolCounters> Counters;
    TAverageCalcer<TDuration> DeliveringDuration;
    ui64 MaxBatchSize = 30;
    const TString PoolName;
    const NActors::TActorId DistributorId;
    const ui64 WorkersPoolId;
    std::optional<TWorkersUpdateState> WorkersUpdate;

    void RemoveFreeWorker(const ui64 workerIdx);
    void UpdateWorkerCPULimit(const ui64 workerIdx, const double newLimit);
    void IncreaseWorkers(const std::vector<double>& desiredCPULimits);
    void DecreaseWorkers(const std::vector<double>& desiredCPULimits);
    bool TryFinishWorkersUpdate();
    void RunTask(std::vector<TWorkerTask>&& tasksBatch, TTaskCompletionContexts&& completionContexts);

public:
    static constexpr double Eps = 1e-6;

    TWorkersPool(const TString& poolName, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
        const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories);

    const std::shared_ptr<TWorkersPoolCounters>& GetCounters() const {
        return Counters;
    }

    [[nodiscard]] bool DrainTasks();

    void AddDeliveryDuration(const TDuration d) {
        DeliveringDuration.Add(d);
    }

    void PutTaskResults(std::vector<TWorkerTaskResult>&& result, const ui64 workersPoolId = 0, const ui64 workerIdx = 0);
    bool HasFreeWorker() const;
    void ReleaseWorker(const ui64 workerIdx);

    bool StartWorkersUpdate(const std::vector<double>& desiredCPULimits);
    bool OnWorkerCPULimitUpdated(const TEvInternal::TEvWorkerCPULimitUpdated& ev);
    bool OnWorkerStopped(const TEvInternal::TEvWorkerStopped& ev);

    bool HasWorkersUpdateInProgress() const {
        return WorkersUpdate.has_value();
    }

    const TString& GetPoolName() const {
        return PoolName;
    }

    ui64 GetMaxBatchSize() const {
        return MaxBatchSize;
    }

    void ApplyTopologyUpdate(const NConfig::TWorkersPool& config,
        const std::vector<std::shared_ptr<TProcessCategory>>& categories);
};

}   // namespace NKikimr::NConveyorComposite
