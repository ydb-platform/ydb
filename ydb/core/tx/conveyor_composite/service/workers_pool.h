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
        Y_ENSURE(Counters, "worker pool category counters are not initialized");
        Y_ENSURE(cat, "worker pool category is not initialized");
        SetWeight(weight);
    }

    double GetWeight() const {
        return Weight;
    }

    void SetWeight(const double weight) {
        Y_ENSURE(std::isfinite(weight) && weight > 0, "invalid worker pool category weight: " << weight);
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
        YDB_READONLY(bool, StopRequested, false);
        TTaskCompletionContexts CompletionContexts;
        TConveyorWorkUnits WorkUnits;

    public:
        TWorkerInfo(std::unique_ptr<TWorker>&& worker, const double cpuLimit)
            : WorkerId(TActivationContext::Register(worker.release()))
            , CPULimit(cpuLimit) {
        }

        void SetCPULimit(const double value) {
            CPULimit = value;
        }

        void RequestStop() {
            StopRequested = true;
        }

        void OnStartTask(TTaskCompletionContexts&& completionContexts) {
            Y_ENSURE(!RunningTask, "worker already has a running task");
            Y_ENSURE(!completionContexts.empty(), "worker task has no completion contexts");
            RunningTask = true;
            CompletionContexts = std::move(completionContexts);
            WorkUnits = std::move(workUnits);
        }

        void OnStopTask() {
            Y_ENSURE(RunningTask, "worker has no running task to stop");
            RunningTask = false;
            CompletionContexts.clear();
        }

        void FinishWorkUnits(const std::vector<TWorkerTaskResult>& results) {
            THashMap<ui64, TDuration> durations;
            for (const auto& result : results) {
                const auto& workloadContext = result.GetWorkloadContext();
                auto unitIt = WorkUnits.find(workloadContext.QueryId);
                if (unitIt == WorkUnits.end()) {
                    continue;
                }
                AFL_VERIFY(unitIt->second->GetContext() == workloadContext);
                durations[workloadContext.QueryId] += result.GetDuration();
            }
            for (auto& [queryId, workUnit] : WorkUnits) {
                auto durationIt = durations.find(queryId);
                AFL_VERIFY(durationIt != durations.end());
                workUnit->Finish(durationIt->second);
            }
            WorkUnits.clear();
        }

        const TTaskCompletionContext& GetCompletionContext(const ESpecialTaskCategory category) const {
            const auto it = CompletionContexts.find(category);
            Y_ENSURE(it != CompletionContexts.end(), "completion context is missing for category " << category);
            return it->second;
        }
    };

    struct TWorkersUpdateState {
        ui64 DesiredWorkersCount = 0;
        THashSet<ui64> WorkersWaitingForRelease;

        bool IsFinished() const {
            return WorkersWaitingForRelease.empty();
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
    void RunTask(std::vector<TWorkerTask>&& tasksBatch, TTaskCompletionContexts&& completionContexts,
        TConveyorWorkUnits&& workUnits);

public:
    static constexpr double Eps = 1e-6;

    TWorkersPool(const TString& poolName, const ui64 workersPoolId, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
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
    bool ReleaseWorker(const ui64 workerIdx);

    bool StartWorkersUpdate(const std::vector<double>& desiredCPULimits);
    bool StartWorkersRetirement();

    bool HasWorkersUpdateInProgress() const {
        return WorkersUpdate.has_value();
    }

    const TString& GetPoolName() const {
        return PoolName;
    }

    ui64 GetMaxBatchSize() const {
        return MaxBatchSize;
    }

    void UpdateMaxBatchSize(const ui64 maxBatchSize) {
        MaxBatchSize = maxBatchSize;
    }

    void ApplyTopologyUpdate(const NConfig::TWorkersPool& config,
        const std::vector<std::shared_ptr<TProcessCategory>>& categories);
    void ClearTopology();
};

}   // namespace NKikimr::NConveyorComposite
