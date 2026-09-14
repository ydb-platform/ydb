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
    YDB_ACCESSOR(bool, StopPrepare, false);
    YDB_READONLY(ui64, InFlightTasks, 0);
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

    void SetWeight(const double weight);

    void OnTaskStarted() {
        ++InFlightTasks;
    }

    void OnTaskFinished() {
        Y_ENSURE(InFlightTasks, "link has no task to finish");
        --InFlightTasks;
    }
};

class TWorkersPool {
private:
    class TWorkerInfo {
        YDB_READONLY(bool, RunningTask, false);
        YDB_READONLY_DEF(NActors::TActorId, WorkerId);
        YDB_READONLY(double, CPULimit, 1);
        YDB_ACCESSOR(bool, StopPrepare, false);

    public:
        TWorkerInfo(std::unique_ptr<TWorker>&& worker, const double cpuLimit)
            : WorkerId(TActivationContext::Register(worker.release()))
            , CPULimit(cpuLimit) {
        }

        void SetCPULimit(const double value) {
            CPULimit = value;
        }

        void OnStartTask();
        void OnStopTask();
    };

    ui64 WorkersCount = 0;
    YDB_READONLY(double, MaxWorkerThreads, 0);
    std::vector<TWeightedCategory> CategoryLinks;
    std::vector<TWorkerInfo> Workers;
    std::vector<ui64> ActiveWorkersIdx;
    std::shared_ptr<TWorkersPoolCounters> Counters;
    TAverageCalcer<TDuration> DeliveringDuration;
    ui64 MaxBatchSize = 30;
    const TString PoolName;
    const NActors::TActorId DistributorId;
    const ui64 WorkersPoolId;

    void RemoveFreeWorker(const ui64 workerIdx);
    void UpdateWorkerCPULimit(const ui64 workerIdx, const double newLimit);
    void IncreaseWorkers(const std::vector<double>& desiredCPULimits);
    void DecreaseWorkers(const std::vector<double>& desiredCPULimits);
    void RunTask(std::vector<TWorkerTask>&& tasksBatch);
    TWeightedCategory& FindCategoryLink(const ESpecialTaskCategory category);

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
    void ReleaseWorker(const ui64 workerIdx);

    void PrepareConfigUpdate(const NConfig::TWorkersPool* target);
    bool IsReadyForUpdate() const;
    void ApplyWorkersUpdate(const std::vector<double>& desiredCPULimits);

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
