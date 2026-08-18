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
    YDB_READONLY(double, Weight, 1);

public:
    TWeightedCategory(const double weight, const std::shared_ptr<TProcessCategory>& cat, const std::shared_ptr<TWPCategorySignals>& counters)
        : Category(cat)
        , Counters(counters)
        , Weight(weight)
    {
        counters->ValueWeight->Set(weight);
        AFL_VERIFY(Counters);
        AFL_VERIFY(cat);
        AFL_VERIFY(Weight);
    }
};

class TWorkersPool {
private:
    class TWorkerInfo {
        YDB_READONLY(bool, RunningTask, false);
        YDB_READONLY_DEF(NActors::TActorId, WorkerId);

    public:
        explicit TWorkerInfo(std::unique_ptr<TWorker>&& worker)
            : WorkerId(TActivationContext::Register(worker.release())) {
        }

        void OnStartTask() {
            AFL_VERIFY(!RunningTask);
            RunningTask = true;
        }

        void OnStopTask() {
            AFL_VERIFY(RunningTask);
            RunningTask = false;
        }
    };

    struct TWorkersUpdateState {
        ui32 DesiredWorkersCount = 0;
        THashSet<ui32> WorkersWaitingForLimitUpdate;
        THashSet<ui32> WorkersWaitingForStop;

        bool IsFinished() const {
            return WorkersWaitingForLimitUpdate.empty() && WorkersWaitingForStop.empty();
        }
    };

    YDB_READONLY(ui32, WorkersCount, 0);
    YDB_READONLY(double, MaxWorkerThreads, 0);
    YDB_READONLY(double, AmountCPULimit, 0);
    std::vector<TWeightedCategory> Processes;
    std::vector<TWorkerInfo> Workers;
    std::vector<ui32> ActiveWorkersIdx;
    std::shared_ptr<TWorkersPoolCounters> Counters;
    TAverageCalcer<TDuration> DeliveringDuration;
    std::deque<TDuration> DeliveryDurations;
    ui64 MaxBatchSize = 30;
    const TString PoolName;
    const NActors::TActorId DistributorId;
    const ui64 WorkersPoolId;
    std::optional<TWorkersUpdateState> WorkersUpdate;

    void RemoveFreeWorker(const ui32 workerIdx);
    void UpdateWorkerCPULimit(const ui32 workerIdx, const double newLimit);
    void IncreaseWorkers(const std::vector<double>& desiredCPULimits);
    void DecreaseWorkers(const std::vector<double>& desiredCPULimits);
    bool TryFinishWorkersUpdate();

public:
    static constexpr double Eps = 1e-6;
    using TPtr = std::shared_ptr<TWorkersPool>;

    TWorkersPool(const TString& poolName, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
        const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories);

    const std::shared_ptr<TWorkersPoolCounters>& GetCounters() const {
        return Counters;
    }

    bool HasTasks() const {
        for (auto&& i : Processes) {
            if (i.GetCategory()->HasTasks()) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool DrainTasks();

    void AddDeliveryDuration(const TDuration d) {
        DeliveringDuration.Add(d);
    }

    void PutTaskResults(std::vector<TWorkerTaskResult>&& result, const ui64 workersPoolId = 0, const ui64 workerIdx = 0);
    bool HasFreeWorker() const;
    void RunTask(std::vector<TWorkerTask>&& tasksBatch);
    void ReleaseWorker(const ui32 workerIdx);

    bool StartWorkersUpdate(const std::vector<double>& desiredCPULimits);
    bool OnWorkerCPULimitUpdated(const TEvInternal::TEvWorkerCPULimitUpdated& ev);
    bool OnWorkerStopped(const TEvInternal::TEvWorkerStopped& ev);

    bool HasWorkersUpdateInProgress() const {
        return WorkersUpdate.has_value();
    }
};

}   // namespace NKikimr::NConveyorComposite
