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
        YDB_READONLY(TWorker*, Worker, nullptr);
        YDB_READONLY_DEF(NActors::TActorId, WorkerId);

    public:
        explicit TWorkerInfo(std::unique_ptr<TWorker>&& worker)
            : Worker(worker.get())
            , WorkerId(TActivationContext::Register(worker.release())) {
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

    YDB_READONLY(ui32, WorkersCount, 0);
    YDB_READONLY(double, MaxWorkerThreads, 0);
    YDB_READONLY(double, AmountCPULimit, 0);
    std::vector<TWeightedCategory> Processes;
    std::vector<TWorkerInfo> Workers;
    std::vector<ui32> ActiveWorkersIdx;
    std::shared_ptr<TWorkersPoolCounters> Counters;
    TAverageCalcer<TDuration> DeliveringDuration;
    std::deque<TDuration> DeliveryDurations;

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

    void PutTaskResults(std::vector<TWorkerTaskResult>&& result);
    bool HasFreeWorker() const;
    void RunTask(std::vector<TWorkerTask>&& tasksBatch);
    void ReleaseWorker(const ui32 workerIdx);
};

}   // namespace NKikimr::NConveyorComposite
