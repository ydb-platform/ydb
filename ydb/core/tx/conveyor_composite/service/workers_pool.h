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
    YDB_READONLY(double, Weight, 1);

public:
    TWeightedCategory(const double weight, const std::shared_ptr<TProcessCategory>& cat)
        : Category(cat)
        , Weight(weight)
    {
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
    TCounters Counters;
    TAverageCalcer<TDuration> DeliveringDuration;
    std::deque<TDuration> DeliveryDurations;

public:
    static constexpr double Eps = 1e-6;
    using TPtr = std::shared_ptr<TWorkersPool>;

    TWorkersPool(const TString& conveyorName, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
        const TCounters& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories);

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

    void PutTaskResult(TWorkerTaskResult&& result) {
//        const ui32 catIdx = (ui32)result.GetCategory();
        for (auto&& i : Processes) {
            if (i.GetCategory()->GetCategory() == result.GetCategory()) {
//                AFL_VERIFY(catIdx < Processes.size());
//                AFL_VERIFY(Processes[catIdx]);
                i.GetCPUUsage()->Exchange(result.GetPredictedDuration(), result.GetStart(), result.GetFinish());
                i.GetCategory()->PutTaskResult(std::move(result));
                return;
            }
        }
        AFL_VERIFY(false);
    }
    bool HasFreeWorker() const;
    void RunTask(std::vector<TWorkerTask>&& tasksBatch);
    void ReleaseWorker(const ui32 workerIdx);
};

}   // namespace NKikimr::NConveyorComposite
