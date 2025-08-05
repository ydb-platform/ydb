#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NConveyorComposite {
TWorkersPool::TWorkersPool(const TString& poolName, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
    const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories)
    : WorkersCount(config.GetWorkersCountInfo().GetThreadsCount(NKqp::TStagePredictor::GetUsableThreads()))
    , Counters(counters) {
    Workers.reserve(WorkersCount);
    for (auto&& i : config.GetLinks()) {
        AFL_VERIFY((ui64)i.GetCategory() < categories.size());
        Processes.emplace_back(TWeightedCategory(i.GetWeight(), categories[(ui64)i.GetCategory()], Counters->GetCategorySignals(i.GetCategory())));
    }
    AFL_VERIFY(Processes.size());
    for (ui32 i = 0; i < WorkersCount; ++i) {
        Workers.emplace_back(std::make_unique<TWorker>(
            poolName, config.GetWorkerCPUUsage(i, NKqp::TStagePredictor::GetUsableThreads()), distributorId, i, config.GetWorkersPoolId()));
        ActiveWorkersIdx.emplace_back(i);
    }
    AFL_VERIFY(WorkersCount)("name", poolName)("action", "conveyor_registered")("config", config.DebugString())("actor_id", distributorId)(
        "count", WorkersCount);
    Counters->AmountCPULimit->Set(0);
    Counters->AvailableWorkersCount->Set(0);
    Counters->WorkersCountLimit->Set(WorkersCount);
}

bool TWorkersPool::HasFreeWorker() const {
    return !ActiveWorkersIdx.empty();
}

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch) {
    AFL_VERIFY(HasFreeWorker());
    const auto workerIdx = ActiveWorkersIdx.back();
    ActiveWorkersIdx.pop_back();
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());

    auto& worker = Workers[workerIdx];
    worker.OnStartTask();
    TActivationContext::Send(worker.GetWorkerId(), std::make_unique<TEvInternal::TEvNewTask>(std::move(tasksBatch)));
}

void TWorkersPool::ReleaseWorker(const ui32 workerIdx) {
    AFL_VERIFY(workerIdx < Workers.size());
    Workers[workerIdx].OnStopTask();
    ActiveWorkersIdx.emplace_back(workerIdx);
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

bool TWorkersPool::DrainTasks() {
    if (ActiveWorkersIdx.empty()) {
        return false;
    }
    const auto predHeap = [](const TWeightedCategory& l, const TWeightedCategory& r) {
        const bool hasL = l.GetCategory()->HasTasks();
        const bool hasR = r.GetCategory()->HasTasks();
        if (!hasL && !hasR) {
            return false;
        } else if (!hasL && hasR) {
            return true;
        } else if (hasL && !hasR) {
            return false;
        }
        return r.GetCPUUsage()->CalcWeight(r.GetWeight()) < l.GetCPUUsage()->CalcWeight(l.GetWeight());
    };
    std::make_heap(Processes.begin(), Processes.end(), predHeap);
    std::vector<TWeightedCategory> procLocal = Processes;
    AFL_VERIFY(procLocal.size());
    bool newTask = false;
    while (ActiveWorkersIdx.size() && procLocal.size() && procLocal.front().GetCategory()->HasTasks()) {
        TDuration predicted = TDuration::Zero();
        std::vector<TWorkerTask> tasks;
        THashSet<TString> scopes;
        while (procLocal.size() && (tasks.empty() || predicted < DeliveringDuration.GetValue() * 10) &&
               procLocal.front().GetCategory()->HasTasks()) {
            std::pop_heap(procLocal.begin(), procLocal.end(), predHeap);
            auto task = procLocal.back().GetCategory()->ExtractTaskWithPrediction(procLocal.back().GetCounters(), scopes);
            if (!task) {
                procLocal.pop_back();
                continue;
            }
            tasks.emplace_back(std::move(*task));
            procLocal.back().GetCPUUsage()->AddPredicted(tasks.back().GetPredictedDuration());
            predicted += tasks.back().GetPredictedDuration();
            std::push_heap(procLocal.begin(), procLocal.end(), predHeap);
        }
        newTask = true;
        if (tasks.size()) {
            RunTask(std::move(tasks));
        }
    }
    for (auto&& i : Processes) {
        if (!i.GetCategory()->HasTasks()) {
            i.GetCounters()->NoTasks->Add(1);
        }
    }
    return newTask;
}

void TWorkersPool::PutTaskResults(std::vector<TWorkerTaskResult>&& result) {
    //        const ui32 catIdx = (ui32)result.GetCategory();
    THashSet<TString> scopeIds;
    for (auto&& t : result) {
        bool found = false;
        for (auto&& i : Processes) {
            if (i.GetCategory()->GetCategory() == t.GetCategory()) {
                i.GetCounters()->WaitingHistogram->Collect((t.GetStart() - t.GetCreateInstant()).MicroSeconds());
                i.GetCounters()->TaskExecuteHistogram->Collect((t.GetFinish() - t.GetStart()).MicroSeconds());
                i.GetCounters()->ExecuteDuration->Add((t.GetFinish() - t.GetStart()).MicroSeconds());
                found = true;
                i.GetCPUUsage()->Exchange(t.GetPredictedDuration(), t.GetStart(), t.GetFinish());
                i.GetCategory()->PutTaskResult(std::move(t), scopeIds);
                break;
            }
        }
        AFL_VERIFY(found);
    }
}

}   // namespace NKikimr::NConveyorComposite
