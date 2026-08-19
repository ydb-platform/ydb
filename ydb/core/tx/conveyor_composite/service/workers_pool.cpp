#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

#include <algorithm>
#include <cmath>
#include <numeric>

namespace NKikimr::NConveyorComposite {
TWorkersPool::TWorkersPool(const TString& poolName, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
    const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories)
    : WorkersCount(config.GetWorkersCountInfo().GetThreadsCount(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , MaxWorkerThreads(config.GetWorkersCountInfo().GetCPUUsageDouble(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , Counters(counters)
    , MaxBatchSize(config.GetMaxBatchSize())
    , PoolName(poolName)
    , DistributorId(distributorId) {
    Workers.reserve(WorkersCount);
    for (auto&& i : config.GetLinks()) {
        AFL_VERIFY((ui64)i.GetCategory() < categories.size());
        Processes.emplace_back(i.GetWeight(), categories[(ui64)i.GetCategory()], Counters->GetCategorySignals(i.GetCategory()));
    }
    for (ui64 i = 0; i < WorkersCount; ++i) {
        const double cpuLimit = config.GetWorkerCPUUsage(i, NKqp::TStagePredictor::GetPossibleMaxLimitThreads());
        Workers.emplace_back(
            std::make_unique<TWorker>(poolName, cpuLimit, distributorId, i, poolName), cpuLimit);
        ActiveWorkersIdx.emplace_back(i);
    }
    AFL_VERIFY(WorkersCount)("name", poolName)("action", "conveyor_registered")("config", config.DebugString())("actor_id", distributorId)(
        "count", WorkersCount);
    Counters->AmountCPULimit->Set(0);
    Counters->AvailableWorkersCount->Set(0);
    Counters->WorkersCountLimit->Set(WorkersCount);
}

void TWorkersPool::RemoveFreeWorker(const ui64 workerIdx) {
    ActiveWorkersIdx.erase(std::find(ActiveWorkersIdx.begin(), ActiveWorkersIdx.end(), workerIdx));
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

void TWorkersPool::UpdateWorkerCPULimit(const ui64 workerIdx, const double newLimit) {
    AFL_VERIFY(WorkersUpdate);

    auto& worker = Workers[workerIdx];
    if (std::abs(worker.GetCPULimit() - newLimit) < Eps) {
        return;
    }
    if (!worker.GetRunningTask()) {
        RemoveFreeWorker(workerIdx);
    }
    worker.SetCPULimit(newLimit);
    WorkersUpdate->WorkersWaitingForLimitUpdate.emplace(workerIdx);
    TActivationContext::Send(worker.GetWorkerId(), std::make_unique<TEvInternal::TEvUpdateWorkerCPULimit>(newLimit));
}

void TWorkersPool::IncreaseWorkers(const std::vector<double>& desiredCPULimits) {
    AFL_VERIFY(WorkersUpdate);

    const ui64 oldWorkersCount = Workers.size();
    AFL_VERIFY(oldWorkersCount < desiredCPULimits.size());

    UpdateWorkerCPULimit(oldWorkersCount - 1, desiredCPULimits[oldWorkersCount - 1]);
    for (ui64 workerIdx = oldWorkersCount; workerIdx < desiredCPULimits.size(); ++workerIdx) {
        Workers.emplace_back(
            std::make_unique<TWorker>(PoolName, desiredCPULimits[workerIdx], DistributorId, workerIdx, PoolName), desiredCPULimits[workerIdx]);
        ActiveWorkersIdx.emplace_back(workerIdx);
    }
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

void TWorkersPool::DecreaseWorkers(const std::vector<double>& desiredCPULimits) {
    // first decrease worker phase
    AFL_VERIFY(WorkersUpdate);

    const ui64 oldWorkersCount = Workers.size();
    AFL_VERIFY(desiredCPULimits.size() < oldWorkersCount);

    UpdateWorkerCPULimit(desiredCPULimits.size() - 1, desiredCPULimits[desiredCPULimits.size() - 1]);
    for (ui64 workerIdx = desiredCPULimits.size(); workerIdx < oldWorkersCount; ++workerIdx) {
        auto& worker = Workers[workerIdx];
        if (!worker.GetRunningTask()) {
            RemoveFreeWorker(workerIdx);
        }
        WorkersUpdate->WorkersWaitingForStop.emplace(workerIdx);
        TActivationContext::Send(worker.GetWorkerId(), std::make_unique<TEvInternal::TEvRetireWorker>());
    }
}

bool TWorkersPool::StartWorkersUpdate(const std::vector<double>& desiredCPULimits) {
    AFL_VERIFY(desiredCPULimits.size());

    AFL_VERIFY(!WorkersUpdate);  // Update entrypoint (another states is process continuation)
    WorkersUpdate.emplace();
    WorkersUpdate->DesiredWorkersCount = desiredCPULimits.size();

    if (Workers.size() < desiredCPULimits.size()) {
        IncreaseWorkers(desiredCPULimits);
    } else if (desiredCPULimits.size() < Workers.size()) {
        DecreaseWorkers(desiredCPULimits);
    } else {
        UpdateWorkerCPULimit(Workers.size() - 1, desiredCPULimits.back());
    }
    return TryFinishWorkersUpdate();
}

bool TWorkersPool::TryFinishWorkersUpdate() {
    if (!WorkersUpdate || !WorkersUpdate->IsFinished()) {
        return false;
    }

    // second decrease worker phase
    const ui64 desiredWorkersCount = WorkersUpdate->DesiredWorkersCount;
    if (desiredWorkersCount < Workers.size()) {
        while (Workers.size() > desiredWorkersCount) {
            Workers.pop_back();
        }
    }

    WorkersCount = desiredWorkersCount;
    MaxWorkerThreads = std::accumulate(Workers.begin(), Workers.end(), 0.0,
        [](const double sum, const TWorkerInfo& worker) {
            return sum + worker.GetCPULimit();
        });
    Counters->WorkersCountLimit->Set(WorkersCount);
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
    WorkersUpdate.reset();
    return true;
}

bool TWorkersPool::OnWorkerCPULimitUpdated(const TEvInternal::TEvWorkerCPULimitUpdated& ev) {
    AFL_VERIFY(WorkersUpdate);
    auto& worker = Workers[ev.WorkerIdx];
    WorkersUpdate->WorkersWaitingForLimitUpdate.erase(ev.WorkerIdx);
    if (!worker.GetRunningTask()) {
        ActiveWorkersIdx.emplace_back(ev.WorkerIdx);
        Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
    }
    return TryFinishWorkersUpdate();
}

bool TWorkersPool::OnWorkerStopped(const TEvInternal::TEvWorkerStopped& ev) {
    AFL_VERIFY(WorkersUpdate);
    WorkersUpdate->WorkersWaitingForStop.erase(ev.WorkerIdx);
    return TryFinishWorkersUpdate();
}

bool TWorkersPool::HasFreeWorker() const {
    return !ActiveWorkersIdx.empty();
}

bool TWorkersPool::CanExecuteCategory(const ESpecialTaskCategory category) const {
    if (!HasFreeWorker()) {
        return false;
    }
    return std::any_of(Processes.begin(), Processes.end(), [category](const TWeightedCategory& process) {
        return process.GetCategory()->GetCategory() == category;
    });
}

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch, TTaskCompletionContexts&& completionContexts) {
    AFL_VERIFY(HasFreeWorker());
    AFL_VERIFY(tasksBatch.size());
    const auto workerIdx = ActiveWorkersIdx.back();
    ActiveWorkersIdx.pop_back();
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());

    auto& worker = Workers[workerIdx];
    worker.OnStartTask(std::move(completionContexts));
    TActivationContext::Send(worker.GetWorkerId(), std::make_unique<TEvInternal::TEvNewTask>(std::move(tasksBatch)));
}

void TWorkersPool::ReleaseWorker(const ui64 workerIdx) {
    AFL_VERIFY(workerIdx < Workers.size());
    auto& worker = Workers[workerIdx];
    worker.OnStopTask();
    const bool waitingForLimitUpdate = WorkersUpdate && WorkersUpdate->WorkersWaitingForLimitUpdate.contains(workerIdx);
    const bool waitingForStop = WorkersUpdate && WorkersUpdate->WorkersWaitingForStop.contains(workerIdx);
    if (!waitingForLimitUpdate && !waitingForStop) {
        ActiveWorkersIdx.emplace_back(workerIdx);
        Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
    }
}

bool TWorkersPool::DrainTasks() {
    if (ActiveWorkersIdx.empty() || Processes.empty()) {
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
    bool newTask = false;
    while (ActiveWorkersIdx.size() && procLocal.size() && procLocal.front().GetCategory()->HasTasks()) {
        TDuration predicted = TDuration::Zero();
        std::vector<TWorkerTask> tasks;
        TTaskCompletionContexts completionContexts;
        THashSet<TString> scopes;
        while (procLocal.size() && (tasks.empty() || (predicted < DeliveringDuration.GetValue() * 10 && tasks.size() < MaxBatchSize)) &&
               procLocal.front().GetCategory()->HasTasks()) {
            std::pop_heap(procLocal.begin(), procLocal.end(), predHeap);
            auto task = procLocal.back().GetCategory()->ExtractTaskWithPrediction(procLocal.back().GetCounters(), scopes);
            if (!task) {
                procLocal.pop_back();
                continue;
            }
            completionContexts.try_emplace(procLocal.back().GetCategory()->GetCategory(), procLocal.back());
            tasks.emplace_back(std::move(*task));
            procLocal.back().GetCPUUsage()->AddPredicted(tasks.back().GetPredictedDuration());
            predicted += tasks.back().GetPredictedDuration();
            std::push_heap(procLocal.begin(), procLocal.end(), predHeap);
        }
        newTask = true;
        if (tasks.size()) {
            RunTask(std::move(tasks), std::move(completionContexts));
        }
    }
    for (auto&& i : Processes) {
        if (!i.GetCategory()->HasTasks()) {
            i.GetCounters()->NoTasks->Add(1);
        }
    }
    return newTask;
}

void TWorkersPool::PutTaskResults(std::vector<TWorkerTaskResult>&& result, const TString& workersPoolId, const ui64 workerIdx) {
    AFL_VERIFY(workersPoolId == PoolName)("workers_pool_id", workersPoolId)("pool_name", PoolName);
    AFL_VERIFY(workerIdx < Workers.size())("workers_pool_id", workersPoolId)("worker_idx", workerIdx)("workers_count", Workers.size());
    const auto& worker = Workers[workerIdx];
    AFL_VERIFY(worker.GetRunningTask())("workers_pool_id", workersPoolId)("worker_idx", workerIdx);

    THashSet<TString> scopeIds;
    for (auto&& t : result) {
        const auto& context = worker.GetCompletionContext(t.GetCategory());
        context.GetCounters()->WaitingHistogram->Collect((t.GetStart() - t.GetCreateInstant()).MicroSeconds());
        context.GetCounters()->TaskExecuteHistogram->Collect((t.GetFinish() - t.GetStart()).MicroSeconds());
        context.GetCounters()->ExecuteDuration->Add((t.GetFinish() - t.GetStart()).MicroSeconds());
        context.GetCPUUsage()->Exchange(t.GetPredictedDuration(), t.GetStart(), t.GetFinish());
        context.GetCategory()->PutTaskResult(std::move(t), scopeIds);
    }
}

void TWorkersPool::ApplyTopologyUpdate(
    const NConfig::TWorkersPool& config, const std::vector<std::shared_ptr<TProcessCategory>>& categories) {
    std::vector<TWeightedCategory> oldProcesses = std::move(Processes);
    std::vector<TWeightedCategory> newProcesses;
    newProcesses.reserve(config.GetLinks().size());

    for (const auto& linkConfig : config.GetLinks()) {
        const auto category = linkConfig.GetCategory();
        auto oldIt = std::find_if(oldProcesses.begin(), oldProcesses.end(), [&](const TWeightedCategory& process) {
            return process.GetCategory()->GetCategory() == category;
        });
        if (oldIt != oldProcesses.end()) {
            oldIt->SetWeight(linkConfig.GetWeight());
            newProcesses.emplace_back(std::move(*oldIt));
            oldProcesses.erase(oldIt);
        } else {
            AFL_VERIFY((ui64)category < categories.size());
            newProcesses.emplace_back(
                linkConfig.GetWeight(), categories[(ui64)category], Counters->GetCategorySignals(category));
        }
    }
    Processes = std::move(newProcesses);
}

}   // namespace NKikimr::NConveyorComposite
