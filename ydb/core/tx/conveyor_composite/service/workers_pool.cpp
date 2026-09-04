#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

#include <algorithm>
#include <cmath>
#include <numeric>

namespace NKikimr::NConveyorComposite {
TWorkersPool::TWorkersPool(const TString& poolName, const ui64 workersPoolId, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
    const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories)
    : WorkersCount(config.GetWorkersCountInfo().GetThreadsCount(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , MaxWorkerThreads(config.GetWorkersCountInfo().GetCPUUsageDouble(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , Counters(counters)
    , MaxBatchSize(config.GetMaxBatchSize())
    , PoolName(poolName)
    , DistributorId(distributorId)
    , WorkersPoolId(workersPoolId) {
    Workers.reserve(WorkersCount);
    for (auto&& i : config.GetLinks()) {
        Y_ENSURE((ui64)i.GetCategory() < categories.size(), "worker pool category index is out of range: " << (ui64)i.GetCategory());
        CategoryLinks.emplace_back(i.GetWeight(), categories[(ui64)i.GetCategory()], Counters->GetCategorySignals(i.GetCategory()));
    }
    for (ui64 i = 0; i < WorkersCount; ++i) {
        const double cpuLimit = config.GetWorkerCPUUsage(i, NKqp::TStagePredictor::GetPossibleMaxLimitThreads());
        Workers.emplace_back(
            std::make_unique<TWorker>(poolName, cpuLimit, distributorId, i, workersPoolId), cpuLimit);
        ActiveWorkersIdx.emplace_back(i);
    }
    Y_ENSURE(WorkersCount, "worker pool has no workers: " << poolName);
    Counters->AmountCPULimit->Set(0);
    Counters->AvailableWorkersCount->Set(0);
    Counters->WorkersCountLimit->Set(WorkersCount);
}

void TWorkersPool::RemoveFreeWorker(const ui64 workerIdx) {
    const auto it = std::find(ActiveWorkersIdx.begin(), ActiveWorkersIdx.end(), workerIdx);
    Y_ENSURE(it != ActiveWorkersIdx.end(), "free worker is missing: " << workerIdx);
    ActiveWorkersIdx.erase(it);
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

void TWorkersPool::UpdateWorkerCPULimit(const ui64 workerIdx, const double newLimit) {
    Y_ENSURE(workerIdx < Workers.size(), "worker CPU limit update index is out of range: " << workerIdx);
    auto& worker = Workers[workerIdx];
    if (std::abs(worker.GetCPULimit() - newLimit) < Eps) {
        return;
    }
    worker.SetCPULimit(newLimit);
}

void TWorkersPool::IncreaseWorkers(const std::vector<double>& desiredCPULimits) {
    Y_ENSURE(WorkersUpdate, "workers increase outside of config update");

    const ui64 oldWorkersCount = Workers.size();
    Y_ENSURE(oldWorkersCount < desiredCPULimits.size(), "workers increase has no additional workers");

    UpdateWorkerCPULimit(oldWorkersCount - 1, desiredCPULimits[oldWorkersCount - 1]);
    for (ui64 workerIdx = oldWorkersCount; workerIdx < desiredCPULimits.size(); ++workerIdx) {
        Workers.emplace_back(
            std::make_unique<TWorker>(PoolName, desiredCPULimits[workerIdx], DistributorId, workerIdx, WorkersPoolId), desiredCPULimits[workerIdx]);
        ActiveWorkersIdx.emplace_back(workerIdx);
    }
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

void TWorkersPool::DecreaseWorkers(const std::vector<double>& desiredCPULimits) {
    // first decrease worker phase
    Y_ENSURE(WorkersUpdate, "workers decrease outside of config update");

    const ui64 oldWorkersCount = Workers.size();
    Y_ENSURE(desiredCPULimits.size() < oldWorkersCount, "workers decrease has no removed workers");

    UpdateWorkerCPULimit(desiredCPULimits.size() - 1, desiredCPULimits[desiredCPULimits.size() - 1]);
    for (ui64 workerIdx = desiredCPULimits.size(); workerIdx < oldWorkersCount; ++workerIdx) {
        auto& worker = Workers[workerIdx];
        worker.RequestStop();
        Y_ENSURE(WorkersUpdate->WorkersWaitingForRelease.emplace(workerIdx).second,
            "worker is already waiting for release: " << workerIdx);
    }
    for (ui64 workerIdx = desiredCPULimits.size(); workerIdx < oldWorkersCount; ++workerIdx) {
        auto& worker = Workers[workerIdx];
        if (!worker.GetRunningTask()) {
            RemoveFreeWorker(workerIdx);
            Y_UNUSED(ReleaseWorker(workerIdx));
        }
    }
}

bool TWorkersPool::StartWorkersUpdate(const std::vector<double>& desiredCPULimits) {
    Y_ENSURE(desiredCPULimits.size(), "workers update requires at least one worker");

    Y_ENSURE(!WorkersUpdate, "another workers update is already in progress");
    WorkersUpdate.emplace();
    WorkersUpdate->DesiredWorkersCount = desiredCPULimits.size();

    if (Workers.size() < desiredCPULimits.size()) {
        IncreaseWorkers(desiredCPULimits);
    } else if (desiredCPULimits.size() < Workers.size()) {
        DecreaseWorkers(desiredCPULimits);
    } else {
        UpdateWorkerCPULimit(Workers.size() - 1, desiredCPULimits.back());
    }
    return !WorkersUpdate || TryFinishWorkersUpdate();
}

bool TWorkersPool::StartWorkersRetirement() {
    Y_ENSURE(!WorkersUpdate, "workers retirement during another workers update");
    WorkersUpdate.emplace();
    WorkersUpdate->DesiredWorkersCount = 0;
    for (ui64 workerIdx = 0; workerIdx < Workers.size(); ++workerIdx) {
        auto& worker = Workers[workerIdx];
        worker.RequestStop();
        Y_ENSURE(WorkersUpdate->WorkersWaitingForRelease.emplace(workerIdx).second,
            "worker is already waiting for release: " << workerIdx);
    }
    for (ui64 workerIdx = 0; workerIdx < Workers.size(); ++workerIdx) {
        auto& worker = Workers[workerIdx];
        if (!worker.GetRunningTask()) {
            RemoveFreeWorker(workerIdx);
            Y_UNUSED(ReleaseWorker(workerIdx));
        }
    }
    return !WorkersUpdate || TryFinishWorkersUpdate();
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

bool TWorkersPool::HasFreeWorker() const {
    return !ActiveWorkersIdx.empty();
}

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch, TTaskCompletionContexts&& completionContexts) {
    Y_ENSURE(HasFreeWorker(), "cannot run a task without a free worker");
    Y_ENSURE(tasksBatch.size(), "cannot run an empty task batch");
    const auto workerIdx = ActiveWorkersIdx.back();
    ActiveWorkersIdx.pop_back();
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());

    auto& worker = Workers[workerIdx];
    worker.OnStartTask(std::move(completionContexts));
    TActivationContext::Send(
        worker.GetWorkerId(), std::make_unique<TEvInternal::TEvNewTask>(std::move(tasksBatch), worker.GetCPULimit()));
}

bool TWorkersPool::ReleaseWorker(const ui64 workerIdx) {
    Y_ENSURE(workerIdx < Workers.size(), "released worker index is out of range: " << workerIdx);
    auto& worker = Workers[workerIdx];
    if (worker.GetRunningTask()) {
        worker.OnStopTask();
    }
    if (worker.GetStopRequested()) {
        Y_ENSURE(WorkersUpdate, "worker stop completion outside of config update");
        Y_ENSURE(WorkersUpdate->WorkersWaitingForRelease.erase(workerIdx),
            "worker is not waiting for release: " << workerIdx);
        TActivationContext::Send(worker.GetWorkerId(), std::make_unique<NActors::TEvents::TEvPoisonPill>());
        return TryFinishWorkersUpdate();
    }
    ActiveWorkersIdx.emplace_back(workerIdx);
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
    return false;
}

bool TWorkersPool::DrainTasks() {
    if (ActiveWorkersIdx.empty() || CategoryLinks.empty()) {
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
    std::make_heap(CategoryLinks.begin(), CategoryLinks.end(), predHeap);
    std::vector<TWeightedCategory> procLocal = CategoryLinks;
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
    for (auto&& i : CategoryLinks) {
        if (!i.GetCategory()->HasTasks()) {
            i.GetCounters()->NoTasks->Add(1);
        }
    }
    return newTask;
}

void TWorkersPool::PutTaskResults(std::vector<TWorkerTaskResult>&& result, const ui64 workersPoolId, const ui64 workerIdx) {
    Y_ENSURE(workerIdx < Workers.size(),
        "task result worker index is out of range: pool=" << workersPoolId << ", worker=" << workerIdx);
    const auto& worker = Workers[workerIdx];
    Y_ENSURE(worker.GetRunningTask(), "task result received from an idle worker: " << workerIdx);

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
    std::vector<TWeightedCategory> oldProcesses = std::move(CategoryLinks);
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
            Y_ENSURE((ui64)category < categories.size(), "worker pool category index is out of range: " << (ui64)category);
            newProcesses.emplace_back(
                linkConfig.GetWeight(), categories[(ui64)category], Counters->GetCategorySignals(category));
        }
    }
    for (auto& process : oldProcesses) {
        process.GetCounters()->ValueWeight->Set(0);
    }
    CategoryLinks = std::move(newProcesses);
}

void TWorkersPool::ClearTopology() {
    for (auto& process : CategoryLinks) {
        process.GetCounters()->ValueWeight->Set(0);
    }
    CategoryLinks.clear();
}

}   // namespace NKikimr::NConveyorComposite
