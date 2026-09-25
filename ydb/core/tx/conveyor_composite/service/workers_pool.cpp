#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <util/generic/ylimits.h>

namespace NKikimr::NConveyorComposite {

namespace {
bool CategoryHeapLess(const TWeightedCategory& l, const TWeightedCategory& r) {
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
}

bool FillBatchForWorker(std::vector<TWeightedCategory>& procLocal, const ui64 workerIdx, const ui64 maxBatchSize,
    const TDuration deliveringDuration, const std::vector<NConfig::THeavyLimit>& heavyLimits, std::vector<TWorkerTask>& tasks) {
    TDuration predicted = TDuration::Zero();
    THashSet<TString> scopes;
    while (procLocal.size() && (tasks.empty() || (predicted < deliveringDuration * 10 && tasks.size() < maxBatchSize)) &&
           procLocal.front().GetCategory()->HasTasks()) {
        std::pop_heap(procLocal.begin(), procLocal.end(), CategoryHeapLess);
        auto task = procLocal.back().GetCategory()->ExtractTaskWithPrediction(
            procLocal.back().GetCounters(), scopes, workerIdx, heavyLimits);
        if (!task) {
            // Drop from this DrainOnWorkers copy only. Processes stay queued;
            // the next band starts with a fresh heap (see DrainTasks).
            procLocal.pop_back();
            continue;
        }
        tasks.emplace_back(std::move(*task));
        procLocal.back().GetCPUUsage()->AddPredicted(tasks.back().GetPredictedDuration());
        predicted += tasks.back().GetPredictedDuration();
        std::push_heap(procLocal.begin(), procLocal.end(), CategoryHeapLess);
    }
    return !tasks.empty();
}

std::vector<TWeightedCategory> CopyActiveCategoryLinks(const std::vector<TWeightedCategory>& categoryLinks) {
    std::vector<TWeightedCategory> procLocal;
    procLocal.reserve(categoryLinks.size());
    for (const auto& link : categoryLinks) {
        if (!link.GetStopPrepare()) {
            procLocal.emplace_back(link);
        }
    }
    return procLocal;
}
}

void TWeightedCategory::SetWeight(const double weight) {
    Y_ENSURE(std::isfinite(weight) && weight > 0, "invalid worker pool category weight: " << weight);
    Weight = weight;
    Counters->ValueWeight->Set(weight);
}

void TWorkersPool::TWorkerInfo::OnStartTask() {
    Y_ENSURE(!RunningTask, "worker already has a running task");
    Y_ENSURE(!StopPrepare, "cannot assign a task to a worker prepared for removal");
    RunningTask = true;
}

void TWorkersPool::TWorkerInfo::OnStopTask() {
    Y_ENSURE(RunningTask, "worker has no running task to stop");
    RunningTask = false;
}

TWorkersPool::TWorkersPool(const TString& poolName, const ui64 workersPoolId, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
    const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories)
    : WorkersCount(config.GetWorkersCountInfo().GetThreadsCount(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , MaxWorkerThreads(config.GetWorkersCountInfo().GetCPUUsageDouble(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , Counters(counters)
    , MaxBatchSize(config.GetMaxBatchSize())
    , HeavyLimits(config.GetHeavyLimits())
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
    const ui64 oldWorkersCount = Workers.size();
    Y_ENSURE(oldWorkersCount < desiredCPULimits.size(), "workers increase has no additional workers");

    for (ui64 workerIdx = oldWorkersCount; workerIdx < desiredCPULimits.size(); ++workerIdx) {
        Workers.emplace_back(
            std::make_unique<TWorker>(PoolName, desiredCPULimits[workerIdx], DistributorId, workerIdx, WorkersPoolId), desiredCPULimits[workerIdx]);
        ActiveWorkersIdx.emplace_back(workerIdx);
    }
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

void TWorkersPool::DecreaseWorkers(const std::vector<double>& desiredCPULimits) {
    const ui64 oldWorkersCount = Workers.size();
    Y_ENSURE(desiredCPULimits.size() < oldWorkersCount, "workers decrease has no removed workers");

    for (ui64 workerIdx = desiredCPULimits.size(); workerIdx < oldWorkersCount; ++workerIdx) {
        const auto& worker = Workers[workerIdx];
        Y_ENSURE(worker.GetStopPrepare() && !worker.GetRunningTask(),
            "worker is not prepared for removal: " << workerIdx);
        TActivationContext::Send(worker.GetWorkerId(), std::make_unique<NActors::TEvents::TEvPoisonPill>());
    }
    while (Workers.size() > desiredCPULimits.size()) {
        Workers.pop_back();
    }
}

void TWorkersPool::ApplyWorkersUpdate(const std::vector<double>& desiredCPULimits) {
    Y_ENSURE(IsReadyForUpdate(), "pool is not prepared for config update");

    if (Workers.size() < desiredCPULimits.size()) {
        IncreaseWorkers(desiredCPULimits);
    } else if (desiredCPULimits.size() < Workers.size()) {
        DecreaseWorkers(desiredCPULimits);
    }
    for (ui64 workerIdx = 0; workerIdx < Workers.size(); ++workerIdx) {
        Y_ENSURE(!Workers[workerIdx].GetStopPrepare(), "retained worker is prepared for removal");
        UpdateWorkerCPULimit(workerIdx, desiredCPULimits[workerIdx]);
    }

    WorkersCount = Workers.size();
    MaxWorkerThreads = std::accumulate(Workers.begin(), Workers.end(), 0.0,
        [](const double sum, const TWorkerInfo& worker) {
            return sum + worker.GetCPULimit();
        });
    Counters->WorkersCountLimit->Set(WorkersCount);
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

void TWorkersPool::PrepareConfigUpdate(const NConfig::TWorkersPool* target) {
    for (auto& link : CategoryLinks) {
        const bool retained = target && std::any_of(target->GetLinks().begin(), target->GetLinks().end(),
            [&](const auto& desiredLink) {
                return desiredLink.GetCategory() == link.GetCategory()->GetCategory();
            });
        link.SetStopPrepare(!retained);
    }

    const ui64 desiredWorkersCount = target ? target->GetWorkersCount(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()) : 0;
    for (ui64 workerIdx = 0; workerIdx < Workers.size(); ++workerIdx) {
        auto& worker = Workers[workerIdx];
        const bool stopPrepare = workerIdx >= desiredWorkersCount;
        if (worker.GetStopPrepare() == stopPrepare) {
            continue;
        }
        worker.SetStopPrepare(stopPrepare);
        if (!worker.GetRunningTask()) {
            if (stopPrepare) {
                RemoveFreeWorker(workerIdx);
            } else {
                ActiveWorkersIdx.emplace_back(workerIdx);
            }
        }
    }
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

bool TWorkersPool::IsReadyForUpdate() const {
    for (const auto& link : CategoryLinks) {
        if (link.GetStopPrepare() && link.GetInFlightTasks()) {
            return false;
        }
    }
    for (const auto& worker : Workers) {
        if (worker.GetStopPrepare() && worker.GetRunningTask()) {
            return false;
        }
    }
    return true;
}

bool TWorkersPool::HasFreeWorker() const {
    return !ActiveWorkersIdx.empty();
}

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch) {
    Y_ENSURE(HasFreeWorker(), "cannot run a task without a free worker");
    RunTask(std::move(tasksBatch), ActiveWorkersIdx.back());
}

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch, const ui64 workerIdx) {
    Y_ENSURE(tasksBatch.size(), "cannot run an empty task batch");
    const auto it = std::find(ActiveWorkersIdx.begin(), ActiveWorkersIdx.end(), workerIdx);
    Y_ENSURE(it != ActiveWorkersIdx.end(), "cannot run a task on an inactive worker: " << workerIdx);
    *it = ActiveWorkersIdx.back();
    ActiveWorkersIdx.pop_back();
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());

    Y_ENSURE(workerIdx < Workers.size(), "worker index is out of range: " << workerIdx);
    auto& worker = Workers[workerIdx];
    worker.OnStartTask();
    for (const auto& task : tasksBatch) {
        auto& link = FindCategoryLink(task.GetCategory());
        Y_ENSURE(!link.GetStopPrepare(), "cannot assign a task to a link prepared for removal");
        link.OnTaskStarted();
    }
    TActivationContext::Send(
        worker.GetWorkerId(), std::make_unique<TEvInternal::TEvNewTask>(std::move(tasksBatch), worker.GetCPULimit()));
}

void TWorkersPool::ReleaseWorker(const ui64 workerIdx) {
    Y_ENSURE(workerIdx < Workers.size(), "released worker index is out of range: " << workerIdx);
    auto& worker = Workers[workerIdx];
    worker.OnStopTask();
    if (!worker.GetStopPrepare()) {
        ActiveWorkersIdx.emplace_back(workerIdx);
    }
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());
}

bool TWorkersPool::DrainOnWorkers(const std::vector<ui64>& workerIdxs) {
    if (workerIdxs.empty()) {
        return false;
    }
    // Per-call copy: popping a category here does not hide it from later bands.
    std::vector<TWeightedCategory> procLocal = CopyActiveCategoryLinks(CategoryLinks);
    if (procLocal.empty()) {
        return false;
    }
    std::make_heap(procLocal.begin(), procLocal.end(), CategoryHeapLess);
    bool newTask = false;
    ui32 nextWorker = 0;
    while (nextWorker < workerIdxs.size() && procLocal.size() && procLocal.front().GetCategory()->HasTasks()) {
        const ui64 workerIdx = workerIdxs[nextWorker];
        std::vector<TWorkerTask> tasks;
        if (!FillBatchForWorker(procLocal, workerIdx, MaxBatchSize, DeliveringDuration.GetValue(), HeavyLimits, tasks)) {
            break;
        }
        RunTask(std::move(tasks), workerIdx);
        ++nextWorker;
        newTask = true;
    }
    return newTask;
}

bool TWorkersPool::DrainTasks() {
    if (ActiveWorkersIdx.empty() || CategoryLinks.empty()) {
        return false;
    }
    if (HeavyLimits.empty()) {
        // Keep the historical drain: pop from ActiveWorkersIdx.back(), and treat "attempted" as
        // success so a restricted CheckToRun() skip still counts as a drain attempt.
        std::vector<TWeightedCategory> procLocal = CopyActiveCategoryLinks(CategoryLinks);
        if (procLocal.empty()) {
            return false;
        }
        std::make_heap(procLocal.begin(), procLocal.end(), CategoryHeapLess);
        bool newTask = false;
        while (ActiveWorkersIdx.size() && procLocal.size() && procLocal.front().GetCategory()->HasTasks()) {
            std::vector<TWorkerTask> tasks;
            newTask = true;
            if (FillBatchForWorker(procLocal, ActiveWorkersIdx.back(), MaxBatchSize, DeliveringDuration.GetValue(), HeavyLimits, tasks)) {
                RunTask(std::move(tasks));
            }
        }
        for (auto&& i : CategoryLinks) {
            if (!i.GetCategory()->HasTasks()) {
                i.GetCounters()->NoTasks->Add(1);
            }
        }
        return newTask;
    }

    ui64 prevLower = Max<ui64>();
    bool newTask = false;
    for (const auto& limit : HeavyLimits) {
        std::vector<ui64> band;
        for (const ui64 idx : ActiveWorkersIdx) {
            if (idx >= limit.GetThreadLimit() && idx < prevLower) {
                band.emplace_back(idx);
            }
        }
        newTask = DrainOnWorkers(band) || newTask;
        prevLower = limit.GetThreadLimit();
    }
    {
        std::vector<ui64> band;
        for (const ui64 idx : ActiveWorkersIdx) {
            if (idx < prevLower) {
                band.emplace_back(idx);
            }
        }
        newTask = DrainOnWorkers(band) || newTask;
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
        auto& link = FindCategoryLink(t.GetCategory());
        link.GetCounters()->WaitingHistogram->Collect((t.GetStart() - t.GetCreateInstant()).MicroSeconds());
        link.GetCounters()->TaskExecuteHistogram->Collect((t.GetFinish() - t.GetStart()).MicroSeconds());
        link.GetCounters()->ExecuteDuration->Add((t.GetFinish() - t.GetStart()).MicroSeconds());
        link.GetCPUUsage()->Exchange(t.GetPredictedDuration(), t.GetStart(), t.GetFinish());
        link.GetCategory()->PutTaskResult(std::move(t), scopeIds);
        link.OnTaskFinished();
    }
}

TWeightedCategory& TWorkersPool::FindCategoryLink(const ESpecialTaskCategory category) {
    const auto it = std::find_if(CategoryLinks.begin(), CategoryLinks.end(), [&](const auto& link) {
        return link.GetCategory()->GetCategory() == category;
    });
    Y_ENSURE(it != CategoryLinks.end(), "worker pool link is missing for category " << category);
    return *it;
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
            Y_ENSURE(!oldIt->GetStopPrepare(), "retained link is prepared for removal");
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
        Y_ENSURE(process.GetStopPrepare() && !process.GetInFlightTasks(), "link is not prepared for removal");
        process.GetCounters()->ValueWeight->Set(0);
    }
    CategoryLinks = std::move(newProcesses);
    HeavyLimits = config.GetHeavyLimits();
}

void TWorkersPool::ClearTopology() {
    for (auto& process : CategoryLinks) {
        Y_ENSURE(process.GetStopPrepare() && !process.GetInFlightTasks(), "link is not prepared for removal");
        process.GetCounters()->ValueWeight->Set(0);
    }
    CategoryLinks.clear();
    HeavyLimits.clear();
}

}   // namespace NKikimr::NConveyorComposite
