#include "workers_pool.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>

#include <algorithm>
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

bool FillBatchForWorker(std::vector<TWeightedCategory>& procLocal, const ui32 workerIdx, const ui64 maxBatchSize,
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
}

TWorkersPool::TWorkersPool(const TString& poolName, const NActors::TActorId& distributorId, const NConfig::TWorkersPool& config,
    const std::shared_ptr<TWorkersPoolCounters>& counters, const std::vector<std::shared_ptr<TProcessCategory>>& categories)
    : WorkersCount(config.GetWorkersCountInfo().GetThreadsCount(NKqp::TStagePredictor::GetPossibleMaxLimitThreads()))
    , Counters(counters)
    , MaxBatchSize(config.GetMaxBatchSize())
    , HeavyLimits(config.GetHeavyLimits()) {
    Workers.reserve(WorkersCount);
    for (auto&& i : config.GetLinks()) {
        AFL_VERIFY((ui64)i.GetCategory() < categories.size());
        Processes.emplace_back(TWeightedCategory(i.GetWeight(), categories[(ui64)i.GetCategory()], Counters->GetCategorySignals(i.GetCategory())));
    }
    AFL_VERIFY(Processes.size());
    for (ui32 i = 0; i < WorkersCount; ++i) {
        Workers.emplace_back(std::make_unique<TWorker>(
            poolName, config.GetWorkerCPUUsage(i, NKqp::TStagePredictor::GetPossibleMaxLimitThreads()), distributorId, i, config.GetWorkersPoolId()));
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

void TWorkersPool::RunTask(std::vector<TWorkerTask>&& tasksBatch, const ui32 workerIdx) {
    auto it = std::find(ActiveWorkersIdx.begin(), ActiveWorkersIdx.end(), workerIdx);
    AFL_VERIFY(it != ActiveWorkersIdx.end())("worker_idx", workerIdx)("active", ActiveWorkersIdx.size());
    *it = ActiveWorkersIdx.back();
    ActiveWorkersIdx.pop_back();
    Counters->AvailableWorkersCount->Set(ActiveWorkersIdx.size());

    AFL_VERIFY(workerIdx < Workers.size());
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

bool TWorkersPool::DrainOnWorkers(const std::vector<ui32>& workerIdxs) {
    if (workerIdxs.empty()) {
        return false;
    }
    // Per-call copy: popping a category here does not hide it from later bands.
    std::vector<TWeightedCategory> procLocal = Processes;
    AFL_VERIFY(procLocal.size());
    std::make_heap(procLocal.begin(), procLocal.end(), CategoryHeapLess);
    bool newTask = false;
    ui32 nextWorker = 0;
    while (nextWorker < workerIdxs.size() && procLocal.size() && procLocal.front().GetCategory()->HasTasks()) {
        const ui32 workerIdx = workerIdxs[nextWorker];
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
    if (ActiveWorkersIdx.empty()) {
        return false;
    }
    if (HeavyLimits.empty()) {
        // Keep the historical drain: pop from ActiveWorkersIdx.back(), and treat "attempted" as
        // success so AFL_VERIFY(HasTasks() => DrainTasks()) still holds when CheckToRun() skips.
        std::make_heap(Processes.begin(), Processes.end(), CategoryHeapLess);
        std::vector<TWeightedCategory> procLocal = Processes;
        AFL_VERIFY(procLocal.size());
        bool newTask = false;
        while (ActiveWorkersIdx.size() && procLocal.size() && procLocal.front().GetCategory()->HasTasks()) {
            std::vector<TWorkerTask> tasks;
            newTask = true;
            if (FillBatchForWorker(procLocal, ActiveWorkersIdx.back(), MaxBatchSize, DeliveringDuration.GetValue(), HeavyLimits, tasks)) {
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

    ui32 prevLower = Max<ui32>();
    bool newTask = false;
    for (const auto& limit : HeavyLimits) {
        std::vector<ui32> band;
        for (const ui32 idx : ActiveWorkersIdx) {
            if (idx >= limit.GetThreadLimit() && idx < prevLower) {
                band.emplace_back(idx);
            }
        }
        newTask = DrainOnWorkers(band) || newTask;
        prevLower = limit.GetThreadLimit();
    }
    {
        std::vector<ui32> band;
        for (const ui32 idx : ActiveWorkersIdx) {
            if (idx < prevLower) {
                band.emplace_back(idx);
            }
        }
        newTask = DrainOnWorkers(band) || newTask;
    }
    for (auto&& i : Processes) {
        if (!i.GetCategory()->HasTasks()) {
            i.GetCounters()->NoTasks->Add(1);
        }
    }
    return newTask;
}

void TWorkersPool::PutTaskResults(std::vector<TWorkerTaskResult>&& result, const ui64 workersPoolId, const ui64 workerIdx) {
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
        if (!found) {
            TStringBuilder linkedCategories;
            for (auto&& i : Processes) {
                linkedCategories << (ui64)i.GetCategory()->GetCategory() << "(" << ::ToString(i.GetCategory()->GetCategory()) << "),";
            }
            AFL_VERIFY(false)("result_category", (ui64)t.GetCategory())("result_category_name", ::ToString(t.GetCategory()))(
                "process_id", t.GetProcessId())("batch_size", result.size())("linked_categories", linkedCategories)(
                "processes_count", Processes.size())("workers_pool_id", workersPoolId)("worker_idx", workerIdx)(
                "workers_count", WorkersCount);
        }
    }
}

}   // namespace NKikimr::NConveyorComposite
