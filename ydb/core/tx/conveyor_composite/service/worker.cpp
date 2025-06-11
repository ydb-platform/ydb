#include "worker.h"

namespace NKikimr::NConveyorComposite {

TDuration TWorker::GetWakeupDuration() const {
    AFL_VERIFY(ExecutionDuration);
    return (*ExecutionDuration) * (1 - CPUSoftLimit) / CPUSoftLimit;
}

void TWorker::ExecuteTask(std::vector<TWorkerTask>&& workerTasks) {
    AFL_VERIFY(!ExecutionDuration && Results.empty());
    std::vector<TWorkerTaskResult> results;
    results.reserve(workerTasks.size());
    const TMonotonic startGlobal = TMonotonic::Now();
    for (auto&& t : workerTasks) {
        const TMonotonic start = TMonotonic::Now();
        t.GetTask()->Execute(t.GetTaskSignals(), t.GetTask());
        results.emplace_back(t.GetResult(start, TMonotonic::Now()));
    }
    if (CPUSoftLimit < 1) {
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_wait_result")("id", SelfId())("count", workerTasks.size());
        ExecutionDuration = TMonotonic::Now() - startGlobal;
        Results = std::move(results);
        Schedule(GetWakeupDuration(), new NActors::TEvents::TEvWakeup(CPULimitGeneration));
        WaitWakeUp = true;
    } else {
        AFL_VERIFY(!!ForwardDuration);
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_result")("id", SelfId())("count", Results.size())("d", TMonotonic::Now() - startGlobal);
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(results), *ForwardDuration, WorkerIdx, WorkersPoolId).SendTo(DistributorId);
        ForwardDuration.reset();
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& ev) {
    const auto evGeneration = ev->Get()->Tag;
    AFL_VERIFY(evGeneration <= CPULimitGeneration);
    if (evGeneration == CPULimitGeneration) {
        OnWakeup();
    }
}

void TWorker::OnWakeup() {
    AFL_VERIFY(ExecutionDuration);
    AFL_VERIFY(Results.size());
    AFL_VERIFY(!!ForwardDuration);
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "wake_up")("id", SelfId())("count", Results.size());
    TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(Results), *ForwardDuration, WorkerIdx, WorkersPoolId).SendTo(DistributorId);
    ForwardDuration.reset();
    Results.clear();
    ExecutionDuration.reset();

    WaitWakeUp = false;
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    AFL_VERIFY(!WaitWakeUp);
    const TMonotonic now = TMonotonic::Now();
    ForwardDuration = now - ev->Get()->GetConstructInstant();
    ExecuteTask(ev->Get()->ExtractTasks());
}

}
