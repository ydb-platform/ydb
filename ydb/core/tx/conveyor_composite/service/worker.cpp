#include "worker.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_CONVEYOR

namespace NKikimr::NConveyorComposite {

TDuration TWorker::GetWakeupDuration() const {
    AFL_VERIFY(ExecutionDuration);
    return (*ExecutionDuration) * (1 - CPULimit) / CPULimit;
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
    if (CPULimit < 1) {
        YDB_LOG_DEBUG("",
            {"action", "to_wait_result"},
            {"id", SelfId()},
            {"count", workerTasks.size()});
        ExecutionDuration = TMonotonic::Now() - startGlobal;
        Results = std::move(results);
        Schedule(GetWakeupDuration(), new NActors::TEvents::TEvWakeup());
        WaitWakeUp = true;
    } else {
        AFL_VERIFY(!!ForwardDuration);
        YDB_LOG_DEBUG("",
            {"action", "to_result"},
            {"id", SelfId()},
            {"count", results.size()},
            {"d", TMonotonic::Now() - startGlobal});
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(results), *ForwardDuration, WorkerIdx, WorkersPoolId).SendTo(DistributorId);
        ForwardDuration.reset();
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    OnWakeup();
}

void TWorker::OnWakeup() {
    AFL_VERIFY(ExecutionDuration);
    AFL_VERIFY(Results.size());
    AFL_VERIFY(!!ForwardDuration);
    YDB_LOG_DEBUG("",
        {"action", "wake_up"},
        {"id", SelfId()},
        {"count", Results.size()});
    TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(Results), *ForwardDuration, WorkerIdx, WorkersPoolId).SendTo(DistributorId);
    ForwardDuration.reset();
    Results.clear();
    ExecutionDuration.reset();

    WaitWakeUp = false;
    if (StopRequested) {
        Stop();
    }
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    AFL_VERIFY(!WaitWakeUp);
    AFL_VERIFY(!StopRequested);
    const TMonotonic now = TMonotonic::Now();
    ForwardDuration = now - ev->Get()->GetConstructInstant();
    ExecuteTask(ev->Get()->ExtractTasks());
}

void TWorker::HandleMain(TEvInternal::TEvUpdateWorkerCPULimit::TPtr& ev) {
    const double newLimit = ev->Get()->NewLimit;
    AFL_VERIFY(0 < newLimit && newLimit <= 1)("new_limit", newLimit);
    CPULimit = newLimit;
    Send(DistributorId, new TEvInternal::TEvWorkerCPULimitUpdated(WorkersPoolId, WorkerIdx));
}

void TWorker::HandleMain(TEvInternal::TEvRetireWorker::TPtr& /*ev*/) {
    if (StopRequested) {
        return;
    }

    StopRequested = true;
    if (!WaitWakeUp) {
        Stop();
    }
}

void TWorker::Stop() {
    AFL_VERIFY(StopRequested);
    AFL_VERIFY(!WaitWakeUp);
    AFL_VERIFY(!ExecutionDuration);
    AFL_VERIFY(Results.empty());
    AFL_VERIFY(!ForwardDuration);

    Send(DistributorId, new TEvInternal::TEvWorkerStopped(WorkersPoolId, WorkerIdx));
    PassAway();
}

}
