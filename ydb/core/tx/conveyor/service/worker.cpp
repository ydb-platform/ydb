#include "worker.h"

namespace NKikimr::NConveyor {

void TWorker::ExecuteTask(std::vector<TWorkerTask>&& workerTasks) {
    AFL_VERIFY(ProcessIds.empty() && Instants.empty());
    std::vector<TMonotonic> instants;
    std::vector<ui64> processes;
    instants.emplace_back(TMonotonic::Now());
    for (auto&& t : workerTasks) {
        Y_UNUSED(t.GetTask()->Execute(t.GetTaskSignals(), t.GetTask()));
        instants.emplace_back(TMonotonic::Now());
        processes.emplace_back(t.GetProcessId());
    }
    if (CPUUsage < 1) {
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_wait_result")("id", SelfId())("count", workerTasks.size());
        ProcessIds = std::move(processes);
        Instants = std::move(instants);
        Schedule((Instants.back() - Instants.front()) * (1 - CPUUsage) / CPUUsage, new NActors::TEvents::TEvWakeup);
        WaitWakeUp = true;
    } else {
        AFL_VERIFY(!!ForwardDuration);
        AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "to_result")("id", SelfId())("count", workerTasks.size())("d", TMonotonic::Now() - instants.front());
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(instants), std::move(processes), *ForwardDuration).SendTo(DistributorId);
        ForwardDuration.reset();
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "wake_up")("id", SelfId())("count", ProcessIds.size());
    AFL_VERIFY(ProcessIds.size());
    AFL_VERIFY(!!ForwardDuration);
    TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(Instants), std::move(ProcessIds), *ForwardDuration).SendTo(DistributorId);
    ForwardDuration.reset();
    ProcessIds.clear();
    Instants.clear();

    WaitWakeUp = false;
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    AFL_VERIFY(!WaitWakeUp);
    const TMonotonic now = TMonotonic::Now();
    ForwardDuration = now - ev->Get()->GetConstructInstant();
    SendFwdHistogram->Collect(ForwardDuration->MicroSeconds());
    SendFwdDuration->Add(ForwardDuration->MicroSeconds());
    ExecuteTask(ev->Get()->ExtractTasks());
}

}
