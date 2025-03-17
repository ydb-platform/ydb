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
        ProcessIds = std::move(processes);
        Instants = std::move(instants);
        Schedule((Instants.back() - Instants.front()) * (1 - CPUUsage) / CPUUsage, new NActors::TEvents::TEvWakeup);
        WaitWakeUp = true;
    } else {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(instants), std::move(processes)).SendTo(DistributorId);
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    AFL_VERIFY(ProcessIds.size());
    TBase::Sender<TEvInternal::TEvTaskProcessedResult>(std::move(Instants), std::move(ProcessIds)).SendTo(DistributorId);
    ProcessIds.clear();
    Instants.clear();

    WaitWakeUp = false;
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    AFL_VERIFY(!WaitWakeUp);
    ExecuteTask(ev->Get()->ExtractTasks());
}

}
