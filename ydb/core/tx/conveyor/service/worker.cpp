#include "worker.h"

namespace NKikimr::NConveyor {

void TWorker::ExecuteTask(const TWorkerTask& workerTask) {
    std::optional<TMonotonic> start;
    if (CPUUsage < 1) {
        start = TMonotonic::Now();
    }
    if (workerTask.GetTask()->Execute(workerTask.GetTaskSignals())) {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(workerTask, workerTask.GetTask()).SendTo(DistributorId);
    } else {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(workerTask, workerTask.GetTask()->GetErrorMessage()).SendTo(DistributorId);
    }
    if (CPUUsage < 1) {
        Schedule((TMonotonic::Now() - *start) * (1 - CPUUsage), new NActors::TEvents::TEvWakeup);
        WaitWakeUp = true;
    }
}

void TWorker::HandleMain(NActors::TEvents::TEvWakeup::TPtr& /*ev*/) {
    WaitWakeUp = false;
    if (WaitTask) {
        ExecuteTask(*WaitTask);
        WaitTask.reset();
    }
}

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    if (!WaitWakeUp) {
        ExecuteTask(ev->Get()->GetTask());
    } else {
        WaitTask = ev->Get()->GetTask();
    }
}

}
