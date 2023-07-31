#include "worker.h"

namespace NKikimr::NConveyor {

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    auto& workerTask = ev->Get()->GetTask();
    if (workerTask.GetTask()->Execute(workerTask.GetTaskSignals())) {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(workerTask, workerTask.GetTask()).SendTo(ev->Sender);
    } else {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(workerTask, workerTask.GetTask()->GetErrorMessage()).SendTo(ev->Sender);
    }
}

}
