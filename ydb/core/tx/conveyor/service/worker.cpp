#include "worker.h"

namespace NKikimr::NConveyor {

void TWorker::HandleMain(TEvInternal::TEvNewTask::TPtr& ev) {
    auto& workerTask = ev->Get()->GetTask();
    if (workerTask.GetTask()->Execute()) {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(workerTask.GetOwnerId(), workerTask.GetTask()).SendTo(ev->Sender);
    } else {
        TBase::Sender<TEvInternal::TEvTaskProcessedResult>(workerTask.GetOwnerId(), "cannot execute task").SendTo(ev->Sender);
    }
}

}
