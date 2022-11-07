#include "task_executor.h"
#include "interrupt.h"
#include "finish_task.h"

namespace NKikimr::NBackgroundTasks {

void TTaskExecutor::Handle(TEvTaskFinished::TPtr& /*ev*/) {
    Register(new TDropTaskActor(Task.GetId(), OwnerController));
    Task.Finished();
    PassAway();
}

void TTaskExecutor::Handle(TEvTaskInterrupted::TPtr& ev) {
    auto nextStartInstant = Task.GetScheduler().GetNextStartInstant(Task.GetStartInstant(), ev->Get()->GetTaskState());
    if (nextStartInstant) {
        Register(new TInterruptTaskActor(OwnerController, Task.GetId(),
            *nextStartInstant, ev->Get()->GetTaskState()));
    } else {
        Register(new TDropTaskActor(Task.GetId(), OwnerController));
        Task.Finished();
    }
    PassAway();
}

void TTaskExecutor::Bootstrap() {
    Become(&TTaskExecutor::StateMain);
    Controller = std::make_shared<TTaskExecutorController>(SelfId(), Task.GetId(), OwnerController->GetRequestConfig());
    Task.Execute(Controller);
}

}
