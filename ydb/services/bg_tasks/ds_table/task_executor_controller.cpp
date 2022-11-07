#include "task_executor.h"
#include "task_executor_controller.h"
#include <ydb/services/bg_tasks/abstract/task.h>

namespace NKikimr::NBackgroundTasks {

void TTaskExecutorController::DoTaskInterrupted(ITaskState::TPtr actualTask) {
    TaskExecutorId.Send(TaskExecutorId, new TEvTaskInterrupted(actualTask));
}

void TTaskExecutorController::DoTaskFinished() {
    TaskExecutorId.Send(TaskExecutorId, new TEvTaskFinished());
}

}
