#include "executor.h"
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/metadata/initializer/events.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NBackgroundTasks {

void TExecutorController::OnAssignFinished() const {
    ExecutorActorId.Send(ExecutorActorId, new TEvAssignFinished());
}

void TExecutorController::OnFetchingFinished() const {
    ExecutorActorId.Send(ExecutorActorId, new TEvFetchingFinished());
}

void TExecutorController::OnTaskFetched(const TTask& task) const {
    ExecutorActorId.Send(ExecutorActorId, new TEvTaskFetched(task));
}

void TExecutorController::OnTaskFinished(const TString& taskId) const {
    ExecutorActorId.Send(ExecutorActorId, new TEvTaskExecutorFinished(taskId));
}

void TExecutorController::OnInitializationFinished(const TString& id) const {
    ExecutorActorId.Send(ExecutorActorId, new NMetadata::NInitializer::TEvInitializationFinished(id));
}

void TExecutorController::OnLockPingerFinished() const {
    ExecutorActorId.Send(ExecutorActorId, new TEvLockPingerFinished());
}

}
