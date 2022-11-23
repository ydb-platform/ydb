#include "executor.h"
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/task.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NBackgroundTasks {

void TExecutorController::AssignFinished() const {
    ExecutorActorId.Send(ExecutorActorId, new TEvAssignFinished());
}

void TExecutorController::FetchingFinished() const {
    ExecutorActorId.Send(ExecutorActorId, new TEvFetchingFinished());
}

void TExecutorController::TaskFetched(const TTask& task) const {
    ExecutorActorId.Send(ExecutorActorId, new TEvTaskFetched(task));
}

void TExecutorController::TaskFinished(const TString& taskId) const {
    ExecutorActorId.Send(ExecutorActorId, new TEvTaskExecutorFinished(taskId));
}

void TExecutorController::InitializationFinished(const TString& id) const {
    ExecutorActorId.Send(ExecutorActorId, new NMetadataInitializer::TEvInitializationFinished(id));
}

void TExecutorController::LockPingerFinished() const {
    ExecutorActorId.Send(ExecutorActorId, new TEvLockPingerFinished());
}

}
