#pragma once
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/common.h>
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/bg_tasks/ds_table/task_executor_controller.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NBackgroundTasks {

class TEvTaskFinished: public TEventLocal<TEvTaskFinished, EEvents::EvTaskFinished> {
};

class TEvTaskInterrupted: public TEventLocal<TEvTaskInterrupted, EEvents::EvTaskInterrupted> {
private:
    YDB_READONLY_DEF(ITaskState::TPtr, TaskState);
public:
    TEvTaskInterrupted(ITaskState::TPtr taskState)
        : TaskState(taskState)
    {

    }
};

class TTaskExecutor: public NActors::TActorBootstrapped<TTaskExecutor> {
private:
    TExecutorController::TPtr OwnerController;
    TTaskExecutorController::TPtr Controller;
    TTask Task;
public:
    TTaskExecutor(const TTask& task, TExecutorController::TPtr controller)
        : OwnerController(controller)
        , Task(task)
    {
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTaskFinished, Handle);
            hFunc(TEvTaskInterrupted, Handle);
            default:
                break;
        }
    }

    void Bootstrap();

    void Handle(TEvTaskFinished::TPtr& /*ev*/);

    void Handle(TEvTaskInterrupted::TPtr& ev);
};

}
