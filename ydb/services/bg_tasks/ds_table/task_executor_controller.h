#pragma once
#include <ydb/services/metadata/request/config.h>
#include <ydb/services/bg_tasks/abstract/task.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NBackgroundTasks {

class TTaskExecutorController: public ITaskExecutorController {
private:
    const NActors::TActorIdentity TaskExecutorId;
    YDB_READONLY_DEF(TString, TaskId);
    YDB_READONLY_DEF(NInternal::NRequest::TConfig, RequestConfig);
protected:
    virtual void DoTaskInterrupted(ITaskState::TPtr actualTask) override;
    virtual void DoTaskFinished() override;
public:
    TTaskExecutorController(const NActors::TActorIdentity& executorId,
        const TString& taskId, const NInternal::NRequest::TConfig& requestConfig)
        : TaskExecutorId(executorId)
        , TaskId(taskId)
        , RequestConfig(requestConfig) {

    }

};

}
