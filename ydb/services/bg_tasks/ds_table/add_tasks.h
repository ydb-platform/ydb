#pragma once
#include "executor_controller.h"
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TAddTasksActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    TExecutorController::TPtr Controller;
    const TTask Task;
    const TActorId ResultWaiter;
protected:
    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& ev) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    TAddTasksActor(TExecutorController::TPtr controller, const TTask& task, const TActorId resultWaiter)
        : TBase(controller->GetRequestConfig(), controller->GetUserToken())
        , Controller(controller)
        , Task(task)
        , ResultWaiter(resultWaiter)
    {
    }
};
}
