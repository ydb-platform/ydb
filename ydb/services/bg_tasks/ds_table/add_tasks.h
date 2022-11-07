#pragma once
#include "executor_controller.h"
#include <ydb/services/bg_tasks/abstract/task.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TAddTasksActor: public NInternal::NRequest::TSessionedActor {
private:
    using TBase = NInternal::NRequest::TSessionedActor;
    TExecutorController::TPtr Controller;
    const TTask Task;
    const TActorId ResultWaiter;
protected:
    virtual void OnResult(const NInternal::NRequest::TDialogYQLRequest::TResponse& ev) override;
    virtual std::optional<NInternal::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    TAddTasksActor(TExecutorController::TPtr controller, const TTask& task, const TActorId resultWaiter)
        : TBase(controller->GetRequestConfig())
        , Controller(controller)
        , Task(task)
        , ResultWaiter(resultWaiter)
    {
    }
};
}
