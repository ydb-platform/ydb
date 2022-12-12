#pragma once
#include "executor_controller.h"

#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TAssignTasksActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    TExecutorController::TPtr Controller;
    const ui32 TasksCount;
    const TString ExecutorId;

    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;
public:
    TAssignTasksActor(const ui32 tasksCount, TExecutorController::TPtr controller, const TString& executorId)
        : TBase(controller->GetRequestConfig(), controller->GetUserToken())
        , Controller(controller)
        , TasksCount(tasksCount)
        , ExecutorId(executorId)
    {

    }
};
}
