#pragma once
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/state.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TUpdateTaskEnabledActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    TExecutorController::TPtr ExecutorController;
    const TString TaskId;
    const bool Enabled = false;
    const TActorId ResultWaiter;
protected:
    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    TUpdateTaskEnabledActor(TExecutorController::TPtr executorController,
        const TString& taskId, const bool enabled, const TActorId& resultWaiter)
        : TBase(executorController->GetRequestConfig(), executorController->GetUserToken())
        , ExecutorController(executorController)
        , TaskId(taskId)
        , Enabled(enabled)
        , ResultWaiter(resultWaiter)
    {

    }
};
}
