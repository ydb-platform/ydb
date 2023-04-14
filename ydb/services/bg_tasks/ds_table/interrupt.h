#pragma once
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/state.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TInterruptTaskActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    TExecutorController::TPtr ExecutorController;
    const TString TaskId;
    const TInstant NextStartInstant;
    TTaskStateContainer State;
protected:
    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>, Handle);
            default:
                TBase::StateMain(ev);
        }
    }

    TInterruptTaskActor(TExecutorController::TPtr executorController,
        const TString& taskId, const TInstant nextStartInstant, TTaskStateContainer state)
        : TBase(executorController->GetRequestConfig(), executorController->GetUserToken())
        , ExecutorController(executorController)
        , TaskId(taskId)
        , NextStartInstant(nextStartInstant)
        , State(state)
    {

    }
};
}
