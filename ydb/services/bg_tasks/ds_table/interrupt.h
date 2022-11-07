#pragma once
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/state.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TInterruptTaskActor: public NInternal::NRequest::TSessionedActor {
private:
    using TBase = NInternal::NRequest::TSessionedActor;
    TExecutorController::TPtr ExecutorController;
    const TString TaskId;
    const TInstant NextStartInstant;
    TTaskStateContainer State;
protected:
    virtual void OnResult(const NInternal::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NInternal::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogYQLRequest>, Handle);
            default:
                TBase::StateMain(ev, ctx);
        }
    }

    TInterruptTaskActor(TExecutorController::TPtr executorController,
        const TString& taskId, const TInstant nextStartInstant, TTaskStateContainer state)
        : TBase(executorController->GetRequestConfig())
        , ExecutorController(executorController)
        , TaskId(taskId)
        , NextStartInstant(nextStartInstant)
        , State(state)
    {

    }
};
}
