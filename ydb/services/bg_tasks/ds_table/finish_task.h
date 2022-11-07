#pragma once
#include "executor_controller.h"

#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TDropTaskActor: public NInternal::NRequest::TSessionedActor {
private:
    using TBase = NInternal::NRequest::TSessionedActor;
    const TString TaskId;
    TExecutorController::TPtr Controller;
protected:
    virtual void OnResult(const NInternal::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NInternal::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:

    TDropTaskActor(const TString& taskId, TExecutorController::TPtr controller)
        : TBase(controller->GetRequestConfig())
        , TaskId(taskId)
        , Controller(controller) {

    }
};
}
