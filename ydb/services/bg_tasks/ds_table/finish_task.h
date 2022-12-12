#pragma once
#include "executor_controller.h"

#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TDropTaskActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    const TString TaskId;
    TExecutorController::TPtr Controller;
protected:
    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:

    TDropTaskActor(const TString& taskId, TExecutorController::TPtr controller)
        : TBase(controller->GetRequestConfig(), controller->GetUserToken())
        , TaskId(taskId)
        , Controller(controller) {

    }
};
}
