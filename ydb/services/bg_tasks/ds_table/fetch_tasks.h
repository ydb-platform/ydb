#pragma once
#include "executor_controller.h"

#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TFetchTasksActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    const std::set<TString> CurrentTaskIds;
    const TString ExecutorId;
    TExecutorController::TPtr Controller;
protected:
    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    TFetchTasksActor(const std::set<TString>& currentTaskIds, const TString& executorId,
        TExecutorController::TPtr controller)
        : TBase(controller->GetRequestConfig(), controller->GetUserToken())
        , CurrentTaskIds(currentTaskIds)
        , ExecutorId(executorId)
        , Controller(controller)
    {

    }
};
}
