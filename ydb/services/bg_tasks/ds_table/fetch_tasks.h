#pragma once
#include "executor_controller.h"

#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TFetchTasksActor: public NInternal::NRequest::TSessionedActor {
private:
    using TBase = NInternal::NRequest::TSessionedActor;
    const std::set<TString> CurrentTaskIds;
    const TString ExecutorId;
    TExecutorController::TPtr Controller;
protected:
    virtual void OnResult(const NInternal::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NInternal::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;

public:
    TFetchTasksActor(const std::set<TString>& currentTaskIds, const TString& executorId,
        TExecutorController::TPtr controller)
        : TBase(controller->GetRequestConfig())
        , CurrentTaskIds(currentTaskIds)
        , ExecutorId(executorId)
        , Controller(controller)
    {

    }
};
}
