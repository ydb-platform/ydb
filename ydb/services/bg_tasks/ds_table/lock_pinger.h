#pragma once
#include "executor_controller.h"

#include <ydb/services/bg_tasks/abstract/state.h>
#include <ydb/services/metadata/request/request_actor.h>

namespace NKikimr::NBackgroundTasks {

class TLockPingerActor: public NMetadata::NRequest::TSessionedActor {
private:
    using TBase = NMetadata::NRequest::TSessionedActor;
    const std::set<TString> TaskIds;
    TExecutorController::TPtr ExecutorController;
protected:
    virtual void OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& result) override;
    virtual std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> OnSessionId(const TString& sessionId) override;
public:
    TLockPingerActor(TExecutorController::TPtr executorController, const std::set<TString>& taskIds)
        : TBase(executorController->GetRequestConfig(), executorController->GetUserToken())
        , TaskIds(taskIds)
        , ExecutorController(executorController) {
        Y_ABORT_UNLESS(TaskIds.size());
    }
};
}
