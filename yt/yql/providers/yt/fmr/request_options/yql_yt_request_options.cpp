#include "yql_yt_request_options.h"

namespace NYql::NFmr {

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId) {
    return MakeIntrusive<TTask>(taskType, taskId, taskParams, sessionId);
}

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage) {
    return MakeIntrusive<TTaskState>(taskStatus, taskId, taskErrorMessage);
}

TTaskResult::TPtr MakeTaskResult(ETaskStatus taskStatus, const TMaybe<TFmrError>& taskErrorMessage) {
    return MakeIntrusive<TTaskResult>(taskStatus, taskErrorMessage);
}

} // namespace NYql::NFmr

template<>
void Out<NYql::NFmr::TFmrError>(IOutputStream& out, const NYql::NFmr::TFmrError& error) {
    out << "FmrError[" << error.Component << "]";
    if (error.Component == NYql::NFmr::EFmrComponent::Worker) {
        out << "(TaskId: " << error.TaskId << " WorkerId: " << error.WorkerId << ") ";
    } else if (error.Component == NYql::NFmr::EFmrComponent::Coordinator) {
        out << "(OperationId: " << error.OperationId <<") ";
    }
    out << error.ErrorMessage;
}
