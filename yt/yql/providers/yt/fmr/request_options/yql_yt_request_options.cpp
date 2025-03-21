#include "yql_yt_request_options.h"

namespace NYql::NFmr {

TTask::TPtr MakeTask(ETaskType taskType, const TString& taskId, const TTaskParams& taskParams, const TString& sessionId, const TClusterConnection& clusterConnection) {
    return MakeIntrusive<TTask>(taskType, taskId, taskParams, sessionId, clusterConnection);
}

TTaskState::TPtr MakeTaskState(ETaskStatus taskStatus, const TString& taskId, const TMaybe<TFmrError>& taskErrorMessage, const TStatistics& stats) {
    return MakeIntrusive<TTaskState>(taskStatus, taskId, taskErrorMessage, stats);
}

TString TFmrChunkMeta::ToString() const {
    return TStringBuilder() << TableId << ":" << PartId << ":" << std::to_string(Chunk);
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

template<>
void Out<NYql::NFmr::TFmrChunkMeta>(IOutputStream& out, const NYql::NFmr::TFmrChunkMeta& meta) {
    out << meta.ToString();
}
