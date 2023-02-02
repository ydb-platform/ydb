#include "finish_task.h"

namespace NKikimr::NBackgroundTasks {

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TDropTaskActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $taskId AS Utf8;" << Endl;
    sb << "DELETE FROM `" + Controller->GetTableName() + "` ON SELECT $taskId AS id";
    request.mutable_query()->set_yql_text(sb);

    auto& idString = (*request.mutable_parameters())["$taskId"];
    idString.mutable_value()->set_text_value(TaskId);
    idString.mutable_type()->set_type_id(Ydb::Type::UTF8);

    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TDropTaskActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& /*result*/) {
    Controller->OnTaskFinished(TaskId);
}

}
