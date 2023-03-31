#include "task_enabled.h"

#include <ydb/services/bg_tasks/service.h>

namespace NKikimr::NBackgroundTasks {

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TUpdateTaskEnabledActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $taskId AS Utf8;" << Endl;
    sb << "UPDATE `" + ExecutorController->GetTableName() + "`" << Endl;
    sb << "SET enabled = " << Enabled << Endl;
    sb << "WHERE id = $taskId" << Endl;
    request.mutable_query()->set_yql_text(sb);

    auto& idString = (*request.mutable_parameters())["$taskId"];
    idString.mutable_value()->set_text_value(TaskId);
    idString.mutable_type()->set_type_id(Ydb::Type::UTF8);

    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TUpdateTaskEnabledActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& /*result*/) {
    Sender<TEvUpdateTaskEnabledResult>(TaskId, true).SendTo(ResultWaiter);
}

}
