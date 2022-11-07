#include "task_enabled.h"

#include <ydb/services/bg_tasks/service.h>

namespace NKikimr::NBackgroundTasks {

std::optional<NInternal::NRequest::TDialogYQLRequest::TRequest> TUpdateTaskEnabledActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $taskId AS String;" << Endl;
    sb << "UPDATE `" + ExecutorController->GetTableName() + "`" << Endl;
    sb << "SET enabled = " << Enabled << Endl;
    sb << "WHERE id = $taskId" << Endl;
    request.mutable_query()->set_yql_text(sb);

    auto& idString = (*request.mutable_parameters())["$taskId"];
    idString.mutable_value()->set_bytes_value(TaskId);
    idString.mutable_type()->set_type_id(Ydb::Type::STRING);

    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TUpdateTaskEnabledActor::OnResult(const NInternal::NRequest::TDialogYQLRequest::TResponse& /*result*/) {
    Sender<TEvUpdateTaskEnabledResult>(TaskId, true).SendTo(ResultWaiter);
}

}
