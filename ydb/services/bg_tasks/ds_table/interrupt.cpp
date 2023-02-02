#include "interrupt.h"

namespace NKikimr::NBackgroundTasks {

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TInterruptTaskActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $taskId AS Utf8;" << Endl;
    sb << "DECLARE $stateString AS String;" << Endl;
    sb << "DECLARE $startInstant AS Uint32;" << Endl;
    sb << "UPDATE `" + ExecutorController->GetTableName() + "`" << Endl;
    sb << "SET lastPing = 0" << Endl;
    sb << ", executorId = null" << Endl;
    sb << ", startInstant = $startInstant" << Endl;
    sb << ", state = $stateString" << Endl;
    sb << "WHERE id = $taskId" << Endl;
    request.mutable_query()->set_yql_text(sb);

    {
        auto& param = (*request.mutable_parameters())["$startInstant"];
        param.mutable_value()->set_uint32_value(NextStartInstant.Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }

    auto& idString = (*request.mutable_parameters())["$taskId"];
    idString.mutable_value()->set_text_value(TaskId);
    idString.mutable_type()->set_type_id(Ydb::Type::UTF8);

    auto& sString = (*request.mutable_parameters())["$stateString"];
    sString.mutable_value()->set_bytes_value(State.SerializeToString());
    sString.mutable_type()->set_type_id(Ydb::Type::STRING);

    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TInterruptTaskActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& /*result*/) {
    ExecutorController->OnTaskFinished(TaskId);
}

}
