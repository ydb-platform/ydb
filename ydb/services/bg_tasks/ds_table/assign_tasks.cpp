#include "assign_tasks.h"

namespace NKikimr::NBackgroundTasks {

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TAssignTasksActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    const auto now = TActivationContext::Now();
    sb << "DECLARE $executorId AS Utf8;" << Endl;
    sb << "DECLARE $lastPingCriticalBorder AS Uint32;" << Endl;
    sb << "DECLARE $lastPingNewValue AS Uint32;" << Endl;
    sb << "$ids = (SELECT id FROM `" << Controller->GetTableName() << "`"
        << " WHERE (lastPing < $lastPingCriticalBorder"
        << " OR executorId IS NULL) AND enabled = true"
        << " LIMIT " << TasksCount << ");" << Endl;
    sb << "UPSERT INTO `" + Controller->GetTableName() + "`"
        << " SELECT id, $executorId as executorId, $lastPingNewValue as lastPing"
        << " FROM $ids";
    {
        auto& param = (*request.mutable_parameters())["$lastPingCriticalBorder"];
        param.mutable_value()->set_uint32_value((now - Controller->GetConfig().GetPingCheckPeriod()).Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }
    {
        auto& param = (*request.mutable_parameters())["$lastPingNewValue"];
        param.mutable_value()->set_uint32_value(now.Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }
    {
        auto& param = (*request.mutable_parameters())["$executorId"];
        param.mutable_value()->set_text_value(ExecutorId);
        param.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    request.mutable_query()->set_yql_text(sb);
    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);
    return request;
}

void TAssignTasksActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& /*result*/) {
    Controller->OnAssignFinished();
}

}
