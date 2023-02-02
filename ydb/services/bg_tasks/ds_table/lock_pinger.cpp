#include "lock_pinger.h"

#include <ydb/services/bg_tasks/service.h>

namespace NKikimr::NBackgroundTasks {

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TLockPingerActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    const auto now = TActivationContext::Now();
    sb << "DECLARE $taskIds AS List<Utf8>;" << Endl;
    sb << "DECLARE $lastPingNewValue AS Uint32;" << Endl;
    sb << "UPDATE `" + ExecutorController->GetTableName() + "`" << Endl;
    sb << "SET lastPing = $lastPingNewValue" << Endl;
    sb << "WHERE id IN $taskIds" << Endl;
    request.mutable_query()->set_yql_text(sb);

    {
        auto& param = (*request.mutable_parameters())["$lastPingNewValue"];
        param.mutable_value()->set_uint32_value(now.Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }

    auto& idStrings = (*request.mutable_parameters())["$taskIds"];
    idStrings.mutable_type()->mutable_list_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    for (auto&& i : TaskIds) {
        idStrings.mutable_value()->add_items()->set_text_value(i);
    }

    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TLockPingerActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& /*ev*/) {
    ExecutorController->OnLockPingerFinished();
}

}
