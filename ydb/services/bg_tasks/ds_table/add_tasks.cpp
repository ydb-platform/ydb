#include "add_tasks.h"

#include <ydb/services/bg_tasks/service.h>

namespace NKikimr::NBackgroundTasks {

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TAddTasksActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $activityString AS String;" << Endl;
    sb << "DECLARE $taskId AS Utf8;" << Endl;
    sb << "DECLARE $schedulerString AS String;" << Endl;
    sb << "DECLARE $enabled AS Bool;" << Endl;
    sb << "DECLARE $className AS Utf8;" << Endl;
    sb << "DECLARE $startInstant AS Uint32;" << Endl;
    sb << "DECLARE $constructInstant AS Uint32;" << Endl;
    sb << "UPSERT INTO `" + Controller->GetTableName() + "` (id, enabled, class, startInstant, constructInstant, activity, scheduler)" << Endl;
    sb << "VALUES" << Endl;
    {
        sb << "(" << Endl;
        sb << "$taskId," << Endl;
        sb << "$enabled," << Endl;
        sb << "$className," << Endl;
        sb << "$startInstant," << Endl;
        sb << "$constructInstant," << Endl;
        sb << "$activityString," << Endl;
        sb << "$schedulerString" << Endl;
        sb << ")" << Endl;
    }
    request.mutable_query()->set_yql_text(sb);

    {
        auto& param = (*request.mutable_parameters())["$enabled"];
        param.mutable_value()->set_bool_value(Task.IsEnabled());
        param.mutable_type()->set_type_id(Ydb::Type::BOOL);
    }

    {
        auto& param = (*request.mutable_parameters())["$constructInstant"];
        param.mutable_value()->set_uint32_value(Task.GetConstructInstant().Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }

    {
        auto& param = (*request.mutable_parameters())["$startInstant"];
        param.mutable_value()->set_uint32_value(Task.GetScheduler().GetStartInstant().Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }

    {
        auto& param = (*request.mutable_parameters())["$className"];
        param.mutable_value()->set_text_value(Task.GetClass());
        param.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }

    auto& idString = (*request.mutable_parameters())["$taskId"];
    idString.mutable_value()->set_text_value(Task.GetId());
    idString.mutable_type()->set_type_id(Ydb::Type::UTF8);

    auto& aString = (*request.mutable_parameters())["$activityString"];
    aString.mutable_value()->set_bytes_value(Task.GetActivity().SerializeToString());
    aString.mutable_type()->set_type_id(Ydb::Type::STRING);

    auto& sString = (*request.mutable_parameters())["$schedulerString"];
    sString.mutable_value()->set_bytes_value(Task.GetScheduler().SerializeToString());
    sString.mutable_type()->set_type_id(Ydb::Type::STRING);

    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
    request.mutable_tx_control()->set_commit_tx(true);

    return request;
}

void TAddTasksActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& /*ev*/) {
    Sender<TEvAddTaskResult>(Task.GetId(), true).SendTo(ResultWaiter);
}

}
