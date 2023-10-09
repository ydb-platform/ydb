#include "fetch_tasks.h"

#include <ydb/services/bg_tasks/abstract/task.h>

namespace NKikimr::NBackgroundTasks {

void TFetchTasksActor::OnResult(const NMetadata::NRequest::TDialogYQLRequest::TResponse& currentFullReply) {
    Ydb::Table::ExecuteQueryResult qResult;
    currentFullReply.operation().result().UnpackTo(&qResult);
    Y_ABORT_UNLESS((size_t)qResult.result_sets().size() == 1);
    TTask::TDecoder decoder(qResult.result_sets()[0]);
    std::vector<TTask> newTasks;
    for (auto&& i : qResult.result_sets()[0].rows()) {
        TTask task;
        if (!task.DeserializeFromRecord(decoder, i)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task record";
            continue;
        }
        Controller->OnTaskFetched(task);
    }
    Controller->OnFetchingFinished();
}

std::optional<NMetadata::NRequest::TDialogYQLRequest::TRequest> TFetchTasksActor::OnSessionId(const TString& sessionId) {
    Ydb::Table::ExecuteDataQueryRequest request;
    TStringBuilder sb;
    sb << "DECLARE $executorId AS Utf8;" << Endl;
    sb << "DECLARE $lastPingCriticalBorder AS Uint32;" << Endl;
    if (CurrentTaskIds.size()) {
        sb << "DECLARE $taskIds AS List<Utf8>;" << Endl;
    }
    sb << "SELECT * FROM `" + Controller->GetTableName() + "`" << Endl;
    sb << "WHERE executorId = $executorId" << Endl;
    sb << "AND enabled = true" << Endl;
    sb << "AND lastPing > $lastPingCriticalBorder" << Endl;
    if (CurrentTaskIds.size()) {
        sb << " AND id NOT IN $taskIds" << Endl;
        auto& idStrings = (*request.mutable_parameters())["$taskIds"];
        idStrings.mutable_type()->mutable_list_type()->mutable_item()->set_type_id(Ydb::Type::UTF8); 
        for (auto&& i : CurrentTaskIds) {
            auto* idString = idStrings.mutable_value()->add_items();
            idString->set_text_value(i);
        }
    }

    {
        auto& param = (*request.mutable_parameters())["$executorId"];
        param.mutable_value()->set_text_value(ExecutorId);
        param.mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    {
        auto& param = (*request.mutable_parameters())["$lastPingCriticalBorder"];
        param.mutable_value()->set_uint32_value((TActivationContext::Now() - Controller->GetConfig().GetPingCheckPeriod()).Seconds());
        param.mutable_type()->set_type_id(Ydb::Type::UINT32);
    }
    request.mutable_query()->set_yql_text(sb);
    request.set_session_id(sessionId);
    request.mutable_tx_control()->mutable_begin_tx()->mutable_snapshot_read_only();

    return request;
}

}
