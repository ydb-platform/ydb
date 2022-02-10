#include "utils.h"

namespace NYq {

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvWriteResultDataRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    TRequestCountersPtr requestCounters = Counters.Requests[RT_WRITE_RESULT_DATA];
    requestCounters->InFly->Inc();

    TEvControlPlaneStorage::TEvWriteResultDataRequest& request = *ev->Get();
    const TString resultId = request.ResultId;
    const int32_t resultSetId = request.ResultSetId;
    const int64_t startRowId = request.StartRowId;
    const TInstant deadline = request.Deadline; 
    const Ydb::ResultSet& resultSet = request.ResultSet;
    const int byteSize = resultSet.ByteSize();

    CPS_LOG_T("WriteResultDataRequest: " << resultId << " " << resultSetId << " " << startRowId << " " << resultSet.ByteSize() << " " << deadline);

    NYql::TIssues issues = ValidateWriteResultData(resultId, resultSet, deadline, Config.ResultSetsTtl);
    if (issues) {
        CPS_LOG_D("WriteResultDataRequest, validation failed: " << resultId << " " << resultSetId << " " << startRowId << " " << resultSet.DebugString() << " " << deadline << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvWriteResultDataResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(WriteResultDataRequest, resultId, resultSetId, startRowId, resultSet.rows().size(), delta, deadline, byteSize, false);
        return;
    }

    NYdb::TValueBuilder itemsAsList;
    itemsAsList.BeginList();

    int64_t rowId = startRowId;
    for (const auto& row : resultSet.rows()) {
        TString serializedRow;
        if (!row.SerializeToString(&serializedRow)) {
            ythrow TControlPlaneStorageException(TIssuesIds::INTERNAL_ERROR) << "Error serialize proto message for row. Please contact internal support";
        }

        itemsAsList.AddListItem()
            .BeginStruct()
            .AddMember("row_id").Int64(rowId)
            .AddMember("result_set").String(row.SerializeAsString()) 
            .EndStruct();
        rowId++;
    }

    itemsAsList.EndList();

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "TaskResultWrite");
    queryBuilder.AddString("result_id", resultId); 
    queryBuilder.AddInt32("result_set_id", resultSetId); 
    queryBuilder.AddTimestamp("expire_at", deadline); 
    queryBuilder.AddValue("items", itemsAsList.Build()); 

    queryBuilder.AddText( 
        "UPSERT INTO `" RESULT_SETS_TABLE_NAME "`\n"
        "SELECT $result_id as result_id, $result_set_id as result_set_id,\n"
        " T.*, $expire_at as expire_at FROM as_table($items) AS T;\n" 
    ); 

    const auto query = queryBuilder.Build(); 
    auto debugInfo = Config.Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{}; 
    TAsyncStatus result = Write(NActors::TActivationContext::ActorSystem(), query.Sql, query.Params, requestCounters, debugInfo); 
    auto prepare = []() { return std::make_tuple<NYql::TIssues>(NYql::TIssues{}); };
    auto success = SendResponseTuple<TEvControlPlaneStorage::TEvWriteResultDataResponse, std::tuple<NYql::TIssues>>(
        "WriteResultDataRequest",
        NActors::TActivationContext::ActorSystem(),
        result,
        SelfId(),
        ev,
        startTime,
        requestCounters,
        prepare,
        debugInfo);

    success.Apply([=](const auto& future) {
            TDuration delta = TInstant::Now() - startTime;
            LWPROBE(WriteResultDataRequest, resultId, resultSetId, startRowId, resultSet.rows().size(), delta, deadline, byteSize, future.GetValue());
        });
}

} // NYq
