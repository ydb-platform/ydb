#include "utils.h"

namespace NFq {

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvWriteResultDataRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    TRequestCounters requestCounters{nullptr, Counters.GetCommonCounters(RTC_WRITE_RESULT_DATA)};
    requestCounters.IncInFly();

    requestCounters.Common->RequestBytes->Add(ev->Get()->GetByteSize());
    auto& request = ev->Get()->Request;
    const TString resultId = request.result_id().value();
    const int32_t resultSetId = request.result_set_id();
    const int64_t startRowId = request.offset();
    const TInstant deadline = NProtoInterop::CastFromProto(request.deadline());
    const Ydb::ResultSet& resultSet = request.result_set();
    const int byteSize = resultSet.ByteSize();


    CPS_LOG_T("WriteResultDataRequest: " << resultId << " " << resultSetId << " " << startRowId << " " << resultSet.ByteSize() << " " << deadline);

    NYql::TIssues issues = ValidateWriteResultData(resultId, resultSet, deadline, Config->ResultSetsTtl);
    if (issues) {
        CPS_LOG_D("WriteResultDataRequest, validation failed: " << resultId << " " << resultSetId << " " << startRowId << " " << resultSet.DebugString() << " " << deadline << " error: " << issues.ToString());
        const TDuration delta = TInstant::Now() - startTime;
        SendResponseIssues<TEvControlPlaneStorage::TEvWriteResultDataResponse>(ev->Sender, issues, ev->Cookie, delta, requestCounters);
        LWPROBE(WriteResultDataRequest, resultId, resultSetId, startRowId, resultSet.rows().size(), delta, deadline, byteSize, false);
        return;
    }

    std::shared_ptr<Fq::Private::WriteTaskResultResult> response = std::make_shared<Fq::Private::WriteTaskResultResult>();
    response->set_request_id(request.request_id());

    NYdb::TValueBuilder itemsAsList;
    itemsAsList.BeginList();

    int64_t rowId = startRowId;
    for (const auto& row : resultSet.rows()) {
        TString serializedRow;
        if (!row.SerializeToString(&serializedRow)) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error serialize proto message for row. Please contact internal support";
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
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus result = Write(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [response] { return *response; };
    auto success = SendResponse<TEvControlPlaneStorage::TEvWriteResultDataResponse, Fq::Private::WriteTaskResultResult>(
        "WriteResultDataRequest - WriteResultDataResult",
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

} // NFq
