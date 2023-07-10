#include "validators.h"
#include "ydb_control_plane_storage_impl.h"

#include <util/string/join.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

namespace NFq {

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvCreateDatabaseRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvCreateDatabaseRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_CREATE_DATABASE, RTC_CREATE_DATABASE);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const FederatedQuery::Internal::ComputeDatabaseInternal& request = event.Record;
    const int byteSize = request.ByteSize();

    CPS_LOG_T(MakeLogPrefix(scope, "internal", request.id())
        << "CreateDatabaseRequest: "
        << request.DebugString());

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "CreateDatabase");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddString("internal", request.SerializeAsString());

    queryBuilder.AddText(
        "INSERT INTO `" COMPUTE_DATABASES_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`) VALUES\n"
        "    ($scope, $internal);"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    TAsyncStatus result = Write(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [] { return std::make_tuple<NYql::TIssues>(NYql::TIssues{}); };
    auto success = SendResponseTuple<TEvControlPlaneStorage::TEvCreateDatabaseResponse, std::tuple<NYql::TIssues>>(
        MakeLogPrefix(scope, "internal", request.id()) + "CreateDatabaseRequest",
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
            LWPROBE(CreateDatabaseRequest, scope, "internal", delta, byteSize, future.GetValue());
        });
}

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvDescribeDatabaseRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDescribeDatabaseRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DESCRIBE_DATABASE, RTC_DESCRIBE_DATABASE);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const auto byteSize = event.GetByteSize();

    CPS_LOG_T(MakeLogPrefix(scope, "internal", scope)
        << "DescribeDatabaseRequest");

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DescribeDatabase");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddText(
        "SELECT `" INTERNAL_COLUMN_NAME "` FROM `" COMPUTE_DATABASES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;"
    );

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto [result, resultSets] = Read(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [=, resultSets=resultSets] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Database does not exist or permission denied. Please check the id database or your access rights";
        }

        FederatedQuery::Internal::ComputeDatabaseInternal result;
        if (!result.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for internal compute database. Please contact internal support";
        }

        return result;
    };

    auto success = SendResponse<TEvControlPlaneStorage::TEvDescribeDatabaseResponse, FederatedQuery::Internal::ComputeDatabaseInternal>(
        MakeLogPrefix(scope, "internal", scope) + "DescribeDatabaseRequest",
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
            LWPROBE(DescribeDatabaseRequest, scope, "internal", delta, byteSize, future.GetValue());
        });
}

} // NFq
