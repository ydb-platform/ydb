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

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ModifyDatabase");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddText(
        "SELECT `" SCOPE_COLUMN_NAME "` FROM `" COMPUTE_DATABASES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;"
    );

    auto prepareParams = [=](const TVector<TResultSet>& resultSets) {
        if (resultSets.size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets.size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets.front());
        if (parser.TryNextRow()) {
            return make_pair(TString{}, NYdb::TParamsBuilder{}.Build()); // already exists
        }

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "CreateDatabase");
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddString("internal", request.SerializeAsString());
        writeQueryBuilder.AddText(
            "INSERT INTO `" COMPUTE_DATABASES_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" INTERNAL_COLUMN_NAME "`, `" CREATED_AT_COLUMN_NAME "`, `" LAST_ACCESS_AT_COLUMN_NAME"`) VALUES\n"
            "    ($scope, $internal, CurrentUtcTimestamp(), CurrentUtcTimestamp());"
        );
        const auto writeQuery = writeQueryBuilder.Build();
        return make_pair(writeQuery.Sql, writeQuery.Params);
    };

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(query.Sql, query.Params, prepareParams, requestCounters, debugInfo);
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
    auto prepare = [=, resultSets=resultSets, commonCounters=requestCounters.Common] {
        if (resultSets->size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets->size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets->front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Database does not exist or permission denied. Please check the id database or your access rights";
        }

        FederatedQuery::Internal::ComputeDatabaseInternal result;
        if (!result.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
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

void TYdbControlPlaneStorageActor::Handle(TEvControlPlaneStorage::TEvModifyDatabaseRequest::TPtr& ev)
{
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvModifyDatabaseRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_MODIFY_DATABASE, RTC_MODIFY_DATABASE);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const auto byteSize = event.GetByteSize();

    CPS_LOG_T(MakeLogPrefix(scope, "internal", scope)
        << "ModifyDatabaseRequest");
    
    // only write part
    if (event.LastAccessAt) {
        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ModifyDatabase(write)");
        writeQueryBuilder.AddTimestamp("last_access_at", *event.LastAccessAt);
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddText(
            "UPSERT INTO `" COMPUTE_DATABASES_TABLE_NAME "` (`" SCOPE_COLUMN_NAME "`, `" LAST_ACCESS_AT_COLUMN_NAME "`)\n"
            "VALUES ($scope, $last_access_at);"
        );
        const auto writeQuery = writeQueryBuilder.Build();
        auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
        auto result = Write(writeQuery.Sql, writeQuery.Params, requestCounters, debugInfo);

        auto prepare = [] { return std::make_tuple<NYql::TIssues>(NYql::TIssues{}); };
        auto success = SendResponseTuple<TEvControlPlaneStorage::TEvModifyDatabaseResponse, std::tuple<NYql::TIssues>>(
            MakeLogPrefix(scope, "internal", scope) + "ModifyDatabaseRequest",
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
                LWPROBE(ModifyDatabaseRequest, scope, "internal", delta, byteSize, future.GetValue());
            });

        return;
    }

    // read write part
    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "ModifyDatabase");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddText(
        "SELECT `" INTERNAL_COLUMN_NAME "` FROM `" COMPUTE_DATABASES_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;"
    );

    auto prepareParams = [=, synchronized = ev->Get()->Synchronized, workloadManagerSynchronized = ev->Get()->WorkloadManagerSynchronized, commonCounters=requestCounters.Common](const TVector<TResultSet>& resultSets) {
        if (resultSets.size() != 1) {
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Result set size is not equal to 1 but equal " << resultSets.size() << ". Please contact internal support";
        }

        TResultSetParser parser(resultSets.front());
        if (!parser.TryNextRow()) {
            ythrow TCodeLineException(TIssuesIds::ACCESS_DENIED) << "Database does not exist or permission denied. Please check the id database or your access rights";
        }

        FederatedQuery::Internal::ComputeDatabaseInternal result;
        if (!result.ParseFromString(*parser.ColumnParser(INTERNAL_COLUMN_NAME).GetOptionalString())) {
            commonCounters->ParseProtobufError->Inc();
            ythrow TCodeLineException(TIssuesIds::INTERNAL_ERROR) << "Error parsing proto message for internal compute database. Please contact internal support";
        }

        if (synchronized) {
            result.set_synchronized(*synchronized);
        }

        if (workloadManagerSynchronized) {
            result.set_workload_manager_synchronized(*workloadManagerSynchronized);
        }

        TSqlQueryBuilder writeQueryBuilder(YdbConnection->TablePathPrefix, "ModifyDatabase(write)");
        writeQueryBuilder.AddString("internal", result.SerializeAsString());
        writeQueryBuilder.AddString("scope", scope);
        writeQueryBuilder.AddText(
            "UPDATE `" COMPUTE_DATABASES_TABLE_NAME "` SET `" INTERNAL_COLUMN_NAME "` = $internal\n"
            "WHERE `" SCOPE_COLUMN_NAME "` = $scope;"
        );
        const auto writeQuery = writeQueryBuilder.Build();
        return make_pair(writeQuery.Sql, writeQuery.Params);
    };

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = ReadModifyWrite(query.Sql, query.Params, prepareParams, requestCounters, debugInfo);


    auto prepare = [] { return std::make_tuple<NYql::TIssues>(NYql::TIssues{}); };
    auto success = SendResponseTuple<TEvControlPlaneStorage::TEvModifyDatabaseResponse, std::tuple<NYql::TIssues>>(
        MakeLogPrefix(scope, "internal", scope) + "ModifyDatabaseRequest",
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
            LWPROBE(ModifyDatabaseRequest, scope, "internal", delta, byteSize, future.GetValue());
        });
}

} // NFq
