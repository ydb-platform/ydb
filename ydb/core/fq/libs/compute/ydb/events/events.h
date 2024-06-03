#pragma once

#include <ydb/core/fq/libs/control_plane_storage/proto/yq_internal.pb.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/fq/libs/protos/fq_private.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <util/generic/set.h>

namespace NFq {

struct TEvYdbCompute {
    // Event ids
    enum EEv : ui32 {
        EvBegin = YqEventSubspaceBegin(NFq::TYqEventSubspace::YdbCompute),

        EvExecuteScriptRequest = EvBegin,
        EvExecuteScriptResponse,
        EvGetOperationRequest,
        EvGetOperationResponse,
        EvFetchScriptResultRequest,
        EvFetchScriptResultResponse,
        EvCancelOperationRequest,
        EvCancelOperationResponse,
        EvForgetOperationRequest,
        EvForgetOperationResponse,
        EvCreateDatabaseRequest,
        EvCreateDatabaseResponse,

        EvInitializerResponse,
        EvExecuterResponse,
        EvStatusTrackerResponse,
        EvResultWriterResponse,
        EvResultSetWriterResponse,
        EvResourcesCleanerResponse,
        EvFinalizerResponse,
        EvStopperResponse,

        EvSynchronizeRequest,
        EvSynchronizeResponse,

        EvListDatabasesRequest,
        EvListDatabasesResponse,

        EvCheckDatabaseRequest,
        EvCheckDatabaseResponse,
        EvAddDatabaseRequest,
        EvAddDatabaseResponse,

        EvInvalidateSynchronizationRequest,
        EvInvalidateSynchronizationResponse,

        EvCpuLoadRequest,
        EvCpuLoadResponse,
        EvCpuQuotaRequest,
        EvCpuQuotaResponse,
        EvCpuQuotaAdjust,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvExecuteScriptRequest : public NActors::TEventLocal<TEvExecuteScriptRequest, EvExecuteScriptRequest> {
        TEvExecuteScriptRequest(TString sql, TString idempotencyKey, const TDuration& resultTtl, const TDuration& operationTimeout, Ydb::Query::Syntax syntax, Ydb::Query::ExecMode execMode, Ydb::Query::StatsMode statsMode, const TString& traceId, const std::map<TString, Ydb::TypedValue>& queryParameters)
            : Sql(std::move(sql))
            , IdempotencyKey(std::move(idempotencyKey))
            , ResultTtl(resultTtl)
            , OperationTimeout(operationTimeout)
            , Syntax(syntax)
            , ExecMode(execMode)
            , StatsMode(statsMode)
            , TraceId(traceId)
            , QueryParameters(queryParameters)
        {}

        TString Sql;
        TString IdempotencyKey;
        TDuration ResultTtl;
        TDuration OperationTimeout;
        Ydb::Query::Syntax Syntax = Ydb::Query::SYNTAX_YQL_V1;
        Ydb::Query::ExecMode ExecMode = Ydb::Query::EXEC_MODE_EXECUTE;
        Ydb::Query::StatsMode StatsMode = Ydb::Query::StatsMode::STATS_MODE_FULL;
        TString TraceId;
        std::map<TString, Ydb::TypedValue> QueryParameters;
    };

    struct TEvExecuteScriptResponse : public NActors::TEventLocal<TEvExecuteScriptResponse, EvExecuteScriptResponse> {
        TEvExecuteScriptResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        TEvExecuteScriptResponse(NYdb::TOperation::TOperationId operationId, const TString& executionId)
            : OperationId(operationId)
            , ExecutionId(executionId)
            , Status(NYdb::EStatus::SUCCESS)
        {}

        NYdb::TOperation::TOperationId OperationId;
        TString ExecutionId;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvGetOperationRequest : public NActors::TEventLocal<TEvGetOperationRequest, EvGetOperationRequest> {
        explicit TEvGetOperationRequest(const NYdb::TOperation::TOperationId& operationId)
            : OperationId(operationId)
        {}

        NYdb::TOperation::TOperationId OperationId;
    };

    struct TEvGetOperationResponse : public NActors::TEventLocal<TEvGetOperationResponse, EvGetOperationResponse> {
        TEvGetOperationResponse(NYql::TIssues issues, NYdb::EStatus status, bool ready)
            : Issues(std::move(issues))
            , Status(status)
            , Ready(ready)
        {}

        TEvGetOperationResponse(NYdb::NQuery::EExecStatus execStatus, Ydb::StatusIds::StatusCode statusCode, const TVector<Ydb::Query::ResultSetMeta>& resultSetsMeta, const Ydb::TableStats::QueryStats& queryStats, NYql::TIssues issues, bool ready = true)
            : ExecStatus(execStatus)
            , StatusCode(statusCode)
            , ResultSetsMeta(resultSetsMeta)
            , QueryStats(queryStats)
            , Issues(std::move(issues))
            , Status(NYdb::EStatus::SUCCESS)
            , Ready(ready)
        {}

        NYdb::NQuery::EExecStatus ExecStatus = NYdb::NQuery::EExecStatus::Unspecified;
        Ydb::StatusIds::StatusCode StatusCode = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        TVector<Ydb::Query::ResultSetMeta> ResultSetsMeta;
        Ydb::TableStats::QueryStats QueryStats;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
        bool Ready;
    };

    struct TEvFetchScriptResultRequest : public NActors::TEventLocal<TEvFetchScriptResultRequest, EvFetchScriptResultRequest> {
        TEvFetchScriptResultRequest(const NKikimr::NOperationId::TOperationId& operationId, int64_t resultSetId, const TString& fetchToken, uint64_t rowsLimit)
            : OperationId(operationId)
            , ResultSetId(resultSetId)
            , FetchToken(fetchToken)
            , RowsLimit(rowsLimit)
        {}

        NKikimr::NOperationId::TOperationId OperationId;
        int64_t ResultSetId = 0;
        TString FetchToken;
        uint64_t RowsLimit = 0;
    };

    struct TEvFetchScriptResultResponse : public NActors::TEventLocal<TEvFetchScriptResultResponse, EvFetchScriptResultResponse> {
        TEvFetchScriptResultResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        explicit TEvFetchScriptResultResponse(NYdb::TResultSet resultSet, const TString& nextFetchToken)
            : ResultSet(std::move(resultSet))
            , NextFetchToken(nextFetchToken)
            , Status(NYdb::EStatus::SUCCESS)
        {}

        TMaybe<NYdb::TResultSet> ResultSet;
        TString NextFetchToken;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvCancelOperationRequest : public NActors::TEventLocal<TEvCancelOperationRequest, EvCancelOperationRequest> {
        explicit TEvCancelOperationRequest(const NYdb::TOperation::TOperationId& operationId)
            : OperationId(operationId)
        {}

        NYdb::TOperation::TOperationId OperationId;
    };

    struct TEvCancelOperationResponse : public NActors::TEventLocal<TEvCancelOperationResponse, EvCancelOperationResponse> {
        TEvCancelOperationResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvForgetOperationRequest : public NActors::TEventLocal<TEvForgetOperationRequest, EvForgetOperationRequest> {
        explicit TEvForgetOperationRequest(const NYdb::TOperation::TOperationId& operationId)
            : OperationId(operationId)
        {}

        NYdb::TOperation::TOperationId OperationId;
    };

    struct TEvForgetOperationResponse : public NActors::TEventLocal<TEvForgetOperationResponse, EvForgetOperationResponse> {
        TEvForgetOperationResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvCreateDatabaseRequest : public NActors::TEventLocal<TEvCreateDatabaseRequest, EvCreateDatabaseRequest> {
        TEvCreateDatabaseRequest(const TString& cloudId, const TString& scope)
            : CloudId(cloudId)
            , Scope(scope)
        {}

        TEvCreateDatabaseRequest(const TString& cloudId,
                                 const TString& scope,
                                 const TString& basePath,
                                 const TString& path,
                                 const NFq::NConfig::TYdbStorageConfig& executionConnection)
            : CloudId(cloudId)
            , Scope(scope)
            , BasePath(basePath)
            , Path(path)
            , ExecutionConnection(executionConnection)
        {}

        TString CloudId;
        TString Scope;
        TString BasePath;
        TString Path;
        NFq::NConfig::TYdbStorageConfig ExecutionConnection;
    };

    struct TEvCreateDatabaseResponse : public NActors::TEventLocal<TEvCreateDatabaseResponse, EvCreateDatabaseResponse> {
        TEvCreateDatabaseResponse()
        {}

        explicit TEvCreateDatabaseResponse(NYql::TIssues issues)
            : Issues(std::move(issues))
        {}

        TEvCreateDatabaseResponse(const FederatedQuery::Internal::ComputeDatabaseInternal& result)
            : Result(result)
        {}

        FederatedQuery::Internal::ComputeDatabaseInternal Result;
        NYql::TIssues Issues;
    };

    struct TEvListDatabasesRequest : public NActors::TEventLocal<TEvListDatabasesRequest, EvListDatabasesRequest> {
    };

    struct TEvListDatabasesResponse : public NActors::TEventLocal<TEvListDatabasesResponse, EvListDatabasesResponse> {
        TSet<TString> Paths;
        NYql::TIssues Issues;
    };

    struct TEvInitializerResponse : public NActors::TEventLocal<TEvInitializerResponse, EvInitializerResponse> {
        TEvInitializerResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvExecuterResponse : public NActors::TEventLocal<TEvExecuterResponse, EvExecuterResponse> {
        TEvExecuterResponse(NYdb::TOperation::TOperationId operationId, const TString& executionId, NYdb::EStatus status)
            : OperationId(operationId)
            , ExecutionId(executionId)
            , Status(status)
        {}

        explicit TEvExecuterResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Status(status)
            , Issues(std::move(issues))
            
        {}

        NYdb::TOperation::TOperationId OperationId;
        TString ExecutionId;
        NYdb::EStatus Status;
        NYql::TIssues Issues;
    };

    struct TEvStatusTrackerResponse : public NActors::TEventLocal<TEvStatusTrackerResponse, EvStatusTrackerResponse> {
        TEvStatusTrackerResponse(NYql::TIssues issues, NYdb::EStatus status, NYdb::NQuery::EExecStatus execStatus, FederatedQuery::QueryMeta::ComputeStatus computeStatus)
            : Issues(std::move(issues))
            , Status(status)
            , ExecStatus(execStatus)
            , ComputeStatus(computeStatus)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
        NYdb::NQuery::EExecStatus ExecStatus;
        FederatedQuery::QueryMeta::ComputeStatus ComputeStatus;
    };

    struct TEvResultWriterResponse : public NActors::TEventLocal<TEvResultWriterResponse, EvResultWriterResponse> {
        TEvResultWriterResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvResourcesCleanerResponse : public NActors::TEventLocal<TEvResourcesCleanerResponse, EvResourcesCleanerResponse> {
        TEvResourcesCleanerResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvFinalizerResponse : public NActors::TEventLocal<TEvFinalizerResponse, EvFinalizerResponse> {
        TEvFinalizerResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvStopperResponse : public NActors::TEventLocal<TEvStopperResponse, EvStopperResponse> {
        TEvStopperResponse(NYql::TIssues issues, NYdb::EStatus status)
            : Issues(std::move(issues))
            , Status(status)
        {}

        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };


    struct TEvResultSetWriterResponse : public NActors::TEventLocal<TEvResultSetWriterResponse, EvResultSetWriterResponse> {
        TEvResultSetWriterResponse(ui64 resultSetId, NYql::TIssues issues, NYdb::EStatus status)
            : ResultSetId(resultSetId)
            , Issues(std::move(issues))
            , Status(status)
        {}

        TEvResultSetWriterResponse(ui64 resultSetId, int64_t rowsCount, bool truncated)
            : ResultSetId(resultSetId)
            , Status(NYdb::EStatus::SUCCESS)
            , RowsCount(rowsCount)
            , Truncated(truncated)
        {}

        const ui64 ResultSetId;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
        int64_t RowsCount = 0;
        bool Truncated = false;
    };

    struct TEvSynchronizeRequest : public NActors::TEventLocal<TEvSynchronizeRequest, EvSynchronizeRequest> {
        TEvSynchronizeRequest(const TString& cloudId, const TString& scope, const NFq::NConfig::TYdbStorageConfig& connectionConfig)
            : CloudId(cloudId)
            , Scope(scope)
            , ConnectionConfig(connectionConfig)
        {}

        TString CloudId;
        TString Scope;
        NFq::NConfig::TYdbStorageConfig ConnectionConfig;
    };

    struct TEvSynchronizeResponse : public NActors::TEventLocal<TEvSynchronizeResponse, EvSynchronizeResponse> {
        TEvSynchronizeResponse(const TString& scope)
            : Scope(scope)
            , Status(NYdb::EStatus::SUCCESS)
        {}

        TEvSynchronizeResponse(const TString& scope, NYql::TIssues issues, NYdb::EStatus status)
            : Scope(scope)
            , Issues(std::move(issues))
            , Status(status)
        {}

        TEvSynchronizeResponse(const TString& scope, NYql::TIssues issues)
            : Scope(scope)
            , Issues(std::move(issues))
            , Status(NYdb::EStatus::SUCCESS)
        {}

        TString Scope;
        NYql::TIssues Issues;
        NYdb::EStatus Status;
    };

    struct TEvCheckDatabaseRequest : public NActors::TEventLocal<TEvCheckDatabaseRequest, EvCheckDatabaseRequest> {
        TEvCheckDatabaseRequest(const TString& path)
            : Path(path)
        {}

        TString Path;
    };

    struct TEvCheckDatabaseResponse : public NActors::TEventLocal<TEvCheckDatabaseResponse, EvCheckDatabaseResponse> {
        TEvCheckDatabaseResponse(bool isExists)
            : IsExists(isExists)
        {}

        TEvCheckDatabaseResponse(const NYql::TIssues& issues)
            : IsExists(false)
            , Issues(issues)
        {}

        bool IsExists = false;
        NYql::TIssues Issues;
    };

    struct TEvAddDatabaseRequest : public NActors::TEventLocal<TEvAddDatabaseRequest, EvAddDatabaseRequest> {
        TEvAddDatabaseRequest(const TString& path)
            : Path(path)
        {}

        TString Path;
    };

    struct TEvAddDatabaseResponse : public NActors::TEventLocal<TEvAddDatabaseResponse, EvAddDatabaseResponse> {
        TEvAddDatabaseResponse()
        {}
    };

    struct TEvInvalidateSynchronizationRequest : public NActors::TEventLocal<TEvInvalidateSynchronizationRequest, EvInvalidateSynchronizationRequest> {
        TEvInvalidateSynchronizationRequest(const TString& scope)
            : Scope(scope)
        {}

        TString Scope;
    };

    struct TEvInvalidateSynchronizationResponse : public NActors::TEventLocal<TEvInvalidateSynchronizationRequest, EvInvalidateSynchronizationRequest> {
        TEvInvalidateSynchronizationResponse(NYql::TIssues issues)
            : Issues(std::move(issues))
        {}

        NYql::TIssues Issues;
    };

    struct TEvCpuLoadRequest : public NActors::TEventLocal<TEvCpuLoadRequest, EvCpuLoadRequest> {
        TEvCpuLoadRequest(const TString& scope)
            : Scope(scope)
        {}

        TString Scope;
    };

    struct TEvCpuLoadResponse : public NActors::TEventLocal<TEvCpuLoadResponse, EvCpuLoadResponse> {
        TEvCpuLoadResponse(double instantLoad = 0.0, double averageLoad = 0.0, ui32 cpuNumber = 0)
            : InstantLoad(instantLoad), AverageLoad(averageLoad), CpuNumber(cpuNumber)
        {}

        TEvCpuLoadResponse(NYql::TIssues issues)
            : InstantLoad(0.0), AverageLoad(0.0), CpuNumber(0), Issues(std::move(issues))
        {}

        double InstantLoad;
        double AverageLoad;
        ui32 CpuNumber;
        NYql::TIssues Issues;
    };

    struct TEvCpuQuotaRequest : public NActors::TEventLocal<TEvCpuQuotaRequest, EvCpuQuotaRequest> {
        TEvCpuQuotaRequest(const TString& scope, TInstant deadline = TInstant::Zero(), double quota = 0.0)
            : Scope(scope), Deadline(deadline), Quota(quota)
        {}

        TString Scope;
        TInstant Deadline;
        double Quota; // if zero, default quota is used
    };

    struct TEvCpuQuotaResponse : public NActors::TEventLocal<TEvCpuQuotaResponse, EvCpuQuotaResponse> {
        TEvCpuQuotaResponse(NYdb::EStatus status = NYdb::EStatus::SUCCESS, NYql::TIssues issues = {})
            : Status(status), Issues(std::move(issues))
        {}

        NYdb::EStatus Status;
        NYql::TIssues Issues;
    };

    struct TEvCpuQuotaAdjust : public NActors::TEventLocal<TEvCpuQuotaAdjust, EvCpuQuotaAdjust> {
        TEvCpuQuotaAdjust(const TString& scope, TDuration duration, double cpuSecondsConsumed, double quota = 0.0)
            : Scope(scope), Duration(duration), CpuSecondsConsumed(cpuSecondsConsumed), Quota(quota)
        {}

        TString Scope;
        TDuration Duration;
        double CpuSecondsConsumed;
        double Quota; // if zero, default quota is used
    };
};

}
