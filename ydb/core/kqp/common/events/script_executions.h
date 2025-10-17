#pragma once
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <ydb/library/actors/core/event_local.h>

#include <util/generic/maybe.h>

#include <google/protobuf/any.pb.h>

namespace NKikimr::NKqp {

enum EFinalizationStatus : i32 {
    FS_COMMIT,
    FS_ROLLBACK,
};

template <typename TEv, ui32 TEventType>
struct TEventWithDatabaseId : public TEventLocal<TEv, TEventType> {
    TEventWithDatabaseId(const TString& database)
        : Database(database)
    {}

    const TString& GetDatabase() const {
        return Database;
    }

    const TString& GetDatabaseId() const {
        return DatabaseId;
    }

    void SetDatabaseId(const TString& databaseId) {
        DatabaseId = databaseId;
    }

    const TString Database;
    TString DatabaseId;
};

struct TEvForgetScriptExecutionOperation : public TEventWithDatabaseId<TEvForgetScriptExecutionOperation, TKqpScriptExecutionEvents::EvForgetScriptExecutionOperation> {
    TEvForgetScriptExecutionOperation(const TString& database, const NOperationId::TOperationId& id, const std::optional<TString>& userSID)
        : TEventWithDatabaseId(database)
        , OperationId(id)
        , UserSID(userSID)
    {}

    const NOperationId::TOperationId OperationId;
    const std::optional<TString> UserSID;
};

struct TEvForgetScriptExecutionOperationResponse : public TEventLocal<TEvForgetScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvForgetScriptExecutionOperationResponse> {
    TEvForgetScriptExecutionOperationResponse(Ydb::StatusIds::StatusCode status,  NYql::TIssues issues) 
        : Status(status)
        , Issues(issues)
    {
    }
    
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvGetScriptExecutionOperation : public TEventWithDatabaseId<TEvGetScriptExecutionOperation, TKqpScriptExecutionEvents::EvGetScriptExecutionOperation> {
    TEvGetScriptExecutionOperation(const TString& database, const NOperationId::TOperationId& id, const std::optional<TString>& userSID)
        : TEventWithDatabaseId(database)
        , OperationId(id)
        , UserSID(userSID)
    {}

    const NOperationId::TOperationId OperationId;
    const std::optional<TString> UserSID;
    bool CheckLeaseState = true;
};

struct TEvGetScriptExecutionOperationQueryResponse : public TEventLocal<TEvGetScriptExecutionOperationQueryResponse, TKqpScriptExecutionEvents::EvGetScriptExecutionOperationQueryResponse> {
    explicit TEvGetScriptExecutionOperationQueryResponse(const TString& executionId)
        : ExecutionId(executionId)
    {}

    bool Ready = false;
    bool LeaseExpired = false;
    TInstant LeaseDeadline;
    std::optional<EFinalizationStatus> FinalizationStatus;
    TActorId RunScriptActorId;
    TString ExecutionId;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    Ydb::Query::ExecuteScriptMetadata Metadata;
    bool WaitRetry = false;
    i64 LeaseGeneration = 0;
    bool StateSaved = false;
    NKikimrKqp::TScriptExecutionRetryState RetryState;
};

struct TEvGetScriptExecutionOperationResponse : public TEventLocal<TEvGetScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvGetScriptExecutionOperationResponse> {
    struct TInfo {
        TMaybe<google::protobuf::Any> Metadata;
        bool Ready = false;
        bool StateSaved = false;
        ui64 RetryCount = 0;
        TInstant LastFailAt;
        TInstant SuspendedUntil;
    };

    TEvGetScriptExecutionOperationResponse(Ydb::StatusIds::StatusCode status, TInfo&& info, NYql::TIssues issues)
        : Ready(info.Ready)
        , Status(status)
        , Issues(std::move(issues))
        , Metadata(std::move(info.Metadata))
        , StateSaved(info.StateSaved)
        , RetryCount(info.RetryCount)
        , LastFailAt(info.LastFailAt)
        , SuspendedUntil(info.SuspendedUntil)
    {}

    TEvGetScriptExecutionOperationResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    bool Ready = false;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    TMaybe<google::protobuf::Any> Metadata;
    bool StateSaved = false;
    ui64 RetryCount = 0;
    TInstant LastFailAt;
    TInstant SuspendedUntil;
};

struct TEvListScriptExecutionOperations : public TEventWithDatabaseId<TEvListScriptExecutionOperations, TKqpScriptExecutionEvents::EvListScriptExecutionOperations> {
    TEvListScriptExecutionOperations(const TString& database, const ui64 pageSize, const TString& pageToken, const std::optional<TString>& userSID)
        : TEventWithDatabaseId(database)
        , PageSize(pageSize)
        , PageToken(pageToken)
        , UserSID(userSID)
    {}

    const ui64 PageSize;
    const TString PageToken;
    const std::optional<TString> UserSID;
};

struct TEvListScriptExecutionOperationsResponse : public TEventLocal<TEvListScriptExecutionOperationsResponse, TKqpScriptExecutionEvents::EvListScriptExecutionOperationsResponse> {
    TEvListScriptExecutionOperationsResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues, const TString& nextPageToken, std::vector<Ydb::Operations::Operation> operations)
        : Status(status)
        , Issues(std::move(issues))
        , NextPageToken(nextPageToken)
        , Operations(std::move(operations))
    {
    }

    TEvListScriptExecutionOperationsResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
    TString NextPageToken;
    std::vector<Ydb::Operations::Operation> Operations;
};

struct TEvScriptLeaseUpdateResponse : public TEventLocal<TEvScriptLeaseUpdateResponse, TKqpScriptExecutionEvents::EvScriptLeaseUpdateResponse> {
    TEvScriptLeaseUpdateResponse(bool executionEntryExists, TInstant currentDeadline, Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : ExecutionEntryExists(executionEntryExists)
        , CurrentDeadline(currentDeadline)
        , Status(status)
        , Issues(std::move(issues))
    {
    }

    bool ExecutionEntryExists;
    TInstant CurrentDeadline;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvCheckAliveRequest : public TEventPB<TEvCheckAliveRequest, NKikimrKqp::TEvCheckAliveRequest, TKqpScriptExecutionEvents::EvCheckAliveRequest> {
};

struct TEvCheckAliveResponse : public TEventPB<TEvCheckAliveResponse, NKikimrKqp::TEvCheckAliveResponse, TKqpScriptExecutionEvents::EvCheckAliveResponse> {
};

struct TEvCancelScriptExecutionOperation : public TEventWithDatabaseId<TEvCancelScriptExecutionOperation, TKqpScriptExecutionEvents::EvCancelScriptExecutionOperation> {
    TEvCancelScriptExecutionOperation(const TString& database, const NOperationId::TOperationId& id, const std::optional<TString>& userSID)
        : TEventWithDatabaseId(database)
        , OperationId(id)
        , UserSID(userSID)
    {}

    const NOperationId::TOperationId OperationId;
    const std::optional<TString> UserSID;
};

struct TEvResetScriptExecutionRetriesResponse : public TEventLocal<TEvResetScriptExecutionRetriesResponse, TKqpScriptExecutionEvents::EvResetScriptExecutionRetriesResponse> {
    explicit TEvResetScriptExecutionRetriesResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvCancelScriptExecutionOperationResponse : public TEventLocal<TEvCancelScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvCancelScriptExecutionOperationResponse> {
    TEvCancelScriptExecutionOperationResponse() = default;

    TEvCancelScriptExecutionOperationResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvScriptExecutionFinished : public TEventLocal<TEvScriptExecutionFinished, TKqpScriptExecutionEvents::EvScriptExecutionFinished> {
    TEvScriptExecutionFinished(bool operationAlreadyFinalized, bool waitingRetry, Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : OperationAlreadyFinalized(operationAlreadyFinalized)
        , WaitingRetry(waitingRetry)
        , Status(status)
        , Issues(std::move(issues))
    {}

    bool OperationAlreadyFinalized = false;
    bool WaitingRetry = false;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvSaveScriptResultMetaFinished : public TEventLocal<TEvSaveScriptResultMetaFinished, TKqpScriptExecutionEvents::EvSaveScriptResultMetaFinished> {
    TEvSaveScriptResultMetaFinished(Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {})
        : Status(status)
        , Issues(std::move(issues))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvSaveScriptResultPartFinished : public TEventLocal<TEvSaveScriptResultPartFinished, TKqpScriptExecutionEvents::EvSaveScriptResultPartFinished> {
    TEvSaveScriptResultPartFinished(Ydb::StatusIds::StatusCode status, i64 savedSize, NYql::TIssues issues = {})
        : Status(status)
        , SavedSize(savedSize)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    i64 SavedSize;
    NYql::TIssues Issues;
};

struct TEvSaveScriptResultFinished : public TEventLocal<TEvSaveScriptResultFinished, TKqpScriptExecutionEvents::EvSaveScriptResultFinished> {
    TEvSaveScriptResultFinished(Ydb::StatusIds::StatusCode status, size_t resultSetId, NYql::TIssues issues = {})
        : Status(status)
        , ResultSetId(resultSetId)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    size_t ResultSetId = 0;
    NYql::TIssues Issues;
};

struct TEvFetchScriptResultsResponse : public TEventLocal<TEvFetchScriptResultsResponse, TKqpScriptExecutionEvents::EvFetchScriptResultsResponse> {
    TEvFetchScriptResultsResponse(Ydb::StatusIds::StatusCode status, std::optional<Ydb::ResultSet>&& resultSet, bool hasMoreResults, NYql::TIssues issues)
        : Status(status)
        , ResultSet(std::move(resultSet))
        , HasMoreResults(hasMoreResults)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    std::optional<Ydb::ResultSet> ResultSet;
    bool HasMoreResults = false;
    NYql::TIssues Issues;
};

struct TEvStartScriptExecutionBackgroundChecks : public TEventLocal<TEvStartScriptExecutionBackgroundChecks, TKqpScriptExecutionEvents::EvStartScriptExecutionBackgroundChecks> {
};

struct TEvSaveScriptExternalEffectRequest : public TEventLocal<TEvSaveScriptExternalEffectRequest, TKqpScriptExecutionEvents::EvSaveScriptExternalEffectRequest> {
    struct TDescription {
        TDescription(const TString& executionId, const TString& database, const TString& customerSuppliedId)
            : ExecutionId(executionId)
            , Database(database)
            , CustomerSuppliedId(customerSuppliedId)
        {}

        TString ExecutionId;
        TString Database;

        TString CustomerSuppliedId;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        std::vector<NKqpProto::TKqpExternalSink> Sinks;
        std::vector<TString> SecretNames;
    };

    TEvSaveScriptExternalEffectRequest(const TString& executionId, const TString& database, const TString& customerSuppliedId)
        : Description(executionId, database, customerSuppliedId)
    {}

    TDescription Description;
};

struct TEvSaveScriptExternalEffectResponse : public TEventLocal<TEvSaveScriptExternalEffectResponse, TKqpScriptExecutionEvents::EvSaveScriptExternalEffectResponse> {
    TEvSaveScriptExternalEffectResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvSaveScriptPhysicalGraphRequest : public TEventLocal<TEvSaveScriptPhysicalGraphRequest, TKqpScriptExecutionEvents::EvSaveScriptPhysicalGraphRequest> {
    explicit TEvSaveScriptPhysicalGraphRequest(NKikimrKqp::TQueryPhysicalGraph physicalGraph)
        : PhysicalGraph(std::move(physicalGraph))
    {}

    NKikimrKqp::TQueryPhysicalGraph PhysicalGraph;
};

struct TEvSaveScriptPhysicalGraphResponse : public TEventLocal<TEvSaveScriptPhysicalGraphResponse, TKqpScriptExecutionEvents::EvSaveScriptPhysicalGraphResponse> {
    TEvSaveScriptPhysicalGraphResponse(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvGetScriptPhysicalGraphResponse : public TEventLocal<TEvGetScriptPhysicalGraphResponse, TKqpScriptExecutionEvents::EvGetScriptPhysicalGraphResponse> {
    TEvGetScriptPhysicalGraphResponse(Ydb::StatusIds::StatusCode status, NKikimrKqp::TQueryPhysicalGraph&& physicalGraph, i64 generation, NYql::TIssues issues)
        : Status(status)
        , PhysicalGraph(std::move(physicalGraph))
        , Generation(generation)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    NKikimrKqp::TQueryPhysicalGraph PhysicalGraph;
    i64 Generation = 0;
    NYql::TIssues Issues;
};

struct TEvScriptFinalizeRequest : public TEventLocal<TEvScriptFinalizeRequest, TKqpScriptExecutionEvents::EvScriptFinalizeRequest> {
    struct TDescription {
        TDescription(EFinalizationStatus finalizationStatus, const TString& executionId, const TString& database,
        Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus, NYql::TIssues issues, std::optional<NKqpProto::TKqpStatsQuery> queryStats,
        std::optional<TString> queryPlan, std::optional<TString> queryAst, i64 leaseGeneration)
            : FinalizationStatus(finalizationStatus)
            , ExecutionId(executionId)
            , Database(database)
            , OperationStatus(operationStatus)
            , ExecStatus(execStatus)
            , Issues(std::move(issues))
            , QueryStats(std::move(queryStats))
            , QueryPlan(std::move(queryPlan))
            , QueryAst(std::move(queryAst))
            , LeaseGeneration(leaseGeneration)
        {}

        EFinalizationStatus FinalizationStatus;
        TString ExecutionId;
        TString Database;
        Ydb::StatusIds::StatusCode OperationStatus;
        Ydb::Query::ExecStatus ExecStatus;
        NYql::TIssues Issues;
        std::optional<NKqpProto::TKqpStatsQuery> QueryStats;
        std::optional<TString> QueryPlan;
        std::optional<TString> QueryAst;
        i64 LeaseGeneration;
        std::optional<TString> QueryPlanCompressionMethod;
        std::optional<TString> QueryAstCompressionMethod;
    };

    TEvScriptFinalizeRequest(EFinalizationStatus finalizationStatus, const TString& executionId, const TString& database,
        Ydb::StatusIds::StatusCode operationStatus, Ydb::Query::ExecStatus execStatus, NYql::TIssues issues, std::optional<NKqpProto::TKqpStatsQuery> queryStats,
        std::optional<TString> queryPlan, std::optional<TString> queryAst, i64 leaseGeneration)
        : Description(finalizationStatus, executionId, database, operationStatus, execStatus, issues, queryStats, queryPlan, queryAst, leaseGeneration)
    {}

    TDescription Description;
};

struct TEvScriptFinalizeResponse : public TEventLocal<TEvScriptFinalizeResponse, TKqpScriptExecutionEvents::EvScriptFinalizeResponse> {
    explicit TEvScriptFinalizeResponse(const TString& executionId)
        : ExecutionId(executionId)
    {}

    TString ExecutionId;
};

struct TEvSaveScriptFinalStatusResponse : public TEventLocal<TEvSaveScriptFinalStatusResponse, TKqpScriptExecutionEvents::EvSaveScriptFinalStatusResponse> {
    bool ApplicateScriptExternalEffectRequired = false;
    bool OperationAlreadyFinalized = false;
    bool WaitRetry = false;
    TString CustomerSuppliedId;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::vector<NKqpProto::TKqpExternalSink> Sinks;
    std::vector<TString> SecretNames;
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;
};

struct TEvDescribeSecretsResponse : public TEventLocal<TEvDescribeSecretsResponse, TKqpScriptExecutionEvents::EvDescribeSecretsResponse> {
    struct TDescription {
        TDescription(Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        TDescription(const std::vector<TString>& secretValues)
            : SecretValues(secretValues)
            , Status(Ydb::StatusIds::SUCCESS)
        {}

        std::vector<TString> SecretValues;
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;
    };

    TEvDescribeSecretsResponse(const TDescription& description)
        : Description(description)
    {}

    TDescription Description;
};

struct TEvScriptExecutionsTablesCreationFinished : public TEventLocal<TEvScriptExecutionsTablesCreationFinished, TKqpScriptExecutionEvents::EvScriptExecutionsTableCreationFinished> {
    TEvScriptExecutionsTablesCreationFinished(bool success, NYql::TIssues issues)
        : Success(success)
        , Issues(std::move(issues))
    {}

    const bool Success;
    const NYql::TIssues Issues;
};

struct TEvScriptExecutionRestarted : public TEventLocal<TEvScriptExecutionRestarted, TKqpScriptExecutionEvents::EvScriptExecutionRestarted> {
    TEvScriptExecutionRestarted(Ydb::StatusIds::StatusCode status, bool leaseGenerationChanged, NYql::TIssues issues)
        : Status(status)
        , LeaseGenerationChanged(leaseGenerationChanged)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status;
    const bool LeaseGenerationChanged = false;
    const NYql::TIssues Issues;
};

struct TEvListExpiredLeasesResponse : public TEventLocal<TEvListExpiredLeasesResponse, TKqpScriptExecutionEvents::EvListExpiredLeasesResponse> {
    struct TLeaseInfo {
        TString Database;
        TString ExecutionId;
    };

    TEvListExpiredLeasesResponse(Ydb::StatusIds::StatusCode status, std::vector<TLeaseInfo>&& leases, NYql::TIssues issues)
        : Status(status)
        , Leases(std::move(leases))
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    std::vector<TLeaseInfo> Leases;
    NYql::TIssues Issues;
};

struct TEvRefreshScriptExecutionLeasesResponse : public TEventLocal<TEvRefreshScriptExecutionLeasesResponse, TKqpScriptExecutionEvents::EvRefreshScriptExecutionLeasesResponse> {
    TEvRefreshScriptExecutionLeasesResponse(bool success, NYql::TIssues issues)
        : Success(success)
        , Issues(std::move(issues))
    {}

    bool Success;
    NYql::TIssues Issues;
};

struct TEvSaveScriptProgressResponse : public TEventLocal<TEvSaveScriptProgressResponse, TKqpScriptExecutionEvents::EvSaveScriptProgressResponse> {
    TEvSaveScriptProgressResponse(Ydb::StatusIds::StatusCode status, bool astSaved, NYql::TIssues issues)
        : Status(status)
        , AstSaved(astSaved)
        , Issues(std::move(issues))
    {}

    Ydb::StatusIds::StatusCode Status;
    bool AstSaved = false;
    NYql::TIssues Issues;
};

} // namespace NKikimr::NKqp
