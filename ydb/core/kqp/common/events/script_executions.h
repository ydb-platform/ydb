#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <google/protobuf/any.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <optional>
#include <vector>

namespace NKikimr::NKqp {

enum EFinalizationStatus : i32 {
    FS_COMMIT,
    FS_ROLLBACK,
};

template <typename TEv, ui32 TEventType>
struct TEventWithDatabaseId : public TEventLocal<TEv, TEventType> {
    explicit TEventWithDatabaseId(TString database)
        : Database(std::move(database))
    {}

    const TString& GetDatabase() const {
        return Database;
    }

    const TString& GetDatabaseId() const {
        return DatabaseId;
    }

    void SetDatabaseId(TString databaseId) {
        DatabaseId = std::move(databaseId);
    }

    TString Database;
    TString DatabaseId;
};

struct TEvForgetScriptExecutionOperation : public TEventWithDatabaseId<TEvForgetScriptExecutionOperation, TKqpScriptExecutionEvents::EvForgetScriptExecutionOperation> {
    struct TSettings {
        const bool FailOnNotFound = true;
        const bool CancelIfRunning = false;
    };

    TEvForgetScriptExecutionOperation(TString database, NOperationId::TOperationId id, std::optional<TString> userSID, TSettings&& settings)
        : TEventWithDatabaseId(std::move(database))
        , OperationId(std::move(id))
        , UserSID(std::move(userSID))
        , Settings(std::move(settings))
    {}

    TEvForgetScriptExecutionOperation(TString database, NOperationId::TOperationId id, std::optional<TString> userSID)
        : TEvForgetScriptExecutionOperation(std::move(database), std::move(id), std::move(userSID), {})
    {}

    const NOperationId::TOperationId OperationId;
    const std::optional<TString> UserSID;
    const TSettings Settings;
};

struct TEvForgetScriptExecutionOperationResponse : public TEventLocal<TEvForgetScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvForgetScriptExecutionOperationResponse> {
    TEvForgetScriptExecutionOperationResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues) 
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
};

struct TEvGetScriptExecutionOperation : public TEventWithDatabaseId<TEvGetScriptExecutionOperation, TKqpScriptExecutionEvents::EvGetScriptExecutionOperation> {
    TEvGetScriptExecutionOperation(TString database, NOperationId::TOperationId id, std::optional<TString> userSID, const bool failOnNotFound = true)
        : TEventWithDatabaseId(std::move(database))
        , OperationId(std::move(id))
        , UserSID(std::move(userSID))
        , FailOnNotFound(failOnNotFound)
    {}

    NOperationId::TOperationId OperationId;
    std::optional<TString> UserSID;
    const bool FailOnNotFound = true;
};

struct TEvGetScriptExecutionOperationResponse : public TEventLocal<TEvGetScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvGetScriptExecutionOperationResponse> {
    struct TInfo {
        TMaybe<google::protobuf::Any> Metadata;
        const bool Ready = false;
        const bool StateSaved = false;
        const bool ExecutionEntryExists = true;
        const ui64 RetryCount = 0;
        const TInstant LastFailAt;
        const TInstant SuspendedUntil;
        const Ydb::StatusIds::StatusCode RequestStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    };

    TEvGetScriptExecutionOperationResponse(const Ydb::StatusIds::StatusCode status, TInfo&& info, NYql::TIssues issues)
        : Ready(info.Ready)
        , Status(status)
        , RequestStatus(info.RequestStatus)
        , Issues(std::move(issues))
        , Metadata(std::move(info.Metadata))
        , StateSaved(info.StateSaved)
        , ExecutionEntryExists(info.ExecutionEntryExists)
        , RetryCount(info.RetryCount)
        , LastFailAt(info.LastFailAt)
        , SuspendedUntil(info.SuspendedUntil)
    {}

    TEvGetScriptExecutionOperationResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const bool Ready = false;
    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const Ydb::StatusIds::StatusCode RequestStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
    TMaybe<google::protobuf::Any> Metadata;
    const bool StateSaved = false;
    const bool ExecutionEntryExists = true;
    const ui64 RetryCount = 0;
    const TInstant LastFailAt;
    const TInstant SuspendedUntil;
};

struct TEvListScriptExecutionOperations : public TEventWithDatabaseId<TEvListScriptExecutionOperations, TKqpScriptExecutionEvents::EvListScriptExecutionOperations> {
    TEvListScriptExecutionOperations(TString database, const ui64 pageSize, TString pageToken, std::optional<TString> userSID)
        : TEventWithDatabaseId(std::move(database))
        , PageSize(pageSize)
        , PageToken(std::move(pageToken))
        , UserSID(std::move(userSID))
    {}

    const ui64 PageSize = 0;
    TString PageToken;
    std::optional<TString> UserSID;
};

struct TEvListScriptExecutionOperationsResponse : public TEventLocal<TEvListScriptExecutionOperationsResponse, TKqpScriptExecutionEvents::EvListScriptExecutionOperationsResponse> {
    TEvListScriptExecutionOperationsResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues, TString nextPageToken, std::vector<Ydb::Operations::Operation> operations)
        : Status(status)
        , Issues(std::move(issues))
        , NextPageToken(std::move(nextPageToken))
        , Operations(std::move(operations))
    {}

    TEvListScriptExecutionOperationsResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
    const TString NextPageToken;
    std::vector<Ydb::Operations::Operation> Operations;
};

struct TEvScriptLeaseUpdateResponse : public TEventLocal<TEvScriptLeaseUpdateResponse, TKqpScriptExecutionEvents::EvScriptLeaseUpdateResponse> {
    TEvScriptLeaseUpdateResponse(const bool executionEntryExists, const TInstant currentDeadline, const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : ExecutionEntryExists(executionEntryExists)
        , CurrentDeadline(currentDeadline)
        , Status(status)
        , Issues(std::move(issues))
    {}

    const bool ExecutionEntryExists = true;
    const TInstant CurrentDeadline;
    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
};

struct TEvCheckAliveRequest : public TEventPB<TEvCheckAliveRequest, NKikimrKqp::TEvCheckAliveRequest, TKqpScriptExecutionEvents::EvCheckAliveRequest> {
};

struct TEvCheckAliveResponse : public TEventPB<TEvCheckAliveResponse, NKikimrKqp::TEvCheckAliveResponse, TKqpScriptExecutionEvents::EvCheckAliveResponse> {
};

struct TEvCancelScriptExecutionOperation : public TEventWithDatabaseId<TEvCancelScriptExecutionOperation, TKqpScriptExecutionEvents::EvCancelScriptExecutionOperation> {
    struct TSettings {
        const bool FailOnNotFound = true;
        const bool FailOnAlreadyStopped = true;
    };

    TEvCancelScriptExecutionOperation(TString database, NOperationId::TOperationId id, std::optional<TString> userSID, TSettings&& settings)
        : TEventWithDatabaseId(std::move(database))
        , OperationId(std::move(id))
        , UserSID(std::move(userSID))
        , Settings(std::move(settings))
    {}

    TEvCancelScriptExecutionOperation(TString database, NOperationId::TOperationId id, std::optional<TString> userSID)
        : TEvCancelScriptExecutionOperation(std::move(database), std::move(id), std::move(userSID), {})
    {}

    const NOperationId::TOperationId OperationId;
    const std::optional<TString> UserSID;
    const TSettings Settings;
};

struct TEvResetScriptExecutionRetriesResponse : public TEventLocal<TEvResetScriptExecutionRetriesResponse, TKqpScriptExecutionEvents::EvResetScriptExecutionRetriesResponse> {
    struct TInfo {
        const bool OperationStillRunning = false;
        const bool ExecutionEntryExists = true;
        const bool AlreadyStopped = false;
    };

    TEvResetScriptExecutionRetriesResponse(const Ydb::StatusIds::StatusCode status, TInfo&& info, NYql::TIssues issues)
        : Status(status)
        , Info(std::move(info))
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const TInfo Info;
    const NYql::TIssues Issues;
};

struct TEvCancelScriptExecutionOperationResponse : public TEventLocal<TEvCancelScriptExecutionOperationResponse, TKqpScriptExecutionEvents::EvCancelScriptExecutionOperationResponse> {
    TEvCancelScriptExecutionOperationResponse(const Ydb::StatusIds::StatusCode status, bool executionEntryExists, NYql::TIssues issues)
        : Status(status)
        , ExecutionEntryExists(executionEntryExists)
        , Issues(std::move(issues))
    {}

    TEvCancelScriptExecutionOperationResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const bool ExecutionEntryExists = true;
    const NYql::TIssues Issues;
};

struct TEvScriptExecutionFinished : public TEventLocal<TEvScriptExecutionFinished, TKqpScriptExecutionEvents::EvScriptExecutionFinished> {
    struct TInfo {
        const bool ExecutionEntryExists = true;
        const bool AlreadyStopped = false;
    };

    TEvScriptExecutionFinished(const Ydb::StatusIds::StatusCode status, TInfo&& info, NYql::TIssues issues)
        : Status(status)
        , Info(std::move(info))
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const TInfo Info;
    NYql::TIssues Issues;
};

struct TEvSaveScriptResultMetaFinished : public TEventLocal<TEvSaveScriptResultMetaFinished, TKqpScriptExecutionEvents::EvSaveScriptResultMetaFinished> {
    TEvSaveScriptResultMetaFinished(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
};

struct TEvSaveScriptResultPartFinished : public TEventLocal<TEvSaveScriptResultPartFinished, TKqpScriptExecutionEvents::EvSaveScriptResultPartFinished> {
    TEvSaveScriptResultPartFinished(const Ydb::StatusIds::StatusCode status, const i64 savedSize, NYql::TIssues issues)
        : Status(status)
        , SavedSize(savedSize)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const i64 SavedSize = 0;
    NYql::TIssues Issues;
};

struct TEvSaveScriptResultFinished : public TEventLocal<TEvSaveScriptResultFinished, TKqpScriptExecutionEvents::EvSaveScriptResultFinished> {
    TEvSaveScriptResultFinished(const Ydb::StatusIds::StatusCode status, const size_t resultSetId, NYql::TIssues issues)
        : Status(status)
        , ResultSetId(resultSetId)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const size_t ResultSetId = 0;
    const NYql::TIssues Issues;
};

struct TEvFetchScriptResultsResponse : public TEventLocal<TEvFetchScriptResultsResponse, TKqpScriptExecutionEvents::EvFetchScriptResultsResponse> {
    TEvFetchScriptResultsResponse(const Ydb::StatusIds::StatusCode status, std::optional<Ydb::ResultSet>&& resultSet, const bool hasMoreResults, NYql::TIssues issues)
        : Status(status)
        , ResultSet(std::move(resultSet))
        , HasMoreResults(hasMoreResults)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    std::optional<Ydb::ResultSet> ResultSet;
    const bool HasMoreResults = false;
    const NYql::TIssues Issues;
};

struct TEvStartScriptExecutionBackgroundChecks : public TEventLocal<TEvStartScriptExecutionBackgroundChecks, TKqpScriptExecutionEvents::EvStartScriptExecutionBackgroundChecks> {
};

struct TEvSaveScriptExternalEffectRequest : public TEventLocal<TEvSaveScriptExternalEffectRequest, TKqpScriptExecutionEvents::EvSaveScriptExternalEffectRequest> {
    struct TDescription {
        explicit TDescription(TString customerSuppliedId)
            : CustomerSuppliedId(std::move(customerSuppliedId))
        {}

        const TString CustomerSuppliedId;
        std::vector<NKqpProto::TKqpExternalSink> Sinks;
        std::vector<TString> SecretNames;
    };

    explicit TEvSaveScriptExternalEffectRequest(TString customerSuppliedId)
        : Description(std::move(customerSuppliedId))
    {}

    TDescription Description;
};

struct TEvSaveScriptExternalEffectResponse : public TEventLocal<TEvSaveScriptExternalEffectResponse, TKqpScriptExecutionEvents::EvSaveScriptExternalEffectResponse> {
    TEvSaveScriptExternalEffectResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
};

struct TEvSaveScriptPhysicalGraphRequest : public TEventLocal<TEvSaveScriptPhysicalGraphRequest, TKqpScriptExecutionEvents::EvSaveScriptPhysicalGraphRequest> {
    explicit TEvSaveScriptPhysicalGraphRequest(NKikimrKqp::TQueryPhysicalGraph physicalGraph)
        : PhysicalGraph(std::move(physicalGraph))
    {}

    NKikimrKqp::TQueryPhysicalGraph PhysicalGraph;
};

struct TEvSaveScriptPhysicalGraphResponse : public TEventLocal<TEvSaveScriptPhysicalGraphResponse, TKqpScriptExecutionEvents::EvSaveScriptPhysicalGraphResponse> {
    TEvSaveScriptPhysicalGraphResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
};

struct TEvGetScriptExecutionPhysicalGraph : public TEventWithDatabaseId<TEvGetScriptExecutionPhysicalGraph, TKqpScriptExecutionEvents::EvGetScriptExecutionPhysicalGraph> {
    TEvGetScriptExecutionPhysicalGraph(TString database, TString executionId)
        : TEventWithDatabaseId(std::move(database))
        , ExecutionId(std::move(executionId))
    {}

    const TString ExecutionId;
};

struct TEvGetScriptPhysicalGraphResponse : public TEventLocal<TEvGetScriptPhysicalGraphResponse, TKqpScriptExecutionEvents::EvGetScriptPhysicalGraphResponse> {
    TEvGetScriptPhysicalGraphResponse(const Ydb::StatusIds::StatusCode status, std::optional<NKikimrKqp::TQueryPhysicalGraph>&& physicalGraph, const i64 generation, const bool executionEntryExists, NYql::TIssues issues)
        : Status(status)
        , PhysicalGraph(std::move(physicalGraph))
        , Generation(generation)
        , ExecutionEntryExists(executionEntryExists)
        , Issues(std::move(issues))
    {}

    TEvGetScriptPhysicalGraphResponse(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    const i64 Generation = 0;
    const bool ExecutionEntryExists = true;
    const NYql::TIssues Issues;
};

struct TEvScriptFinalizeRequest : public TEventLocal<TEvScriptFinalizeRequest, TKqpScriptExecutionEvents::EvScriptFinalizeRequest> {
    struct TDescription {
        TDescription(const EFinalizationStatus finalizationStatus, TString executionId, TString database,
            const Ydb::StatusIds::StatusCode operationStatus, const Ydb::Query::ExecStatus execStatus, NYql::TIssues issues, std::optional<NKqpProto::TKqpStatsQuery> queryStats,
            std::optional<TString> queryPlan, std::optional<TString> queryAst, const i64 leaseGeneration, const bool cancelledByUser)
            : FinalizationStatus(finalizationStatus)
            , ExecutionId(std::move(executionId))
            , Database(std::move(database))
            , OperationStatus(operationStatus)
            , ExecStatus(execStatus)
            , Issues(std::move(issues))
            , QueryStats(std::move(queryStats))
            , QueryPlan(std::move(queryPlan))
            , QueryAst(std::move(queryAst))
            , LeaseGeneration(leaseGeneration)
            , CancelledByUser(cancelledByUser)
        {}

        EFinalizationStatus FinalizationStatus = EFinalizationStatus::FS_COMMIT;
        const TString ExecutionId;
        const TString Database;
        const Ydb::StatusIds::StatusCode OperationStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        const Ydb::Query::ExecStatus ExecStatus = Ydb::Query::EXEC_STATUS_UNSPECIFIED;
        NYql::TIssues Issues;
        const std::optional<NKqpProto::TKqpStatsQuery> QueryStats;
        std::optional<TString> QueryPlan;
        std::optional<TString> QueryAst;
        const i64 LeaseGeneration = 0;
        const bool CancelledByUser = false; // User cancel API; false when status is CANCELLED from execution (e.g. streaming retry).
        std::optional<TString> QueryPlanCompressionMethod;
        std::optional<TString> QueryAstCompressionMethod;
    };

    TEvScriptFinalizeRequest(const EFinalizationStatus finalizationStatus, TString executionId, TString database,
        const Ydb::StatusIds::StatusCode operationStatus, const Ydb::Query::ExecStatus execStatus, NYql::TIssues issues,
        std::optional<NKqpProto::TKqpStatsQuery> queryStats, std::optional<TString> queryPlan, std::optional<TString> queryAst,
        const i64 leaseGeneration, const bool cancelledByUser)
        : Description(
            finalizationStatus, std::move(executionId), std::move(database), operationStatus, execStatus, std::move(issues),
            std::move(queryStats), std::move(queryPlan), std::move(queryAst), leaseGeneration, cancelledByUser
        )
    {}

    TDescription Description;
};

struct TEvScriptFinalizeResponse : public TEventLocal<TEvScriptFinalizeResponse, TKqpScriptExecutionEvents::EvScriptFinalizeResponse> {
    explicit TEvScriptFinalizeResponse(TString executionId)
        : ExecutionId(std::move(executionId))
    {}

    const TString ExecutionId;
};

struct TEvSaveScriptFinalStatusResponse : public TEventLocal<TEvSaveScriptFinalStatusResponse, TKqpScriptExecutionEvents::EvSaveScriptFinalStatusResponse> {
    bool ApplicateScriptExternalEffectRequired = false;
    bool FinalStatusAlreadySaved = false;
    bool ExecutionEntryExists = true;
    TString CustomerSuppliedId;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::vector<NKqpProto::TKqpExternalSink> Sinks;
    std::vector<TString> SecretNames;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    NYql::TIssues Issues;
};

struct TEvDescribeSecretsResponse : public TEventLocal<TEvDescribeSecretsResponse, TKqpScriptExecutionEvents::EvDescribeSecretsResponse> {
    struct TDescription {
        TDescription(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        explicit TDescription(std::vector<TString> secretValues)
            : SecretValues(std::move(secretValues))
            , Status(Ydb::StatusIds::SUCCESS)
        {}

        const std::vector<TString> SecretValues;
        const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        const NYql::TIssues Issues;
    };

    explicit TEvDescribeSecretsResponse(TDescription description)
        : Description(std::move(description))
    {}

    const TDescription Description;
};

struct TEvDescribeResourceIdResponse : public TEventLocal<TEvDescribeResourceIdResponse, TKqpScriptExecutionEvents::EvDescribeResourceIdResponse> {
    struct TDescription {
        TDescription(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
            : Status(status)
            , Issues(std::move(issues))
        {}

        explicit TDescription(TString resourceId)
            : ResourceId(std::move(resourceId))
            , Status(Ydb::StatusIds::SUCCESS)
        {}

        const TString ResourceId;
        const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        const NYql::TIssues Issues;
    };

    explicit TEvDescribeResourceIdResponse(TDescription description)
        : Description(std::move(description))
    {}

    const TDescription Description;
};

struct TEvScriptExecutionsTablesCreationFinished : public TEventLocal<TEvScriptExecutionsTablesCreationFinished, TKqpScriptExecutionEvents::EvScriptExecutionsTableCreationFinished> {
    TEvScriptExecutionsTablesCreationFinished(const bool success, NYql::TIssues issues)
        : Success(success)
        , Issues(std::move(issues))
    {}

    const bool Success = true;
    const NYql::TIssues Issues;
};

struct TEvScriptExecutionRestarted : public TEventLocal<TEvScriptExecutionRestarted, TKqpScriptExecutionEvents::EvScriptExecutionRestarted> {
    TEvScriptExecutionRestarted(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues)
        : Status(status)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const NYql::TIssues Issues;
};

struct TEvListExpiredLeasesResponse : public TEventLocal<TEvListExpiredLeasesResponse, TKqpScriptExecutionEvents::EvListExpiredLeasesResponse> {
    struct TLeaseInfo {
        TString Database;
        TString ExecutionId;
    };

    TEvListExpiredLeasesResponse(const Ydb::StatusIds::StatusCode status, std::vector<TLeaseInfo>&& leases, NYql::TIssues issues)
        : Status(status)
        , Leases(std::move(leases))
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const std::vector<TLeaseInfo> Leases;
    const NYql::TIssues Issues;
};

struct TEvRefreshScriptExecutionLeasesResponse : public TEventLocal<TEvRefreshScriptExecutionLeasesResponse, TKqpScriptExecutionEvents::EvRefreshScriptExecutionLeasesResponse> {
    TEvRefreshScriptExecutionLeasesResponse(const bool success, NYql::TIssues issues)
        : Success(success)
        , Issues(std::move(issues))
    {}

    const bool Success = true;
    const NYql::TIssues Issues;
};

struct TEvSaveScriptProgressResponse : public TEventLocal<TEvSaveScriptProgressResponse, TKqpScriptExecutionEvents::EvSaveScriptProgressResponse> {
    TEvSaveScriptProgressResponse(const Ydb::StatusIds::StatusCode status, const bool astSaved, NYql::TIssues issues)
        : Status(status)
        , AstSaved(astSaved)
        , Issues(std::move(issues))
    {}

    const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    const bool AstSaved = false;
    const NYql::TIssues Issues;
};

} // namespace NKikimr::NKqp
