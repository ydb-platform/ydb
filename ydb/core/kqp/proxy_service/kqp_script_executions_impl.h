#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NKqp::NPrivate {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvCreateScriptOperationResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvLeaseCheckResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvCreateScriptOperationResponse : public NActors::TEventLocal<TEvCreateScriptOperationResponse, EvCreateScriptOperationResponse> {
        TEvCreateScriptOperationResponse(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues)
            : Status(statusCode)
            , Issues(std::move(issues))
        {
        }

        TEvCreateScriptOperationResponse(TString executionId)
            : Status(Ydb::StatusIds::SUCCESS)
            , ExecutionId(std::move(executionId))
        {
        }

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        const TString ExecutionId;
    };

    struct TEvLeaseCheckResult : public NActors::TEventLocal<TEvLeaseCheckResult, EvLeaseCheckResult> {
        TEvLeaseCheckResult(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues)
            : Status(statusCode)
            , Issues(std::move(issues))
            , LeaseExpired(false)
            , RetryRequired(false)
        {}

        TEvLeaseCheckResult(TMaybe<Ydb::StatusIds::StatusCode> operationStatus, TMaybe<Ydb::Query::ExecStatus> executionStatus,
            TMaybe<NYql::TIssues> operationIssues, const NActors::TActorId& runScriptActorId, bool leaseExpired,
            TMaybe<EFinalizationStatus> finalizationStatus, bool retryRequired, i64 leaseGeneration, bool hasRetryPolicy)
            : Status(Ydb::StatusIds::SUCCESS)
            , OperationStatus(operationStatus)
            , ExecutionStatus(executionStatus)
            , OperationIssues(operationIssues)
            , RunScriptActorId(runScriptActorId)
            , LeaseExpired(leaseExpired)
            , FinalizationStatus(finalizationStatus)
            , RetryRequired(retryRequired)
            , LeaseGeneration(leaseGeneration)
            , HasRetryPolicy(hasRetryPolicy)
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
        TMaybe<Ydb::Query::ExecStatus> ExecutionStatus;
        TMaybe<NYql::TIssues> OperationIssues;
        const NActors::TActorId RunScriptActorId;
        const bool LeaseExpired = false;
        const TMaybe<EFinalizationStatus> FinalizationStatus;
        const bool RetryRequired = false;
        const i64 LeaseGeneration = 0;
        const bool HasRetryPolicy = false;
    };
};

// stored in column "lease_state" of .metadata/script_execution_leases table
enum class ELeaseState {
    ScriptRunning = 0,
    ScriptFinalizing = 1,
    WaitRetry = 2
};

// Writes new script into db.
// If lease duration is zero, default one will be taken.
NActors::IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record,
                                                       const NKikimrKqp::TScriptExecutionOperationMeta& meta);

// Checks lease of execution, finishes execution if its lease is off, returns current status
NActors::IActor* CreateCheckLeaseStatusActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, ui64 cookie = 0);

} // namespace NKikimr::NKqp::NPrivate
