#pragma once

#include "kqp_script_executions.h"

#include <library/cpp/actors/core/event_local.h>

namespace NKikimr::NKqp::NPrivate {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvCreateScriptOperationResponse = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateTableResponse,
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

    struct TEvCreateTableResponse : public NActors::TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {
        TEvCreateTableResponse() = default;
    };

    struct TEvLeaseCheckResult : public NActors::TEventLocal<TEvLeaseCheckResult, EvLeaseCheckResult> {
        TEvLeaseCheckResult(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues)
            : Status(statusCode)
            , Issues(std::move(issues))
        {
        }

        TEvLeaseCheckResult(TMaybe<Ydb::StatusIds::StatusCode> operationStatus,
            TMaybe<Ydb::Query::ExecStatus> executionStatus,
            TMaybe<NYql::TIssues> operationIssues,
            const NActors::TActorId& runScriptActorId)
            : Status(Ydb::StatusIds::SUCCESS)
            , OperationStatus(operationStatus)
            , ExecutionStatus(executionStatus)
            , OperationIssues(operationIssues)
            , RunScriptActorId(runScriptActorId)
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        const TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
        const TMaybe<Ydb::Query::ExecStatus> ExecutionStatus;
        const TMaybe<NYql::TIssues> OperationIssues;
        const NActors::TActorId RunScriptActorId;
    };
};

// Writes new script into db.
// If lease duration is zero, default one will be taken.
NActors::IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record, TDuration leaseDuration = TDuration::Zero());

// Checks lease of execution, finishes execution if its lease is off, returns current status
NActors::IActor* CreateCheckLeaseStatusActor(const TString& database, const TString& executionId, Ydb::StatusIds::StatusCode statusOnExpiredLease = Ydb::StatusIds::ABORTED, ui64 cookie = 0);

} // namespace NKikimr::NKqp::NPrivate
