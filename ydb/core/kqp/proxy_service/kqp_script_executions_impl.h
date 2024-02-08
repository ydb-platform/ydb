#pragma once

#include "kqp_script_executions.h"

#include <ydb/library/actors/core/event_local.h>

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
        {
        }

        TEvLeaseCheckResult(TMaybe<Ydb::StatusIds::StatusCode> operationStatus,
            TMaybe<Ydb::Query::ExecStatus> executionStatus,
            TMaybe<NYql::TIssues> operationIssues,
            const NActors::TActorId& runScriptActorId,
            bool leaseExpired,
            TMaybe<EFinalizationStatus> finalizationStatus)
            : Status(Ydb::StatusIds::SUCCESS)
            , OperationStatus(operationStatus)
            , ExecutionStatus(executionStatus)
            , OperationIssues(operationIssues)
            , RunScriptActorId(runScriptActorId)
            , LeaseExpired(leaseExpired)
            , FinalizationStatus(finalizationStatus)
        {}

        const Ydb::StatusIds::StatusCode Status;
        const NYql::TIssues Issues;
        TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
        TMaybe<Ydb::Query::ExecStatus> ExecutionStatus;
        TMaybe<NYql::TIssues> OperationIssues;
        const NActors::TActorId RunScriptActorId;
        const bool LeaseExpired;
        const TMaybe<EFinalizationStatus> FinalizationStatus;
    };
};

// Writes new script into db.
// If lease duration is zero, default one will be taken.
NActors::IActor* CreateCreateScriptOperationQueryActor(const TString& executionId, const NActors::TActorId& runScriptActorId, const NKikimrKqp::TEvQueryRequest& record,
                                                       TDuration operationTtl, TDuration resultsTtl, TDuration leaseDuration = TDuration::Zero());

// Checks lease of execution, finishes execution if its lease is off, returns current status
NActors::IActor* CreateCheckLeaseStatusActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, ui64 cookie = 0);

} // namespace NKikimr::NKqp::NPrivate
