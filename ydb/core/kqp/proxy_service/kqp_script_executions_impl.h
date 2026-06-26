#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/public/api/protos/ydb_query.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/maybe.h>
#include <util/system/types.h>

#include <optional>
#include <vector>

namespace NKikimr::NKqp::NPrivate {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvCreateScriptOperationResponse = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvLeaseCheckResult,
        EvFinalizeScriptLeaseResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    // Events
    struct TEvCreateScriptOperationResponse : public TEventLocal<TEvCreateScriptOperationResponse, EvCreateScriptOperationResponse> {
        TEvCreateScriptOperationResponse(Ydb::StatusIds::StatusCode statusCode, NYql::TIssues&& issues)
            : Status(statusCode)
            , Issues(std::move(issues))
        {}

        TEvCreateScriptOperationResponse(TString executionId, NYql::TIssues&& issues)
            : Status(Ydb::StatusIds::SUCCESS)
            , ExecutionId(std::move(executionId))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        const TString ExecutionId;
        const NYql::TIssues Issues;
    };

    struct TEvLeaseCheckResult : public TEventLocal<TEvLeaseCheckResult, EvLeaseCheckResult> {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues Issues;
        TMaybe<Ydb::StatusIds::StatusCode> OperationStatus;
        TMaybe<Ydb::Query::ExecStatus> ExecutionStatus;
        TMaybe<EFinalizationStatus> FinalizationStatus;
        NYql::TIssues OperationIssues;
        TActorId RunScriptActorId;
        bool EntryExists = true;
        bool HasRetryPolicy = false;
        bool LeaseExpired = false;
        bool RetryRequired = false;
        i64 LeaseGeneration = 0;
        std::optional<std::vector<Ydb::Query::ResultSetMeta>> ResultSetMetas;
    };

    struct TEvFinalizeScriptLeaseResult : public TEventLocal<TEvFinalizeScriptLeaseResult, EvFinalizeScriptLeaseResult> {
        struct TInfo {
            const bool LeaseVerified = false;
            const bool ExecutionEntryExists = true;
        };

        TEvFinalizeScriptLeaseResult(const Ydb::StatusIds::StatusCode status, TInfo&& info, NYql::TIssues issues = {})
            : Status(status)
            , Info(std::move(info))
            , Issues(std::move(issues))
        {}

        const Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        const TInfo Info;
        const NYql::TIssues Issues;
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
IActor* CreateCreateScriptOperationQueryActor(TString executionId, const TActorId& runScriptActorId, NKikimrKqp::TEvQueryRequest record, NKikimrKqp::TScriptExecutionOperationMeta meta);

// Get current status of execution
IActor* CreateCheckLeaseStatusActor(TString database, TString executionId);

// Checks lease of execution, finishes execution if its lease is off, returns current status
IActor* CreateFinalizeScriptLeaseActor(const TActorId& replyActorId, TString database, TString executionId);

} // namespace NKikimr::NKqp::NPrivate
