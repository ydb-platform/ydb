#pragma once
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

namespace NKikimr::NKqp {

// Creates all needed tables.
// Sends result event back when the work is done.
NActors::IActor* CreateScriptExecutionsTablesCreator(THolder<NActors::IEventBase> resultEvent);

// Create script execution and run it.
NActors::IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, TDuration maxRunTime = SCRIPT_TIMEOUT_LIMIT);

// Operation API impl.
NActors::IActor* CreateForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr ev);
NActors::IActor* CreateGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr ev);
NActors::IActor* CreateListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr ev);
NActors::IActor* CreateCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr ev);

// Updates status in database.
NActors::IActor* CreateScriptExecutionFinisher(
    const TString& executionId,
    const TString& database,
    ui64 leaseGeneration,
    Ydb::StatusIds::StatusCode operationStatus,
    Ydb::Query::ExecStatus execStatus,
    NYql::TIssues issues,
    TMaybe<NKqpProto::TKqpStatsQuery> queryStats = Nothing(),
    TMaybe<TString> queryPlan = Nothing(),
    TMaybe<TString> queryAst = Nothing()
);

// Updates lease deadline in database.
NActors::IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration, TIntrusivePtr<TKqpCounters> counters);

// Store and fetch results.
NActors::IActor* CreateSaveScriptExecutionResultMetaActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, const TString& serializedMeta);
NActors::IActor* CreateSaveScriptExecutionResultActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, i32 resultSetId, TInstant expireAt, i64 firstRow, std::vector<TString>&& serializedRows);
NActors::IActor* CreateGetScriptExecutionResultActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, i32 resultSetId, i64 offset, i64 limit);

} // namespace NKikimr::NKqp
