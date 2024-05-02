#pragma once
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

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

// Updates lease deadline in database.
NActors::IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, const TString& database, const TString& executionId, TDuration leaseDuration, TIntrusivePtr<TKqpCounters> counters);

// Store and fetch results.
NActors::IActor* CreateSaveScriptExecutionResultMetaActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, const TString& serializedMeta);
NActors::IActor* CreateSaveScriptExecutionResultActor(const NActors::TActorId& runScriptActorId, const TString& database, const TString& executionId, i32 resultSetId, TMaybe<TInstant> expireAt, i64 firstRow, i64 accumulatedSize, Ydb::ResultSet&& resultSet);
NActors::IActor* CreateGetScriptExecutionResultActor(const NActors::TActorId& replyActorId, const TString& database, const TString& executionId, i32 resultSetIndex, i64 offset, i64 rowsLimit, i64 sizeLimit, TInstant operationDeadline);

// Compute external effects and updates status in database
NActors::IActor* CreateSaveScriptExternalEffectActor(TEvSaveScriptExternalEffectRequest::TPtr ev);
NActors::IActor* CreateSaveScriptFinalStatusActor(const NActors::TActorId& finalizationActorId, TEvScriptFinalizeRequest::TPtr ev);
NActors::IActor* CreateScriptFinalizationFinisherActor(const NActors::TActorId& finalizationActorId, const TString& executionId, const TString& database, std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues);
NActors::IActor* CreateScriptProgressActor(const TString& executionId, const TString& database, const TString& queryPlan, const TString& queryStats);

} // namespace NKikimr::NKqp
