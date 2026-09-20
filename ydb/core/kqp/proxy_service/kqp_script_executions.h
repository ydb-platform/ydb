#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>

#include <optional>

namespace Ydb {

class ResultSet;

} // namespace Ydb

namespace NKikimrConfig {

class TQueryServiceConfig;

} // namespace NKikimrConfig

namespace NKikimrKqp {

class TQueryPhysicalGraph;

} // namespace NKikimrKqp

namespace NKikimr::NKqp {

// Creates all needed tables.
// Sends result event back when the work is done.
IActor* CreateScriptExecutionsTablesCreator(const bool enableSecureScriptExecutions, const ui64 generation = 0);

// Create script execution and run it.
IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters, const TDuration maxRunTime);

// Operation API impl.
IActor* CreateForgetScriptExecutionOperationActor(TEvForgetScriptExecutionOperation::TPtr&& ev);
IActor* CreateGetScriptExecutionOperationActor(TEvGetScriptExecutionOperation::TPtr&& ev);
IActor* CreateListScriptExecutionOperationsActor(TEvListScriptExecutionOperations::TPtr&& ev);
IActor* CreateCancelScriptExecutionOperationActor(TEvCancelScriptExecutionOperation::TPtr&& ev, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters);

// Updates lease deadline in database.
IActor* CreateScriptLeaseUpdateActor(const TActorId& runScriptActorId, TString database, TString executionId, const TDuration leaseDuration, const i64 leaseGeneration);

// Store and fetch results.
IActor* CreateSaveScriptExecutionResultMetaActor(const TActorId& runScriptActorId, TString database, TString executionId, TString serializedMeta, const i64 leaseGeneration);
IActor* CreateSaveScriptExecutionResultActor(const TActorId& runScriptActorId, TString database, TString executionId, const i32 resultSetId, const std::optional<TInstant> expireAt, const i64 firstRow, const i64 accumulatedSize, Ydb::ResultSet&& resultSet);
IActor* CreateGetScriptExecutionResultActor(const TActorId& replyActorId, TString database, TString executionId, std::optional<TString> userSID, const i32 resultSetIndex, const i64 offset, const i64 rowsLimit, const i64 sizeLimit, const TInstant operationDeadline);

// Compute external effects and updates status in database
IActor* CreateSaveScriptExternalEffectActor(const TActorId& replyActorId, TString database, TString executionId, TEvSaveScriptExternalEffectRequest::TDescription&& info, const i64 leaseGeneration);
IActor* CreateSaveScriptFinalStatusActor(const TActorId& finalizationActorId, TEvScriptFinalizeRequest::TPtr&& ev);
IActor* CreateScriptFinalizationFinisherActor(const TActorId& finalizationActorId, TString database, TString executionId, const std::optional<Ydb::StatusIds::StatusCode> operationStatus, NYql::TIssues operationIssues, const i64 leaseGeneration);
IActor* CreateScriptProgressActor(TString database, TString executionId, std::optional<TString> queryPlan, std::optional<TString> ast, const i64 leaseGeneration, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig);

// Check lease expiration for running script execution operations
IActor* CreateRefreshScriptExecutionLeasesActor(const TActorId& replyActorId, NKikimrConfig::TQueryServiceConfig queryServiceConfig, TIntrusivePtr<TKqpCounters> counters);

// Script execution physical graph management
IActor* CreateSaveScriptExecutionPhysicalGraphActor(const TActorId& replyActorId, TString database, TString executionId, NKikimrKqp::TQueryPhysicalGraph physicalGraph, const i64 leaseGeneration, NKikimrConfig::TQueryServiceConfig queryServiceConfig);
IActor* CreateGetScriptExecutionPhysicalGraphActor(const TActorId& replyActorId, TString database, TString executionId);

} // namespace NKikimr::NKqp
