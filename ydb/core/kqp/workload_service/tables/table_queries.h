#pragma once

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NKqp::NWorkload {

// Creates all needed tables for workload service
NActors::IActor* CreateTablesCreator();

// Removes all queries for local node from tables
NActors::IActor* CreateCleanupTablesActor();

// Updates pool lease and returns pool description
NActors::IActor* CreateRefreshPoolStateActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters);

// Push / Start / Finish requests in pool
NActors::IActor* CreateDelayRequestActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, const TString& sessionId, TInstant startTime, TMaybe<TInstant> waitDeadline, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters);
NActors::IActor* CreateStartRequestActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, const std::optional<TString>& sessionId, TDuration leaseDuration, NMonitoring::TDynamicCounterPtr counters);
NActors::IActor* CreateCleanupRequestsActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, const std::vector<TString>& sessionIds, NMonitoring::TDynamicCounterPtr counters);

}  // NKikimr::NKqp::NWorkload
