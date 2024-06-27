#pragma once

#include <ydb/core/kqp/common/events/workload_service.h>


namespace NKikimr::NKqp::NWorkload {

// Pool state holder
NActors::IActor* CreatePoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters);

// Fetch and create pool in scheme shard
NActors::IActor* CreatePoolFetcherActor(TEvPlaceRequestIntoPool::TPtr event);
NActors::IActor* CreatePoolCreatorActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, TIntrusiveConstPtr<NACLib::TUserToken> userToken);

}  // NKikimr::NKqp::NWorkload
