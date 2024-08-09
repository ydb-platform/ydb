#pragma once

#include <ydb/core/kqp/common/events/workload_service.h>


namespace NKikimr::NKqp::NWorkload {

// Pool state holder
NActors::IActor* CreatePoolHandlerActor(const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, NMonitoring::TDynamicCounterPtr counters);

// Fetch pool and create default pool if needed
NActors::IActor* CreatePoolResolverActor(TEvPlaceRequestIntoPool::TPtr event, bool defaultPoolExists);

// Fetch and create pool in scheme shard
NActors::IActor* CreatePoolFetcherActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, TIntrusiveConstPtr<NACLib::TUserToken> userToken);
NActors::IActor* CreatePoolCreatorActor(const NActors::TActorId& replyActorId, const TString& database, const TString& poolId, const NResourcePool::TPoolSettings& poolConfig, TIntrusiveConstPtr<NACLib::TUserToken> userToken, NACLibProto::TDiffACL diffAcl);

// Checks that database is serverless
NActors::IActor* CreateDatabaseFetcherActor(const NActors::TActorId& replyActorId, const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr, NACLib::EAccessRights checkAccess = NACLib::EAccessRights::NoAccess);

// Cpu load fetcher actor
NActors::IActor* CreateCpuLoadFetcherActor(const NActors::TActorId& replyActorId);

}  // NKikimr::NKqp::NWorkload
