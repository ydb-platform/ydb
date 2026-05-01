#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <ydb/library/yql/providers/solomon/actors/dq_solomon_actors_util.h>
#include <ydb/library/yql/providers/solomon/common/util.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqSolomonReadActor(
    NYql::NSo::NProto::TDqSolomonSource&& source,
    ui64 inputIndex,
    ui64 metricsQueueConsumersCountDelta,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const NActors::TActorId& computeActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    NKikimr::NMiniKQL::TProgramBuilder& programBuilder,
    const THashMap<TString, TString>& secureParams,
    IMemoryQuotaManager::TPtr memoryQuotaManager,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NSo::TSolomonReadActorConfig& cfg);

void RegisterDQSolomonReadActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq
