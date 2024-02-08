#pragma once

#include <ydb/library/yql/utils/actors/http_sender.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {

constexpr i64 DqSolomonDefaultFreeSpace = 16_MB;

std::pair<NYql::NDq::IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqSolomonWriteActor(
    NYql::NSo::NProto::TDqSolomonShard&& settings,
    ui64 outputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    const THashMap<TString, TString>& secureParams,
    NYql::NDq::IDqComputeActorAsyncOutput::ICallbacks* callbacks,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    i64 freeSpace = DqSolomonDefaultFreeSpace);

void RegisterDQSolomonWriteActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

TString GetSolomonUrl(const TString& endpoint, bool useSsl, const TString& project, const TString& cluster, const TString& service, const ::NYql::NSo::NProto::ESolomonClusterType& type);

} // namespace NYql::NDq
