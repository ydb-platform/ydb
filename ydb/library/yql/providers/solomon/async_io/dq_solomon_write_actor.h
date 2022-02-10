#pragma once

#include <ydb/library/yql/utils/actors/http_sender.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sinks.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/http/http_proxy.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NYql::NDq {

constexpr i64 DqSolomonDefaultFreeSpace = 16_MB;

std::pair<NYql::NDq::IDqSinkActor*, NActors::IActor*> CreateDqSolomonWriteActor(
    NYql::NSo::NProto::TDqSolomonShard&& settings,
    ui64 outputIndex,
    const THashMap<TString, TString>& secureParams,
    NYql::NDq::IDqSinkActor::ICallbacks* callbacks,
    const NMonitoring::TDynamicCounterPtr& counters, 
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, 
    i64 freeSpace = DqSolomonDefaultFreeSpace);

void RegisterDQSolomonWriteActorFactory(TDqSinkFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory); 

} // namespace NYql::NDq
