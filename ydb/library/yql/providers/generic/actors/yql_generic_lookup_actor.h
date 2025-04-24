#pragma once

#include <ydb/library/actors/core/actor.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NDq {
    std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*>
    CreateGenericLookupActor(
        NConnector::IClient::TPtr connectorClient,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        NActors::TActorId parentId,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> keyTypeHelper,
        Generic::TLookupSource&& lookupSource,
        const NKikimr::NMiniKQL::TStructType* keyType,
        const NKikimr::NMiniKQL::TStructType* payloadType,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const size_t maxKeysInRequest);

} // namespace NYql::NDq
