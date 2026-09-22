#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

namespace NYql::NDq {

// Query-scoped coordinator for PQ readers: coalesces and caches consumer descriptions
// per connection, returning offsets only for the requested partitions. Fatal failures
// fail pending and future requests; error delivery is retried with backoff on transport
// failures. The query owner stops the actor with TEvPoison.
NActors::IActor* CreateDqPqControlPlaneActor(
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IPqStaticGateway::TPtr pqGateway,
    const THashMap<TString, TString>& secureParams);

void RegisterDqPqControlPlaneActorFactory(
    TDqAsyncIoFactory& factory,
    NYdb::TDriver driver,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IPqStaticGateway::TPtr pqGateway);

} // namespace NYql::NDq
