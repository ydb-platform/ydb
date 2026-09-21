#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

namespace NYql::NDq {

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
