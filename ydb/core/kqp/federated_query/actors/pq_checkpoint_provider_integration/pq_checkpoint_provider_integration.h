#pragma once

#include <ydb/core/fq/libs/checkpointing/checkpoint_provider_integration.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

namespace NKikimr::NKqp {

NFq::ICheckpointProviderIntegration::TPtr CreatePqCheckpointProviderIntegration(
    NActors::TActorSystem* actorSystem,
    NYql::IPqStaticGateway::TPtr pqGateway,
    NYdb::TDriver driver,
    NYql::IStructuredTokenCredentialsFactory::TPtr credentialsFactory);

} // namespace NKikimr::NKqp
