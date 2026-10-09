#pragma once

#include <ydb/core/fq/libs/checkpointing/checkpoint_provider_integration.h>
#include <ydb/core/kqp/common/kqp_streaming_query_controller.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

namespace NKikimr::NKqp {

IKqpStreamingQueryControllerFactory::TPtr CreateStreamingQueryControllerFactory(
    NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
    NFq::TCheckpointProviderIntegrations checkpointProviderIntegrations);

} // namespace NKikimr::NKqp
