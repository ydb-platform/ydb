#pragma once 
 
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
 

namespace NYql::NDq { 
 
void RegisterS3ReadActorFactory(
    TDqSourceFactory& factory,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway = IHTTPGateway::Make(),
    const std::shared_ptr<NYql::NS3::TRetryConfig>& retryConfig = nullptr);
 
} 
