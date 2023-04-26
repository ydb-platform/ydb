#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>


namespace NYql::NDq {

void RegisterS3WriteActorFactory(
    TDqAsyncIoFactory& factory,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy = GetHTTPDefaultRetryPolicy());
}
