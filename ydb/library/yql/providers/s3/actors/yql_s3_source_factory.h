#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>

#include <util/generic/size_literals.h>

namespace NYql::NDq {

struct TS3ReadActorFactoryConfig {
    ui64 RowsInBatch = 1000;
    ui64 MaxInflight = 20;
    ui64 DataInflight = 200_MB;
    ui64 FileSizeLimit = 2_GB;
    ui64 BlockFileSizeLimit = 50_GB;
    std::unordered_map<TString, ui64> FormatSizeLimits;
};

void RegisterS3ReadActorFactory(
    TDqAsyncIoFactory& factory,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway = IHTTPGateway::Make(),
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy = GetHTTPDefaultRetryPolicy(),
    const TS3ReadActorFactoryConfig& = {},
    ::NMonitoring::TDynamicCounterPtr counters = nullptr);

TS3ReadActorFactoryConfig CreateReadActorFactoryConfig(const ::NYql::TS3GatewayConfig& s3Config);

}
