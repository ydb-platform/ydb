#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <library/cpp/actors/core/actor.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqSourceActor*, NActors::IActor*> CreateS3ReadActor(
    IHTTPGateway::TPtr gateway,
    NS3::TSource&& params,
    ui64 inputIndex,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    IDqSourceActor::ICallbacks* callback,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const std::shared_ptr<NYql::NS3::TRetryConfig>& retryConfig = nullptr);

} // namespace NYql::NDq
