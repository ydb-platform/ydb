#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/clickhouse/proto/source.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h> 
#include <library/cpp/actors/core/actor.h>

namespace NYql::NDq {

std::pair<NYql::NDq::IDqSourceActor*, NActors::IActor*> CreateClickHouseReadActor(
    IHTTPGateway::TPtr gateway,
    NCH::TSource&& params,
    ui64 inputIndex,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    IDqSourceActor::ICallbacks* callback,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

} // namespace NYql::NDq

