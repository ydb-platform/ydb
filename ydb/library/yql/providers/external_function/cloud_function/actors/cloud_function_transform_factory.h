#pragma once

#include <ydb/library/yql/dq/actors/transform/dq_transform_actor_factory.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NYql::NDq {

void RegisterTransformCloudFunction(TDqTransformActorFactory& factory, IHTTPGateway::TPtr gateway,
                                    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory);

}