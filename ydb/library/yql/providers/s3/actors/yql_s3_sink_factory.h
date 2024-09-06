#pragma once

#include "yql_s3_actors_factory_impl.h"

namespace NYql::NDq {

void RegisterS3WriteActorFactory(
    TDqAsyncIoFactory& factory,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy = GetHTTPDefaultRetryPolicy()) {
        CreateS3ActorsFactory()->RegisterS3WriteActorFactory(factory, credentialsFactory, gateway, retryPolicy);
}

}
