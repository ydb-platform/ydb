#pragma once

#include "yql_s3_actors_factory_impl.h"

namespace NYql::NDq {

void RegisterS3ReadActorFactory(
    TDqAsyncIoFactory& factory,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway = IHTTPGateway::Make(),
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy = GetHTTPDefaultRetryPolicy(),
    const TS3ReadActorFactoryConfig& factoryConfig = {},
    ::NMonitoring::TDynamicCounterPtr counters = nullptr,
    bool allowLocalFiles = false) {
        CreateS3ActorsFactory()->RegisterS3ReadActorFactory(
            factory, credentialsFactory, gateway, retryPolicy, factoryConfig, counters, allowLocalFiles
        );
}

}
