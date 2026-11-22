#pragma once

#include "yql_s3_actors_factory_impl.h"

namespace NYql::NDq {

<<<<<<< HEAD
inline void RegisterS3ReadActorFactory(
    TDqAsyncIoFactory& factory,
    IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway = IHTTPGateway::Make(),
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy = GetHTTPDefaultRetryPolicy(),
    const TS3ReadActorFactoryConfig& factoryConfig = {},
    ::NMonitoring::TDynamicCounterPtr counters = nullptr,
    bool allowLocalFiles = false) {
        CreateS3ActorsFactory()->RegisterS3ReadActorFactory(
            factory, credentialsFactory, gateway, retryPolicy, factoryConfig, counters, allowLocalFiles
        );
=======
inline void RegisterS3ReadActorFactory(TDqAsyncIoFactory& factory, IStructuredTokenCredentialsFactory::TPtr credentialsFactory,
    IHTTPGateway::TPtr gateway = IHTTPGateway::Make(), const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy = GetHTTPDefaultRetryPolicy(),
    const TS3ReadActorFactoryConfig& factoryConfig = {}, ::NMonitoring::TDynamicCounterPtr counters = nullptr, bool allowLocalFiles = false,
    ::NMonitoring::TDynamicCounterPtr cpuCounters = nullptr) {
    CreateS3ActorsFactory()->RegisterS3ReadActorFactory(
        factory, credentialsFactory, gateway, retryPolicy, factoryConfig, counters, allowLocalFiles, cpuCounters);
>>>>>>> 66cd1770f22 (Fix S3 reader CPU quota leaks on early exit and cancellation while waiting for admission (#59849))
}

}
