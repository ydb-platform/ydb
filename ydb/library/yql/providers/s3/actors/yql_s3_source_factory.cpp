#include "yql_s3_source_factory.h"
#include "yql_s3_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>

namespace NYql::NDq {

void RegisterS3ReadActorFactory(
        TDqSourceFactory& factory,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        IHTTPGateway::TPtr gateway,
        const std::shared_ptr<NYql::NS3::TRetryConfig>& retryConfig) {
    factory.Register<NS3::TSource>("S3Source",
        [credentialsFactory, gateway, retryConfig](NS3::TSource&& settings, IDqSourceActorFactory::TArguments&& args) {
                return CreateS3ReadActor(gateway, std::move(settings), args.InputIndex, args.SecureParams, args.TaskParams, args.ComputeActorId, credentialsFactory, retryConfig);
        });
}

}
