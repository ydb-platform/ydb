#include "yql_s3_sink_factory.h"
#include "yql_s3_write_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

void RegisterS3WriteActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, IHTTPGateway::TPtr gateway, const std::shared_ptr<NYql::NS3::TRetryConfig>&) {
    factory.RegisterSink<NS3::TSink>("S3Sink",
        [credentialsFactory, gateway](NS3::TSink&& settings, IDqAsyncIoFactory::TSinkArguments&& args) {
            return CreateS3WriteActor(args.TypeEnv, *args.HolderFactory.GetFunctionRegistry(), args.RandomProvider, gateway, std::move(settings), args.OutputIndex, args.TxId, args.SecureParams, args.Callback, credentialsFactory);
        });
}

}
