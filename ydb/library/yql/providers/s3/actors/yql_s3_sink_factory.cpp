#include "yql_s3_sink_factory.h"
#include "yql_s3_write_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

void RegisterS3WriteActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, IHTTPGateway::TPtr gateway, const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy) {
    factory.RegisterSink<NS3::TSink>("S3Sink",
        [credentialsFactory, gateway, retryPolicy](NS3::TSink&& settings, IDqAsyncIoFactory::TSinkArguments&& args) {

            TStringBuilder prefixBuilder;

            auto jobId = args.TaskParams.Value("fq.job_id", "");
            if (jobId) {
                prefixBuilder << jobId << "_";
            }

            auto restartCount = args.TaskParams.Value("fq.restart_count", "");
            if (restartCount) {
                prefixBuilder << restartCount << "_";
            }

            return CreateS3WriteActor(args.TypeEnv, *args.HolderFactory.GetFunctionRegistry(), args.RandomProvider,
                gateway, std::move(settings), args.OutputIndex, args.StatsLevel, args.TxId, prefixBuilder,
                args.SecureParams, args.Callback, credentialsFactory, retryPolicy);
        });
}

}
