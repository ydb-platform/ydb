#include "yql_s3_actors_factory_impl.h"
#include "yql_s3_write_actor.h"
#include "yql_s3_applicator_actor.h"

#include <util/system/platform.h>
#if defined(_linux_) || defined(_darwin_)
#include "yql_s3_read_actor.h"
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/registerFormats.h>
#endif

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>


namespace NYql::NDq {

    class TS3ActorsFactory : public IS3ActorsFactory {
    public:
        THolder<NActors::IActor> CreateS3ApplicatorActor(
            NActors::TActorId parentId,
            IHTTPGateway::TPtr gateway,
            const TString& queryId,
            const TString& jobId,
            std::optional<ui32> restartNumber,
            bool commit,
            const THashMap<TString, TString>& secureParams,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            const NYql::NDqProto::TExternalEffect& externalEffect) override {

            return MakeS3ApplicatorActor(
                parentId, gateway, queryId, jobId, restartNumber, commit, secureParams, credentialsFactory, externalEffect
            );
        }

        void RegisterS3WriteActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy) override {

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

        void RegisterS3ReadActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
            const TS3ReadActorFactoryConfig& cfg,
            ::NMonitoring::TDynamicCounterPtr counters,
            bool allowLocalFiles) override {

            #if defined(_linux_) || defined(_darwin_)
                NDB::registerFormats();
                factory.RegisterSource<NS3::TSource>("S3Source",
                    [credentialsFactory, gateway, retryPolicy, cfg, counters, allowLocalFiles](NS3::TSource&& settings, IDqAsyncIoFactory::TSourceArguments&& args) {
                        return CreateS3ReadActor(args.TypeEnv, args.HolderFactory, gateway,
                            std::move(settings), args.InputIndex, args.StatsLevel, args.TxId, args.SecureParams,
                            args.TaskParams, args.ReadRanges, args.ComputeActorId, credentialsFactory, retryPolicy, cfg,
                            counters, args.TaskCounters, args.MemoryQuotaManager, allowLocalFiles);
                    });
            #else
                Y_UNUSED(factory);
                Y_UNUSED(credentialsFactory);
                Y_UNUSED(gateway);
                Y_UNUSED(counters);
            #endif
        }
    };

    std::shared_ptr<IS3ActorsFactory> CreateS3ActorsFactory() {
        return std::make_shared<TS3ActorsFactory>();
    }

}
