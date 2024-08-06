#include "yql_s3_actors_factory.h"

namespace NYql::NDq {

    class TDefaultS3ActorsFactory : public IS3ActorsFactory {
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
            
            Y_UNUSED(parentId);
            Y_UNUSED(gateway);
            Y_UNUSED(queryId);
            Y_UNUSED(jobId);
            Y_UNUSED(restartNumber);
            Y_UNUSED(commit);
            Y_UNUSED(secureParams);
            Y_UNUSED(credentialsFactory);
            Y_UNUSED(externalEffect);
            return nullptr;
        }

        void RegisterS3WriteActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy) override {

            Y_UNUSED(factory);
            Y_UNUSED(credentialsFactory);
            Y_UNUSED(gateway);
            Y_UNUSED(retryPolicy);
        }

        void RegisterS3ReadActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
            const TS3ReadActorFactoryConfig& factoryConfig = {},
            ::NMonitoring::TDynamicCounterPtr counters = nullptr) override {

            Y_UNUSED(factory);
            Y_UNUSED(credentialsFactory);
            Y_UNUSED(gateway);
            Y_UNUSED(retryPolicy);
            Y_UNUSED(factoryConfig);
            Y_UNUSED(counters);
        }
    };

    std::shared_ptr<IS3ActorsFactory> CreateDefaultS3ActorsFactory() {
        return std::make_shared<TDefaultS3ActorsFactory>();
    }

    TS3ReadActorFactoryConfig CreateReadActorFactoryConfig(const ::NYql::TS3GatewayConfig& s3Config) {
        TS3ReadActorFactoryConfig s3ReadActoryConfig;
        if (const ui64 rowsInBatch = s3Config.GetRowsInBatch()) {
            s3ReadActoryConfig.RowsInBatch = rowsInBatch;
        }
        if (const ui64 maxInflight = s3Config.GetMaxInflight()) {
            s3ReadActoryConfig.MaxInflight = maxInflight;
        }
        if (const ui64 dataInflight = s3Config.GetDataInflight()) {
            s3ReadActoryConfig.DataInflight = dataInflight;
        }
        for (auto& formatSizeLimit: s3Config.GetFormatSizeLimit()) {
            if (formatSizeLimit.GetName()) { // ignore unnamed limits
                s3ReadActoryConfig.FormatSizeLimits.emplace(
                    formatSizeLimit.GetName(), formatSizeLimit.GetFileSizeLimit());
            }
        }
        if (s3Config.HasFileSizeLimit()) {
            s3ReadActoryConfig.FileSizeLimit = s3Config.GetFileSizeLimit();
        }
        if (s3Config.HasBlockFileSizeLimit()) {
            s3ReadActoryConfig.BlockFileSizeLimit = s3Config.GetBlockFileSizeLimit();
        }
        return s3ReadActoryConfig;
    }
}
