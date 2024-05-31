#include "yql_data_provider.h"
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_source_factory.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/core/fq/libs/protos/dq_effects.pb.h>

namespace NYql {
    TIntrusivePtr<IDataProvider> CreateS3DataSource(TS3State::TPtr state) {
        Y_UNUSED(state);
        return new TDefaultDataProvider();
    }

    TIntrusivePtr<IDataProvider> CreateS3DataSink(TS3State::TPtr state) {
        Y_UNUSED(state);
        return new TDefaultDataProvider();
    }

    TDataProviderInitializer GetS3DataProviderInitializer(IHTTPGateway::TPtr gateway, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, bool allowLocalFiles) {
        Y_UNUSED(gateway);
        Y_UNUSED(credentialsFactory);
        Y_UNUSED(allowLocalFiles);
        return nullptr;
    }

    namespace NDq {

        class TDummyActor : public NActors::TActor<TDummyActor> {
        public:
            TDummyActor()
                : TActor<TDummyActor>(&TDummyActor::StateWork)
            {
            }

            STFUNC(StateWork) {
                Y_UNUSED(ev);
            }
        };
        
        THolder<NActors::IActor> MakeS3ApplicatorActor(
            NActors::TActorId parentId,
            IHTTPGateway::TPtr gateway,
            const TString& queryId,
            const TString& jobId,
            std::optional<ui32> restartNumber,
            bool commit,
            const THashMap<TString, TString>& secureParams,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            const NYql::NDqProto::TExternalEffect& externalEffect) {

                Y_UNUSED(parentId);
                Y_UNUSED(gateway);
                Y_UNUSED(queryId);
                Y_UNUSED(jobId);
                Y_UNUSED(restartNumber);
                Y_UNUSED(commit);
                Y_UNUSED(secureParams);
                Y_UNUSED(credentialsFactory);
                Y_UNUSED(externalEffect);
                return MakeHolder<TDummyActor>();
            }

        void RegisterS3WriteActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy) {

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
            const TS3ReadActorFactoryConfig& cfg,
            ::NMonitoring::TDynamicCounterPtr counters) {

            Y_UNUSED(factory);
            Y_UNUSED(credentialsFactory);
            Y_UNUSED(gateway);
            Y_UNUSED(retryPolicy);
            Y_UNUSED(cfg);
            Y_UNUSED(counters);
        }

}}
