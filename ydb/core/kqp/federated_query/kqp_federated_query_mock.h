#pragma once

#include "kqp_federated_query_helpers.h"

namespace NKikimr::NKqp {

    struct TKqpFederatedQuerySetupFactoryMock : public IKqpFederatedQuerySetupFactory {
        TKqpFederatedQuerySetupFactoryMock() = delete;

        TKqpFederatedQuerySetupFactoryMock(
            NYql::IHTTPGateway::TPtr httpGateway,
            NYql::NConnector::IClient::TPtr connectorClient,
            NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            NYql::IDatabaseAsyncResolver::TPtr databaseAsyncResolver,
            const NYql::TS3GatewayConfig& s3GatewayConfig,
            const NYql::TGenericGatewayConfig& genericGatewayConfig,
            const NYql::TYtGatewayConfig& ytGatewayConfig,
            NYql::IYtGateway::TPtr ytGateway,
            const NYql::TSolomonGatewayConfig& solomonGatewayConfig,
            NMiniKQL::TComputationNodeFactory computationFactory,
            const NYql::NDq::TS3ReadActorFactoryConfig& s3ReadActorFactoryConfig,
            NYql::TTaskTransformFactory dqTaskTransformFactory,
            const NYql::TPqGatewayConfig& pqGatewayConfig,
            NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
            NKikimr::TDeferredActorLogBackend::TSharedAtomicActorSystemPtr actorSystemPtr,
            std::shared_ptr<NYdb::TDriver> driver)
            : HttpGateway(httpGateway)
            , ConnectorClient(connectorClient)
            , CredentialsFactory(credentialsFactory)
            , DatabaseAsyncResolver(databaseAsyncResolver)
            , S3GatewayConfig(s3GatewayConfig)
            , GenericGatewayConfig(genericGatewayConfig)
            , YtGatewayConfig(ytGatewayConfig)
            , YtGateway(ytGateway)
            , SolomonGatewayConfig(solomonGatewayConfig)
            , ComputationFactory(computationFactory)
            , S3ReadActorFactoryConfig(s3ReadActorFactoryConfig)
            , DqTaskTransformFactory(dqTaskTransformFactory)
            , PqGatewayConfig(pqGatewayConfig)
            , PqGatewayFactory(pqGatewayFactory)
            , ActorSystemPtr(actorSystemPtr)
            , Driver(driver)
        {
        }

        void SetScriptExecutionSettings(const TScriptExecutionSettings& settings) override {
            ScriptExecutionSettings = settings;
        }

        std::optional<TKqpFederatedQuerySetup> Make(NActors::TActorSystem* actorSystem) override {
            if (!ActorSystemPtr->load(std::memory_order_relaxed)) {
                ActorSystemPtr->store(actorSystem, std::memory_order_relaxed);
            }

            auto credentialsFactory = CreateSecuredTokenFactoryForIamAuth(AppData(actorSystem), CredentialsFactory);
            if (!PqGatewayFactory) {
                PqGatewayFactory = MakePqGatewayFactory(
                    Driver,
                    CredentialsFactory,
                    NKqp::TLocalTopicClientSettings{
                        .ActorSystem = actorSystem,
                        // .ChannelBufferSize = AppData(actorSystem)......GetTableServiceConfig().GetResourceManager().GetChannelBufferSize(),
                    });
            }
            return TKqpFederatedQuerySetup{
                Driver, HttpGateway, ConnectorClient,
                credentialsFactory,
                DatabaseAsyncResolver, S3GatewayConfig, GenericGatewayConfig,
                YtGatewayConfig, YtGateway, SolomonGatewayConfig,
                ComputationFactory, S3ReadActorFactoryConfig,
                DqTaskTransformFactory, PqGatewayConfig, PqGatewayFactory, ActorSystemPtr,
                ScriptExecutionSettings};
        }

        void Cleanup() override {
            HttpGateway.reset();
            PqGatewayFactory.Reset();
        }

    private:
        NYql::IHTTPGateway::TPtr HttpGateway;
        NYql::NConnector::IClient::TPtr ConnectorClient;
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
        NYql::IDatabaseAsyncResolver::TPtr DatabaseAsyncResolver;
        NYql::TS3GatewayConfig S3GatewayConfig;
        NYql::TGenericGatewayConfig GenericGatewayConfig;
        NYql::TYtGatewayConfig YtGatewayConfig;
        NYql::IYtGateway::TPtr YtGateway;
        NYql::TSolomonGatewayConfig SolomonGatewayConfig;
        NMiniKQL::TComputationNodeFactory ComputationFactory;
        NYql::NDq::TS3ReadActorFactoryConfig S3ReadActorFactoryConfig;
        NYql::TTaskTransformFactory DqTaskTransformFactory;
        NYql::TPqGatewayConfig PqGatewayConfig;
        NYql::IPqGatewayFactory::TPtr PqGatewayFactory;
        NKikimr::TDeferredActorLogBackend::TSharedAtomicActorSystemPtr ActorSystemPtr;
        std::shared_ptr<NYdb::TDriver> Driver;
        TScriptExecutionSettings ScriptExecutionSettings;
    };

}  // namespace NKikimr::NKqp
