#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/core/base/appdata.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/mdb_endpoint_generator.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
}

namespace NKikimr::NKqp {
    NYql::IYtGateway::TPtr MakeYtGateway(const NMiniKQL::IFunctionRegistry* functionRegistry, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig);

    NYql::IHTTPGateway::TPtr MakeHttpGateway(const NYql::THttpGatewayConfig& httpGatewayConfig, NMonitoring::TDynamicCounterPtr countersRoot);

    struct TKqpFederatedQuerySetup {
        NYql::IHTTPGateway::TPtr HttpGateway;
        NYql::NConnector::IClient::TPtr ConnectorClient;
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
        NYql::IDatabaseAsyncResolver::TPtr DatabaseAsyncResolver;
        NYql::TS3GatewayConfig S3GatewayConfig;
        NYql::TGenericGatewayConfig GenericGatewayConfig;
        NYql::TYtGatewayConfig YtGatewayConfig;
        NYql::IYtGateway::TPtr YtGateway;
        NMiniKQL::TComputationNodeFactory ComputationFactory;
        NYql::NDq::TS3ReadActorFactoryConfig S3ReadActorFactoryConfig;
    };

    struct IKqpFederatedQuerySetupFactory {
        using TPtr = std::shared_ptr<IKqpFederatedQuerySetupFactory>;
        virtual std::optional<TKqpFederatedQuerySetup> Make(NActors::TActorSystem* actorSystem) = 0;
        virtual ~IKqpFederatedQuerySetupFactory() = default;
    };

    struct TKqpFederatedQuerySetupFactoryNoop: public IKqpFederatedQuerySetupFactory {
        std::optional<TKqpFederatedQuerySetup> Make(NActors::TActorSystem*) override {
            return std::nullopt;
        }
    };

    struct TKqpFederatedQuerySetupFactoryDefault: public IKqpFederatedQuerySetupFactory {
        TKqpFederatedQuerySetupFactoryDefault(){};

        TKqpFederatedQuerySetupFactoryDefault(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData,
            const NKikimrConfig::TAppConfig& appConfig);

        std::optional<TKqpFederatedQuerySetup> Make(NActors::TActorSystem* actorSystem) override;

    private:
        NYql::THttpGatewayConfig HttpGatewayConfig;
        NYql::IHTTPGateway::TPtr HttpGateway;
        NYql::TS3GatewayConfig S3GatewayConfig;
        NYql::TGenericGatewayConfig GenericGatewaysConfig;
        NYql::TYtGatewayConfig YtGatewayConfig;
        NYql::IYtGateway::TPtr YtGateway;
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
        NYql::NConnector::IClient::TPtr ConnectorClient;
        std::optional<NActors::TActorId> DatabaseResolverActorId;
        NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;
        NYql::NDq::TS3ReadActorFactoryConfig S3ReadActorFactoryConfig;
    };

    struct TKqpFederatedQuerySetupFactoryMock: public IKqpFederatedQuerySetupFactory {
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
            NMiniKQL::TComputationNodeFactory computationFactories)
            : HttpGateway(httpGateway)
            , ConnectorClient(connectorClient)
            , CredentialsFactory(credentialsFactory)
            , DatabaseAsyncResolver(databaseAsyncResolver)
            , S3GatewayConfig(s3GatewayConfig)
            , GenericGatewayConfig(genericGatewayConfig)
            , YtGatewayConfig(ytGatewayConfig)
            , YtGateway(ytGateway)
            , ComputationFactories(computationFactories)
        {
        }

        std::optional<TKqpFederatedQuerySetup> Make(NActors::TActorSystem*) override {
            return TKqpFederatedQuerySetup{
                HttpGateway, ConnectorClient, CredentialsFactory, DatabaseAsyncResolver, S3GatewayConfig, GenericGatewayConfig, YtGatewayConfig, YtGateway, ComputationFactories, S3ReadActorFactoryConfig};
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
        NMiniKQL::TComputationNodeFactory ComputationFactories;
        NYql::NDq::TS3ReadActorFactoryConfig S3ReadActorFactoryConfig;
    };

    IKqpFederatedQuerySetupFactory::TPtr MakeKqpFederatedQuerySetupFactory(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData,
        const NKikimrConfig::TAppConfig& config);

    NMiniKQL::TComputationNodeFactory MakeKqpFederatedQueryComputeFactory(NMiniKQL::TComputationNodeFactory baseComputeFactory, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup);

    // Used only for unit tests
    bool WaitHttpGatewayFinalization(NMonitoring::TDynamicCounterPtr countersRoot, TDuration timeout = TDuration::Minutes(1), TDuration refreshPeriod = TDuration::MilliSeconds(100));
}  // namespace NKikimr::NKqp
