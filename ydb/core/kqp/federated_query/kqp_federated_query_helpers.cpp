#include "kqp_federated_query_helpers.h"

#include <library/cpp/actors/http/http_proxy.h>

#include <ydb/core/base/counters.h>

#include <ydb/core/fq/libs/actors/database_resolver.h>
#include <ydb/core/fq/libs/actors/proxy.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>

namespace NKikimr::NKqp {

    NYql::THttpGatewayConfig DefaultHttpGatewayConfig() {
        NYql::THttpGatewayConfig config;
        config.SetMaxInFlightCount(2000);
        config.SetMaxSimulatenousDownloadsSize(2000000000);
        config.SetBuffersSizePerStream(5000000);
        config.SetConnectionTimeoutSeconds(15);
        config.SetRequestTimeoutSeconds(0);
        return config;
    }

    std::pair<TString, bool> ParseGrpcEndpoint(const TString& endpoint) {
        TStringBuf scheme;
        TStringBuf host;
        TStringBuf uri;
        NHttp::CrackURL(endpoint, scheme, host, uri);

        return std::make_pair(ToString(host), scheme == "grpcs");
    }

    // TKqpFederatedQuerySetupFactoryDefault contains network clients and service actors necessary
    // for federated queries. HTTP Gateway (required by S3 provider) is run by default even without
    // explicit configuration. Token Accessor and Connector Client are run only if config is provided.
    TKqpFederatedQuerySetupFactoryDefault::TKqpFederatedQuerySetupFactoryDefault(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData,
        const NKikimrConfig::TAppConfig& appConfig) {
        const auto& queryServiceConfig = appConfig.GetQueryServiceConfig();

        // Initialize HTTP Gateway
        TIntrusivePtr<::NMonitoring::TDynamicCounters> httpGatewayGroup = GetServiceCounters(
            appData->Counters, "utils")->GetSubgroup("subcomponent", "http_gateway");

        HttpGatewayConfig = queryServiceConfig.HasHttpGateway() ? queryServiceConfig.GetHttpGateway() : DefaultHttpGatewayConfig();
        HttpGateway = NYql::IHTTPGateway::Make(&HttpGatewayConfig, httpGatewayGroup);

        S3GatewayConfig = queryServiceConfig.GetS3();

        // Initialize Token Accessor
        if (appConfig.GetAuthConfig().HasTokenAccessorConfig()) {
            const auto& tokenAccessorConfig = appConfig.GetAuthConfig().GetTokenAccessorConfig();
            TString caContent;
            if (const auto& path = tokenAccessorConfig.GetSslCaCert()) {
                caContent = TUnbufferedFileInput(path).ReadAll();
            }

            auto parsed = ParseGrpcEndpoint(tokenAccessorConfig.GetEndpoint());
            CredentialsFactory = NYql::CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(
                parsed.first,
                parsed.second,
                caContent,
                tokenAccessorConfig.GetConnectionPoolSize());
        }

        // Initialize Connector client
        if (queryServiceConfig.HasConnector()) {
            ConnectorClient = NYql::NConnector::MakeClientGRPC(queryServiceConfig.GetConnector());

            if (queryServiceConfig.HasMdbGateway()) {
                MdbGateway = queryServiceConfig.GetMdbGateway();
            }

            if (queryServiceConfig.HasMdbTransformHost()) {
                MdbEndpointGenerator = NFq::MakeMdbEndpointGeneratorGeneric(queryServiceConfig.GetMdbTransformHost());
            }

            // Create actors required for MDB database resolving
            if (CredentialsFactory) {
                auto httpProxyActor = NHttp::CreateHttpProxy();
                auto httpProxyActorId = NFq::MakeYqlAnalyticsHttpProxyId();
                setup->LocalServices.push_back(
                    std::make_pair(
                        httpProxyActorId,
                        TActorSetupCmd(httpProxyActor, TMailboxType::HTSwap, appData->UserPoolId)));

                // FIXME: how to choose appropriate ActorID?
                DatabaseResolverActorId = NFq::MakeDatabaseResolverActorId();
                auto databaseResolverActor = NFq::CreateDatabaseResolver(httpProxyActorId, CredentialsFactory);
                setup->LocalServices.push_back(
                    std::make_pair(DatabaseResolverActorId.value(),
                                   TActorSetupCmd(databaseResolverActor, TMailboxType::HTSwap, appData->UserPoolId)));
            }
        }
    }

    std::optional<TKqpFederatedQuerySetup> TKqpFederatedQuerySetupFactoryDefault::Make(NActors::TActorSystem* actorSystem) {
        auto result = TKqpFederatedQuerySetup{
            HttpGateway,
            ConnectorClient,
            CredentialsFactory,
            nullptr,
            S3GatewayConfig};

        // Init DatabaseAsyncResolver only if all requirements are met
        if (DatabaseResolverActorId && MdbGateway && MdbEndpointGenerator) {
            result.DatabaseAsyncResolver = std::make_shared<NFq::TDatabaseAsyncResolverImpl>(
                actorSystem,
                DatabaseResolverActorId.value(),
                "", // TODO: use YDB Gateway endpoint?
                MdbGateway.value(),
                MdbEndpointGenerator);
        }

        return result;
    }

    IKqpFederatedQuerySetupFactory::TPtr MakeKqpFederatedQuerySetupFactory(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData,
        const NKikimrConfig::TAppConfig& appConfig) {
        // If Query Service is disabled, just do nothing
        if (!appData->FeatureFlags.GetEnableScriptExecutionOperations()) {
            return std::make_shared<TKqpFederatedQuerySetupFactoryNoop>();
        }

        return std::make_shared<NKikimr::NKqp::TKqpFederatedQuerySetupFactoryDefault>(setup, appData, appConfig);
    }
}
