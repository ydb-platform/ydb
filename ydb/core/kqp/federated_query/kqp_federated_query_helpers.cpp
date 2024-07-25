#include "kqp_federated_query_helpers.h"

#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/core/fq/libs/actors/database_resolver.h>
#include <ydb/core/fq/libs/actors/proxy.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>

#include <ydb/library/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>

#include <util/system/file.h>
#include <util/stream/file.h>

#include <ydb/core/protos/auth.pb.h>

namespace NKikimr::NKqp {
    NYql::IYtGateway::TPtr MakeYtGateway(const NMiniKQL::IFunctionRegistry* functionRegistry, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig) {
        NYql::TYtNativeServices ytServices;
        ytServices.FunctionRegistry = functionRegistry;
        ytServices.FileStorage = WithAsync(CreateFileStorage(queryServiceConfig.GetFileStorage(), {MakeYtDownloader(queryServiceConfig.GetFileStorage())}));
        ytServices.Config = std::make_shared<NYql::TYtGatewayConfig>(queryServiceConfig.GetYt());
        return CreateYtNativeGateway(ytServices);
    }

    NMonitoring::TDynamicCounterPtr HttpGatewayGroupCounters(NMonitoring::TDynamicCounterPtr countersRoot) {
        return GetServiceCounters(countersRoot, "utils")->GetSubgroup("subcomponent", "http_gateway");
    }

    NYql::IHTTPGateway::TPtr MakeHttpGateway(const NYql::THttpGatewayConfig& httpGatewayConfig, NMonitoring::TDynamicCounterPtr countersRoot) {
        NMonitoring::TDynamicCounterPtr httpGatewayGroup = HttpGatewayGroupCounters(countersRoot);
        return NYql::IHTTPGateway::Make(&httpGatewayConfig, httpGatewayGroup);
    }

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
        HttpGatewayConfig = queryServiceConfig.HasHttpGateway() ? queryServiceConfig.GetHttpGateway() : DefaultHttpGatewayConfig();
        HttpGateway = MakeHttpGateway(HttpGatewayConfig, appData->Counters);

        S3GatewayConfig = queryServiceConfig.GetS3();

        S3ReadActorFactoryConfig = NYql::NDq::CreateReadActorFactoryConfig(S3GatewayConfig);

        YtGatewayConfig = queryServiceConfig.GetYt();
        YtGateway = MakeYtGateway(appData->FunctionRegistry, queryServiceConfig);

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
        if (queryServiceConfig.HasGeneric()) {
            GenericGatewaysConfig = queryServiceConfig.GetGeneric();
            ConnectorClient = NYql::NConnector::MakeClientGRPC(GenericGatewaysConfig.GetConnector());

            if (queryServiceConfig.HasMdbTransformHost()) {
                MdbEndpointGenerator = NFq::MakeMdbEndpointGeneratorGeneric(queryServiceConfig.GetMdbTransformHost());
            }

            // Create actors required for MDB database resolving
            auto httpProxyActor = NHttp::CreateHttpProxy();
            auto httpProxyActorId = NFq::MakeYqlAnalyticsHttpProxyId();
            setup->LocalServices.push_back(
                std::make_pair(
                    httpProxyActorId,
                    TActorSetupCmd(httpProxyActor, TMailboxType::HTSwap, appData->UserPoolId)));

            DatabaseResolverActorId = NFq::MakeDatabaseResolverActorId();
            // NOTE: it's ok for CredentialsFactory to be null
            auto databaseResolverActor = NFq::CreateDatabaseResolver(httpProxyActorId, CredentialsFactory);
            setup->LocalServices.push_back(
                std::make_pair(DatabaseResolverActorId.value(),
                               TActorSetupCmd(databaseResolverActor, TMailboxType::HTSwap, appData->UserPoolId)));
        }
    }

    std::optional<TKqpFederatedQuerySetup> TKqpFederatedQuerySetupFactoryDefault::Make(NActors::TActorSystem* actorSystem) {
        auto result = TKqpFederatedQuerySetup{
            HttpGateway,
            ConnectorClient,
            CredentialsFactory,
            nullptr,
            S3GatewayConfig,
            GenericGatewaysConfig,
            YtGatewayConfig,
            YtGateway,
            nullptr,
            S3ReadActorFactoryConfig};

        // Init DatabaseAsyncResolver only if all requirements are met
        if (DatabaseResolverActorId && MdbEndpointGenerator &&
            (GenericGatewaysConfig.HasMdbGateway() || GenericGatewaysConfig.HasYdbMvpEndpoint())) {
            result.DatabaseAsyncResolver = std::make_shared<NFq::TDatabaseAsyncResolverImpl>(
                actorSystem,
                DatabaseResolverActorId.value(),
                GenericGatewaysConfig.GetYdbMvpEndpoint(),
                GenericGatewaysConfig.GetMdbGateway(),
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

    NMiniKQL::TComputationNodeFactory MakeKqpFederatedQueryComputeFactory(NMiniKQL::TComputationNodeFactory baseComputeFactory, const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup) {
        auto ytComputeFactory = NYql::GetDqYtFactory();
        auto federatedComputeFactory = federatedQuerySetup ? federatedQuerySetup->ComputationFactory : nullptr;

        return [baseComputeFactory, ytComputeFactory, federatedComputeFactory]
            (NMiniKQL::TCallable& callable, const NMiniKQL::TComputationNodeFactoryContext& ctx) -> NMiniKQL::IComputationNode* {
                if (auto compute = baseComputeFactory(callable, ctx)) {
                    return compute;
                }

                if (auto ytCompute = ytComputeFactory(callable, ctx)) {
                    return ytCompute;
                }

                if (federatedComputeFactory) {
                    if (auto compute = federatedComputeFactory(callable, ctx)) {
                        return compute;
                    }
                }

                return nullptr;
            };
    }

    bool WaitHttpGatewayFinalization(NMonitoring::TDynamicCounterPtr countersRoot, TDuration timeout, TDuration refreshPeriod) {
        NMonitoring::TDynamicCounters::TCounterPtr httpRequestsInFlight = HttpGatewayGroupCounters(countersRoot)->GetCounter("InFlight");

        TInstant deadline = TInstant::Now() + timeout;
        do {
            if (httpRequestsInFlight->Val() == 0) {
                return true;
            }

            Sleep(refreshPeriod);
        } while (TInstant::Now() <= deadline);

        return false;
    }
}  // namespace NKikimr::NKqp
