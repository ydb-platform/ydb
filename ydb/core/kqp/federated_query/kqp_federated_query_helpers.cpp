#include "kqp_federated_query_helpers.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/database_resolver.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/http_proxy.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>
#include <ydb/core/local_proxy/local_pq_client/local_topic_client_factory.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/database_type.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/extensions/discovery_mutator/discovery_mutator.h>

#include <yql/essentials/public/issue/yql_issue_utils.h>

#include <yt/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>
#include <yt/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>

#include <util/system/file.h>

namespace NKikimr::NKqp {

namespace {

    bool ValidateExternalSink(const NKqpProto::TKqpExternalSink& sink) {
        if (sink.GetType() != "S3Sink") {
            return false;
        }

        NYql::NS3::TSink sinkSettings;
        sink.GetSettings().UnpackTo(&sinkSettings);

        return sinkSettings.GetAtomicUploadCommit();
    }

    NThreading::TFuture<TGetSchemeEntryResult> GetSchemeEntryTypeImpl(
        TActorSystem* actorSystem,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        const TString& endpoint,
        const TString& database,
        bool useTls,
        const TString& structuredTokenJson,
        const TString& path,
        bool addRoot) {
        if (!federatedQuerySetup || !federatedQuerySetup->Driver || !endpoint || !database) {
            LOG_NOTICE_S(*NActors::TActivationContext::ActorSystem(), NKikimrServices::KQP_GATEWAY, "Skipped describe for path '" << path << "' in external YDB database '" << database << "' with endpoint '" << endpoint << "'");
            return NThreading::MakeFuture<TGetSchemeEntryResult>(TGetSchemeEntryResult{.EntryType = NYdb::NScheme::ESchemeEntryType::Table}); 
        }
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory = NYql::CreateCredentialsProviderFactoryForStructuredToken(nullptr, structuredTokenJson, false);
        auto driver = federatedQuerySetup->Driver;

        NYdb::TCommonClientSettings opts;
        opts
            .DiscoveryEndpoint(endpoint)
            .Database(addRoot ? "/Root" + database : database)
            .SslCredentials(NYdb::TSslCredentials(useTls))
            .DiscoveryMode(NYdb::EDiscoveryMode::Async)
            .CredentialsProviderFactory(credentialsProviderFactory);
        auto schemeClient = std::make_shared<NYdb::NScheme::TSchemeClient>(*driver, opts);

        return schemeClient->DescribePath(addRoot ? "/Root" + path : path)
            .Apply([actorSystem, p = path, sc = schemeClient, database, endpoint, f = federatedQuerySetup, useTls, structuredTokenJson, addRoot](const NThreading::TFuture<NYdb::NScheme::TDescribePathResult>& result) {
                auto describePathResult = result.GetValue();
                TGetSchemeEntryResult res;
                if (!describePathResult.IsSuccess()) {
                    if (describePathResult.GetStatus() == NYdb::EStatus::CLIENT_UNAUTHENTICATED && !addRoot) {
                        return GetSchemeEntryTypeImpl(actorSystem, f, endpoint, database, useTls, structuredTokenJson, p, true);
                    }
                    TString message = TStringBuilder() <<  "Describe path '" << p << "' in external YDB database '" << database << "' with endpoint '" << endpoint << "' failed.";
                    LOG_WARN_S(*actorSystem, NKikimrServices::KQP_GATEWAY, message + describePathResult.GetIssues().ToString());
                    auto rootIssue = NYql::TIssue(message);
                    for (const auto& issue : describePathResult.GetIssues()) {
                        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(NYdb::NAdapters::ToYqlIssue(issue)));
                    }
                    res.Issues.AddIssue(rootIssue);
                } else {
                    NYdb::NScheme::TSchemeEntry entry = describePathResult.GetEntry();
                    res.EntryType = entry.Type;
                }
                return NThreading::MakeFuture<TGetSchemeEntryResult>(res);
            });
    }

}  // anonymous namespace

    bool CheckNestingDepth(const google::protobuf::Message& message, ui32 maxDepth) {
        if (!maxDepth) {
            return false;
        }
        --maxDepth;

        const auto* descriptor = message.GetDescriptor();
        const auto* reflection = message.GetReflection();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
                continue;
            }

            if (field->is_repeated()) {
                for (int j = 0; j < reflection->FieldSize(message, field); ++j) {
                    if (!CheckNestingDepth(reflection->GetRepeatedMessage(message, field, j), maxDepth)) {
                        return false;
                    }
                }
            } else if (reflection->HasField(message, field) && !CheckNestingDepth(reflection->GetMessage(message, field), maxDepth)) {
                return false;
            }
        }

        return true;
    }

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

    std::shared_ptr<NYdb::TDriver> MakeSharedYdbDriverWithStop(std::unique_ptr<NYdb::TDriver> driver) {
        if (!driver) {
            return nullptr;
        }

        return std::shared_ptr<NYdb::TDriver>(driver.release(), [](NYdb::TDriver* d) {
            if (!d) {
                return;
            }

            // Stop requests and wait for their completion
            d->Stop(true);
            delete d;
        });
    }

    std::unique_ptr<NYdb::TDriver> MakeYdbDriver(NKikimr::TDeferredActorLogBackend::TSharedAtomicActorSystemPtr actorSystemPtr, const NKikimrConfig::TStreamingQueriesConfig::TExternalTopicsSettings& config) {
        NYdb::TDriverConfig cfg;
        cfg.SetLog(std::make_unique<NKikimr::TDeferredActorLogBackend>(actorSystemPtr, NKikimrServices::EServiceKikimr::YDB_SDK));
        cfg.SetDiscoveryMode(NYdb::EDiscoveryMode::Async);
        cfg.SetMaxQueuedRequests(std::numeric_limits<i64>::max());

        auto driver = std::make_unique<NYdb::TDriver>(cfg);

        if (const auto& patchPrefix = config.GetDiscoveryCommonHostnamePrefixPatch()) {
            driver->AddExtension<NDiscoveryMutator::TDiscoveryMutator>(NDiscoveryMutator::TDiscoveryMutator::TParams([patchPrefix](Ydb::Discovery::ListEndpointsResult* proto, NYdb::TStatus status, const NYdb::IDiscoveryMutatorApi::TAuxInfo& aux) {
                if (!aux.DiscoveryEndpoint.starts_with(patchPrefix) || !proto) {
                    return status;
                }

                for (auto& endpointInfo : *proto->Mutableendpoints()) {
                    if (const auto& address = endpointInfo.address(); !address.StartsWith(patchPrefix)) {
                        endpointInfo.set_address(TStringBuilder() << patchPrefix << address);
                    }
                }

                return status;
            }));
        }

        return driver;
    }

    NYdb::NTopic::TTopicClientSettings MakeCommonTopicClientSettings(ui64 handlersExecutorThreadsNum, ui64 compressionExecutorThreadsNum) {
        NYdb::NTopic::TTopicClientSettings settings;

        if (handlersExecutorThreadsNum) {
            auto threadPool = CreateThreadPool(handlersExecutorThreadsNum, 0, IThreadPool::TParams().SetThreadNamePrefix("ydb_sdk_client").SetBlocking(true).SetCatching(false));
            settings.DefaultHandlersExecutor(NYdb::CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool>(threadPool.Release())));
        }

        if (compressionExecutorThreadsNum) {
            auto threadPool = CreateThreadPool(compressionExecutorThreadsNum, 0, IThreadPool::TParams().SetThreadNamePrefix("ydb_sdk_compression").SetBlocking(true).SetCatching(false));
            settings.DefaultCompressionExecutor(NYdb::CreateExternalThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool>(threadPool.Release())));
        }

        return settings;
    }

    NYql::IPqGateway::TPtr MakePqGateway(const std::shared_ptr<NYdb::TDriver>& driver, const std::optional<TLocalTopicClientSettings>& localTopicClientSettings) {
        auto settings = MakeCommonTopicClientSettings(1, 2);

        return CreatePqNativeGateway(NYql::TPqGatewayServices(
            *driver,
            nullptr,
            nullptr,
            std::make_shared<NYql::TPqGatewayConfig>(),
            nullptr,
            nullptr,
            settings,
            localTopicClientSettings ? CreateLocalTopicClientFactory(*localTopicClientSettings) : nullptr
        ));
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

    bool IsValidExternalDataSourceType(const TString& type) {
        static auto allTypes = NYql::GetAllExternalDataSourceTypes();
        return allTypes.contains(type);
    }

    void IKqpFederatedQuerySetupFactory::Cleanup() {
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

        SolomonGatewayConfig = queryServiceConfig.GetSolomon();
        SolomonGateway = NYql::CreateSolomonGateway(SolomonGatewayConfig);

        S3ReadActorFactoryConfig = NYql::NDq::CreateReadActorFactoryConfig(S3GatewayConfig);

        YtGatewayConfig = queryServiceConfig.GetYt();
        YtGateway = MakeYtGateway(appData->FunctionRegistry, queryServiceConfig);
        DqTaskTransformFactory = NYql::CreateYtDqTaskTransformFactory(true);

        ActorSystemPtr = std::make_shared<NKikimr::TDeferredActorLogBackend::TAtomicActorSystemPtr>(nullptr);
        Driver = MakeYdbDriver(ActorSystemPtr, queryServiceConfig.GetStreamingQueries().GetTopicSdkSettings());

        if (appConfig.GetFeatureFlags().GetEnableTopicsSqlIoOperations()) {
            LocalTopicClientSettings.emplace();
            LocalTopicClientSettings->ChannelBufferSize = appConfig.GetTableServiceConfig().GetResourceManager().GetChannelBufferSize();
        }

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
            ConnectorClient = NYql::NConnector::MakeClientGRPC(GenericGatewaysConfig);

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
        if (!ActorSystemPtr->load(std::memory_order_relaxed)) {
            ActorSystemPtr->store(actorSystem, std::memory_order_relaxed);
        }

        if (LocalTopicClientSettings) {
            LocalTopicClientSettings->ActorSystem = actorSystem;
        }

        auto result = TKqpFederatedQuerySetup{
            Driver,
            HttpGateway,
            ConnectorClient,
            CredentialsFactory,
            nullptr,
            S3GatewayConfig,
            GenericGatewaysConfig,
            YtGatewayConfig,
            YtGateway,
            SolomonGatewayConfig,
            SolomonGateway,
            nullptr,
            S3ReadActorFactoryConfig,
            DqTaskTransformFactory,
            PqGatewayConfig,
            MakePqGateway(Driver, LocalTopicClientSettings),
            ActorSystemPtr
        };

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

    void TKqpFederatedQuerySetupFactoryDefault::Cleanup() {
        HttpGateway.reset();
    }

    IKqpFederatedQuerySetupFactory::TPtr MakeKqpFederatedQuerySetupFactory(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData,
        const NKikimrConfig::TAppConfig& appConfig) {
        // If Query Service is disabled, just do nothing
        if (!appData->FeatureFlags.GetEnableScriptExecutionOperations()) {
            return std::make_shared<TKqpFederatedQuerySetupFactoryNoop>();
        }

        for (const auto& source : appConfig.GetQueryServiceConfig().GetAvailableExternalDataSources()) {
            if (!IsValidExternalDataSourceType(source)) {
                ythrow yexception() << "wrong AvailableExternalDataSources \"" << source << "\"";
            }
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

    NYql::TIssues TruncateIssues(const NYql::TIssues& issues, ui32 maxLevels, ui32 keepTailLevels) {
        const auto options = NYql::TTruncateIssueOpts()
            .SetMaxLevels(maxLevels)
            .SetKeepTailLevels(keepTailLevels);

        NYql::TIssues result;
        result.Reserve(issues.Size());
        for (const auto& issue : issues) {
            result.AddIssue(NYql::TruncateIssueLevels(issue, options));
        }
        return result;
    }

    NYql::TIssues ValidateResultSetColumns(const google::protobuf::RepeatedPtrField<Ydb::Column>& columns, ui32 maxNestingDepth) {
        NYql::TIssues issues;
        for (const auto& column : columns) {
            if (!CheckNestingDepth(column.type(), maxNestingDepth)) {
                issues.AddIssue(NYql::TIssue(TStringBuilder() << "Nesting depth of type for result column '" << column.name() << "' large than allowed limit " << maxNestingDepth));
            }
        }
        return issues;
    }

    NThreading::TFuture<TGetSchemeEntryResult> GetSchemeEntryType(
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        const TString& endpoint,
        const TString& database,
        bool useTls,
        const TString& structuredTokenJson,
        const TString& path) {
        return GetSchemeEntryTypeImpl(NActors::TActivationContext::ActorSystem(), federatedQuerySetup, endpoint, NKikimr::CanonizePath(database), useTls, structuredTokenJson, path, false);
    };

    std::vector<NKqpProto::TKqpExternalSink> FilterExternalSinksWithEffects(const std::vector<NKqpProto::TKqpExternalSink>& sinks) {
        std::vector<NKqpProto::TKqpExternalSink> filteredSinks;
        filteredSinks.reserve(sinks.size());
        for (const auto& sink : sinks) {
            if (ValidateExternalSink(sink)) {
                filteredSinks.push_back(sink);
            }
        }

        return filteredSinks;
    }

}  // namespace NKikimr::NKqp
