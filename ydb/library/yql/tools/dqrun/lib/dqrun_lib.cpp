#include "dqrun_lib.h"

#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_comp_nodes.h>
#include <yt/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <yt/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/core/dq_integration/transform/yql_dq_task_transform.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>

#include <ydb/core/fq/libs/row_dispatcher/row_dispatcher_service.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/database_resolver.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>
#include <ydb/core/fq/libs/shared_resources/interface/shared_resources.h>
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/fq/libs/init/init.h>
#include <ydb/library/yql/providers/clickhouse/provider/yql_clickhouse_provider.h>
#include <ydb/library/yql/providers/clickhouse/actors/yql_ch_source_factory.h>
#include <ydb/library/yql/providers/ydb/provider/yql_ydb_provider.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_factory.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_dq_transform.h>
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_provider.h>
#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_dummy_gateway.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>
#include <ydb/library/yql/providers/solomon/gateway/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/solomon/actors/dq_solomon_read_actor.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_provider.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_provider_factories.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <ydb/library/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/helper/yql_dq_helper_impl.h>
#include <ydb/library/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>
#include <ydb/library/yql/providers/yt/actors/yql_yt_provider_factories.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/dq/actors/input_transforms/dq_input_transform_lookup_factory.h>
#include <ydb/library/yql/utils/bindings/utils.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/string/builder.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#endif

#ifdef __unix__
#include <sys/resource.h>
#endif

namespace NYql {

const std::initializer_list<TString> SUPPORTED_GATEWAYS = {
    TString{DqProviderName},
    TString{YtProviderName},
    TString{SolomonProviderName},
    TString{ClickHouseProviderName},
    TString{YdbProviderName},
    TString{PqProviderName},
    TString{S3ProviderName},
    TString{GenericProviderName},
    TString{"hybrid"},
};

TDqRunTool::TDqRunTool()
    : TYtRunTool("dqrun")
{
    GetRunOptions().SetSupportedGateways(SUPPORTED_GATEWAYS);
    GetRunOptions().GatewayTypes.insert(SUPPORTED_GATEWAYS);

    GetRunOptions().EnableCredentials = true;
    GetRunOptions().EnableQPlayer = true;
    GetRunOptions().ResultStream = &Cout;
    GetRunOptions().ResultsFormat = NYson::EYsonFormat::Text;
    GetRunOptions().CustomTests = true;
    GetRunOptions().Verbosity = TLOG_INFO;

    GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {

        opts.AddLongOption("dq-host", "Dq host")
            .RequiredArgument("HOST")
            .Handler1T<TString>([this](const TString& host) {
                DqHost_ = host;
            });
        opts.AddLongOption("dq-port", "Dq port")
            .RequiredArgument("PORT")
            .Handler1T<int>([this](int port) {
                DqPort_ = port;
            });
        opts.AddLongOption("analyze-query", "enable analyze query")
            .Optional()
            .NoArgument()
            .SetFlag(&AnalyzeQuery_);
        opts.AddLongOption("no-force-dq", "don't set force dq mode")
            .Optional()
            .NoArgument()
            .SetFlag(&NoForceDq_);
        opts.AddLongOption("enable-spilling", "Enable disk spilling")
            .NoArgument()
            .SetFlag(&EnableSpilling_);
        opts.AddLongOption("tmp-dir", "directory for temporary tables")
            .StoreResult<TString>(&TmpDir_);
        opts.AddLongOption('t', "table", "Table mapping").RequiredArgument("table@file")
            .KVHandler([&](TString name, TString path) {
                if (name.empty() || path.empty()) {
                    throw yexception() << "Incorrect table mapping, expected form table@file, e.g. yt.plato.Input@input.txt";
                }
                TablesMapping_[name] = path;
            }, '@');
        opts.AddLongOption('C', "cluster", "Cluster to service mapping").RequiredArgument("name@service")
            .KVHandler([&](TString cluster, TString provider) {
                if (cluster.empty() || provider.empty()) {
                    throw yexception() << "Incorrect service mapping, expected form cluster@provider, e.g. plato@yt";
                }
                AddClusterMapping(std::move(cluster), std::move(provider));
            }, '@');
        opts.AddLongOption("dq-threads", "DQ gateway threads")
            .Optional()
            .RequiredArgument("COUNT")
            .StoreResult(&DqThreads_);
        opts.AddLongOption("fq-cfg", "federated query configuration file")
            .Optional()
            .RequiredArgument("FILE")
            .Handler1T<TString>([this](const TString& file) {
                FqConfig_ = TFacadeRunOptions::ParseProtoConfig<NFq::NConfig::TConfig>(file);
            });
        opts.AddLongOption("metrics", "Print execution metrics")
            .Optional()
            .OptionalArgument("FILE")
            .Handler1T<TString>([this](const TString& file) {
                if (file) {
                    MetricsStreamHolder_ = MakeHolder<TFileOutput>(file);
                    MetricsStream_ = MetricsStreamHolder_.Get();
                } else {
                    MetricsStream_ = &Cerr;
                }
            });
        opts.AddLongOption('E', "emulate-yt", "Emulate YT tables")
            .Optional()
            .NoArgument()
            .SetFlag(&EmulateYt_);
        opts.AddLongOption("emulate-pq", "Emulate YDS with local file")
            .RequiredArgument("topic@file")
            .KVHandler([&](TString name, TString path) {
                if (name.empty() || path.empty()) {
                    throw yexception() << "Incorrect topic mapping, expected form topic@file";
                }
                TopicMapping_[name] = path;
            }, '@');

        opts.AddLongOption("bindings-file", "Bindings File")
            .Optional()
            .RequiredArgument("FILE")
            .Handler1T<TString>([this](const TString& file) {
                TFileInput input(file);
                LoadBindings(GetRunOptions().Bindings, input.ReadAll());
            });
        opts.AddLongOption("token-accessor-endpoint", "Network address of Token Accessor service in format grpc(s)://host:port")
            .Optional()
            .RequiredArgument("ENDPOINT")
            .Handler1T<TString>([this](const TString& endpoint) {
                TVector<TString> ss = StringSplitter(endpoint).SplitByString("://");
                if (ss.size() != 2) {
                    throw yexception() << "Invalid tokenAccessorEndpoint: " << endpoint;
                }
                CredentialsFactory_ = NYql::CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(ss[1], ss[0] == "grpcs", "");
            });
    });

    GetRunOptions().AddOptHandler([this](const NLastGetopt::TOptsParseResult& res) {
        Y_UNUSED(res);

        if (EmulateYt_ && DqPort_) {
            throw yexception() << "Remote DQ instance cannot work with the emulated YT cluster";
        }
        if (EmulateYt_) {
            GetRunOptions().GatewayTypes.emplace(YtProviderName);
        }

        GetRunOptions().EnableResultPosition = !EmulateYt_;
        GetRunOptions().UseRepeatableRandomAndTimeProviders = EmulateYt_;

        if (!GetRunOptions().GatewaysConfig) {
            GetRunOptions().GatewaysConfig = MakeHolder<TGatewaysConfig>();
        }

        auto dqConfig = GetRunOptions().GatewaysConfig->MutableDq();

        if (AnalyzeQuery_) {
            auto* setting = dqConfig->AddDefaultSettings();
            setting->SetName("AnalyzeQuery");
            setting->SetValue("1");
        }

        if (EnableSpilling_) {
            auto* setting = dqConfig->AddDefaultSettings();
            setting->SetName("SpillingEngine");
            setting->SetValue("file");
        }
        if (EmulateYt_) {
            AddClusterMapping(TString{"plato"}, TString{YtProviderName});
            // Clusters from gateways are filled in YtRunLib
        }

        if (GetRunOptions().GatewaysConfig->HasClickHouse()) {
            FillClusterMapping(GetRunOptions().GatewaysConfig->GetClickHouse(), TString{ClickHouseProviderName});
        }

        if (GetRunOptions().GatewaysConfig->HasGeneric()) {
            FillClusterMapping(GetRunOptions().GatewaysConfig->GetGeneric(), TString{GenericProviderName});
        }

        if (GetRunOptions().GatewaysConfig->HasYdb()) {
            FillClusterMapping(GetRunOptions().GatewaysConfig->GetYdb(), TString{YdbProviderName});
        }

        if (GetRunOptions().GatewaysConfig->HasS3()) {
            GetRunOptions().GatewaysConfig->MutableS3()->SetAllowLocalFiles(true);
            FillClusterMapping(GetRunOptions().GatewaysConfig->GetS3(), TString{S3ProviderName});
        }

        if (GetRunOptions().GatewaysConfig->HasPq()) {
            FillClusterMapping(GetRunOptions().GatewaysConfig->GetPq(), TString{PqProviderName});
        }

        if (GetRunOptions().GatewaysConfig->HasSolomon()) {
            FillClusterMapping(GetRunOptions().GatewaysConfig->GetSolomon(), TString{SolomonProviderName});
        }

        if (!FqConfig_) {
            FqConfig_ = MakeHolder<NFq::NConfig::TConfig>();
        }

        GetRunOptions().SqlFlags.insert("DqEngineEnable");
        if (!AnalyzeQuery_ && !NoForceDq_) {
            GetRunOptions().SqlFlags.insert("DqEngineForce");
        }

    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewaysConfig->HasClickHouse()) {
            return GetClickHouseDataProviderInitializer(GetHttpGateway());
        }
        return {};
    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewaysConfig->HasGeneric()) {
            return GetGenericDataProviderInitializer(GetGenericClient(), GetDbResolver(), CredentialsFactory_);
        }
        return {};
    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewaysConfig->HasYdb()) {
            return GetYdbDataProviderInitializer(GetYdbDriver());
        }
        return {};
    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewaysConfig->HasS3()) {
            return GetS3DataProviderInitializer(GetHttpGateway(), nullptr);
        }
        return {};
    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewaysConfig->HasPq() || !TopicMapping_.empty()) {
            return GetPqDataProviderInitializer(GetPqGateway(), false, GetDbResolver());
        }
        return {};
    });

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewaysConfig->HasSolomon()) {
            return GetSolomonDataProviderInitializer(CreateSolomonGateway(GetRunOptions().GatewaysConfig->GetSolomon()), nullptr, false);
        }
        return {};
    });


    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        auto compFactory = CreateCompNodeFactory();
        TIntrusivePtr<IDqGateway> dqGateway;
        if (DqPort_) {
            dqGateway = CreateDqGateway(DqHost_.GetOrElse("localhost"), *DqPort_);
        } else {
            std::function<NActors::IActor*(void)> metricsPusherFactory = {};
            dqGateway = CreateLocalDqGateway(GetFuncRegistry().Get(), compFactory, CreateDqTaskTransformFactory(), CreateDqTaskPreprocessorFactories(),
                 EnableSpilling_, CreateAsyncIoFactory(), DqThreads_, GetMetricsRegistry(), metricsPusherFactory, GetFqServices());
        }

        return GetDqDataProviderInitializer(&CreateDqExecTransformer, dqGateway, compFactory, {}, GetFileStorage());
    });
}

TDqRunTool::~TDqRunTool() {
    try {
        if (Driver_) {
            Driver_->Stop(true);
        }
    } catch (...) {
        Cerr << "Error while stopping YDB driver: " << CurrentExceptionMessage() << Endl;
    }
}

IYtGateway::TPtr TDqRunTool::CreateYtGateway() {
    if (EmulateYt_) {
        return CreateYtFileGateway(GetYtFileServices());
    }
    return TYtRunTool::CreateYtGateway();
}

IOptimizerFactory::TPtr TDqRunTool::CreateCboFactory() {
    return NDq::MakeCBOOptimizerFactory();
}

IDqHelper::TPtr TDqRunTool::CreateDqHelper() {
    return MakeDqHelper();
}

NYdb::TDriver TDqRunTool::GetYdbDriver() {
    if (!Driver_) {
        const auto driverConfig = NYdb::TDriverConfig().SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr").Release()));
        Driver_.ConstructInPlace(driverConfig);
    }
    return *Driver_;
}

NFile::TYtFileServices::TPtr TDqRunTool::GetYtFileServices() {
    if (!YtFileServices_) {
        YtFileServices_ = NFile::TYtFileServices::Make(GetFuncRegistry().Get(), TablesMapping_, GetFileStorage(), TmpDir_, KeepTemp_);
    }
    return YtFileServices_;
}

IHTTPGateway::TPtr TDqRunTool::GetHttpGateway() {
    if (!HttpGateway_) {
        HttpGateway_ = IHTTPGateway::Make(GetRunOptions().GatewaysConfig->HasHttpGateway() ? &GetRunOptions().GatewaysConfig->GetHttpGateway() : nullptr);
    }
    return HttpGateway_;
}

IMetricsRegistryPtr TDqRunTool::GetMetricsRegistry() {
    if (!MetricsRegistry_) {
        MetricsRegistry_ = CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq));
    }
    return MetricsRegistry_;
}

NActors::NLog::EPriority TDqRunTool::YqlToActorsLogLevel(NYql::NLog::ELevel yqlLevel) {
    switch (yqlLevel) {
        case NYql::NLog::ELevel::FATAL:
            return NActors::NLog::PRI_CRIT;
        case NYql::NLog::ELevel::ERROR:
            return NActors::NLog::PRI_ERROR;
        case NYql::NLog::ELevel::WARN:
            return NActors::NLog::PRI_WARN;
        case NYql::NLog::ELevel::INFO:
            return NActors::NLog::PRI_INFO;
        case NYql::NLog::ELevel::DEBUG:
            return NActors::NLog::PRI_DEBUG;
        case NYql::NLog::ELevel::TRACE:
            return NActors::NLog::PRI_TRACE;
        default:
            ythrow yexception() << "unexpected level: " << int(yqlLevel);
    }
}

struct TDqRunTool::TActorIds {
    NActors::TActorId DatabaseResolver;
    NActors::TActorId HttpProxy;
};

std::tuple<std::unique_ptr<TActorSystemManager>, TDqRunTool::TActorIds> TDqRunTool::RunActorSystem() {
    auto actorSystemManager = std::make_unique<TActorSystemManager>(GetMetricsRegistry(), YqlToActorsLogLevel(NYql::NLog::ELevelHelpers::FromInt(GetRunOptions().Verbosity)));
    TActorIds actorIds;

    // Run actor system only if necessary
    auto needActorSystem = GetRunOptions().GatewaysConfig->HasGeneric() || GetRunOptions().GatewaysConfig->HasDbResolver();
    if (!needActorSystem) {
        return std::make_tuple(std::move(actorSystemManager), std::move(actorIds));
    }

    // One can modify actor system setup via actorSystemManager->ApplySetupModifier().
    // TODO: https://st.yandex-team.ru/YQL-16131
    // This will be useful for DQ Gateway initialization refactoring.
    actorSystemManager->Start();

    // Actor system is initialized; start actor registration.
    if (needActorSystem) {
        auto httpProxy = NHttp::CreateHttpProxy();
        actorIds.HttpProxy = actorSystemManager->GetActorSystem()->Register(httpProxy);

        auto databaseResolver = NFq::CreateDatabaseResolver(actorIds.HttpProxy, CredentialsFactory_);
        actorIds.DatabaseResolver = actorSystemManager->GetActorSystem()->Register(databaseResolver);
    }

    return std::make_tuple(std::move(actorSystemManager), std::move(actorIds));
}

NYql::IDatabaseAsyncResolver::TPtr TDqRunTool::GetDbResolver() {
    if (!DbResolver_ && GetRunOptions().GatewaysConfig->HasDbResolver()) {
        std::unique_ptr<TActorSystemManager> actorSystemManager;
        TActorIds actorIds;
        std::tie(actorSystemManager, actorIds) = RunActorSystem();

        DbResolver_ = std::make_shared<NFq::TDatabaseAsyncResolverImpl>(
            actorSystemManager->GetActorSystem(),
            actorIds.DatabaseResolver,
            GetRunOptions().GatewaysConfig->GetDbResolver().GetYdbMvpEndpoint(),
            GetRunOptions().GatewaysConfig->HasGeneric() ? GetRunOptions().GatewaysConfig->GetGeneric().GetMdbGateway() : "",
            NFq::MakeMdbEndpointGeneratorGeneric(false)
        );
    }
    return DbResolver_;
}

NConnector::IClient::TPtr TDqRunTool::GetGenericClient() {
    if (!GenericClient_ && GetRunOptions().GatewaysConfig->HasGeneric()) {
        GenericClient_ = NConnector::MakeClientGRPC(GetRunOptions().GatewaysConfig->GetGeneric());
    }
    return GenericClient_;
}

IPqGateway::TPtr TDqRunTool::GetPqGateway() {
    if (!PqGateway_ && (GetRunOptions().GatewaysConfig->HasPq() || !TopicMapping_.empty())) {
        if (!TopicMapping_.empty()) {
            auto fileGateway = MakeIntrusive<TDummyPqGateway>();
            for (const auto& pair: TopicMapping_) {
                fileGateway->AddDummyTopic(TDummyTopic("pq", pair.first, pair.second));
            }
            PqGateway_ = fileGateway;
        } else {
            TPqGatewayServices pqServices(
                GetYdbDriver(),
                nullptr,
                nullptr, // credentials factory
                std::make_shared<TPqGatewayConfig>(GetRunOptions().GatewaysConfig->GetPq()),
                GetFuncRegistry().Get()
            );
            PqGateway_ = CreatePqNativeGateway(pqServices);
        }
    }
    return PqGateway_;
}

TVector<std::pair<TActorId, TActorSetupCmd>> TDqRunTool::GetFqServices() {
    TVector<std::pair<TActorId, TActorSetupCmd>> services;
    if (FqConfig_->HasRowDispatcher() && FqConfig_->GetRowDispatcher().GetEnabled()) {
        NFq::IYqSharedResources::TPtr iSharedResources = NFq::CreateYqSharedResources(
            *FqConfig_,
            NKikimr::CreateYdbCredentialsProviderFactory,
            MakeIntrusive<NMonitoring::TDynamicCounters>());
        NFq::TYqSharedResources::TPtr yqSharedResources = NFq::TYqSharedResources::Cast(iSharedResources);
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;

        auto rowDispatcher = NFq::NewRowDispatcherService(
            FqConfig_->GetRowDispatcher(),
            NKikimr::CreateYdbCredentialsProviderFactory,
            yqSharedResources,
            credentialsFactory,
            "/tenant",
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            GetPqGateway());

        services.emplace_back(
            NFq::RowDispatcherServiceActorId(),
            TActorSetupCmd(rowDispatcher.release(), TMailboxType::Simple, 0));
    }
    return services;
}

NKikimr::NMiniKQL::TComputationNodeFactory TDqRunTool::CreateCompNodeFactory() {
    TVector<NKikimr::NMiniKQL::TComputationNodeFactory> factories = {
        GetCommonDqFactory(),
        NKikimr::NMiniKQL::GetYqlFactory(),
        GetPgFactory()
    };

    if (GetRunOptions().GatewaysConfig->HasYt() || EmulateYt_) {
        factories.push_back(GetDqYtFactory());
    }
    if (EmulateYt_) {
        factories.push_back(GetYtFileFactory(GetYtFileServices()));
    }
    if (GetRunOptions().GatewaysConfig->HasYdb()) {
        factories.push_back(GetDqYdbFactory(GetYdbDriver()));
    }
    return NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(factories);
}

TTaskTransformFactory TDqRunTool::CreateDqTaskTransformFactory() {
    TVector<TTaskTransformFactory> factories = {
        CreateCommonDqTaskTransformFactory()
    };

    if (GetRunOptions().GatewaysConfig->HasYt()) {
        factories.push_back(CreateYtDqTaskTransformFactory());
    }
    if (GetRunOptions().GatewaysConfig->HasYdb()) {
        factories.push_back(CreateYdbDqTaskTransformFactory());
    }

    return CreateCompositeTaskTransformFactory(std::move(factories));
}

TDqTaskPreprocessorFactoryCollection TDqRunTool::CreateDqTaskPreprocessorFactories() {
    TDqTaskPreprocessorFactoryCollection factories;

    if (GetRunOptions().GatewaysConfig->HasYt() || EmulateYt_) {
        factories.push_back(NDq::CreateYtDqTaskPreprocessorFactory(EmulateYt_, GetFuncRegistry()));
    }
    return factories;
}

NDq::IDqAsyncIoFactory::TPtr TDqRunTool::CreateAsyncIoFactory() {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterDqInputTransformLookupActorFactory(*factory);
    if (EmulateYt_) {
        RegisterYtLookupActorFactory(*factory, GetYtFileServices(), *GetFuncRegistry());
    }
    if (GetRunOptions().GatewaysConfig->HasPq() || !TopicMapping_.empty()) {
        RegisterDqPqReadActorFactory(*factory, GetYdbDriver(), nullptr, GetPqGateway());
        RegisterDqPqWriteActorFactory(*factory, GetYdbDriver(), nullptr, GetPqGateway());
    }
    if (GetRunOptions().GatewaysConfig->HasYdb()) {
        RegisterYdbReadActorFactory(*factory, GetYdbDriver(), nullptr);
    }
    if (GetRunOptions().GatewaysConfig->HasSolomon()) {
        RegisterDQSolomonReadActorFactory(*factory, nullptr);
    }
    if (GetRunOptions().GatewaysConfig->HasClickHouse()) {
        RegisterClickHouseReadActorFactory(*factory, nullptr, GetHttpGateway());
    }
    if (GetRunOptions().GatewaysConfig->HasGeneric()) {
        RegisterGenericProviderFactories(*factory, CredentialsFactory_, GetGenericClient());
    }
    if (GetRunOptions().GatewaysConfig->HasS3()) {
        auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();

        const size_t requestTimeout = GetRunOptions().GatewaysConfig->HasHttpGateway() && GetRunOptions().GatewaysConfig->GetHttpGateway().HasRequestTimeoutSeconds()
            ? GetRunOptions().GatewaysConfig->GetHttpGateway().GetRequestTimeoutSeconds()
            : 100;
        const size_t maxRetries = GetRunOptions().GatewaysConfig->HasHttpGateway() && GetRunOptions().GatewaysConfig->GetHttpGateway().HasMaxRetries()
            ? GetRunOptions().GatewaysConfig->GetHttpGateway().GetMaxRetries()
            : 2;

        s3ActorsFactory->RegisterS3WriteActorFactory(*factory, nullptr, GetHttpGateway(), GetHTTPDefaultRetryPolicy());
        s3ActorsFactory->RegisterS3ReadActorFactory(*factory, nullptr, GetHttpGateway(), GetHTTPDefaultRetryPolicy(TDuration::Seconds(requestTimeout), maxRetries));
    }

    return factory;
}


TProgram::TStatus TDqRunTool::DoRunProgram(TProgramPtr program) {
#ifdef PROFILE_MEMORY_ALLOCATIONS
    NAllocProfiler::StartAllocationSampling(true);
#endif
    const TProgram::TStatus status = TYtRunTool::DoRunProgram(program);
#ifdef PROFILE_MEMORY_ALLOCATIONS
    NAllocProfiler::StopAllocationSampling(Cout);
#endif
    return status;
}

} // NYql
