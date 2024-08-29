#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_comp_nodes.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/actors/yql_yt_provider_factories.h>
#include <ydb/library/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <ydb/library/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>
#include <ydb/library/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <ydb/library/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <ydb/library/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>

#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>
#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>
#include <ydb/library/yql/utils/actor_system/manager.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <ydb/library/yql/dq/actors/input_transforms/dq_input_transform_lookup_factory.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/clickhouse/actors/yql_ch_source_factory.h>
#include <ydb/library/yql/providers/clickhouse/provider/yql_clickhouse_provider.h>
#include <ydb/library/yql/providers/generic/actors/yql_generic_provider_factories.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_provider.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_provider.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>
#include <ydb/library/yql/providers/ydb/provider/yql_ydb_provider.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_factory.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_dq_transform.h>
#include <ydb/library/yql/providers/function/gateway/dq_function_gateway.h>
#include <ydb/library/yql/providers/function/provider/dq_function_provider.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/library/yql/providers/solomon/async_io/dq_solomon_read_actor.h>
#include <ydb/library/yql/providers/solomon/gateway/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>
#include <ydb/library/yql/providers/pg/provider/yql_pg_provider.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/common/metrics/protos/metrics_registry.pb.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_outproc_udf_resolver.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_utils.h>
#include <ydb/library/yql/protos/yql_mount.pb.h>
#include <ydb/library/yql/protos/pg_ext.pb.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/file_storage/http_download/http_download.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/core/url_lister/url_lister_manager.h>
#include <ydb/library/yql/core/yql_library_compiler.h>
#include <ydb/library/yql/core/pg_ext/yql_pg_ext.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/parser.h>
#include <ydb/library/yql/utils/log/tls_backend.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/bindings/utils.h>
#include <ydb/library/yql/core/qplayer/storage/file/yql_qstorage_file.h>

#include <ydb/core/fq/libs/actors/database_resolver.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>
#include <ydb/core/util/pb.h>

#include <yt/cpp/mapreduce/interface/init.h>

#include <library/cpp/yson/public.h>
#include <library/cpp/yson/writer.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/logger/priority.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/digest/md5/md5.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/system/user.h>
#include <util/system/env.h>
#include <util/system/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#endif

using namespace NKikimr;
using namespace NYql;

struct TRunOptions {
    bool Sql = false;
    bool Pg = false;
    TString User;
    TMaybe<TString> BindingsFile;
    NYson::EYsonFormat ResultsFormat;
    bool ValidateOnly = false;
    bool LineageOnly = false;
    IOutputStream* LineageStream = nullptr;
    bool OptimizeOnly = false;
    bool PeepholeOnly = false;
    bool TraceOpt = false;
    IOutputStream* StatisticsStream = nullptr;
    bool PrintPlan = false;
    bool AnalyzeQuery = false;
    bool NoForceDq = false;
    bool AnsiLexer = false;
    IOutputStream* ExprOut = nullptr;
    IOutputStream* ResultOut = &Cout;
    IOutputStream* ErrStream = &Cerr;
    IOutputStream* TracePlan = &Cerr;
    bool UseMetaFromGraph = false;
    bool WithFinalIssues = false;
};

class TStoreMappingFunctor: public NLastGetopt::IOptHandler {
public:
    TStoreMappingFunctor(THashMap<TString, TString>* target, char delim = '@')
        : Target(target)
        , Delim(delim)
    {
    }

    void HandleOpt(const NLastGetopt::TOptsParser* parser) final {
        const TStringBuf val(parser->CurValOrDef());
        const auto service = TString(val.After(Delim));
        auto res = Target->emplace(TString(val.Before(Delim)), service);
        if (!res.second) {
            /// force replace already exist parameter
            res.first->second = service;
        }
    }

private:
    THashMap<TString, TString>* Target;
    char Delim;
};

void ReadGatewaysConfig(const TString& configFile, TGatewaysConfig* config, THashSet<TString>& sqlFlags) {
    auto configData = TFileInput(configFile ? configFile : "../../../../../yql/cfg/local/gateways.conf").ReadAll();

    using ::google::protobuf::TextFormat;
    if (!TextFormat::ParseFromString(configData, config)) {
        ythrow yexception() << "Bad format of gateways configuration";
    }

    if (config->HasSqlCore()) {
        sqlFlags.insert(config->GetSqlCore().GetTranslationFlags().begin(), config->GetSqlCore().GetTranslationFlags().end());
    }
}

void PatchGatewaysConfig(TGatewaysConfig* config, const TString& mrJobBin, const TString& mrJobUdfsDir,
    size_t numThreads, bool keepTemp)
{
    auto ytConfig = config->MutableYt();
    ytConfig->SetGatewayThreads(numThreads);
    if (mrJobBin.empty()) {
        ytConfig->ClearMrJobBin();
    } else {
        ytConfig->SetMrJobBin(mrJobBin);
        ytConfig->SetMrJobBinMd5(MD5::File(mrJobBin));
    }

    if (mrJobUdfsDir.empty()) {
        ytConfig->ClearMrJobUdfsDir();
    } else {
        ytConfig->SetMrJobUdfsDir(mrJobUdfsDir);
    }
    auto attr = ytConfig->MutableDefaultSettings()->Add();
    attr->SetName("KeepTempTables");
    attr->SetValue(keepTemp ? "yes" : "no");
}

TFileStoragePtr CreateFS(const TString& paramsFile, const TString& defYtServer) {
    TFileStorageConfig params;
    LoadFsConfigFromFile(paramsFile ? paramsFile : "../../../../../yql/cfg/local/fs.conf", params);
    return WithAsync(CreateFileStorage(params, {MakeYtDownloader(params, defYtServer)}));
}

void FillUsedFiles(
        const TVector<TString>& filesMappingList,
        TUserDataTable& filesMapping)
{
    for (auto& s : filesMappingList) {
        TStringBuf fileName, filePath;
        TStringBuf(s).Split('@', fileName, filePath);
        if (fileName.empty() || filePath.empty()) {
            ythrow yexception() << "Incorrect file mapping, expected form "
                                   "name@path, e.g. MyFile@file.txt";
        }

        auto& entry = filesMapping[TUserDataKey::File(GetDefaultFilePrefix() + fileName)];
        entry.Type = EUserDataType::PATH;
        entry.Data = filePath;
    }
}

bool FillUsedUrls(
        const TVector<TString>& urlMappingList,
        TUserDataTable& filesMapping)
{
    for (auto& s : urlMappingList) {
        TStringBuf name, url;
        TStringBuf(s).Split('@', name, url);
        if (name.empty() || url.empty()) {
            Cerr << "Incorrect url mapping, expected form name@url, "
                    "e.g. MyUrl@http://example.com/file" << Endl;
            return false;
        }

        auto& entry = filesMapping[TUserDataKey::File(GetDefaultFilePrefix() + name)];
        entry.Type = EUserDataType::URL;
        entry.Data = url;
    }
    return true;
}

class TOptPipelineConfigurator : public IPipelineConfigurator {
public:
    TOptPipelineConfigurator(TProgramPtr prg, bool printPlan, IOutputStream* tracePlan)
        : Program(std::move(prg)), PrintPlan(printPlan), TracePlan(tracePlan)
    {
    }

    void AfterCreate(TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        pipeline->Add(TExprLogTransformer::Sync("OptimizedExpr", NYql::NLog::EComponent::Core, NYql::NLog::ELevel::TRACE),
            "OptTrace", TIssuesIds::CORE, "OptTrace");
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        if (PrintPlan) {
            pipeline->Add(TPlanOutputTransformer::Sync(TracePlan, Program->GetPlanBuilder(), Program->GetOutputFormat()), "PlanOutput");
        }
    }

private:
    TProgramPtr Program;
    bool PrintPlan;
    IOutputStream* TracePlan;
};

NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory(
    const NYdb::TDriver& driver,
    IHTTPGateway::TPtr httpGateway,
    NFile::TYtFileServices::TPtr ytFileServices,
    NYql::NConnector::IClient::TPtr genericClient,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    size_t HTTPmaxTimeSeconds, 
    size_t maxRetriesCount) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterDqInputTransformLookupActorFactory(*factory);
    if (ytFileServices) {
        RegisterYtLookupActorFactory(*factory, ytFileServices, functionRegistry);
    }
    RegisterDqPqReadActorFactory(*factory, driver, nullptr);
    RegisterYdbReadActorFactory(*factory, driver, nullptr);
    RegisterDQSolomonReadActorFactory(*factory, nullptr);
    RegisterClickHouseReadActorFactory(*factory, nullptr, httpGateway);
    RegisterGenericProviderFactories(*factory, credentialsFactory, genericClient);
    RegisterDqPqWriteActorFactory(*factory, driver, nullptr);

    auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
    s3ActorsFactory->RegisterS3WriteActorFactory(*factory, nullptr, httpGateway, GetHTTPDefaultRetryPolicy());
    s3ActorsFactory->RegisterS3ReadActorFactory(*factory, nullptr, httpGateway, GetHTTPDefaultRetryPolicy(TDuration::Seconds(HTTPmaxTimeSeconds), maxRetriesCount));

    return factory;
}

NActors::NLog::EPriority YqlToActorsLogLevel(NYql::NLog::ELevel yqlLevel) {
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

struct TActorIds {
    NActors::TActorId DatabaseResolver;
    NActors::TActorId HttpProxy;
};

std::tuple<std::unique_ptr<TActorSystemManager>, TActorIds> RunActorSystem(
    const TGatewaysConfig& gatewaysConfig,
    IMetricsRegistryPtr& metricsRegistry,
    NYql::NLog::ELevel loggingLevel,
    ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory
) {
    auto actorSystemManager = std::make_unique<TActorSystemManager>(metricsRegistry, YqlToActorsLogLevel(loggingLevel));
    TActorIds actorIds;

    // Run actor system only if necessary
    auto needActorSystem = gatewaysConfig.HasGeneric() ||  gatewaysConfig.HasDbResolver();
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

        auto databaseResolver = NFq::CreateDatabaseResolver(actorIds.HttpProxy, credentialsFactory);
        actorIds.DatabaseResolver = actorSystemManager->GetActorSystem()->Register(databaseResolver);
    }

    return std::make_tuple(std::move(actorSystemManager), std::move(actorIds));
}

int RunProgram(TProgramPtr program, const TRunOptions& options, const THashMap<TString, TString>& clusters, const THashSet<TString>& sqlFlags) {
    program->SetUseTableMetaFromGraph(options.UseMetaFromGraph);
    bool fail = true;
    if (options.Sql || options.Pg) {
        Cout << "Parse SQL..." << Endl;
        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.PgParser = options.Pg;
        sqlSettings.ClusterMapping = clusters;
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.Flags = sqlFlags;
        sqlSettings.AnsiLexer = options.AnsiLexer;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        sqlSettings.Flags.insert("DqEngineEnable");
        if (!options.AnalyzeQuery && !options.NoForceDq) {
            sqlSettings.Flags.insert("DqEngineForce");
        }

        if (options.BindingsFile) {
            TFileInput input(*options.BindingsFile);
            LoadBindings(sqlSettings.Bindings, input.ReadAll());
        }

        fail = !program->ParseSql(sqlSettings);
    } else {
        Cout << "Parse YQL..." << Endl;
        fail = !program->ParseYql();
    }
    program->PrintErrorsTo(*options.ErrStream);
    if (fail) {
        return 1;
    }
    program->SetAbortHidden([](){
        Cout << "hidden pseudo-aborted" << Endl;
    });
    Cout << "Compile program..." << Endl;
    fail = !program->Compile(options.User);
    program->PrintErrorsTo(*options.ErrStream);
    if (options.TraceOpt) {
        program->Print(&Cerr, nullptr);
    }
    if (fail) {
        return 1;
    }

    TProgram::TStatus status = TProgram::TStatus::Error;
    if (options.ValidateOnly) {
        Cout << "Validate program..." << Endl;
        status = program->Validate(options.User);
    } else if (options.LineageOnly) {
        Cout << "Calculate lineage..." << Endl;
        auto config = TOptPipelineConfigurator(program, options.PrintPlan, options.TracePlan);
        status = program->LineageWithConfig(options.User, config);
    } else if (options.OptimizeOnly) {
        Cout << "Optimize program..." << Endl;
        auto config = TOptPipelineConfigurator(program, options.PrintPlan, options.TracePlan);
        status = program->OptimizeWithConfig(options.User, config);
    } else {
        Cout << "Run program..." << Endl;
        auto config = TOptPipelineConfigurator(program, options.PrintPlan, options.TracePlan);
        status = program->RunWithConfig(options.User, config);
    }
    if (options.WithFinalIssues) {
        program->FinalizeIssues();
    }
    program->PrintErrorsTo(*options.ErrStream);
    if (status == TProgram::TStatus::Error) {
        if (options.TraceOpt) {
            program->Print(&Cerr, nullptr);
        }
        return 1;
    }
    program->Print(options.ExprOut, (options.ValidateOnly || options.LineageOnly) ? nullptr : options.TracePlan);

    Cout << "Getting results..." << Endl;
    if (program->HasResults()) {
        NYson::TYsonWriter yson(options.ResultOut, options.ResultsFormat);
        yson.OnBeginList();
        for (const auto& result: program->Results()) {
            yson.OnListItem();
            yson.OnRaw(result);
        }
        yson.OnEndList();
    }

    if (options.LineageStream) {
        if (auto st = program->GetLineage()) {
            TStringInput in(*st);
            NYson::ReformatYsonStream(&in, options.LineageStream, NYson::EYsonFormat::Pretty);
        }
    }

    if (options.StatisticsStream) {
        if (auto st = program->GetStatistics(true)) {
            TStringInput in(*st);
            NYson::ReformatYsonStream(&in, options.StatisticsStream, NYson::EYsonFormat::Pretty);
        }
    }

    Cout << Endl << "Done" << Endl;
    return 0;
}

int RunMain(int argc, const char* argv[])
{
    TString gatewaysCfgFile;
    TString progFile;
    TVector<TString> tablesMappingList;
    THashMap<TString, TString> tablesMapping;
    TString user = GetUsername();
    TString format;
    TVector<TString> filesMappingList;
    TVector<TString> urlMappingList;
    TString exprFile;
    TString resultFile;
    TString planFile;
    TString errFile;
    TString paramsFile;
    TString fileStorageCfg;
    TVector<TString> udfsPaths;
    TString udfsDir;
    TMaybe<TString> dqHost;
    TMaybe<int> dqPort;
    int threads = 16;
    TString tmpDir;
    const bool hasValidate = false; // todo
    THashSet<TString> gatewayTypes; // yqlrun compat, unused
    ui16 syntaxVersion; // yqlrun compat, unused
    bool emulateOutputForMultirun = false;
    THashMap<TString, TString> clusterMapping;
    THashSet<TString> sqlFlags;
    IMetricsRegistryPtr metricsRegistry = CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq));
    clusterMapping["plato"] = YtProviderName;
    clusterMapping["pg_catalog"] = PgProviderName;
    clusterMapping["information_schema"] = PgProviderName;

    TString pgExtConfig;
    TString mountConfig;
    TString mestricsPusherConfig;
    TString udfResolver;
    TString tokenAccessorEndpoint;
    bool udfResolverFilterSyscalls = false;
    TString statFile;
    TString metricsFile;
    int verbosity = 3;
    bool showLog = false;
    bool emulateYt = false;
    TString mrJobBin;
    TString mrJobUdfsDir;
    size_t numYtThreads = 1;
    TString token = GetEnv("YQL_TOKEN");
    if (!token) {
        TString home = GetEnv("HOME");
        auto tokenPath = TFsPath(home) / ".yql" / "token";
        if (tokenPath.Exists()) {
            token = StripStringRight(TFileInput(tokenPath).ReadAll());
        }
    }
    THashMap<TString, TString> customTokens;
    THashMap<TString, std::pair<ui32, ui32>> failureInjections;
    TString folderId;

    TRunOptions runOptions;
    TString qStorageDir;
    TString opId;
    IQStoragePtr qStorage;
    TQContext qContext;
    TString ysonAttrs;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('p', "program", "Program to execute (use '-' to read from stdin)")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&progFile);
    opts.AddLongOption('s', "sql", "Program is SQL query")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.Sql);
    opts.AddLongOption("pg", "Program has PG syntax").NoArgument().SetFlag(&runOptions.Pg);
    opts.AddLongOption('t', "table", "table@file").AppendTo(&tablesMappingList);
    opts.AddLongOption('C', "cluster", "set cluster to service mapping").RequiredArgument("name@service").Handler(new TStoreMappingFunctor(&clusterMapping));
    opts.AddLongOption('u', "user", "MR user")
        .Optional()
        .RequiredArgument("USER")
        .StoreResult(&user);
    opts.AddLongOption("format", "results format, one of { binary | text | pretty }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue("text")
        .StoreResult(&format);
    opts.AddLongOption('f', "file", "name@path").AppendTo(&filesMappingList);
    opts.AddLongOption("url", "name@url").AppendTo(&urlMappingList);
    opts.AddLongOption("gateways-cfg", "gateways configuration file")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&gatewaysCfgFile);
    opts.AddLongOption("fs-cfg", "Path to file storage config")
        .Optional()
        .StoreResult(&fileStorageCfg);
    opts.AddLongOption("udf", "Load shared library with UDF by given path")
        .AppendTo(&udfsPaths);
    opts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found"
                                   " in given directory")
        .StoreResult(&udfsDir);
    opts.AddLongOption("validate", "validate expression")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.ValidateOnly);
    opts.AddLongOption("lineage", "lineage expression")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.LineageOnly);
    opts.AddLongOption('O', "optimize", "optimize expression")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.OptimizeOnly);
    opts.AddLongOption("peephole", "perform peephole optimization")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.PeepholeOnly);
    opts.AddLongOption("trace-opt", "print AST in the begin of each transformation")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.TraceOpt);
    opts.AddLongOption("print-expr", "print rebuild AST before execution").NoArgument();
    opts.AddLongOption("expr-file", "print AST to that file instead of stdout").StoreResult<TString>(&exprFile);
    opts.AddLongOption("result-file", "print program execution result to file").StoreResult<TString>(&resultFile);
    opts.AddLongOption("plan-file", "print program plan to file").StoreResult<TString>(&planFile);
    opts.AddLongOption("err-file", "print validate/optimize/runtime errors to file").StoreResult<TString>(&errFile);
    opts.AddLongOption("params-file", "Query parameters values in YSON format").StoreResult(&paramsFile);
    opts.AddLongOption("tmp-dir", "directory for temporary tables").StoreResult<TString>(&tmpDir);
    opts.AddLongOption('G', "gateways", "used gateways").SplitHandler(&gatewayTypes, ',').DefaultValue(DqProviderName);
    opts.AddLongOption("sql-flags", "SQL translator pragma flags").SplitHandler(&sqlFlags, ',');
    opts.AddLongOption("syntax-version", "SQL syntax version").StoreResult(&syntaxVersion).DefaultValue(1);
    opts.AddLongOption('m', "mounts", "Mount points config file.").StoreResult(&mountConfig);
    opts.AddLongOption('R',"run", "run expression using input/output tables").NoArgument(); // yqlrun compat
    opts.AddLongOption('L', "show-log", "show transformation log")
        .Optional()
        .NoArgument()
        .SetFlag(&showLog);
    opts.AddLongOption("udf-resolver", "Path to udf-resolver")
        .Optional()
        .RequiredArgument("PATH")
        .StoreResult(&udfResolver);
    opts.AddLongOption("udf-resolver-filter-syscalls", "Filter syscalls in udf resolver")
        .Optional()
        .NoArgument()
        .SetFlag(&udfResolverFilterSyscalls);
    opts.AddLongOption("mrjob-bin", "Path to mrjob binary")
        .Optional()
        .StoreResult(&mrJobBin);
    opts.AddLongOption("mrjob-udfsdir", "Path to udfs for mr jobs")
        .Optional()
        .StoreResult(&mrJobUdfsDir);
    opts.AddLongOption("yt-threads", "YT gateway threads")
        .Optional()
        .RequiredArgument("COUNT")
        .StoreResult(&numYtThreads);
    opts.AddLongOption('v', "verbosity", "Log verbosity level")
        .Optional()
        .RequiredArgument("LEVEL")
        .DefaultValue("6")
        .StoreResult(&verbosity);
    opts.AddLongOption("token", "YQL token")
        .Optional()
        .RequiredArgument("VALUE")
        .StoreResult(&token);
    opts.AddLongOption("custom-tokens", "Custom tokens")
        .Optional()
        .RequiredArgument("NAME=VALUE or NAME=@PATH")
        .KVHandler([&customTokens](TString key, TString value) {
            if (value.StartsWith('@')) {
                customTokens[key] = StripStringRight(TFileInput(value.substr(1)).ReadAll());
            } else {
                customTokens[key] = value;
            }
        });
    opts.AddLongOption("folderId", "Yandex Cloud folder ID (resolve objects inside this folder)")
        .Optional()
        .RequiredArgument("VALUE")
        .StoreResult(&folderId);
    opts.AddLongOption("use-graph-meta", "Use tables metadata from graph")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.UseMetaFromGraph);
    opts.AddLongOption("stat", "Print execution statistics")
        .Optional()
        .OptionalArgument("FILE")
        .StoreResult(&statFile);
    opts.AddLongOption("metrics", "Print execution metrics")
        .Optional()
        .OptionalArgument("FILE")
        .StoreResult(&metricsFile);
    opts.AddLongOption("print-plan", "Print basic and detailed plan")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.PrintPlan);
    opts.AddLongOption("keep-temp", "keep temporary tables").NoArgument();
    opts.AddLongOption("analyze-query", "enable analyze query").Optional().NoArgument().SetFlag(&runOptions.AnalyzeQuery);
    opts.AddLongOption("no-force-dq", "don't set force dq mode").Optional().NoArgument().SetFlag(&runOptions.NoForceDq);
    opts.AddLongOption("ansi-lexer", "Use ansi lexer").Optional().NoArgument().SetFlag(&runOptions.AnsiLexer);
    opts.AddLongOption('E', "emulate-yt", "Emulate YT tables").Optional().NoArgument().SetFlag(&emulateYt);
    opts.AddLongOption("qstorage-dir", "directory for QStorage").StoreResult(&qStorageDir).DefaultValue(".");
    opts.AddLongOption("op-id", "QStorage operation id").StoreResult(&opId).DefaultValue("dummy_op");
    opts.AddLongOption("capture", "write query metadata to QStorage").NoArgument();
    opts.AddLongOption("replay", "read query metadata from QStorage").NoArgument();

    opts.AddLongOption("dq-host", "Dq Host");
    opts.AddLongOption("dq-port", "Dq Port");
    opts.AddLongOption("threads", "Threads");
    opts.AddLongOption("bindings-file", "Bindings File")
        .StoreResult(&runOptions.BindingsFile);
    opts.AddLongOption("metrics-pusher-config", "Metrics Pusher Config")
        .StoreResult(&mestricsPusherConfig);
    opts.AddLongOption("enable-spilling", "Enable disk spilling").NoArgument();
    opts.AddLongOption("failure-inject", "Activate failure injection")
        .Optional()
        .RequiredArgument("INJECTION_NAME=FAIL_COUNT or INJECTION_NAME=SKIP_COUNT/FAIL_COUNT")
        .KVHandler([&failureInjections](TString key, TString value) {
            TStringBuf fail = value;
            TStringBuf skip;
            if (TStringBuf(value).TrySplit('/', skip, fail)) {
                failureInjections[key] = std::make_pair(FromString<ui32>(skip), FromString<ui32>(fail));
            } else {
                failureInjections[key] = std::make_pair(ui32(0), FromString<ui32>(fail));
            }
        });
    opts.AddLongOption("token-accessor-endpoint", "Network address of Token Accessor service in format grpc(s)://host:port")
        .Optional()
        .RequiredArgument("ENDPOINT")
        .StoreResult(&tokenAccessorEndpoint);
    opts.AddLongOption("yson-attrs", "Provide operation yson attribues").StoreResult(&ysonAttrs);
    opts.AddLongOption("pg-ext", "pg extensions config file").StoreResult(&pgExtConfig);
    opts.AddLongOption("with-final-issues").NoArgument();
    opts.AddHelpOption('h');

    opts.SetFreeArgsNum(0);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (!res.Has("program") && !res.Has("replay")) {
        YQL_LOG(ERROR) << "Either program or replay option should be specified";
        return 1;
    }

    if (runOptions.PeepholeOnly) {
        Cerr << "Peephole optimization is not supported yet" << Endl;
        return 1;
    }

    if (res.Has("replay")) {
        qStorage = MakeFileQStorage(qStorageDir);
        qContext = TQContext(qStorage->MakeReader(opId, {}));
    } else if (res.Has("capture")) {
        qStorage = MakeFileQStorage(qStorageDir);
        qContext = TQContext(qStorage->MakeWriter(opId, {}));
    }

    if (res.Has("dq-host")) {
        dqHost = res.Get<TString>("dq-host");
    }
    if (res.Has("dq-port")) {
        dqPort = res.Get<int>("dq-port");
    }
    if (res.Has("threads")) {
        threads = res.Get<int>("threads");
    }

    THolder<TFixedBufferFileOutput> exprFileHolder;
    if (res.Has("print-expr")) {
        runOptions.ExprOut = &Cout;
    } else if (!exprFile.empty()) {
        exprFileHolder.Reset(new TFixedBufferFileOutput(exprFile));
        runOptions.ExprOut = exprFileHolder.Get();
    }
    THolder<TFixedBufferFileOutput> errFileHolder;
    if (!errFile.empty()) {
        errFileHolder.Reset(new TFixedBufferFileOutput(errFile));
        runOptions.ErrStream = errFileHolder.Get();
    }
    THolder<TFixedBufferFileOutput> resultFileHolder;
    if (!resultFile.empty()) {
        resultFileHolder.Reset(new TFixedBufferFileOutput(resultFile));
        runOptions.ResultOut = resultFileHolder.Get();
    }
    THolder<TFixedBufferFileOutput> planFileHolder;
    if (!planFile.empty()) {
        planFileHolder.Reset(new TFixedBufferFileOutput(planFile));
        runOptions.TracePlan = planFileHolder.Get();
    }

    for (auto& s: tablesMappingList) {
        TStringBuf tableName, filePath;
        TStringBuf(s).Split('@', tableName, filePath);
        if (tableName.empty() || filePath.empty()) {
            Cerr << "Incorrect table mapping, expected form table@file, e.g. yt.plato.Input@input.txt" << Endl;
            return 1;
        }
        tablesMapping[tableName] = filePath;
    }

    // Reinit logger with new level
    NYql::NLog::ELevel loggingLevel = NYql::NLog::ELevelHelpers::FromInt(verbosity);
    if (verbosity != LOG_DEF_PRIORITY) {
        NYql::NLog::EComponentHelpers::ForEach([loggingLevel](NYql::NLog::EComponent c) {
            NYql::NLog::YqlLogger().SetComponentLevel(c, loggingLevel);
        });
    }

    if (runOptions.TraceOpt) {
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::Core, NYql::NLog::ELevel::TRACE);
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::CoreEval, NYql::NLog::ELevel::TRACE);
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::CorePeepHole, NYql::NLog::ELevel::TRACE);
    } else if (showLog) {
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::Core, NYql::NLog::ELevel::DEBUG);
    }

    YQL_LOG(INFO) << "dqrun ABI version: " << NUdf::CurrentAbiVersionStr();

    if (emulateYt && dqPort) {
        YQL_LOG(ERROR) << "Remote DQ instance cannot work with the emulated YT cluster";
        return 1;
    }

    if (!failureInjections.empty()) {
        TFailureInjector::Activate();
        for (auto& [name, count]: failureInjections) {
            TFailureInjector::Set(name, count.first, count.second);
        }
    }

    runOptions.ResultsFormat =
            (format == TStringBuf("binary")) ? NYson::EYsonFormat::Binary
          : (format == TStringBuf("text")) ? NYson::EYsonFormat::Text
          : NYson::EYsonFormat::Pretty;

    runOptions.User = user;

    NPg::SetSqlLanguageParser(NSQLTranslationPG::CreateSqlLanguageParser());
    NPg::LoadSystemFunctions(*NSQLTranslationPG::CreateSystemFunctionsParser());
    if (!pgExtConfig.empty()) {
        NProto::TPgExtensions config;
        Y_ABORT_UNLESS(NKikimr::ParsePBFromFile(pgExtConfig, &config));
        TVector<NPg::TExtensionDesc> extensions;
        PgExtensionsFromProto(config, extensions);
        NPg::RegisterExtensions(extensions, false,
            *NSQLTranslationPG::CreateExtensionSqlParser(),
            NKikimr::NMiniKQL::CreateExtensionLoader().get());
    }

    NPg::GetSqlLanguageParser()->Freeze();

    TUserDataTable dataTable;
    FillUsedFiles(filesMappingList, dataTable);
    FillUsedUrls(urlMappingList, dataTable);

    NMiniKQL::FindUdfsInDir(udfsDir, &udfsPaths);
    auto funcRegistry = NMiniKQL::CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NMiniKQL::CreateBuiltinRegistry(), false, udfsPaths)->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*funcRegistry);

    TGatewaysConfig gatewaysConfig;
    ReadGatewaysConfig(gatewaysCfgFile, &gatewaysConfig, sqlFlags);
    PatchGatewaysConfig(&gatewaysConfig, mrJobBin, mrJobUdfsDir, numYtThreads, res.Has("keep-temp"));
    if (runOptions.AnalyzeQuery) {
        auto* setting = gatewaysConfig.MutableDq()->AddDefaultSettings();
        setting->SetName("AnalyzeQuery");
        setting->SetValue("1");
    }

    if (res.Has("enable-spilling")) {
        auto* setting = gatewaysConfig.MutableDq()->AddDefaultSettings();
        setting->SetName("SpillingEngine");
        setting->SetValue("file");
    }

    TString defYtServer = gatewaysConfig.HasYt() ? NYql::TConfigClusters::GetDefaultYtServer(gatewaysConfig.GetYt()) : TString();
    auto storage = CreateFS(fileStorageCfg, defYtServer);

    THashMap<TString, TString> clusters;
    clusters["pg_catalog"] = PgProviderName;
    clusters["information_schema"] = PgProviderName;

    TVector<TDataProviderInitializer> dataProvidersInit;
    dataProvidersInit.push_back(GetPgDataProviderInitializer());

    const auto driverConfig = NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(driverConfig);

    Y_DEFER {
        driver.Stop(true);
    };

    TVector<NKikimr::NMiniKQL::TComputationNodeFactory> factories = {
        GetDqYtFactory(),
        GetDqYdbFactory(driver),
        GetCommonDqFactory(),
        NMiniKQL::GetYqlFactory(),
        GetPgFactory()
    };

    NFile::TYtFileServices::TPtr ytFileServices;

    if (emulateYt) {
        ytFileServices = NFile::TYtFileServices::Make(funcRegistry.Get(), tablesMapping, storage, tmpDir, res.Has("keep-temp"));
        for (auto& cluster: gatewaysConfig.GetYt().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{YtProviderName});
        }
        factories.push_back(GetYtFileFactory(ytFileServices));
        clusters["plato"] = YtProviderName;
        auto ytNativeGateway = CreateYtFileGateway(ytFileServices, &emulateOutputForMultirun);
        dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));
    } else if (gatewaysConfig.HasYt()) {
        TYtNativeServices ytServices;
        ytServices.FunctionRegistry = funcRegistry.Get();
        ytServices.FileStorage = storage;
        ytServices.Config = std::make_shared<TYtGatewayConfig>(gatewaysConfig.GetYt());
        auto ytNativeGateway = CreateYtNativeGateway(ytServices);

        for (auto& cluster: gatewaysConfig.GetYt().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{YtProviderName});
        }
        dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));
    }

    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory;

    if (tokenAccessorEndpoint) {
        TVector<TString> ss = StringSplitter(tokenAccessorEndpoint).SplitByString("://");
        YQL_ENSURE(ss.size() == 2, "Invalid tokenAccessorEndpoint: " << tokenAccessorEndpoint); 

        credentialsFactory = NYql::CreateSecuredServiceAccountCredentialsOverTokenAccessorFactory(ss[1], ss[0] == "grpcs", "");
    }

    auto dqCompFactory = NMiniKQL::GetCompositeWithBuiltinFactory(factories);

    // Actor system starts here and will be automatically destroyed when goes out of the scope.
    std::unique_ptr<TActorSystemManager> actorSystemManager;
    TActorIds actorIds;
    std::tie(actorSystemManager, actorIds) = RunActorSystem(gatewaysConfig, metricsRegistry, loggingLevel, credentialsFactory);

    IHTTPGateway::TPtr httpGateway;
    if (gatewaysConfig.HasClickHouse()) {
        for (auto& cluster: gatewaysConfig.GetClickHouse().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{ClickHouseProviderName});
        }
        if (!httpGateway) {
            httpGateway = IHTTPGateway::Make(gatewaysConfig.HasHttpGateway() ? &gatewaysConfig.GetHttpGateway() : nullptr);
        }
        dataProvidersInit.push_back(GetClickHouseDataProviderInitializer(httpGateway));
    }

    std::shared_ptr<NFq::TDatabaseAsyncResolverImpl> dbResolver;
    if (gatewaysConfig.HasDbResolver()) {
        dbResolver = std::make_shared<NFq::TDatabaseAsyncResolverImpl>(
            actorSystemManager->GetActorSystem(),
            actorIds.DatabaseResolver,
            gatewaysConfig.GetDbResolver().GetYdbMvpEndpoint(),
            gatewaysConfig.HasGeneric() ? gatewaysConfig.GetGeneric().GetMdbGateway() : "",
            NFq::MakeMdbEndpointGeneratorGeneric(false)
        );
    }

    NConnector::IClient::TPtr genericClient;
    if (gatewaysConfig.HasGeneric()) {
        for (auto& cluster : *gatewaysConfig.MutableGeneric()->MutableClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{GenericProviderName});
        }

        genericClient = NConnector::MakeClientGRPC(gatewaysConfig.GetGeneric().GetConnector());

        dataProvidersInit.push_back(GetGenericDataProviderInitializer(genericClient, dbResolver, credentialsFactory));
    }

    if (gatewaysConfig.HasYdb()) {
        for (auto& cluster: gatewaysConfig.GetYdb().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{YdbProviderName});
        }
        dataProvidersInit.push_back(GetYdbDataProviderInitializer(driver));
    }

    if (gatewaysConfig.HasS3()) {
        for (auto& cluster: gatewaysConfig.GetS3().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{S3ProviderName});
        }
        if (!httpGateway) {
            httpGateway = IHTTPGateway::Make(gatewaysConfig.HasHttpGateway() ? &gatewaysConfig.GetHttpGateway() : nullptr);
        }
        dataProvidersInit.push_back(GetS3DataProviderInitializer(httpGateway, nullptr, true, nullptr));
    }

    if (gatewaysConfig.HasPq()) {
        TPqGatewayServices pqServices(
            driver,
            nullptr,
            nullptr, // credentials factory
            std::make_shared<TPqGatewayConfig>(gatewaysConfig.GetPq()),
            funcRegistry.Get()
        );
        auto pqGateway = CreatePqNativeGateway(pqServices);
        for (auto& cluster: gatewaysConfig.GetPq().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{PqProviderName});
        }

        dataProvidersInit.push_back(GetPqDataProviderInitializer(pqGateway, false, dbResolver));
    }

    if (gatewaysConfig.HasSolomon()) {
        auto solomonConfig = gatewaysConfig.GetSolomon();
        auto solomonGateway = CreateSolomonGateway(solomonConfig);

        dataProvidersInit.push_back(NYql::GetSolomonDataProviderInitializer(solomonGateway, false));
        for (const auto& cluster: gatewaysConfig.GetSolomon().GetClusterMapping()) {
            clusters.emplace(to_lower(cluster.GetName()), TString{NYql::SolomonProviderName});
        }
    }

    std::function<NActors::IActor*(void)> metricsPusherFactory = {};

    {
        TIntrusivePtr<IDqGateway> dqGateway;
        if (dqPort) {
            dqGateway = CreateDqGateway(dqHost.GetOrElse("localhost"), *dqPort);
        } else {
            auto dqTaskTransformFactory = CreateCompositeTaskTransformFactory({
                CreateCommonDqTaskTransformFactory(),
                CreateYtDqTaskTransformFactory(),
                CreateYdbDqTaskTransformFactory()
            });

            TDqTaskPreprocessorFactoryCollection dqTaskPreprocessorFactories = {
                NDq::CreateYtDqTaskPreprocessorFactory(emulateYt, funcRegistry)
            };

            size_t requestTimeout = gatewaysConfig.HasHttpGateway() && gatewaysConfig.GetHttpGateway().HasRequestTimeoutSeconds() ? gatewaysConfig.GetHttpGateway().GetRequestTimeoutSeconds() : 100;
            size_t maxRetries = gatewaysConfig.HasHttpGateway() && gatewaysConfig.GetHttpGateway().HasMaxRetries() ? gatewaysConfig.GetHttpGateway().GetMaxRetries() : 2;

            bool enableSpilling = res.Has("enable-spilling");
            dqGateway = CreateLocalDqGateway(funcRegistry.Get(), dqCompFactory, dqTaskTransformFactory, dqTaskPreprocessorFactories, enableSpilling,
                CreateAsyncIoFactory(driver, httpGateway, ytFileServices, genericClient, credentialsFactory, *funcRegistry, requestTimeout, maxRetries), threads,
                metricsRegistry, metricsPusherFactory);
        }

        dataProvidersInit.push_back(GetDqDataProviderInitializer(&CreateDqExecTransformer, dqGateway, dqCompFactory, {}, storage));
    }

    TExprContext ctx;
    ctx.NextUniqueId = NPg::GetSqlLanguageParser()->GetContext().NextUniqueId;
    IModuleResolver::TPtr moduleResolver;
    if (!mountConfig.empty()) {
        TModulesTable modules;
        NYqlMountConfig::TMountConfig mount;
        Y_ABORT_UNLESS(NKikimr::ParsePBFromFile(mountConfig, &mount));
        FillUserDataTableFromFileSystem(mount, dataTable);

        if (!CompileLibraries(dataTable, ctx, modules)) {
            *runOptions.ErrStream << "Errors on compile libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(*runOptions.ErrStream);
            return -1;
        }

        moduleResolver = std::make_shared<TModuleResolver>(std::move(modules), ctx.NextUniqueId, clusterMapping, sqlFlags, hasValidate);
    } else {
        if (!GetYqlDefaultModuleResolver(ctx, moduleResolver, clusters)) {
            *runOptions.ErrStream << "Errors loading default YQL libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(*runOptions.ErrStream);
            return 1;
        }
    }

    TExprContext::TFreezeGuard freezeGuard(ctx);

    TProgramFactory progFactory(emulateYt, funcRegistry.Get(), ctx.NextUniqueId, dataProvidersInit, "dqrun");
    progFactory.AddUserDataTable(std::move(dataTable));
    progFactory.SetModules(moduleResolver);
    IUdfResolver::TPtr udfResolverImpl;
    if (udfResolver) {
        udfResolverImpl = NCommon::CreateOutProcUdfResolver(funcRegistry.Get(), storage,
            udfResolver, {}, {}, udfResolverFilterSyscalls, {});
    } else {
        udfResolverImpl = NCommon::CreateSimpleUdfResolver(funcRegistry.Get(), storage, true);
    }

    progFactory.SetUdfResolver(udfResolverImpl);
    progFactory.SetFileStorage(storage);
    progFactory.SetUrlPreprocessing(new TUrlPreprocessing(gatewaysConfig));
    progFactory.SetGatewaysConfig(&gatewaysConfig);
    TCredentials::TPtr creds = MakeIntrusive<TCredentials>();
    if (token) {
        if (!emulateYt) {
            creds->AddCredential("default_yt", TCredential("yt", "", token));
        }
        creds->AddCredential("default_ydb", TCredential("ydb", "", token));
        creds->AddCredential("default_pq", TCredential("pq", "", token));
        creds->AddCredential("default_s3", TCredential("s3", "", token));
        creds->AddCredential("default_solomon", TCredential("solomon", "", token));
        creds->AddCredential("default_generic", TCredential("generic", "", token));
    }
    if (!customTokens.empty()) {
        for (auto& [key, value]: customTokens) {
            creds->AddCredential(key, TCredential("custom", "", value));
        }
    }
    progFactory.SetCredentials(creds);

    progFactory.SetUrlListerManager(
        MakeUrlListerManager(
            {MakeYtUrlLister()}
        )
    );

    TProgramPtr program;
    if (res.Has("replay") && res.Has("capture")) {
        YQL_LOG(ERROR) << "replay and capture options can't be used simultaneously";
        return 1;
    }

    if (res.Has("replay")) {
        program = progFactory.Create("-replay-", "", opId, EHiddenMode::Disable, qContext);
    } else if (progFile == TStringBuf("-")) {
        program = progFactory.Create("-stdin-", Cin.ReadAll(), opId, EHiddenMode::Disable, qContext);
    } else {
        program = progFactory.Create(TFile(progFile, RdOnly), opId, qContext);
        program->SetQueryName(progFile);
    }

    if (paramsFile) {
        TString parameters = TFileInput(paramsFile).ReadAll();
        program->SetParametersYson(parameters);
    }

    if (!emulateYt) {
        program->EnableResultPosition();
    }

    THolder<IOutputStream> statStreamHolder;
    if (res.Has("stat")) {
        if (statFile) {
            statStreamHolder = MakeHolder<TFileOutput>(statFile);
            runOptions.StatisticsStream = statStreamHolder.Get();
        } else {
            runOptions.StatisticsStream = &Cerr;
        }
    }

    if (runOptions.LineageOnly) {
        runOptions.LineageStream = &Cout;
    }

    if (ysonAttrs) {
        program->SetOperationAttrsYson(ysonAttrs);
    }

    if (res.Has("with-final-issues")) {
        runOptions.WithFinalIssues = true;
    }

    int result = RunProgram(std::move(program), runOptions, clusters, sqlFlags);
    if (res.Has("metrics")) {
        NProto::TMetricsRegistrySnapshot snapshot;
        snapshot.SetDontIncrement(true);
        metricsRegistry->TakeSnapshot(&snapshot);
        auto output = MakeHolder<TFileOutput>(metricsFile);
        SerializeToTextFormat(snapshot, *output.Get());
    }

    if (result == 0 && res.Has("capture")) {
        qContext.GetWriter()->Commit().GetValueSync();
    }

    return result;
}

int main(int argc, const char* argv[])
{
    std::set_terminate([] () {
        FormatBackTrace(&Cerr);
        abort();
    });
    Y_UNUSED(NUdf::GetStaticSymbols());
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    NYT::Initialize(argc, argv);

    // Instead of hardcoding logging level, use CLI args:
    // ./dqrun ... -v 6  <- INFO
    // ./dqrun ... -v 7  <- DEBUG
    // ./dqrun ... -v 8  <- TRACE
    auto loggerConfig = NYql::NProto::TLoggingConfig();
    NYql::NLog::InitLogger(loggerConfig, false);

    auto oldBackend = NYql::NLog::YqlLogger().ReleaseBackend();
    NYql::NLog::YqlLogger().ResetBackend(THolder(new NYql::NLog::TTlsLogBackend(oldBackend)));

    //NYql::NLog::YqlLoggerScope logger(&Cerr);

    try {
#ifdef PROFILE_MEMORY_ALLOCATIONS
        NAllocProfiler::StartAllocationSampling(true);
#endif
        const int res = RunMain(argc, argv);
#ifdef PROFILE_MEMORY_ALLOCATIONS
        NAllocProfiler::StopAllocationSampling(Cout);
#endif
        return res;
    } catch (...) {
        Cerr <<  CurrentExceptionMessage() << Endl;
        return 1;
    }
}
