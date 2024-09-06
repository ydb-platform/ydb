#include "mrrun.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs/codecs.h>

#include <ydb/library/yql/providers/yt/lib/log/yt_logger.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/providers/yt/lib/yt_url_lister/yt_url_lister.h>
#include <ydb/library/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/mkql_dq/yql_yt_dq_transform.h>
#include <ydb/library/yql/providers/yt/comp_nodes/dq/dq_yt_factory.h>
#include <ydb/library/yql/providers/yt/dq_task_preprocessor/yql_yt_dq_task_preprocessor.h>

#include <ydb/library/yql/core/url_preprocessing/url_preprocessing.h>

#include <ydb/core/util/pb.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_outproc_udf_resolver.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/clickhouse/actors/yql_ch_source_factory.h>
#include <ydb/library/yql/providers/clickhouse/provider/yql_clickhouse_provider.h>
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h>
#include <ydb/library/yql/providers/ydb/provider/yql_ydb_provider.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_dq_transform.h>
#include <ydb/library/yql/providers/ydb/comp_nodes/yql_ydb_factory.h>
#include <ydb/library/yql/providers/pg/provider/yql_pg_provider.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <ydb/library/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/core/yql_library_compiler.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/file_storage/http_download/http_download.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/core/url_lister/url_lister_manager.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

#include <yt/cpp/mapreduce/client/init.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/malloc/api/malloc.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/logger/priority.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/yson/writer.h>
#include <library/cpp/sighandler/async_signals_handler.h>

#include <google/protobuf/text_format.h>

#include <util/generic/scope.h>
#include <util/system/user.h>
#include <util/system/env.h>
#include <util/stream/length.h>
#include <util/stream/str.h>
#include <util/stream/null.h>
#include <util/string/join.h>
#include <util/string/strip.h>


using namespace NKikimr;
using namespace NYql;

namespace {

struct TRunOptions {
    bool Sql = false;
    TString User;
    NYson::EYsonFormat ResultsFormat;
    bool OptimizeOnly = false;
    bool PeepholeOnly = false;
    bool PrintExpr = false;
    bool TraceOpt = false;
    IOutputStream* TracePlanStream = nullptr;
    bool ShowPropgress = false;
    ui16 SyntaxVersion;
    bool UseMetaFromGrpah = false;
    bool DiscoveryMode = false;
    bool LineageMode = false;
    bool FullStatistics = false;
    IOutputStream* StatisticsStream = nullptr;
    THashSet<TString> SqlFlags;
};

class TOptPipelineConfigurator : public IPipelineConfigurator {
public:
    TOptPipelineConfigurator(TProgramPtr prg, IOutputStream* planStream)
        : Program(std::move(prg))
        , PlanStream(planStream)
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
        if (PlanStream) {
            pipeline->Add(TPlanOutputTransformer::Sync(PlanStream, Program->GetPlanBuilder(), Program->GetOutputFormat()), "PlanOutput");
        }
    }
private:
    TProgramPtr Program;
    IOutputStream* PlanStream;
};

class TPeepHolePipelineConfigurator : public IPipelineConfigurator {
public:
    TPeepHolePipelineConfigurator()
        : Inner({}, nullptr)
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
        pipeline->Add(MakePeepholeOptimization(pipeline->GetTypeAnnotationContext(), &Inner), "PeepHole");
    }

    TOptPipelineConfigurator Inner;
};

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

void ReadGatewaysConfig(const TString& configFile, TGatewaysConfig* config)
{
    Y_ENSURE(!configFile.empty());
    TString configData = TFileInput(configFile).ReadAll();

    using ::google::protobuf::TextFormat;
    if (!TextFormat::ParseFromString(configData, config)) {
        ythrow yexception() << "Bad format of gateways configuration";
    }
}

TFileStoragePtr CreateFS(const TString& paramsFile, const TString& defYtServer)
{
    TFileStorageConfig params;
    Y_ENSURE(paramsFile);
    LoadFsConfigFromFile(paramsFile, params);

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

NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory(const NYdb::TDriver& driver, IHTTPGateway::TPtr httpGateway) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterDqPqReadActorFactory(*factory, driver, nullptr);
    RegisterYdbReadActorFactory(*factory, driver, nullptr);
    RegisterClickHouseReadActorFactory(*factory, nullptr, httpGateway);
    RegisterDqPqWriteActorFactory(*factory, driver, nullptr);

    auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
    auto retryPolicy = GetHTTPDefaultRetryPolicy();
    s3ActorsFactory->RegisterS3WriteActorFactory(*factory, nullptr, httpGateway, retryPolicy);
    s3ActorsFactory->RegisterS3ReadActorFactory(*factory, nullptr, httpGateway, retryPolicy);

    return factory;
}

int RunProgram(TProgramPtr program, const TRunOptions& options, const THashMap<TString, TString>& clusters)
{
    if (options.ShowPropgress) {
        program->SetProgressWriter([](const TOperationProgress& progress) {
            TStringBuilder remoteId;
            if (progress.RemoteId) {
                remoteId << ", remoteId: " << progress.RemoteId;
            }
            TStringBuilder counters;
            if (progress.Counters) {
                if (progress.Counters->Running) {
                    counters << ' ' << progress.Counters->Running;
                }
                if (progress.Counters->Total) {
                    counters << TStringBuf(" (") << (100ul * progress.Counters->Completed / progress.Counters->Total) << TStringBuf("%)");
                }
            }
            Cerr << "Operation: [" << progress.Category << "] " << progress.Id
                << ", state: " << progress.State << remoteId << counters
                << ", current stage: " << progress.Stage.first << Endl;
        });
    }
    program->SetUseTableMetaFromGraph(options.UseMetaFromGrpah);

    bool fail = true;
    if (options.Sql) {
        Cerr << "Parse SQL..." << Endl;
        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = clusters;
        sqlSettings.SyntaxVersion = options.SyntaxVersion;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Report;
        if (options.DiscoveryMode) {
            sqlSettings.Mode = NSQLTranslation::ESqlMode::DISCOVERY;
        }
        sqlSettings.Flags = options.SqlFlags;
        fail = !program->ParseSql(sqlSettings);
    } else {
        Cerr << "Parse YQL..." << Endl;
        fail = !program->ParseYql();
    }
    program->PrintErrorsTo(Cerr);
    if (options.TraceOpt) {
        if (auto ast = program->GetQueryAst()) {
            Cerr << *ast << Endl;
        }
    }
    if (fail) {
        return 1;
    }

    Cerr << "Compile program..." << Endl;
    fail = !program->Compile(options.User);
    program->PrintErrorsTo(Cerr);
    if (options.TraceOpt) {
        program->Print(&Cerr, nullptr);
    }
    if (fail) {
        return 1;
    }

    auto sigHandler = [program](int) {
        Cerr << "Aborting..." << Endl;
        try {
            program->Abort().GetValueSync();
        } catch (...) {
            Cerr << CurrentExceptionMessage();
        }
    };
    SetAsyncSignalFunction(SIGINT, sigHandler);
    SetAsyncSignalFunction(SIGTERM, sigHandler);

    TProgram::TStatus status = TProgram::TStatus::Error;
    if (options.PeepholeOnly) {
        Cerr << "Peephole..." << Endl;
        auto config = TPeepHolePipelineConfigurator();
        status = program->OptimizeWithConfig(options.User, config);
    } else if (options.DiscoveryMode) {
        Cerr << "Discover program..." << Endl;
        status = program->Discover(options.User);
    } else if (options.LineageMode) {
        Cerr << "Calculating lineage in program..." << Endl;
        auto config = TOptPipelineConfigurator(program, options.TracePlanStream);
        status = program->LineageWithConfig(options.User, config);
    } else if (options.OptimizeOnly) {
        Cerr << "Optimize program..." << Endl;
        auto config = TOptPipelineConfigurator(program, options.TracePlanStream);
        status = program->OptimizeWithConfig(options.User, config);
    } else {
        Cerr << "Run program..." << Endl;
        auto config = TOptPipelineConfigurator(program, options.TracePlanStream);
        status = program->RunWithConfig(options.User, config);
    }

    auto dummySigHandler = [](int) { };
    SetAsyncSignalFunction(SIGINT, dummySigHandler);
    SetAsyncSignalFunction(SIGTERM, dummySigHandler);

    program->PrintErrorsTo(Cerr);
    if (status == TProgram::TStatus::Error) {
        if (options.TraceOpt) {
            program->Print(&Cerr, nullptr);
        }
        return 1;
    }
    if (options.PrintExpr) {
        program->Print(&Cout, nullptr);
    }

    Cerr << "Getting results..." << Endl;
    if (options.DiscoveryMode) {
        if (auto data = program->GetDiscoveredData()) {
            TStringInput in(*data);
            NYson::ReformatYsonStream(&in, &Cout, options.ResultsFormat);
        }
    } else if (options.LineageMode) {
        if (auto data = program->GetLineage()) {
            TStringInput in(*data);
            NYson::ReformatYsonStream(&in, &Cout, options.ResultsFormat);
        }
    } else if (program->HasResults()) {
        NYson::TYsonWriter yson(&Cout, options.ResultsFormat);
        yson.OnBeginList();
        for (const auto& result: program->Results()) {
            yson.OnListItem();
            yson.OnRaw(result);
        }
        yson.OnEndList();
    }

    if (options.StatisticsStream) {
        if (auto st = program->GetStatistics(!options.FullStatistics)) {
            TStringInput in(*st);
            NYson::ReformatYsonStream(&in, options.StatisticsStream, NYson::EYsonFormat::Pretty);
        }
    }

    Cerr << Endl << "Done" << Endl;
    return 0;
}

int RunMain(int argc, const char* argv[])
{
    TString gatewaysCfgFile;
    TString progFile;
    TString user = GetUsername();
    TString format;
    TVector<TString> filesMappingList;
    TVector<TString> urlMappingList;
    TString fileStorageCfg;
    TVector<TString> udfsPaths;
    TString udfsDir;
    THashSet<TString> gatewayTypes;
    THashSet<TString> defGatewayTypes = {
        TString(YtProviderName),
        TString(YdbProviderName),
        TString(DqProviderName),
    };
    TString mountConfig;
    TString udfResolver;
    bool udfResolverFilterSyscalls = false;
    TString mrJobBin;
    TString mrJobUdfsDir;
    TString statFile;
    int verbosity = 3;
    bool showLog = false;
    bool tracePlan;
    size_t numThreads = 1;
    TString planFile;
    TString paramsFile;
    bool keepTemp = false;
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

    TRunOptions runOptions;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('p', "program", "Program to execute (use '-' to read "
                                       "from stdin)")
        .Required()
        .RequiredArgument("FILE")
        .StoreResult(&progFile);
    opts.AddLongOption('s', "sql", "Program is SQL query")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.Sql);
    opts.AddLongOption('u', "user", "MR user")
        .Optional()
        .RequiredArgument("USER")
        .StoreResult(&user);
    opts.AddLongOption("format", "results format, one of "
                                 "{ binary | text | pretty }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue("text")
        .StoreResult(&format);
    opts.AddLongOption('f', "file", "name@path").AppendTo(&filesMappingList);
    opts.AddLongOption("url", "name@url").AppendTo(&urlMappingList);
    opts.AddLongOption("gateways-cfg", "gateways configuration file")
        .Optional()
        .DefaultValue("gateways.conf")
        .RequiredArgument("FILE")
        .StoreResult(&gatewaysCfgFile);
    opts.AddLongOption("fs-cfg", "Path to file storage config")
        .Optional()
        .DefaultValue("fs.conf")
        .StoreResult(&fileStorageCfg);
    opts.AddLongOption("udf", "Load shared library with UDF by given path")
        .AppendTo(&udfsPaths);
    opts.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found"
                                   " in given directory")
        .StoreResult(&udfsDir);
    opts.AddLongOption('O',"optimize", "optimize expression")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.OptimizeOnly);
    opts.AddLongOption('D', "discover", "discover tables in the program")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.DiscoveryMode);
    opts.AddLongOption("lineage", "calclulate data lineage in the program")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.LineageMode);
    opts.AddLongOption("peephole", "make peephole optimization")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.PeepholeOnly);
    opts.AddLongOption("print-expr", "print rebuild AST before execution")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.PrintExpr);
    opts.AddLongOption('P',"trace-plan", "print plan before execution")
        .Optional()
        .NoArgument()
        .SetFlag(&tracePlan);
    opts.AddLongOption("plan-file", "file path for plan output")
        .Optional()
        .StoreResult(&planFile);
    opts.AddLongOption("params-file", "Query parameters values in YSON format").StoreResult(&paramsFile);
    opts.AddLongOption("trace-opt", "print AST in the begin of each transformation")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.TraceOpt);
    opts.AddLongOption('L', "show-log", "show transformation log")
        .Optional()
        .NoArgument()
        .SetFlag(&showLog);
    opts.AddLongOption('G', "gateways", "used gateways")
        .SplitHandler(&gatewayTypes, ',')
        .DefaultValue(JoinSeq(",", defGatewayTypes));
    opts.AddLongOption('m', "mounts", "Mount points config file.")
        .StoreResult(&mountConfig);
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
    opts.AddLongOption('v', "verbosity", "Log verbosity level")
        .Optional()
        .RequiredArgument("LEVEL")
        .DefaultValue("6")
        .StoreResult(&verbosity);
    opts.AddLongOption("show-progress", "report operation progress")
        .NoArgument()
        .SetFlag(&runOptions.ShowPropgress);;
    opts.AddLongOption("threads", "gateway threads")
        .Optional()
        .RequiredArgument("COUNT")
        .StoreResult(&numThreads);
    opts.AddLongOption("keep-temp", "keep temporary tables")
        .Optional()
        .NoArgument()
        .SetFlag(&keepTemp);
    opts.AddLongOption("syntax-version", "SQL syntax version").StoreResult(&runOptions.SyntaxVersion).DefaultValue(1);
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
    opts.AddLongOption("use-graph-meta", "Use tables metadata from graph")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.UseMetaFromGrpah);
    opts.AddLongOption("stat", "Print execution statistics")
        .Optional()
        .OptionalArgument("FILE")
        .StoreResult(&statFile);
    opts.AddLongOption("full-stat", "Output full execution statistics")
        .Optional()
        .NoArgument()
        .SetFlag(&runOptions.FullStatistics);
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

    opts.SetFreeArgsNum(0);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);
    Y_UNUSED(res);

    // Reinit logger with new level
    if (verbosity != LOG_DEF_PRIORITY) {
        NYql::NLog::ELevel level = NYql::NLog::ELevelHelpers::FromInt(verbosity);
        NYql::NLog::EComponentHelpers::ForEach([level](NYql::NLog::EComponent c) {
            NYql::NLog::YqlLogger().SetComponentLevel(c, level);
        });
    }

    if (runOptions.TraceOpt) {
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::Core, NYql::NLog::ELevel::TRACE);
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::CoreEval, NYql::NLog::ELevel::TRACE);
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::CorePeepHole, NYql::NLog::ELevel::TRACE);
    } else if (showLog) {
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::Core, NYql::NLog::ELevel::DEBUG);
    }

    runOptions.ResultsFormat =
            (format == TStringBuf("binary")) ? NYson::EYsonFormat::Binary
          : (format == TStringBuf("text")) ? NYson::EYsonFormat::Text
          : NYson::EYsonFormat::Pretty;

    runOptions.User = user;

    for (auto& gateway: gatewayTypes) {
        if (!defGatewayTypes.contains(gateway)) {
            Cerr << "Unsupported gateway " << gateway.Quote() << Endl;
            return 1;
        }
    }

    if (!failureInjections.empty()) {
        TFailureInjector::Activate();
        for (auto& [name, count]: failureInjections) {
            TFailureInjector::Set(name, count.first, count.second);
        }
    }

    TUserDataTable dataTable;
    FillUsedFiles(filesMappingList, dataTable);
    FillUsedUrls(urlMappingList, dataTable);

    NMiniKQL::FindUdfsInDir(udfsDir, &udfsPaths);
    auto funcRegistry = NMiniKQL::CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NMiniKQL::CreateBuiltinRegistry(), false, udfsPaths);

    TGatewaysConfig gatewaysConfig;
    ReadGatewaysConfig(gatewaysCfgFile, &gatewaysConfig);
    PatchGatewaysConfig(&gatewaysConfig, mrJobBin, mrJobUdfsDir, numThreads, keepTemp);

    TString defYtServer = gatewaysConfig.HasYt() ? NYql::TConfigClusters::GetDefaultYtServer(gatewaysConfig.GetYt()) : TString();
    auto storage = CreateFS(fileStorageCfg, defYtServer);

    auto httpGateway = IHTTPGateway::Make();
    TVector<TIntrusivePtr<TThrRefBase>> gateways;
    THashMap<TString, TString> clusters;
    TVector<TDataProviderInitializer> dataProvidersInit;
    clusters["pg_catalog"] = PgProviderName;
    dataProvidersInit.push_back(GetPgDataProviderInitializer());

    if (gatewayTypes.contains(YtProviderName) && gatewaysConfig.HasYt()) {
        TYtNativeServices services;
        services.FunctionRegistry = funcRegistry.Get();
        services.FileStorage = storage;
        services.Config = std::make_shared<TYtGatewayConfig>(gatewaysConfig.GetYt());
        auto ytNativeGateway = CreateYtNativeGateway(services);
        gateways.emplace_back(ytNativeGateway);
        FillClusterMapping(clusters, gatewaysConfig.GetYt(), TString{YtProviderName});
        dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytNativeGateway));
    }

    if (gatewayTypes.contains(ClickHouseProviderName) && gatewaysConfig.HasClickHouse()) {
        FillClusterMapping(clusters, gatewaysConfig.GetClickHouse(), TString{ClickHouseProviderName});
        dataProvidersInit.push_back(GetClickHouseDataProviderInitializer(httpGateway));
    }

    const auto driverConfig = NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(driverConfig);

    Y_DEFER {
        driver.Stop(true);
    };

    if (gatewayTypes.contains(YdbProviderName) && gatewaysConfig.HasYdb()) {
        FillClusterMapping(clusters, gatewaysConfig.GetYdb(), TString{YdbProviderName});
        dataProvidersInit.push_back(GetYdbDataProviderInitializer(driver));
    }

#ifndef _win_
    if (gatewayTypes.contains(DqProviderName)) {
        auto dqTaskTransformFactory = CreateCompositeTaskTransformFactory({
            CreateYtDqTaskTransformFactory(),
            CreateYdbDqTaskTransformFactory()
        });

        auto dqCompFactory = NMiniKQL::GetCompositeWithBuiltinFactory({
            GetCommonDqFactory(),
            GetDqYtFactory(),
            GetDqYdbFactory(driver),
            NMiniKQL::GetYqlFactory(),
            GetPgFactory()
        });

        TDqTaskPreprocessorFactoryCollection dqTaskPreprocessorFactories = {
            NDq::CreateYtDqTaskPreprocessorFactory(false, funcRegistry)
        };

        const bool enableSpilling = true;
        auto dqGateway = CreateLocalDqGateway(funcRegistry.Get(), dqCompFactory, dqTaskTransformFactory, dqTaskPreprocessorFactories,
            enableSpilling, CreateAsyncIoFactory(driver, httpGateway));
        gateways.emplace_back(dqGateway);
        dataProvidersInit.push_back(GetDqDataProviderInitializer(&CreateDqExecTransformer, dqGateway, dqCompFactory, {}, storage));
    }
#endif

    if (gatewaysConfig.HasSqlCore()) {
        runOptions.SqlFlags.insert(gatewaysConfig.GetSqlCore().GetTranslationFlags().begin(), gatewaysConfig.GetSqlCore().GetTranslationFlags().end());
    }

    TExprContext ctx;
    IModuleResolver::TPtr moduleResolver;
    if (!mountConfig.empty()) {
        TModulesTable modules;
        NYqlMountConfig::TMountConfig mount;
        Y_ABORT_UNLESS(NKikimr::ParsePBFromFile(mountConfig, &mount));
        FillUserDataTableFromFileSystem(mount, dataTable);

        if (!CompileLibraries(dataTable, ctx, modules)) {
            Cerr << "Errors on compile libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(Cerr);
            return 1;
        }

        moduleResolver = std::make_shared<TModuleResolver>(std::move(modules), ctx.NextUniqueId, clusters, THashSet<TString>());
    } else {
        if (!GetYqlDefaultModuleResolver(ctx, moduleResolver, clusters)) {
            Cerr << "Errors loading default YQL libraries:" << Endl;
            ctx.IssueManager.GetIssues().PrintTo(Cerr);
            return 1;
        }
    }

    TExprContext::TFreezeGuard freezeGuard(ctx);

    TProgramFactory progFactory(false, funcRegistry.Get(), ctx.NextUniqueId, dataProvidersInit, "mrrun");
    progFactory.AddUserDataTable(std::move(dataTable));
    progFactory.SetModules(moduleResolver);
    if (udfResolver) {
        progFactory.SetUdfResolver(NCommon::CreateOutProcUdfResolver(funcRegistry.Get(), storage,
            udfResolver, {}, {}, udfResolverFilterSyscalls, {}));
    } else {
        progFactory.SetUdfResolver(NCommon::CreateSimpleUdfResolver(funcRegistry.Get(), storage));
    }
    progFactory.SetFileStorage(storage);
    progFactory.SetUrlPreprocessing(new TUrlPreprocessing(gatewaysConfig));
    progFactory.SetGatewaysConfig(&gatewaysConfig);
    TCredentials::TPtr creds = MakeIntrusive<TCredentials>();
    if (token) {
        creds->AddCredential("default_yt", TCredential("yt", "", token));
        creds->AddCredential("default_ydb", TCredential("ydb", "", token));
        creds->AddCredential("default_pq", TCredential("pq", "", token));
        creds->AddCredential("default_solomon", TCredential("solomon", "", token));
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
    if (progFile == TStringBuf("-")) {
        program = progFactory.Create("-stdin-", Cin.ReadAll());
    } else {
        program = progFactory.Create(TFile(progFile, RdOnly));
        program->SetQueryName(progFile);
    }

    if (paramsFile) {
        TString parameters = TFileInput(paramsFile).ReadAll();
        program->SetParametersYson(parameters);
    }

    program->EnableResultPosition();
    THolder<IOutputStream> planStreamHolder;
    if (tracePlan) {
        if (!planFile.empty()) {
            planStreamHolder = MakeHolder<TFileOutput>(planFile);
            runOptions.TracePlanStream = planStreamHolder.Get();
        } else {
            runOptions.TracePlanStream = &Cerr;
        }
    } else if (runOptions.ShowPropgress) {
        runOptions.TracePlanStream = &Cnull;
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

    return RunProgram(std::move(program), runOptions, clusters);
}

} // namespace

void FlushYtDebugLogOnSignal() {
    if (!NMalloc::IsAllocatorCorrupted) {
        NYql::FlushYtDebugLog();
    }
}

int main(int argc, const char *argv[])
{
    Y_UNUSED(NUdf::GetStaticSymbols());
    NYql::NBacktrace::AddAfterFatalCallback([](int){ FlushYtDebugLogOnSignal(); });
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    // Init MR/YT for proper work of embedded agent
    NYT::Initialize(argc, argv);

    NYql::NLog::YqlLoggerScope logger(&Cerr);
    NYql::SetYtLoggerGlobalBackend(LOG_DEF_PRIORITY);

    YQL_LOG(INFO) << "mrrun ABI version: " << NUdf::CurrentAbiVersionStr();

    if (NYT::TConfig::Get()->Prefix.empty()) {
        NYT::TConfig::Get()->Prefix = "//";
    }

    try {
        int res = RunMain(argc, argv);
        if (0 == res) {
            NYql::DropYtDebugLog();
        }
        return res;
    } catch (...) {
        Cerr <<  CurrentExceptionMessage() << Endl;
        return 1;
    }
}
