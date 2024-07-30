#include "src/kqp_runner.h"

#include <cstdio>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>

#include <util/stream/file.h>
#include <util/system/env.h>

#include <ydb/core/base/backtrace.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_comp_nodes.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/public/udf/udf_static_registry.h>


struct TExecutionOptions {
    enum class EExecutionCase {
        GenericScript,
        GenericQuery,
        YqlScript,
        AsyncQuery
    };

    std::vector<TString> ScriptQueries;
    TString SchemeQuery;

    ui32 LoopCount = 1;
    TDuration LoopDelay;

    bool ForgetExecution = false;
    std::vector<EExecutionCase> ExecutionCases;
    std::vector<NKikimrKqp::EQueryAction> ScriptQueryActions;
    std::vector<TString> TraceIds;
    std::vector<TString> PoolIds;
    std::vector<TString> UserSIDs;

    const TString DefaultTraceId = "kqprun";

    bool HasResults() const {
        if (ScriptQueries.empty()) {
            return false;
        }

        for (size_t i = 0; i < ExecutionCases.size(); ++i) {
            if (GetScriptQueryAction(i) != NKikimrKqp::EQueryAction::QUERY_ACTION_EXECUTE) {
                continue;
            }
            if (ExecutionCases[i] != EExecutionCase::AsyncQuery) {
                return true;
            }
        }
        return false;
    }

    EExecutionCase GetExecutionCase(size_t index) const {
        return GetValue(index, ExecutionCases, EExecutionCase::GenericScript);
    }

    NKikimrKqp::EQueryAction GetScriptQueryAction(size_t index) const {
        return GetValue(index, ScriptQueryActions, NKikimrKqp::EQueryAction::QUERY_ACTION_EXECUTE);
    }

    NKqpRun::TRequestOptions GetSchemeQueryOptions() const {
        return {
            .Query = SchemeQuery,
            .Action = NKikimrKqp::EQueryAction::QUERY_ACTION_EXECUTE,
            .TraceId = DefaultTraceId,
            .PoolId = "",
            .UserSID = BUILTIN_ACL_ROOT
        };
    }

    NKqpRun::TRequestOptions GetScriptQueryOptions(size_t index, TInstant startTime) const {
        Y_ABORT_UNLESS(index < ScriptQueries.size());
        return {
            .Query = ScriptQueries[index],
            .Action = GetScriptQueryAction(index),
            .TraceId = TStringBuilder() << GetValue(index, TraceIds, DefaultTraceId) << "-" << startTime.ToString(),
            .PoolId = GetValue(index, PoolIds, TString()),
            .UserSID = GetValue(index, UserSIDs, TString(BUILTIN_ACL_ROOT))
        };
    }

private:
    template <typename TValue>
    static TValue GetValue(size_t index, const std::vector<TValue>& values, TValue defaultValue) {
        if (values.empty()) {
            return defaultValue;
        }
        return values[std::min(index, values.size() - 1)];
    }
};


void RunArgumentQueries(const TExecutionOptions& executionOptions, NKqpRun::TKqpRunner& runner) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    if (executionOptions.SchemeQuery) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Executing scheme query..." << colors.Default() << Endl;
        if (!runner.ExecuteSchemeQuery(executionOptions.GetSchemeQueryOptions())) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Scheme query execution failed";
        }
    }

    const size_t numberQueries = executionOptions.ScriptQueries.size();
    const size_t numberLoops = executionOptions.LoopCount;
    for (size_t queryId = 0; queryId < numberQueries * numberLoops || numberLoops == 0; ++queryId) {
        size_t id = queryId % numberQueries;
        if (id == 0 && queryId > 0) {
            Sleep(executionOptions.LoopDelay);
        }

        const TInstant startTime = TInstant::Now();
        const auto executionCase = executionOptions.GetExecutionCase(id);
        if (executionCase != TExecutionOptions::EExecutionCase::AsyncQuery) {
            Cout << colors.Yellow() << startTime.ToIsoStringLocal() << " Executing script";
            if (numberQueries > 1) {
                Cout << " " << id;
            }
            if (numberLoops != 1) {
                Cout << ", loop " << queryId / numberQueries;
            }
            Cout << "..." << colors.Default() << Endl;
        }

        switch (executionCase) {
        case TExecutionOptions::EExecutionCase::GenericScript:
            if (!runner.ExecuteScript(executionOptions.GetScriptQueryOptions(id, startTime))) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Script execution failed";
            }
            Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Fetching script results..." << colors.Default() << Endl;
            if (!runner.FetchScriptResults()) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Fetch script results failed";
            }
            if (executionOptions.ForgetExecution) {
                Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Forgetting script execution operation..." << colors.Default() << Endl;
                if (!runner.ForgetExecutionOperation()) {
                    ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Forget script execution operation failed";
                }
            }
            break;

        case TExecutionOptions::EExecutionCase::GenericQuery:
            if (!runner.ExecuteQuery(executionOptions.GetScriptQueryOptions(id, startTime))) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Query execution failed";
            }
            break;

        case TExecutionOptions::EExecutionCase::YqlScript:
            if (!runner.ExecuteYqlScript(executionOptions.GetScriptQueryOptions(id, startTime))) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Yql script execution failed";
            }
            break;

        case TExecutionOptions::EExecutionCase::AsyncQuery:
            runner.ExecuteQueryAsync(executionOptions.GetScriptQueryOptions(id, startTime));
            break;
        }
    }
    runner.WaitAsyncQueries();

    if (executionOptions.HasResults()) {
        try {
            runner.PrintScriptResults();
        } catch (...) {
            ythrow yexception() << "Failed to print script results, reason:\n" <<  CurrentExceptionMessage();
        }
    }
}


void RunAsDaemon() {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Started reading commands" << colors.Default() << Endl;
    while (true) {
        TString command;
        Cin >> command;

        if (command == "exit") {
            break;
        }
        Cerr << colors.Red() << TInstant::Now().ToIsoStringLocal() << " Invalid command '" << command << "'" << colors.Default() << Endl;
    }
}


void RunScript(const TExecutionOptions& executionOptions, const NKqpRun::TRunnerOptions& runnerOptions) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Initialization of kqp runner..." << colors.Default() << Endl;
    NKqpRun::TKqpRunner runner(runnerOptions);

    try {
        RunArgumentQueries(executionOptions, runner);
    } catch (const yexception& exception) {
        if (runnerOptions.YdbSettings.MonitoringEnabled) {
            Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        } else {
            throw exception;
        }
    }

    if (runnerOptions.YdbSettings.MonitoringEnabled || runnerOptions.YdbSettings.GrpcEnabled) {
        RunAsDaemon();
    }

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Finalization of kqp runner..." << colors.Default() << Endl;
}


TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> CreateFunctionRegistry(const TString& udfsDirectory, TVector<TString> udfsPaths, bool excludeLinkedUdfs) {
    if (!udfsDirectory.empty() || !udfsPaths.empty()) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Fetching udfs..." << colors.Default() << Endl;
    }

    NKikimr::NMiniKQL::FindUdfsInDir(udfsDirectory, &udfsPaths);
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, udfsPaths)->Clone();

    if (excludeLinkedUdfs) {
        for (const auto& wrapper : NYql::NUdf::GetStaticUdfModuleWrapperList()) {
            auto [name, ptr] = wrapper();
            if (!functionRegistry->IsLoadedUdfModule(name)) {
                functionRegistry->AddModule(TString(NKikimr::NMiniKQL::StaticModulePrefix) + name, name, std::move(ptr));
            }
        }
    } else {
        NKikimr::NMiniKQL::FillStaticModules(*functionRegistry);
    }

    return functionRegistry;
}


class TMain : public TMainClassArgs {
    inline static const TString YqlToken = GetEnv(NKqpRun::YQL_TOKEN_VARIABLE);
    inline static std::vector<std::unique_ptr<TFileOutput>> FileHolders;

    TExecutionOptions ExecutionOptions;
    NKqpRun::TRunnerOptions RunnerOptions;

    THashMap<TString, TString> TablesMapping;
    TVector<TString> UdfsPaths;
    TString UdfsDirectory;
    bool ExcludeLinkedUdfs = false;
    ui64 ResultsRowsLimit = 1000;
    bool EmulateYt = false;

    static TString LoadFile(const TString& file) {
        return TFileInput(file).ReadAll();
    }

    static void ReplaceTemplate(const TString& variableName, const TString& variableValue, TString& query) {
        TString variableTemplate = TStringBuilder() << "${" << variableName << "}";
        for (size_t position = query.find(variableTemplate); position != TString::npos; position = query.find(variableTemplate, position)) {
            query.replace(position, variableTemplate.size(), variableValue);
            position += variableValue.size();
        }
    }

    static IOutputStream* GetDefaultOutput(const TString& file) {
        if (file == "-") {
            return &Cout;
        }
        if (file) {
            FileHolders.emplace_back(new TFileOutput(file));
            return FileHolders.back().get();
        }
        return nullptr;
    }

    template <typename TResult>
    class TChoices {
    public:
        explicit TChoices(std::map<TString, TResult> choicesMap)
            : ChoicesMap(std::move(choicesMap))
        {}

        TResult operator()(const TString& choice) const {
            return ChoicesMap.at(choice);
        }

        TVector<TString> GetChoices() const {
            TVector<TString> choices;
            choices.reserve(ChoicesMap.size());
            for (const auto& [choice, _] : ChoicesMap) {
                choices.emplace_back(choice);
            }
            return choices;
        }

    private:
        const std::map<TString, TResult> ChoicesMap;
    };

protected:
    void RegisterOptions(NLastGetopt::TOpts& options) override {
        options.SetTitle("KqpRun -- tool to execute queries by using kikimr provider (instead of dq provider in DQrun tool)");
        options.AddHelpOption('h');
        options.SetFreeArgsNum(0);

        // Inputs

        options.AddLongOption('s', "scheme-query", "Scheme query to execute (typically DDL/DCL query)")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                ExecutionOptions.SchemeQuery = LoadFile(option->CurVal());
                ReplaceTemplate(NKqpRun::YQL_TOKEN_VARIABLE, YqlToken, ExecutionOptions.SchemeQuery);
            });
        options.AddLongOption('p', "script-query", "Script query to execute (typically DML query)")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                ExecutionOptions.ScriptQueries.emplace_back(LoadFile(option->CurVal()));
            });

        options.AddLongOption('t', "table", "File with input table (can be used by YT with -E flag), table@file")
            .RequiredArgument("table@file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf tableName;
                TStringBuf filePath;
                TStringBuf(option->CurVal()).Split('@', tableName, filePath);
                if (tableName.empty() || filePath.empty()) {
                    ythrow yexception() << "Incorrect table mapping, expected form table@file, e.g. yt.Root/plato.Input@input.txt";
                }
                if (TablesMapping.contains(tableName)) {
                    ythrow yexception() << "Got duplicate table name: " << tableName;
                }
                TablesMapping[tableName] = filePath;
            });

        options.AddLongOption('c', "app-config", "File with app config (TAppConfig for ydb tennant)")
            .RequiredArgument("file")
            .DefaultValue("./configuration/app_config.conf")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TString file(option->CurValOrDef());
                if (file.EndsWith(".yaml")) {
                    auto document = NKikimr::NFyaml::TDocument::Parse(LoadFile(file));
                    RunnerOptions.YdbSettings.AppConfig = NKikimr::NYamlConfig::YamlToProto(document.Root());
                } else if (!google::protobuf::TextFormat::ParseFromString(LoadFile(file), &RunnerOptions.YdbSettings.AppConfig)) {
                    ythrow yexception() << "Bad format of app configuration";
                }
            });

        options.AddLongOption('u', "udf", "Load shared library with UDF by given path")
            .RequiredArgument("file")
            .EmplaceTo(&UdfsPaths);
        options.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory")
            .RequiredArgument("directory")
            .StoreResult(&UdfsDirectory);
        options.AddLongOption("exclude-linked-udfs", "Exclude linked udfs when same udf passed from -u or --udfs-dir")
            .NoArgument()
            .SetFlag(&ExcludeLinkedUdfs);

        // Outputs

        options.AddLongOption("log-file", "File with execution logs (writes in stderr if empty)")
            .RequiredArgument("file")
            .StoreResult(&RunnerOptions.YdbSettings.LogOutputFile)
            .Handler1([](const NLastGetopt::TOptsParser* option) {
                if (const TString& file = option->CurVal()) {
                    std::remove(file.c_str());
                }
            });
        TChoices<NKqpRun::TRunnerOptions::ETraceOptType> traceOpt({
            {"all", NKqpRun::TRunnerOptions::ETraceOptType::All},
            {"scheme", NKqpRun::TRunnerOptions::ETraceOptType::Scheme},
            {"script", NKqpRun::TRunnerOptions::ETraceOptType::Script},
            {"disabled", NKqpRun::TRunnerOptions::ETraceOptType::Disabled}
        });
        options.AddLongOption('T', "trace-opt", "print AST in the begin of each transformation")
            .RequiredArgument("trace-opt-query")
            .DefaultValue("disabled")
            .Choices(traceOpt.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.TraceOptType, [this, traceOpt](const TString& choise) {
                auto traceOptType = traceOpt(choise);
                RunnerOptions.YdbSettings.TraceOptEnabled = traceOptType != NKqpRun::TRunnerOptions::ETraceOptType::Disabled;
                return traceOptType;
            });
        options.AddLongOption("trace-id", "Trace id for -p queries")
            .RequiredArgument("id")
            .EmplaceTo(&ExecutionOptions.TraceIds);

        options.AddLongOption("result-file", "File with script execution results (use '-' to write in stdout)")
            .RequiredArgument("file")
            .DefaultValue("-")
            .StoreMappedResultT<TString>(&RunnerOptions.ResultOutput, &GetDefaultOutput);
        options.AddLongOption('L', "result-rows-limit", "Rows limit for script execution results")
            .RequiredArgument("uint")
            .DefaultValue(ResultsRowsLimit)
            .StoreResult(&ResultsRowsLimit);
        TChoices<NKqpRun::TRunnerOptions::EResultOutputFormat> resultFormat({
            {"rows", NKqpRun::TRunnerOptions::EResultOutputFormat::RowsJson},
            {"full-json", NKqpRun::TRunnerOptions::EResultOutputFormat::FullJson},
            {"full-proto", NKqpRun::TRunnerOptions::EResultOutputFormat::FullProto}
        });
        options.AddLongOption('R', "result-format", "Script query result format")
            .RequiredArgument("result-format")
            .DefaultValue("rows")
            .Choices(resultFormat.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.ResultOutputFormat, resultFormat);

        options.AddLongOption("scheme-ast-file", "File with scheme query ast (use '-' to write in stdout)")
            .RequiredArgument("file")
            .StoreMappedResultT<TString>(&RunnerOptions.SchemeQueryAstOutput, &GetDefaultOutput);

        options.AddLongOption("script-ast-file", "File with script query ast (use '-' to write in stdout)")
            .RequiredArgument("file")
            .StoreMappedResultT<TString>(&RunnerOptions.ScriptQueryAstOutput, &GetDefaultOutput);

        options.AddLongOption("script-plan-file", "File with script query plan (use '-' to write in stdout)")
            .RequiredArgument("file")
            .StoreMappedResultT<TString>(&RunnerOptions.ScriptQueryPlanOutput, &GetDefaultOutput);
        options.AddLongOption("script-statistics", "File with script inprogress statistics")
            .RequiredArgument("file")
            .StoreResult(&RunnerOptions.InProgressStatisticsOutputFile);
        TChoices<NYdb::NConsoleClient::EOutputFormat> planFormat({
            {"pretty", NYdb::NConsoleClient::EOutputFormat::Pretty},
            {"table", NYdb::NConsoleClient::EOutputFormat::PrettyTable},
            {"json", NYdb::NConsoleClient::EOutputFormat::JsonUnicode},
        });
        options.AddLongOption('P', "plan-format", "Script query plan format")
            .RequiredArgument("plan-format")
            .DefaultValue("pretty")
            .Choices(planFormat.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.PlanOutputFormat, planFormat);

        // Pipeline settings

        TChoices<TExecutionOptions::EExecutionCase> executionCase({
            {"script", TExecutionOptions::EExecutionCase::GenericScript},
            {"query", TExecutionOptions::EExecutionCase::GenericQuery},
            {"yql-script", TExecutionOptions::EExecutionCase::YqlScript},
            {"async", TExecutionOptions::EExecutionCase::AsyncQuery}
        });
        options.AddLongOption('C', "execution-case", "Type of query for -p argument")
            .RequiredArgument("query-type")
            .DefaultValue("script")
            .Choices(executionCase.GetChoices())
            .Handler1([this, executionCase](const NLastGetopt::TOptsParser* option) {
                TString choice(option->CurValOrDef());
                ExecutionOptions.ExecutionCases.emplace_back(executionCase(choice));
            });
        options.AddLongOption("inflight-limit", "In flight limit for async queries (use 0 for unlimited)")
            .RequiredArgument("uint")
            .DefaultValue(RunnerOptions.YdbSettings.AsyncQueriesSettings.InFlightLimit)
            .StoreResult(&RunnerOptions.YdbSettings.AsyncQueriesSettings.InFlightLimit);
        TChoices<NKqpRun::TAsyncQueriesSettings::EVerbose> verbose({
            {"each-query", NKqpRun::TAsyncQueriesSettings::EVerbose::EachQuery},
            {"final", NKqpRun::TAsyncQueriesSettings::EVerbose::Final}
        });
        options.AddLongOption("async-verbose", "Verbose type for async queries")
            .RequiredArgument("type")
            .DefaultValue("each-query")
            .Choices(verbose.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.YdbSettings.AsyncQueriesSettings.Verbose, verbose);

        TChoices<NKikimrKqp::EQueryAction> scriptAction({
            {"execute", NKikimrKqp::QUERY_ACTION_EXECUTE},
            {"explain", NKikimrKqp::QUERY_ACTION_EXPLAIN}
        });
        options.AddLongOption('A', "script-action", "Script query execute action")
            .RequiredArgument("script-action")
            .DefaultValue("execute")
            .Choices(scriptAction.GetChoices())
            .Handler1([this, scriptAction](const NLastGetopt::TOptsParser* option) {
                TString choice(option->CurValOrDef());
                ExecutionOptions.ScriptQueryActions.emplace_back(scriptAction(choice));
            });

        options.AddLongOption('F', "forget", "Forget script execution operation after fetching results")
            .NoArgument()
            .SetFlag(&ExecutionOptions.ForgetExecution);

        options.AddLongOption("loop-count", "Number of runs of the script query (use 0 to start infinite loop)")
            .RequiredArgument("uint")
            .DefaultValue(ExecutionOptions.LoopCount)
            .StoreResult(&ExecutionOptions.LoopCount);
        options.AddLongOption("loop-delay", "Delay in milliseconds between loop steps")
            .RequiredArgument("uint")
            .DefaultValue(1000)
            .StoreMappedResultT<ui64>(&ExecutionOptions.LoopDelay, &TDuration::MilliSeconds<ui64>);

        options.AddLongOption('U', "user", "User SID for -p queries")
            .RequiredArgument("user-SID")
            .EmplaceTo(&ExecutionOptions.UserSIDs);

        options.AddLongOption("pool", "Workload manager pool in which queries will be executed")
            .RequiredArgument("pool-id")
            .EmplaceTo(&ExecutionOptions.PoolIds);

        // Cluster settings

        options.AddLongOption('N', "node-count", "Number of nodes to create")
            .RequiredArgument("uint")
            .DefaultValue(RunnerOptions.YdbSettings.NodeCount)
            .StoreMappedResultT<ui32>(&RunnerOptions.YdbSettings.NodeCount, [](ui32 nodeCount) {
                if (nodeCount < 1) {
                    ythrow yexception() << "Number of nodes less than one";
                }
                return nodeCount;
            });

        options.AddLongOption('M', "monitoring", "Embedded UI port (use 0 to start on random free port), if used kqprun will be runs as daemon")
            .RequiredArgument("uint")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                if (const TString& port = option->CurVal()) {
                    RunnerOptions.YdbSettings.MonitoringEnabled = true;
                    RunnerOptions.YdbSettings.MonitoringPortOffset = FromString(port);
                }
            });

        options.AddLongOption('G', "grpc", "gRPC port (use 0 to start on random free port), if used kqprun will be runs as daemon")
            .RequiredArgument("uint")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                if (const TString& port = option->CurVal()) {
                    RunnerOptions.YdbSettings.GrpcEnabled = true;
                    RunnerOptions.YdbSettings.GrpcPort = FromString(port);
                }
            });

        options.AddLongOption('E', "emulate-yt", "Emulate YT tables (use file gateway instead of native gateway)")
            .NoArgument()
            .SetFlag(&EmulateYt);

        TChoices<std::function<void()>> backtrace({
            {"heavy", &NKikimr::EnableYDBBacktraceFormat},
            {"light", []() { SetFormatBackTraceFn(FormatBackTrace); }}
        });
        options.AddLongOption("backtrace", "Default backtrace format function")
            .RequiredArgument("backtrace-type")
            .DefaultValue("heavy")
            .Choices(backtrace.GetChoices())
            .Handler1([backtrace](const NLastGetopt::TOptsParser* option) {
                TString choice(option->CurValOrDef());
                backtrace(choice)();
            });
    }

    int DoRun(NLastGetopt::TOptsParseResult&&) override {
        if (!ExecutionOptions.SchemeQuery && ExecutionOptions.ScriptQueries.empty() && !RunnerOptions.YdbSettings.MonitoringEnabled && !RunnerOptions.YdbSettings.GrpcEnabled) {
            ythrow yexception() << "Nothing to execute";
        }

        RunnerOptions.YdbSettings.YqlToken = YqlToken;
        RunnerOptions.YdbSettings.FunctionRegistry = CreateFunctionRegistry(UdfsDirectory, UdfsPaths, ExcludeLinkedUdfs).Get();
        RunnerOptions.YdbSettings.AppConfig.MutableQueryServiceConfig()->SetScriptResultRowsLimit(ResultsRowsLimit);

        if (EmulateYt) {
            const auto& fileStorageConfig = RunnerOptions.YdbSettings.AppConfig.GetQueryServiceConfig().GetFileStorage();
            auto fileStorage = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));
            auto ytFileServices = NYql::NFile::TYtFileServices::Make(RunnerOptions.YdbSettings.FunctionRegistry.Get(), TablesMapping, fileStorage);
            RunnerOptions.YdbSettings.YtGateway = NYql::CreateYtFileGateway(ytFileServices);
            RunnerOptions.YdbSettings.ComputationFactory = NYql::NFile::GetYtFileFactory(ytFileServices);
        } else if (!TablesMapping.empty()) {
            ythrow yexception() << "Tables mapping is not supported without emulate YT mode";
        }

        RunScript(ExecutionOptions, RunnerOptions);
        return 0;
    }
};


void KqprunTerminateHandler() {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= terminate() call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    Cerr << colors.Red() << "=======================================" << colors.Default() << Endl;

    abort();
}


void SegmentationFaultHandler(int) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

    Cerr << colors.Red() << "======= segmentation fault call stack ========" << colors.Default() << Endl;
    FormatBackTrace(&Cerr);
    Cerr << colors.Red() << "==============================================" << colors.Default() << Endl;

    abort();
}


int main(int argc, const char* argv[]) {
    std::set_terminate(KqprunTerminateHandler);
    signal(SIGSEGV, &SegmentationFaultHandler);

    try {
        TMain().Run(argc, argv);
    } catch (...) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

        Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        return 1;
    }

    return 0;
}
