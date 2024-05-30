#include "src/kqp_runner.h"

#include <cstdio>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>
#include <util/system/env.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_comp_nodes.h>
#include <ydb/library/yql/providers/yt/lib/yt_download/yt_download.h>
#include <ydb/library/yql/public/udf/udf_static_registry.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>


struct TExecutionOptions {
    enum class EClearExecutionCase {
        Disabled,
        GenericQuery,
        YqlScript
    };

    std::vector<TString> ScriptQueries;
    TString SchemeQuery;

    bool ForgetExecution = false;
    EClearExecutionCase ClearExecution = EClearExecutionCase::Disabled;
    NKikimrKqp::EQueryAction ScriptQueryAction = NKikimrKqp::QUERY_ACTION_EXECUTE;

    TString TraceId = "kqprun_" + CreateGuidAsString();

    bool HasResults() const {
        return !ScriptQueries.empty() && ScriptQueryAction == NKikimrKqp::QUERY_ACTION_EXECUTE;
    }
};


void RunScript(const TExecutionOptions& executionOptions, const NKqpRun::TRunnerOptions& runnerOptions) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Initialization of kqp runner..." << colors.Default() << Endl;
    NKqpRun::TKqpRunner runner(runnerOptions);

    if (executionOptions.SchemeQuery) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Executing scheme query..." << colors.Default() << Endl;
        if (!runner.ExecuteSchemeQuery(executionOptions.SchemeQuery, executionOptions.TraceId)) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Scheme query execution failed";
        }
    }

    for (size_t id = 0; id < executionOptions.ScriptQueries.size(); ++id) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Executing script" << (executionOptions.ScriptQueries.size() > 1 ? TStringBuilder() << " " << id : TString()) << "..." << colors.Default() << Endl;
        switch (executionOptions.ClearExecution) {
        case TExecutionOptions::EClearExecutionCase::Disabled:
            if (!runner.ExecuteScript(executionOptions.ScriptQueries[id], executionOptions.ScriptQueryAction, executionOptions.TraceId)) {
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

        case TExecutionOptions::EClearExecutionCase::GenericQuery:
            if (!runner.ExecuteQuery(executionOptions.ScriptQueries[id], executionOptions.ScriptQueryAction, executionOptions.TraceId)) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Query execution failed";
            }
            break;

        case TExecutionOptions::EClearExecutionCase::YqlScript:
            if (!runner.ExecuteYqlScript(executionOptions.ScriptQueries[id], executionOptions.ScriptQueryAction, executionOptions.TraceId)) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Yql script execution failed";
            }
            break;
        }
    }

    if (executionOptions.HasResults()) {
        try {
            runner.PrintScriptResults();
        } catch (...) {
            ythrow yexception() << "Failed to print script results, reason:\n" <<  CurrentExceptionMessage();
        }
    }

    if (runnerOptions.YdbSettings.MonitoringEnabled) {
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

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Finalization of kqp runner..." << colors.Default() << Endl;
}


THolder<TFileOutput> SetupDefaultFileOutput(const TString& filePath, IOutputStream*& stream) {
    THolder<TFileOutput> fileHolder;
    if (filePath == "-") {
        stream = &Cout;
    } else if (filePath) {
        fileHolder.Reset(new TFileOutput(filePath));
        stream = fileHolder.Get();
    }
    return fileHolder;
}


template <typename EnumType>
EnumType GetCaseVariant(const TString& optionName, const TString& caseName, const std::map<TString, EnumType>& casesMap) {
    auto it = casesMap.find(caseName);
    if (it == casesMap.end()) {
        ythrow yexception() << "Option '" << optionName << "' has no case '" << caseName << "'";
    }
    return it->second;
}


void ReplaceTemplate(const TString& variableName, const TString& variableValue, TString& query) {
    TString variableTemplate = TStringBuilder() << "${" << variableName << "}";
    for (size_t position = query.find(variableTemplate); position != TString::npos; position = query.find(variableTemplate, position)) {
        query.replace(position, variableTemplate.size(), variableValue);
        position += variableValue.size();
    }
}


TIntrusivePtr<NKikimr::NMiniKQL::IMutableFunctionRegistry> CreateFunctionRegistry(const TString& udfsDirectory, TVector<TString> udfsPaths, bool excludeLinkedUdfs) {
    if (!udfsDirectory.empty() || !udfsPaths.empty()) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Fetching udfs..." << colors.Default() << Endl;
    }

    NKikimr::NMiniKQL::FindUdfsInDir(udfsDirectory, &udfsPaths);
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, udfsPaths)->Clone();

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


void RunMain(int argc, const char* argv[]) {
    TExecutionOptions executionOptions;
    NKqpRun::TRunnerOptions runnerOptions;

    std::vector<TString> scriptQueryFiles;
    TString schemeQueryFile;
    TString resultOutputFile = "-";
    TString schemeQueryAstFile;
    TString scriptQueryAstFile;
    TString scriptQueryPlanFile;
    TString inProgressStatisticsFile;
    TString logFile = "-";
    TString appConfigFile = "./configuration/app_config.conf";
    std::vector<TString> tablesMappingList;

    TString clearExecutionType = "disabled";
    TString traceOptType = "disabled";
    TString scriptQueryAction = "execute";
    TString planOutputFormat = "pretty";
    TString resultOutputFormat = "rows";
    i64 resultsRowsLimit = 1000;
    bool emulateYt = false;

    TVector<TString> udfsPaths;
    TString udfsDirectory;
    bool excludeLinkedUdfs = false;

    NLastGetopt::TOpts options = NLastGetopt::TOpts::Default();
    options.AddLongOption('p', "script-query", "Script query to execute")
        .Optional()
        .RequiredArgument("FILE")
        .AppendTo(&scriptQueryFiles);
    options.AddLongOption('s', "scheme-query", "Scheme query to execute")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&schemeQueryFile);
    options.AddLongOption('c', "app-config", "File with app config (TAppConfig)")
        .Optional()
        .RequiredArgument("FILE")
        .DefaultValue(appConfigFile)
        .StoreResult(&appConfigFile);
    options.AddLongOption('t', "table", "File with table (can be used by YT with -E flag), table@file")
        .Optional()
        .RequiredArgument("FILE")
        .AppendTo(&tablesMappingList);

    options.AddLongOption("log-file", "File with execution logs (use '-' to write in stderr)")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&logFile);
    options.AddLongOption("result-file", "File with script execution results (use '-' to write in stdout)")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&resultOutputFile);
    options.AddLongOption("scheme-ast-file", "File with scheme query ast (use '-' to write in stdout)")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&schemeQueryAstFile);
    options.AddLongOption("script-ast-file", "File with script query ast (use '-' to write in stdout)")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&scriptQueryAstFile);
    options.AddLongOption("script-plan-file", "File with script query plan (use '-' to write in stdout)")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&scriptQueryPlanFile);
    options.AddLongOption("in-progress-statistics", "File with script inprogress statistics")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&inProgressStatisticsFile);

    options.AddLongOption('C', "clear-execution", "Execute script query without creating additional tables, one of { query | yql-script }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(clearExecutionType)
        .StoreResult(&clearExecutionType);
    options.AddLongOption('F', "forget", "Forget script execution operation after fetching results, cannot be used with -C")
        .Optional()
        .NoArgument()
        .DefaultValue(executionOptions.ForgetExecution)
        .SetFlag(&executionOptions.ForgetExecution);
    options.AddLongOption('T', "trace-opt", "print AST in the begin of each transformation, one of { scheme | script | all }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(traceOptType)
        .StoreResult(&traceOptType);
    options.AddLongOption('A', "script-action", "Script query execute action, one of { execute | explain }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(scriptQueryAction)
        .StoreResult(&scriptQueryAction);
    options.AddLongOption('P', "plan-format", "Script query plan format, one of { pretty | table | json }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(planOutputFormat)
        .StoreResult(&planOutputFormat);
    options.AddLongOption('R', "result-format", "Script query result format, one of { rows | full-json | full-proto }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(resultOutputFormat)
        .StoreResult(&resultOutputFormat);
    options.AddLongOption('L', "result-rows-limit", "Rows limit for script execution results")
        .Optional()
        .RequiredArgument("INT")
        .DefaultValue(resultsRowsLimit)
        .StoreResult(&resultsRowsLimit);
    options.AddLongOption('E', "emulate-yt", "Emulate YT tables")
        .Optional()
        .NoArgument()
        .DefaultValue(emulateYt)
        .SetFlag(&emulateYt);
    options.AddLongOption('N', "node-count", "Number of nodes to create")
        .Optional()
        .RequiredArgument("INT")
        .DefaultValue(runnerOptions.YdbSettings.NodeCount)
        .StoreResult(&runnerOptions.YdbSettings.NodeCount);
    options.AddLongOption('M', "monitoring", "Enable embedded UI access and run kqprun as deamon")
        .Optional()
        .NoArgument()
        .DefaultValue(runnerOptions.YdbSettings.MonitoringEnabled)
        .SetFlag(&runnerOptions.YdbSettings.MonitoringEnabled);

    options.AddLongOption('u', "udf", "Load shared library with UDF by given path")
        .Optional()
        .RequiredArgument("FILE")
        .AppendTo(&udfsPaths);
    options.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory")
        .Optional()
        .RequiredArgument("PATH")
        .StoreResult(&udfsDirectory);
    options.AddLongOption("exclude-linked-udfs", "Exclude linked udfs when same udf passed from -u or --udfs-dir")
        .Optional()
        .NoArgument()
        .DefaultValue(excludeLinkedUdfs)
        .SetFlag(&excludeLinkedUdfs);

    NLastGetopt::TOptsParseResult parsedOptions(&options, argc, argv);

    // Environment variables

    const TString& yqlToken = GetEnv(NKqpRun::YQL_TOKEN_VARIABLE);

    // Execution options

    if (!schemeQueryFile && scriptQueryFiles.empty() && !runnerOptions.YdbSettings.MonitoringEnabled) {
        ythrow yexception() << "Nothing to execute";
    }

    if (schemeQueryFile) {
        executionOptions.SchemeQuery = TFileInput(schemeQueryFile).ReadAll();
        ReplaceTemplate(NKqpRun::YQL_TOKEN_VARIABLE, yqlToken, executionOptions.SchemeQuery);
    }

    executionOptions.ScriptQueries.reserve(scriptQueryFiles.size());
    for (const TString& scriptQueryFile : scriptQueryFiles) {
        executionOptions.ScriptQueries.emplace_back(TFileInput(scriptQueryFile).ReadAll());
    }

    executionOptions.ClearExecution = GetCaseVariant<TExecutionOptions::EClearExecutionCase>("clear-execution", clearExecutionType, {
        {"query", TExecutionOptions::EClearExecutionCase::GenericQuery},
        {"yql-script", TExecutionOptions::EClearExecutionCase::YqlScript},
        {"disabled", TExecutionOptions::EClearExecutionCase::Disabled}
    });

    executionOptions.ScriptQueryAction = GetCaseVariant<NKikimrKqp::EQueryAction>("script-action", scriptQueryAction, {
        {"execute", NKikimrKqp::QUERY_ACTION_EXECUTE},
        {"explain", NKikimrKqp::QUERY_ACTION_EXPLAIN}
    });

    // Runner options

    THolder<TFileOutput> resultFileHolder = SetupDefaultFileOutput(resultOutputFile, runnerOptions.ResultOutput);
    THolder<TFileOutput> schemeQueryAstFileHolder = SetupDefaultFileOutput(schemeQueryAstFile, runnerOptions.SchemeQueryAstOutput);
    THolder<TFileOutput> scriptQueryAstFileHolder = SetupDefaultFileOutput(scriptQueryAstFile, runnerOptions.ScriptQueryAstOutput);
    THolder<TFileOutput> scriptQueryPlanFileHolder = SetupDefaultFileOutput(scriptQueryPlanFile, runnerOptions.ScriptQueryPlanOutput);

    if (inProgressStatisticsFile) {
        runnerOptions.InProgressStatisticsOutputFile = inProgressStatisticsFile;
    }

    runnerOptions.TraceOptType = GetCaseVariant<NKqpRun::TRunnerOptions::ETraceOptType>("trace-opt", traceOptType, {
        {"all", NKqpRun::TRunnerOptions::ETraceOptType::All},
        {"scheme", NKqpRun::TRunnerOptions::ETraceOptType::Scheme},
        {"script", NKqpRun::TRunnerOptions::ETraceOptType::Script},
        {"disabled", NKqpRun::TRunnerOptions::ETraceOptType::Disabled}
    });
    runnerOptions.YdbSettings.TraceOptEnabled = runnerOptions.TraceOptType != NKqpRun::TRunnerOptions::ETraceOptType::Disabled;

    runnerOptions.ResultOutputFormat = GetCaseVariant<NKqpRun::TRunnerOptions::EResultOutputFormat>("result-format", resultOutputFormat, {
        {"rows", NKqpRun::TRunnerOptions::EResultOutputFormat::RowsJson},
        {"full-json", NKqpRun::TRunnerOptions::EResultOutputFormat::FullJson},
        {"full-proto", NKqpRun::TRunnerOptions::EResultOutputFormat::FullProto}
    });

    runnerOptions.PlanOutputFormat = GetCaseVariant<NYdb::NConsoleClient::EOutputFormat>("plan-format", planOutputFormat, {
        {"pretty", NYdb::NConsoleClient::EOutputFormat::Pretty},
        {"table", NYdb::NConsoleClient::EOutputFormat::PrettyTable},
        {"json", NYdb::NConsoleClient::EOutputFormat::JsonUnicode},
    });

    // Ydb settings

    if (runnerOptions.YdbSettings.NodeCount < 1) {
        ythrow yexception() << "Number of nodes less than one";
    }

    if (logFile != "-") {
        runnerOptions.YdbSettings.LogOutputFile = logFile;
        std::remove(logFile.c_str());
    }

    runnerOptions.YdbSettings.YqlToken = yqlToken;
    runnerOptions.YdbSettings.FunctionRegistry = CreateFunctionRegistry(udfsDirectory, udfsPaths, excludeLinkedUdfs).Get();

    TString appConfigData = TFileInput(appConfigFile).ReadAll();
    if (!google::protobuf::TextFormat::ParseFromString(appConfigData, &runnerOptions.YdbSettings.AppConfig)) {
        ythrow yexception() << "Bad format of app configuration";
    }

    if (resultsRowsLimit < 0) {
        ythrow yexception() << "Results rows limit less than zero";
    }
    runnerOptions.YdbSettings.AppConfig.MutableQueryServiceConfig()->SetScriptResultRowsLimit(resultsRowsLimit);

    if (emulateYt) {
        THashMap<TString, TString> tablesMapping;
        for (const auto& tablesMappingItem: tablesMappingList) {
            TStringBuf tableName;
            TStringBuf filePath;
            TStringBuf(tablesMappingItem).Split('@', tableName, filePath);
            if (tableName.empty() || filePath.empty()) {
                ythrow yexception() << "Incorrect table mapping, expected form table@file, e.g. yt.Root/plato.Input@input.txt";
            }
            tablesMapping[tableName] = filePath;
        }

        const auto& fileStorageConfig = runnerOptions.YdbSettings.AppConfig.GetQueryServiceConfig().GetFileStorage();
        auto fileStorage = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));
        auto ytFileServices = NYql::NFile::TYtFileServices::Make(runnerOptions.YdbSettings.FunctionRegistry.Get(), tablesMapping, fileStorage);
        runnerOptions.YdbSettings.YtGateway = NYql::CreateYtFileGateway(ytFileServices);
        runnerOptions.YdbSettings.ComputationFactory = NYql::NFile::GetYtFileFactory(ytFileServices);
    } else if (!tablesMappingList.empty()) {
        ythrow yexception() << "Tables mapping is not supported without emulate YT mode";
    }

    RunScript(executionOptions, runnerOptions);
}


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
        RunMain(argc, argv);
    } catch (...) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

        Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        return 1;
    }

    return 0;
}
