#include "src/kqp_runner.h"

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>
#include <util/system/env.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>


struct TExecutionOptions {
    TString ScriptQuery;
    TString SchemeQuery;

    NKikimrKqp::EQueryAction ScriptQueryAction = NKikimrKqp::QUERY_ACTION_EXECUTE;

    TString ScriptTraceId = "kqprun";
};


void RunScript(const TExecutionOptions& executionOptions, const NKqpRun::TRunnerOptions& runnerOptions) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << "Initialization of kqp runner..." << colors.Default() << Endl;
    NKqpRun::TKqpRunner runner(runnerOptions);

    if (executionOptions.SchemeQuery) {
        Cout << colors.Yellow() << "Executing scheme query..." << colors.Default() << Endl;
        if (!runner.ExecuteSchemeQuery(executionOptions.SchemeQuery)) {
            ythrow yexception() << "Scheme query execution failed";
        }
    }

    if (executionOptions.ScriptQuery) {
        Cout << colors.Yellow() << "Executing script..." << colors.Default() << Endl;
        if (!runner.ExecuteScript(executionOptions.ScriptQuery, executionOptions.ScriptQueryAction, executionOptions.ScriptTraceId)) {
            ythrow yexception() << "Script execution failed";
        }
    }

    if (executionOptions.ScriptQueryAction == NKikimrKqp::QUERY_ACTION_EXECUTE) {
        Cout << colors.Yellow() << "Writing script results..." << colors.Default() << Endl;
        if (!runner.WriteScriptResults()) {
            ythrow yexception() << "Writing script results failed";
        }
    }
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


void RunMain(int argc, const char* argv[]) {
    TString scriptQueryFile;
    TString schemeQueryFile;
    TString resultOutputFile = "-";
    TString schemeQueryAstFile;
    TString scriptQueryAstFile;
    TString scriptQueryPlanFile;
    TString logFile = "-";

    TString scriptQueryAction = "execute";
    TString planOutputFormat = "pretty";
    i64 resultsRowsLimit = 1000;

    TVector<TString> udfsPaths;
    TString udfsDirectory;

    NLastGetopt::TOpts options = NLastGetopt::TOpts::Default();
    options.AddLongOption('p', "script-query", "Script query to execute")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&scriptQueryFile);
    options.AddLongOption('s', "scheme-query", "Scheme query to execute")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&schemeQueryFile);

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

    options.AddLongOption("script-action", "Script query execute action, one of { execute | explain }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(scriptQueryAction)
        .StoreResult(&scriptQueryAction);
    options.AddLongOption("plan-format", "Script query plan format, one of { pretty | table | json }")
        .Optional()
        .RequiredArgument("STR")
        .DefaultValue(planOutputFormat)
        .StoreResult(&planOutputFormat);
    options.AddLongOption("results-limit", "Rows limit for script execution results")
        .Optional()
        .RequiredArgument("INT")
        .DefaultValue(resultsRowsLimit)
        .StoreResult(&resultsRowsLimit);

    options.AddLongOption("udf", "Load shared library with UDF by given path")
        .Optional()
        .RequiredArgument("FILE")
        .AppendTo(&udfsPaths);
    options.AddLongOption("udfs-dir", "Load all shared libraries with UDFs found in given directory")
        .Optional()
        .RequiredArgument("PATH")
        .StoreResult(&udfsDirectory);

    NLastGetopt::TOptsParseResult parsedOptions(&options, argc, argv);

    // Execution options

    TExecutionOptions executionOptions;

    if (!schemeQueryFile && !scriptQueryFile) {
        ythrow yexception() << "Nothing to execute";
    }
    if (schemeQueryFile) {
        executionOptions.SchemeQuery = TFileInput(schemeQueryFile).ReadAll();
    }
    if (scriptQueryFile) {
        executionOptions.ScriptQuery = TFileInput(scriptQueryFile).ReadAll();
    }

    executionOptions.ScriptQueryAction =
              (scriptQueryAction == TStringBuf("execute")) ? NKikimrKqp::QUERY_ACTION_EXECUTE
            : (scriptQueryAction == TStringBuf("explain")) ? NKikimrKqp::QUERY_ACTION_EXPLAIN
            : NKikimrKqp::QUERY_ACTION_EXECUTE;

    // Runner options

    NKqpRun::TRunnerOptions runnerOptions;

    if (resultsRowsLimit >= 0) {
        runnerOptions.ResultsRowsLimit = resultsRowsLimit;
    } else {
        ythrow yexception() << "Results rows limit less than zero";
    }

    THolder<TFileOutput> resultFileHolder = SetupDefaultFileOutput(resultOutputFile, runnerOptions.ResultOutput);
    THolder<TFileOutput> schemeQueryAstFileHolder = SetupDefaultFileOutput(schemeQueryAstFile, runnerOptions.SchemeQueryAstOutput);
    THolder<TFileOutput> scriptQueryAstFileHolder = SetupDefaultFileOutput(scriptQueryAstFile, runnerOptions.ScriptQueryAstOutput);
    THolder<TFileOutput> scriptQueryPlanFileHolder = SetupDefaultFileOutput(scriptQueryPlanFile, runnerOptions.ScriptQueryPlanOutput);

    runnerOptions.PlanOutputFormat =
              (planOutputFormat == TStringBuf("pretty")) ? NYdb::NConsoleClient::EOutputFormat::Pretty
            : (planOutputFormat == TStringBuf("table")) ? NYdb::NConsoleClient::EOutputFormat::PrettyTable
            : (planOutputFormat == TStringBuf("json")) ? NYdb::NConsoleClient::EOutputFormat::JsonUnicode
            : NYdb::NConsoleClient::EOutputFormat::Default;

    // Ydb settings

    if (logFile != "-") {
        runnerOptions.YdbSettings.LogOutputFile = logFile;
    }

    runnerOptions.YdbSettings.YqlToken = GetEnv("YQL_TOKEN");

    NKikimr::NMiniKQL::FindUdfsInDir(udfsDirectory, &udfsPaths);
    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(&NYql::NBacktrace::KikimrBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, udfsPaths)->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*functionRegistry);
    runnerOptions.YdbSettings.FunctionRegistry = functionRegistry.Get();

    TString appConfigData = TFileInput("./configuration/app_config.conf").ReadAll();
    if (!google::protobuf::TextFormat::ParseFromString(appConfigData, &runnerOptions.YdbSettings.AppConfig)) {
        ythrow yexception() << "Bad format of app configuration";
    }

    RunScript(executionOptions, runnerOptions);
}


int main(int argc, const char* argv[]) {
    try {
        RunMain(argc, argv);
    } catch (...) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

        Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        return 1;
    }

    return 0;
}
