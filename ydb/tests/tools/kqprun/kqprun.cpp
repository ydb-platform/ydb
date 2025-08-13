#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>

#include <util/stream/file.h>
#include <util/system/env.h>

#include <ydb/core/blob_depot/mon_main.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_dummy_gateway.h>
#include <ydb/tests/tools/kqprun/runlib/application.h>
#include <ydb/tests/tools/kqprun/runlib/utils.h>
#include <ydb/tests/tools/kqprun/src/kqp_runner.h>

#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_comp_nodes.h>
#include <yt/yql/providers/yt/lib/yt_download/yt_download.h>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <yql/essentials/minikql/mkql_buffer.h>
#endif

using namespace NKikimrRun;

namespace NKqpRun {

namespace {

struct TExecutionOptions {
    enum class EExecutionCase {
        GenericScript,
        GenericQuery,
        YqlScript,
        AsyncQuery
    };

    std::vector<TString> ScriptQueries;
    TString SchemeQuery;
    std::unordered_map<TString, Ydb::TypedValue> Params;
    bool UseTemplates = false;

    ui32 LoopCount = 1;
    TDuration QueryDelay;
    TDuration LoopDelay;
    bool ContinueAfterFail = false;

    bool ForgetExecution = false;
    std::vector<EExecutionCase> ExecutionCases;
    std::vector<NKikimrKqp::EQueryAction> ScriptQueryActions;
    std::vector<TString> Databases;
    std::vector<TString> TraceIds;
    std::vector<TString> PoolIds;
    std::vector<TString> UserSIDs;
    std::vector<TDuration> Timeouts;
    std::vector<std::optional<TVector<NACLib::TSID>>> GroupSIDs;
    ui64 ResultsRowsLimit = 0;

    const TString DefaultTraceId = "kqprun";

    bool HasResults() const {
        for (size_t i = 0; i < ScriptQueries.size(); ++i) {
            if (GetScriptQueryAction(i) != NKikimrKqp::EQueryAction::QUERY_ACTION_EXECUTE) {
                continue;
            }
            if (GetExecutionCase(i) != EExecutionCase::AsyncQuery) {
                return true;
            }
        }
        return false;
    }

    bool HasExecutionCase(EExecutionCase executionCase) const {
        if (ExecutionCases.empty()) {
            return executionCase == EExecutionCase::GenericScript;
        }
        return std::find(ExecutionCases.begin(), ExecutionCases.end(), executionCase) != ExecutionCases.end();
    }

    EExecutionCase GetExecutionCase(size_t index) const {
        return GetValue(index, ExecutionCases, EExecutionCase::GenericScript);
    }

    NKikimrKqp::EQueryAction GetScriptQueryAction(size_t index) const {
        return GetValue(index, ScriptQueryActions, NKikimrKqp::EQueryAction::QUERY_ACTION_EXECUTE);
    }

    TRequestOptions GetSchemeQueryOptions() const {
        TString sql = SchemeQuery;

        return {
            .Query = sql,
            .Action = NKikimrKqp::EQueryAction::QUERY_ACTION_EXECUTE,
            .TraceId = DefaultTraceId,
            .PoolId = "",
            .UserSID = BUILTIN_ACL_ROOT,
            .Database = GetValue(0, Databases, TString()),
            .Timeout = TDuration::Zero()
        };
    }

    TRequestOptions GetScriptQueryOptions(size_t index, size_t loopId, size_t queryId, TInstant startTime) const {
        Y_ABORT_UNLESS(index < ScriptQueries.size());

        TString sql = ScriptQueries[index];
        if (UseTemplates) {
            SubstGlobal(sql, "${LOOP_ID}", ToString(loopId));
            SubstGlobal(sql, "${QUERY_ID}", ToString(queryId));
        }

        return {
            .Query = sql,
            .Action = GetScriptQueryAction(index),
            .TraceId = TStringBuilder() << GetValue(index, TraceIds, DefaultTraceId) << "-" << startTime.ToString(),
            .PoolId = GetValue(index, PoolIds, TString()),
            .UserSID = GetValue(index, UserSIDs, TString(BUILTIN_ACL_ROOT)),
            .Database = GetValue(index, Databases, TString()),
            .Timeout = GetValue(index, Timeouts, TDuration::Zero()),
            .QueryId = queryId,
            .Params = Params,
            .GroupSIDs = GetValue<std::optional<TVector<NACLib::TSID>>>(index, GroupSIDs, std::nullopt)
        };
    }

    void Validate(const TRunnerOptions& runnerOptions) const {
        if (!SchemeQuery && ScriptQueries.empty() && !runnerOptions.YdbSettings.MonitoringEnabled && !runnerOptions.YdbSettings.GrpcEnabled) {
            ythrow yexception() << "Nothing to execute and is not running as daemon";
        }

        ValidateOptionsSizes(runnerOptions);
        ValidateSchemeQueryOptions(runnerOptions);
        ValidateScriptExecutionOptions(runnerOptions);
        ValidateAsyncOptions(runnerOptions.YdbSettings.AsyncQueriesSettings);
        ValidateTraceOpt(runnerOptions);
        ValidateStorageSettings(runnerOptions.YdbSettings);
    }

private:
    void ValidateOptionsSizes(const TRunnerOptions& runnerOptions) const {
        const auto checker = [numberQueries = ScriptQueries.size()](size_t checkSize, const TString& optionName, bool useInSchemeQuery = false) {
            if (checkSize > std::max(numberQueries, static_cast<size_t>(useInSchemeQuery ? 1 : 0))) {
                ythrow yexception() << "Too many " << optionName << ". Specified " << checkSize << ", when number of script queries is " << numberQueries;
            }
        };

        checker(ExecutionCases.size(), "execution cases");
        checker(ScriptQueryActions.size(), "script query actions");
        checker(Databases.size(), "databases", true);
        checker(TraceIds.size(), "trace ids");
        checker(PoolIds.size(), "pool ids");
        checker(UserSIDs.size(), "user SIDs");
        checker(Timeouts.size(), "timeouts");
        checker(GroupSIDs.size(), "group SIDs");
        checker(runnerOptions.ScriptQueryAstOutputs.size(), "ast output files");
        checker(runnerOptions.ScriptQueryPlanOutputs.size(), "plan output files");
        checker(runnerOptions.ScriptQueryTimelineFiles.size(), "timeline files");
        checker(runnerOptions.InProgressStatisticsOutputFiles.size(), "statistics files");
    }

    void ValidateSchemeQueryOptions(const TRunnerOptions& runnerOptions) const {
        if (SchemeQuery) {
            return;
        }
        if (runnerOptions.SchemeQueryAstOutput) {
            ythrow yexception() << "Scheme query AST output can not be used without scheme query";
        }
    }

    void ValidateScriptExecutionOptions(const TRunnerOptions& runnerOptions) const {
        if (runnerOptions.YdbSettings.SameSession && HasExecutionCase(EExecutionCase::AsyncQuery)) {
            ythrow yexception() << "Same session can not be used with async quries";
        }

        // Script specific
        if (HasExecutionCase(EExecutionCase::GenericScript)) {
            return;
        }
        if (ForgetExecution) {
            ythrow yexception() << "Forget execution can not be used without generic script queries";
        }
        if (runnerOptions.ScriptCancelAfter) {
            ythrow yexception() << "Cancel after can not be used without generic script queries";
        }

        // Script/Query specific
        if (HasExecutionCase(EExecutionCase::GenericQuery)) {
            return;
        }
        if (ResultsRowsLimit) {
            ythrow yexception() << "Result rows limit can not be used without script queries";
        }
        if (!runnerOptions.InProgressStatisticsOutputFiles.empty()) {
            ythrow yexception() << "Script statistics can not be used without script queries";
        }

        // Common specific
        if (HasExecutionCase(EExecutionCase::YqlScript)) {
            return;
        }
        if (!runnerOptions.ScriptQueryAstOutputs.empty()) {
            ythrow yexception() << "Script query AST output can not be used without script/yql queries";
        }
        if (!runnerOptions.ScriptQueryPlanOutputs.empty()) {
            ythrow yexception() << "Script query plan output can not be used without script/yql queries";
        }
        if (runnerOptions.YdbSettings.SameSession) {
            ythrow yexception() << "Same session can not be used without script/yql queries";
        }
    }

    void ValidateAsyncOptions(const TAsyncQueriesSettings& asyncQueriesSettings) const {
        if (asyncQueriesSettings.InFlightLimit && !HasExecutionCase(EExecutionCase::AsyncQuery)) {
            ythrow yexception() << "In flight limit can not be used without async queries";
        }

        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        if (LoopCount && asyncQueriesSettings.InFlightLimit && asyncQueriesSettings.InFlightLimit > ScriptQueries.size() * LoopCount) {
            Cout << colors.Red() << "Warning: inflight limit is " << asyncQueriesSettings.InFlightLimit << ", that is larger than max possible number of queries " << ScriptQueries.size() * LoopCount << colors.Default() << Endl;
        }
    }

    void ValidateTraceOpt(const TRunnerOptions& runnerOptions) const {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        switch (runnerOptions.TraceOptType) {
            case TRunnerOptions::ETraceOptType::Scheme: {
                if (!SchemeQuery) {
                    ythrow yexception() << "Trace opt type scheme cannot be used without scheme query";
                }
                break;
            }
            case TRunnerOptions::ETraceOptType::Script: {
                if (ScriptQueries.empty()) {
                    ythrow yexception() << "Trace opt type script cannot be used without script queries";
                }
            }
            case TRunnerOptions::ETraceOptType::All: {
                if (!SchemeQuery && ScriptQueries.empty()) {
                    ythrow yexception() << "Trace opt type all cannot be used without any queries";
                }
            }
            case TRunnerOptions::ETraceOptType::Disabled: {
                break;
            }
        }

        if (const auto traceOptId = runnerOptions.TraceOptScriptId) {
            if (runnerOptions.TraceOptType != TRunnerOptions::ETraceOptType::Script) {
                ythrow yexception() << "Trace opt id allowed only for trace opt type script (used " << runnerOptions.TraceOptType << ")";
            }

            const ui64 scriptNumber = ScriptQueries.size() * LoopCount;
            if (*traceOptId >= scriptNumber) {
                ythrow yexception() << "Invalid trace opt id " << *traceOptId << ", it should be less than number of script queries " << scriptNumber;
            }
            if (scriptNumber == 1) {
                Cout << colors.Red() << "Warning: trace opt id is not necessary for single script mode" << Endl;
            }
        }
    }

    static void ValidateStorageSettings(const TYdbSetupSettings& ydbSettings) {
        if (ydbSettings.DisableDiskMock) {
            if (ydbSettings.NodeCount + ydbSettings.Tenants.size() > 1) {
                ythrow yexception() << "Disable disk mock cannot be used for multi node clusters (already disabled)";
            } else if (ydbSettings.PDisksPath) {
                ythrow yexception() << "Disable disk mock cannot be used with real PDisks (already disabled)";
            }
        }
        if (ydbSettings.FormatStorage && !ydbSettings.PDisksPath) {
            ythrow yexception() << "Cannot format storage without real PDisks, please use --storage-path";
        }
    }
};


void RunArgumentQuery(size_t index, size_t loopId, size_t queryId, TInstant startTime, const TExecutionOptions& executionOptions, TKqpRunner& runner) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    switch (executionOptions.GetExecutionCase(index)) {
        case TExecutionOptions::EExecutionCase::GenericScript: {
            if (!runner.ExecuteScript(executionOptions.GetScriptQueryOptions(index, loopId, queryId, startTime))) {
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
        }

        case TExecutionOptions::EExecutionCase::GenericQuery: {
            if (!runner.ExecuteQuery(executionOptions.GetScriptQueryOptions(index, loopId, queryId, startTime))) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Query execution failed";
            }
            break;
        }

        case TExecutionOptions::EExecutionCase::YqlScript: {
            if (!runner.ExecuteYqlScript(executionOptions.GetScriptQueryOptions(index, loopId, queryId, startTime))) {
                ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Yql script execution failed";
            }
            break;
        }

        case TExecutionOptions::EExecutionCase::AsyncQuery: {
            runner.ExecuteQueryAsync(executionOptions.GetScriptQueryOptions(index, loopId, queryId, startTime));
            break;
        }
    }
}


void RunArgumentQueries(const TExecutionOptions& executionOptions, TKqpRunner& runner) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    if (executionOptions.SchemeQuery) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Executing scheme query..." << colors.Default() << Endl;
        if (!runner.ExecuteSchemeQuery(executionOptions.GetSchemeQueryOptions())) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Scheme query execution failed";
        }
    }

    const size_t numberQueries = executionOptions.ScriptQueries.size();
    if (!numberQueries) {
        return;
    }

    const size_t numberLoops = executionOptions.LoopCount;
    for (size_t queryId = 0; queryId < numberQueries * numberLoops || numberLoops == 0; ++queryId) {
        size_t id = queryId % numberQueries;
        if (queryId > 0) {
            Sleep(id == 0 ? executionOptions.LoopDelay : executionOptions.QueryDelay);
        }

        const TInstant startTime = TInstant::Now();
        const size_t loopId = queryId / numberQueries;
        if (executionOptions.GetExecutionCase(id) != TExecutionOptions::EExecutionCase::AsyncQuery) {
            Cout << colors.Yellow() << startTime.ToIsoStringLocal() << " Executing script";
            if (numberQueries > 1) {
                Cout << " " << id;
            }
            if (numberLoops != 1) {
                Cout << ", loop " << loopId;
            }
            Cout << "..." << colors.Default() << Endl;
        }

        try {
            RunArgumentQuery(id, loopId, queryId, startTime, executionOptions, runner);
        } catch (const yexception& exception) {
            if (executionOptions.ContinueAfterFail) {
                Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
            } else {
                throw exception;
            }
        }
    }
    runner.FinalizeRunner();

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

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Initialization finished" << colors.Default() << Endl;
    while (true) {
        Sleep(TDuration::Seconds(1));
    }
}


void RunScript(const TExecutionOptions& executionOptions, const TRunnerOptions& runnerOptions) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Initialization of kqp runner..." << colors.Default() << Endl;
    TKqpRunner runner(runnerOptions);

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


class TMain : public TMainBase {
    using EVerbose = TYdbSetupSettings::EVerbose;

    inline static const TString YqlToken = GetEnv(YQL_TOKEN_VARIABLE);

    TDuration PingPeriod;
    TExecutionOptions ExecutionOptions;
    TRunnerOptions RunnerOptions;

    std::unordered_map<TString, TString> Templates;
    THashMap<TString, TString> TablesMapping;
    bool EmulateYt = false;

    struct TTopicSettings {
        bool CancelOnFileFinish = false;
    };
    std::unordered_map<TString, TTopicSettings> TopicsSettings;
    std::unordered_map<TString, NYql::TDummyTopic> PqFilesMapping;

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
            });

        options.AddLongOption('p', "script-query", "Script query to execute (typically DML query)")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                ExecutionOptions.ScriptQueries.emplace_back(LoadFile(option->CurVal()));
            });

        options.AddLongOption("sql", "Script query SQL text to execute (typically DML query)")
            .RequiredArgument("str")
            .AppendTo(&ExecutionOptions.ScriptQueries);

        options.AddLongOption("templates", "Enable templates for -s and -p queries, such as ${YQL_TOKEN} and ${QUERY_ID}")
            .NoArgument()
            .SetFlag(&ExecutionOptions.UseTemplates);

        options.AddLongOption("var-template", "Add template from environment variables or file for -s and -p queries (use variable@file for files)")
            .RequiredArgument("variable")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf variable;
                TStringBuf filePath;
                TStringBuf(option->CurVal()).Split('@', variable, filePath);
                if (variable.empty()) {
                    ythrow yexception() << "Variable name should not be empty";
                }

                TString value;
                if (!filePath.empty()) {
                    value = LoadFile(TString(filePath));
                } else {
                    value = GetEnv(TString(variable));
                    if (!value) {
                        ythrow yexception() << "Invalid env template, can not find value for variable '" << variable << "'";
                    }
                }

                if (!Templates.emplace(variable, value).second) {
                    ythrow yexception() << "Got duplicated template variable name '" << variable << "'";
                }
            });

        options.AddLongOption("param", "Add query parameter from file to -p queries, use name@file (param value is protobuf Ydb::TypedValue)")
            .RequiredArgument("name@file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf name;
                TStringBuf filePath;
                TStringBuf(option->CurVal()).Split('@', name, filePath);
                if (name.empty() || filePath.empty()) {
                    ythrow yexception() << "Incorrect query parameter, expected form name@file";
                }

                Ydb::TypedValue value;
                if (!google::protobuf::TextFormat::ParseFromString(LoadFile(TString(filePath)), &value)) {
                    ythrow yexception() << "Failed to parse query parameter from file '" << filePath << "'";
                }
                if (!ExecutionOptions.Params.emplace(TStringBuilder() << "$" << name, value).second) {
                    ythrow yexception() << "Got duplicated parameter name '" << name << "'";
                }
            });

        options.AddLongOption('t', "table", "File with input table (can be used by YT with -E flag), table@file")
            .RequiredArgument("table@file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf tableName;
                TStringBuf filePath;
                TStringBuf(option->CurVal()).Split('@', tableName, filePath);
                if (tableName.empty() || filePath.empty()) {
                    ythrow yexception() << "Incorrect table mapping, expected form table@file, e. g. yt.Root/plato.Input@input.txt";
                }
                if (!TablesMapping.emplace(tableName, filePath).second) {
                    ythrow yexception() << "Got duplicated table name: " << tableName;
                }
            });

        options.AddLongOption("emulate-pq", "Emulate YDS with local file, accepts list of tables to emulate with following format: topic@file (can be used in query from cluster `pq`)")
            .RequiredArgument("topic@file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf topicName;
                TStringBuf others;
                TStringBuf(option->CurVal()).Split('@', topicName, others);

                TStringBuf path;
                TStringBuf partitionCountStr;
                others.Split(':', path, partitionCountStr);
                const size_t partitionCount = !partitionCountStr.empty() ? FromString<size_t>(partitionCountStr) : 1;
                if (!partitionCount) {
                    ythrow yexception() << "Topic partition count should be at least one";
                }

                if (topicName.empty() || path.empty()) {
                    ythrow yexception() << "Incorrect PQ file mapping, expected form topic@path[:partitions_count]";
                }

                if (!PqFilesMapping.emplace(topicName, NYql::TDummyTopic("pq", TString(topicName), TString(path), partitionCount)).second) {
                    ythrow yexception() << "Got duplicated topic name: " << topicName;
                }
            });

        options.AddLongOption("cancel-on-file-finish", "Cancel emulate YDS topics when topic file finished")
            .RequiredArgument("topic")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TopicsSettings[option->CurVal()].CancelOnFileFinish = true;
            });

        options.AddLongOption('c', "app-config", "File with app config (TAppConfig for ydb tenant)")
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

        // Outputs

        TChoices<TRunnerOptions::ETraceOptType> traceOpt({
            {"all", TRunnerOptions::ETraceOptType::All},
            {"scheme", TRunnerOptions::ETraceOptType::Scheme},
            {"script", TRunnerOptions::ETraceOptType::Script},
            {"disabled", TRunnerOptions::ETraceOptType::Disabled}
        });
        options.AddLongOption('T', "trace-opt", "Print AST in the begin of each transformation")
            .RequiredArgument("trace-opt-query")
            .DefaultValue("disabled")
            .Choices(traceOpt.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.TraceOptType, [this, traceOpt](const TString& choise) {
                auto traceOptType = traceOpt(choise);
                RunnerOptions.YdbSettings.TraceOptEnabled = traceOptType != NKqpRun::TRunnerOptions::ETraceOptType::Disabled;
                return traceOptType;
            });

        options.AddLongOption('I', "trace-opt-index", "Index of -p query to use --trace-opt, starts from zero")
            .RequiredArgument("uint")
            .StoreResult(&RunnerOptions.TraceOptScriptId);

        options.AddLongOption("trace-id", "Trace id for -p queries")
            .RequiredArgument("id")
            .EmplaceTo(&ExecutionOptions.TraceIds);

        options.AddLongOption("result-file", "File with script execution results (use '-' to write in stdout)")
            .RequiredArgument("file")
            .DefaultValue("-")
            .StoreMappedResultT<TString>(&RunnerOptions.ResultOutput, &GetDefaultOutput);

        options.AddLongOption('L', "result-rows-limit", "Rows limit for script execution results")
            .RequiredArgument("uint")
            .DefaultValue(0)
            .StoreResult(&ExecutionOptions.ResultsRowsLimit);

        TChoices<EResultOutputFormat> resultFormat({
            {"rows", EResultOutputFormat::RowsJson},
            {"full-json", EResultOutputFormat::FullJson},
            {"full-proto", EResultOutputFormat::FullProto}
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
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                RunnerOptions.ScriptQueryAstOutputs.emplace_back(GetDefaultOutput(TString(option->CurValOrDef())));
            });

        options.AddLongOption("script-plan-file", "File with script query plan (use '-' to write in stdout)")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                RunnerOptions.ScriptQueryPlanOutputs.emplace_back(GetDefaultOutput(TString(option->CurValOrDef())));
            });

        options.AddLongOption("script-statistics", "File with script inprogress statistics")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                const TString file(option->CurValOrDef());
                if (file == "-") {
                    ythrow yexception() << "Script in progress statistics cannot be printed to stdout, please specify file name";
                }
                RunnerOptions.InProgressStatisticsOutputFiles.emplace_back(file);
            });

        TChoices<NYdb::NConsoleClient::EDataFormat> planFormat({
            {"pretty", NYdb::NConsoleClient::EDataFormat::Pretty},
            {"table", NYdb::NConsoleClient::EDataFormat::PrettyTable},
            {"json", NYdb::NConsoleClient::EDataFormat::JsonUnicode},
        });
        options.AddLongOption('P', "plan-format", "Script query plan format")
            .RequiredArgument("plan-format")
            .DefaultValue("pretty")
            .Choices(planFormat.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.PlanOutputFormat, planFormat);

        options.AddLongOption("script-timeline-file", "File with script query timline in svg format")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                const TString file(option->CurValOrDef());
                if (file == "-") {
                    ythrow yexception() << "Script timline cannot be printed to stdout, please specify file name";
                }
                RunnerOptions.ScriptQueryTimelineFiles.emplace_back(file);
            });

        // Pipeline settings

        TChoices<TExecutionOptions::EExecutionCase> executionCase({
            {"script", TExecutionOptions::EExecutionCase::GenericScript},
            {"query", TExecutionOptions::EExecutionCase::GenericQuery},
            {"yql-script", TExecutionOptions::EExecutionCase::YqlScript},
            {"async", TExecutionOptions::EExecutionCase::AsyncQuery}
        });
        options.AddLongOption('C', "execution-case", "Type of query for -p argument")
            .RequiredArgument("query-type")
            .Choices(executionCase.GetChoices())
            .Handler1([this, executionCase](const NLastGetopt::TOptsParser* option) {
                TString choice(option->CurValOrDef());
                ExecutionOptions.ExecutionCases.emplace_back(executionCase(choice));
            });

        options.AddLongOption("inflight-limit", "In flight limit for async queries (use 0 for unlimited)")
            .RequiredArgument("uint")
            .DefaultValue(0)
            .StoreResult(&RunnerOptions.YdbSettings.AsyncQueriesSettings.InFlightLimit);

        options.AddLongOption("verbose", TStringBuilder() << "Common verbose level (max level " << static_cast<ui32>(EVerbose::Max) - 1 << ")")
            .RequiredArgument("uint")
            .DefaultValue(static_cast<ui8>(EVerbose::Info))
            .StoreMappedResultT<ui8>(&RunnerOptions.YdbSettings.VerboseLevel, [](ui8 value) {
                return static_cast<EVerbose>(std::min(value, static_cast<ui8>(EVerbose::Max)));
            });

        TChoices<TAsyncQueriesSettings::EVerbose> verbose({
            {"each-query", TAsyncQueriesSettings::EVerbose::EachQuery},
            {"final", TAsyncQueriesSettings::EVerbose::Final}
        });
        options.AddLongOption("async-verbose", "Verbose type for async queries")
            .RequiredArgument("type")
            .DefaultValue("each-query")
            .Choices(verbose.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.YdbSettings.AsyncQueriesSettings.Verbose, verbose);

        options.AddLongOption("ping-period", "Query ping period in milliseconds")
            .RequiredArgument("uint")
            .DefaultValue(1000)
            .StoreMappedResultT<ui64>(&PingPeriod, &TDuration::MilliSeconds<ui64>);

        TChoices<NKikimrKqp::EQueryAction> scriptAction({
            {"execute", NKikimrKqp::QUERY_ACTION_EXECUTE},
            {"explain", NKikimrKqp::QUERY_ACTION_EXPLAIN}
        });
        options.AddLongOption('A', "script-action", "Script query execute action")
            .RequiredArgument("script-action")
            .Choices(scriptAction.GetChoices())
            .Handler1([this, scriptAction](const NLastGetopt::TOptsParser* option) {
                TString choice(option->CurValOrDef());
                ExecutionOptions.ScriptQueryActions.emplace_back(scriptAction(choice));
            });

        options.AddLongOption("timeout", "Timeout in milliseconds for -p queries")
            .RequiredArgument("uint")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                ExecutionOptions.Timeouts.emplace_back(TDuration::MilliSeconds<ui64>(FromString(option->CurValOrDef())));
            });

        options.AddLongOption("cancel-after", "Cancel script execution operation after specified delay in milliseconds")
            .RequiredArgument("uint")
            .StoreMappedResultT<ui64>(&RunnerOptions.ScriptCancelAfter, &TDuration::MilliSeconds<ui64>);

        options.AddLongOption('F', "forget", "Forget script execution operation after fetching results")
            .NoArgument()
            .SetFlag(&ExecutionOptions.ForgetExecution);

        options.AddLongOption("loop-count", "Number of runs of the script query (use 0 to start infinite loop)")
            .RequiredArgument("uint")
            .DefaultValue(ExecutionOptions.LoopCount)
            .StoreResult(&ExecutionOptions.LoopCount);

        options.AddLongOption("loop-delay", "Delay in milliseconds between loop steps")
            .RequiredArgument("uint")
            .DefaultValue(0)
            .StoreMappedResultT<ui64>(&ExecutionOptions.LoopDelay, &TDuration::MilliSeconds<ui64>);

        options.AddLongOption("query-delay", "Delay in milliseconds between queries starts")
            .RequiredArgument("uint")
            .DefaultValue(0)
            .StoreMappedResultT<ui64>(&ExecutionOptions.QueryDelay, &TDuration::MilliSeconds<ui64>);

        options.AddLongOption("continue-after-fail", "Don't not stop requests execution after fails")
            .NoArgument()
            .SetFlag(&ExecutionOptions.ContinueAfterFail);

        options.AddLongOption('D', "database", "Database path for -p queries")
            .RequiredArgument("path")
            .EmplaceTo(&ExecutionOptions.Databases);

        options.AddLongOption('U', "user", "User SID for -p queries")
            .RequiredArgument("user-SID")
            .EmplaceTo(&ExecutionOptions.UserSIDs);

        options.AddLongOption("group", "User group SIDs (should be split by ',') -p queries")
            .RequiredArgument("SIDs")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                ExecutionOptions.GroupSIDs.emplace_back(TVector<NACLib::TSID>());
                StringSplitter(option->CurValOrDef()).Split(',').SkipEmpty().Collect(&(*ExecutionOptions.GroupSIDs.back()));
            });

        options.AddLongOption("pool", "Workload manager pool in which queries will be executed")
            .RequiredArgument("pool-id")
            .EmplaceTo(&ExecutionOptions.PoolIds);

        options.AddLongOption("same-session", "Run all -p requests in one session")
            .NoArgument()
            .SetFlag(&RunnerOptions.YdbSettings.SameSession);

        options.AddLongOption("retry", "Retry queries which failed with specific status")
            .RequiredArgument("status")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                const TString statusName(option->CurValOrDef());
                Ydb::StatusIds::StatusCode status;
                if (!Ydb::StatusIds::StatusCode_Parse(statusName, &status)) {
                    ythrow yexception() << "Invalid status to retry: " << statusName << ", should be one of Ydb::StatusIds::StatusCode";
                }
                if (!RunnerOptions.RetryableStatuses.emplace(status).second) {
                    ythrow yexception() << "Got duplicated status to retry: " << statusName;
                }
            });

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

        options.AddLongOption("dc-count", "Number of data centers")
            .RequiredArgument("uint")
            .DefaultValue(1)
            .StoreMappedResultT<ui32>(&RunnerOptions.YdbSettings.DcCount, [](ui32 dcCount) {
                if (dcCount < 1) {
                    ythrow yexception() << "Number of data centers less than one";
                }
                return dcCount;
            });

        options.AddLongOption('E', "emulate-yt", "Emulate YT tables (use file gateway instead of native gateway)")
            .NoArgument()
            .SetFlag(&EmulateYt);

        options.AddLongOption('H', "health-check", TStringBuilder() << "Level of health check before start (max level " << static_cast<ui32>(TYdbSetupSettings::EHealthCheck::Max) - 1 << ")")
            .RequiredArgument("uint")
            .DefaultValue(static_cast<ui8>(TYdbSetupSettings::EHealthCheck::FetchDatabase))
            .StoreMappedResultT<ui8>(&RunnerOptions.YdbSettings.HealthCheckLevel, [](ui8 value) {
                return static_cast<TYdbSetupSettings::EHealthCheck>(std::min(value, static_cast<ui8>(TYdbSetupSettings::EHealthCheck::Max)));
            });

        options.AddLongOption("health-check-timeout", "Health check timeout in seconds")
            .RequiredArgument("uint")
            .DefaultValue(10)
            .StoreMappedResultT<ui64>(&RunnerOptions.YdbSettings.HealthCheckTimeout, &TDuration::Seconds<ui64>);

        const auto addTenant = [this](const TString& type, TStorageMeta::TTenant::EType protoType, const NLastGetopt::TOptsParser* option) {
            TStringBuf tenant;
            TStringBuf nodesCountStr;
            TStringBuf(option->CurVal()).Split(':', tenant, nodesCountStr);
            if (tenant.empty()) {
                ythrow yexception() << type << " tenant name should not be empty";
            }

            TStorageMeta::TTenant tenantInfo;
            tenantInfo.SetType(protoType);
            tenantInfo.SetNodesCount(nodesCountStr ? FromString<ui32>(nodesCountStr) : 1);
            if (tenantInfo.GetNodesCount() == 0) {
                ythrow yexception() << type << " tenant should have at least one node";
            }

            if (!RunnerOptions.YdbSettings.Tenants.emplace(tenant, tenantInfo).second) {
                ythrow yexception() << "Got duplicated tenant name: " << tenant;
            }
        };
        options.AddLongOption("dedicated", "Dedicated tenant path, relative inside domain (for node count use dedicated-name:node-count)")
            .RequiredArgument("path")
            .Handler1(std::bind(addTenant, "Dedicated", TStorageMeta::TTenant::DEDICATED, std::placeholders::_1));

        options.AddLongOption("shared", "Shared tenant path, relative inside domain (for node count use dedicated-name:node-count)")
            .RequiredArgument("path")
            .Handler1(std::bind(addTenant, "Shared", TStorageMeta::TTenant::SHARED, std::placeholders::_1));

        options.AddLongOption("serverless", "Serverless tenant path, relative inside domain (use string serverless-name@shared-name to specify shared database)")
            .RequiredArgument("path")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf serverless;
                TStringBuf shared;
                TStringBuf(option->CurVal()).Split('@', serverless, shared);
                if (serverless.empty()) {
                    ythrow yexception() << "Serverless tenant name should not be empty";
                }

                TStorageMeta::TTenant tenantInfo;
                tenantInfo.SetType(TStorageMeta::TTenant::SERVERLESS);
                tenantInfo.SetSharedTenant(TString(shared));
                if (!RunnerOptions.YdbSettings.Tenants.emplace(serverless, tenantInfo).second) {
                    ythrow yexception() << "Got duplicated tenant name: " << serverless;
                }
            });

        options.AddLongOption("storage-size", TStringBuilder() << "Domain storage size in gigabytes (" << NKikimr::NBlobDepot::FormatByteSize(DEFAULT_STORAGE_SIZE) << " by default)")
            .RequiredArgument("uint")
            .StoreMappedResultT<ui32>(&RunnerOptions.YdbSettings.DiskSize, [](ui32 diskSize) {
                return static_cast<ui64>(diskSize) << 30;
            });

        options.AddLongOption("storage-path", "Use real PDisks by specified path instead of in memory PDisks (also disable disk mock), use '-' to use temp directory")
            .RequiredArgument("directory")
            .StoreResult(&RunnerOptions.YdbSettings.PDisksPath);

        options.AddLongOption("format-storage", "Clear storage if it exists on --storage-path")
            .NoArgument()
            .SetFlag(&RunnerOptions.YdbSettings.FormatStorage);

        options.AddLongOption("disable-disk-mock", "Disable disk mock on single node cluster")
            .NoArgument()
            .SetFlag(&RunnerOptions.YdbSettings.DisableDiskMock);

        RegisterKikimrOptions(options, RunnerOptions.YdbSettings);
    }

    int DoRun(NLastGetopt::TOptsParseResult&&) override {
        ExecutionOptions.Validate(RunnerOptions);

        ReplaceTemplates(ExecutionOptions.SchemeQuery);
        for (auto& sql : ExecutionOptions.ScriptQueries) {
            ReplaceTemplates(sql);
        }

        RunnerOptions.YdbSettings.YqlToken = YqlToken;
        RunnerOptions.YdbSettings.FunctionRegistry = CreateFunctionRegistry().Get();

        auto& appConfig = RunnerOptions.YdbSettings.AppConfig;
        auto& queryService = *appConfig.MutableQueryServiceConfig();
        if (ExecutionOptions.ResultsRowsLimit) {
            queryService.SetScriptResultRowsLimit(ExecutionOptions.ResultsRowsLimit);
        }
        queryService.SetProgressStatsPeriodMs(PingPeriod.MilliSeconds());

        SetupLogsConfig();

        if (EmulateYt) {
            const auto& fileStorageConfig = appConfig.GetQueryServiceConfig().GetFileStorage();
            auto fileStorage = WithAsync(CreateFileStorage(fileStorageConfig, {MakeYtDownloader(fileStorageConfig)}));
            auto ytFileServices = NYql::NFile::TYtFileServices::Make(RunnerOptions.YdbSettings.FunctionRegistry.Get(), TablesMapping, fileStorage);
            RunnerOptions.YdbSettings.YtGateway = NYql::CreateYtFileGateway(ytFileServices);
            RunnerOptions.YdbSettings.ComputationFactory = NYql::NFile::GetYtFileFactory(ytFileServices);
        } else if (!TablesMapping.empty()) {
            ythrow yexception() << "Tables mapping is not supported without emulate YT mode";
        }

        if (!PqFilesMapping.empty()) {
            const auto fileGateway = MakeIntrusive<NYql::TDummyPqGateway>(true);
            for (auto [_, topic] : PqFilesMapping) {
                if (const auto it = TopicsSettings.find(topic.TopicName); it != TopicsSettings.end()) {
                    topic.CancelOnFileFinish = it->second.CancelOnFileFinish;
                    TopicsSettings.erase(it);
                }
                fileGateway->AddDummyTopic(topic);
            }
            RunnerOptions.YdbSettings.PqGateway = fileGateway;
        }
        if (!TopicsSettings.empty()) {
            ythrow yexception() << "Found topic settings for not existing topic: '" << TopicsSettings.begin()->first << "'";
        }

#ifdef PROFILE_MEMORY_ALLOCATIONS
        if (RunnerOptions.YdbSettings.VerboseLevel >= EVerbose::Info) {
            Cout << CoutColors.Cyan() << "Starting profile memory allocations" << CoutColors.Default() << Endl;
        }
        NAllocProfiler::StartAllocationSampling(true);
#else
        if (ProfileAllocationsOutput) {
            ythrow yexception() << "Profile memory allocations disabled, please rebuild kqprun with flag `-D PROFILE_MEMORY_ALLOCATIONS`";
        }
#endif

        RunScript(ExecutionOptions, RunnerOptions);

#ifdef PROFILE_MEMORY_ALLOCATIONS
        if (RunnerOptions.YdbSettings.VerboseLevel >= EVerbose::Info) {
            Cout << CoutColors.Cyan() << "Finishing profile memory allocations" << CoutColors.Default() << Endl;
        }
        FinishProfileMemoryAllocations();
#endif

        return 0;
    }

private:
    void ReplaceTemplates(TString& sql) const {
        for (const auto& [variable, value] : Templates) {
            SubstGlobal(sql, TStringBuilder() << "${" << variable <<"}", value);
        }
        if (ExecutionOptions.UseTemplates) {
            const TString tokenVariableName = TStringBuilder() << "${" << YQL_TOKEN_VARIABLE << "}";
            if (const TString& yqlToken = GetEnv(YQL_TOKEN_VARIABLE)) {
                SubstGlobal(sql, tokenVariableName, yqlToken);
            } else if (sql.Contains(tokenVariableName)) {
                ythrow yexception() << "Failed to replace ${YQL_TOKEN} template, please specify YQL_TOKEN environment variable";
            }
        }
    }

    void SetupLogsConfig() {
        auto& logConfig = *RunnerOptions.YdbSettings.AppConfig.MutableLogConfig();
        if (DefaultLogPriority) {
            logConfig.SetDefaultLevel(*DefaultLogPriority);
        }
        ModifyLogPriorities(LogPriorities, logConfig);
    }
};

}  // anonymous namespace

}  // namespace NKqpRun

int main(int argc, const char* argv[]) {
    SetupSignalActions();

#ifdef PROFILE_MEMORY_ALLOCATIONS
    NMonitoring::TDynamicCounterPtr memoryProfilingCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    NKikimr::NMiniKQL::InitializeGlobalPagedBufferCounters(memoryProfilingCounters);
#endif

    try {
        NKqpRun::TMain().Run(argc, argv);
    } catch (...) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

        Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        return 1;
    }

    return 0;
}
