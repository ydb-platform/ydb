#include "ydb_sql.h"

#include <library/cpp/json/json_reader.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <util/generic/queue.h>
#include <google/protobuf/text_format.h>

namespace NYdb {
namespace NConsoleClient {

TCommandSql::TCommandSql()
    : TYdbCommand("sql", {}, "Execute SQL query")
{}

TCommandSql::TCommandSql(TString query, TString collectStatsMode)
    : TYdbCommand("sql", {}, "Execute SQL query")
{
    Query = std::move(query);
    CollectStatsMode = std::move(collectStatsMode);
}

void TCommandSql::Config(TConfig& config) {
    TYdbCommand::Config(config);
    AddExamplesOption(config);
    config.Opts->AddLongOption("async", "Execute script (query) asynchronously. "
            "Operation will be started on server and its Id will be printed. "
            "To get operation status use \"ydb operation get\" command. "
            "To fetch query results use \"--fetch <operation_id>\" option of this command.\n"
            "Note: query results will be stored on server and thus consume storage resources.")
        .StoreTrue(&RunAsync);
    config.Opts->AddLongOption("fetch", "Fetch results of existing operation. "
            "Operation Id should be provided with this option.").RequiredArgument("STRING")
        .StoreResult(&OperationIdToFetch);
    config.Opts->AddLongOption("async-wait",
            "Execute script (query) asynchronously and wait for results (using polling).\n"
            "The difference between using and not-using this option:\n"
            "  - If YDB CLI loses connection to the server when running a query without this option, "
            "the stream breaks and command execution finishes with error. "
            "If this option is used, the polling process continues and query results may still be received after reconnect.\n"
            "  - Using this option will probably reduce performance due to artifitial delays between polling requests.\n"
            "Note: query results will be stored on server and thus consume storage resources.")
        .StoreTrue(&AsyncWait);
    config.Opts->AddLongOption("explain", "Execute explain request for the query. Shows query plan. "
            "The query is not actually executed, thus does not affect the database.")
        .StoreTrue(&ExplainMode);
    config.Opts->AddLongOption("explain-analyze", "Execute query in explain-analyze mode. Shows query plan only. "
            "Query results are ignored.\n"
            "Important note: The query is actually executed, so any changes will be applied in the database.")
        .StoreTrue(&ExplainAnalyzeMode);
    config.Opts->AddLongOption("execution-plan", "Show execution plan instead of logical plan. "
            "Execution plan is more informative but less readable.")
        .StoreTrue(&ExecutionPlan);
    config.Opts->AddLongOption("stats", "Statistics collection mode [none, basic, full, profile]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption('s', "script", "Script (query) text to execute").RequiredArgument("[String]")
        .StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to file with script (query) text").RequiredArgument("PATH")
        .StoreResult(&QueryFile);
    config.Opts->AddLongOption("syntax", "Query syntax [yql, pg]")
        .RequiredArgument("[String]").DefaultValue("yql").StoreResult(&Syntax)
        .Hidden();

    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array,
        EOutputFormat::Csv,
        EOutputFormat::Tsv,
        EOutputFormat::Parquet,
    });

    AddParametersOption(config);

    AddInputFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64
    });

    AddStdinFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64,
        EOutputFormat::Raw,
        EOutputFormat::Csv,
        EOutputFormat::Tsv
    }, {
        EOutputFormat::NoFraming,
        EOutputFormat::NewlineDelimited
    });

    AddParametersStdinOption(config, "query");

    CheckExamples(config);

    config.SetFreeArgsNum(0);
}

void TCommandSql::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    if (Query && QueryFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of query\" (\"--query\", \"-q\") "
            << "and \"Path to file with query text\" (\"--file\", \"-f\") were provided.";
    }
    if (ExplainMode && ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExplainAnalyzeMode && !CollectStatsMode.Empty()) {
        auto optStatsMode = NQuery::ParseStatsMode(CollectStatsMode);
        if (optStatsMode.has_value()) {
            auto statsMode = optStatsMode.value();
            switch (statsMode) {
                case NQuery::EStatsMode::None:
                case NQuery::EStatsMode::Basic:
                throw TMisuseException() << "Statistics collection mode \"" << CollectStatsMode
                    << "\" is too low for explain-analyze mode to show any statistics. Use at least \"full\" mode.";
                default:
                break;
            }
        }
        if (ExplainAnalyzeMode && (CollectStatsMode == "none" || CollectStatsMode == "basic")) {
            throw TMisuseException() << "\"" << CollectStatsMode
                << "\" stats collection mode is too low for Explain-analyze mode to  show any info";
        }
        if (ExecutionPlan && !ExplainMode
                && (CollectStatsMode.Empty() || CollectStatsMode == "none"|| CollectStatsMode == "basic")) {
            throw TMisuseException() << "--execution-plan has no effect with \"" << CollectStatsMode
                << "\" stats collection mode. It's too low to show query plan.";
        }
    }
    if (QueryFile) {
        Query = ReadFromFile(QueryFile, "query");
    }
    ParseParameters(config);
}

int TCommandSql::Run(TConfig& config) {
    return RunCommand(config);
}

int TCommandSql::RunCommand(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient client(driver);
    
    if (RunAsync) {
        // ExecuteScript
        NQuery::TExecuteScriptSettings settings;

        if (ExplainMode) {
            // Execute explain request for the query
            settings.ExecMode(Ydb::Query::EXEC_MODE_EXPLAIN);
        } else {
            // Execute query
            settings.ExecMode(Ydb::Query::EXEC_MODE_EXECUTE);
            auto defaultStatsMode = ExplainAnalyzeMode ? NQuery::EStatsMode::Full : NQuery::EStatsMode::None;
            auto actualStatsMode = ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode);
            Ydb::Query::StatsMode protoStatsMode;
            switch (actualStatsMode) {
                case NQuery::EStatsMode::Unspecified:
                    protoStatsMode = Ydb::Query::StatsMode::STATS_MODE_UNSPECIFIED;
                    break;
                case NQuery::EStatsMode::None:
                    protoStatsMode = Ydb::Query::StatsMode::STATS_MODE_NONE;
                    break;
                case NQuery::EStatsMode::Basic:
                    protoStatsMode = Ydb::Query::StatsMode::STATS_MODE_BASIC;
                    break;
                case NQuery::EStatsMode::Full:
                    protoStatsMode = Ydb::Query::StatsMode::STATS_MODE_FULL;
                    break;
                case NQuery::EStatsMode::Profile:
                    protoStatsMode = Ydb::Query::StatsMode::STATS_MODE_PROFILE;
                    break;
                default:
                    throw TMisuseException() << "Unknown stats collection mode \"" << CollectStatsMode << "\"";
            }
            settings.StatsMode(protoStatsMode);
        }

        if (!Parameters.empty() || !IsStdinInteractive()) {
            // Execute query with parameters
            THolder<TParamsBuilder> paramBuilder;
            while (!IsInterrupted() && GetNextParams(paramBuilder)) {
                auto asyncResult = client.ExecuteScript(
                        Query,
                        paramBuilder->Build(),
                        settings
                    );

                auto result = asyncResult.GetValueSync();
                ThrowOnError(result);
                if (!PrintResponse(result)) {
                    return EXIT_FAILURE;
                }
            }
        } else {
            // Execute query without parameters
            auto asyncResult = client.ExecuteScript(
                Query,
                settings
            );

            auto result = asyncResult.GetValueSync();
            ThrowOnError(result);
            if (!PrintResponse(result)) {
                return EXIT_FAILURE;
            }
        }

    } else {
        // Single stream execution
        SetInterruptHandlers();
        NQuery::TExecuteQuerySettings settings;

        if (ExplainMode) {
            // Execute explain request for the query
            settings.ExecMode(NQuery::EExecMode::Explain);
        } else {
            // Execute query
            settings.ExecMode(NQuery::EExecMode::Execute);
            auto defaultStatsMode = ExplainAnalyzeMode ? NQuery::EStatsMode::Full : NQuery::EStatsMode::None;
            settings.StatsMode(ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode));
        }
        if (!Parameters.empty() || !IsStdinInteractive()) {
            // Execute query with parameters
            THolder<TParamsBuilder> paramBuilder;
            while (!IsInterrupted() && GetNextParams(paramBuilder)) {
                auto asyncResult = client.StreamExecuteQuery(
                        Query,
                        // TODO: NoTx by default
                        NQuery::TTxControl::BeginTx().CommitTx(),
                        paramBuilder->Build(),
                        settings
                    );

                auto result = asyncResult.GetValueSync();
                ThrowOnError(result);
                if (!PrintResponse(result)) {
                    return EXIT_FAILURE;
                }
            }
        } else {
            // Execute query without parameters
            auto asyncResult = client.StreamExecuteQuery(
                Query,
                // TODO: NoTx by default
                NQuery::TTxControl::BeginTx().CommitTx(),
                settings
            );

            auto result = asyncResult.GetValueSync();
            ThrowOnError(result);
            if (!PrintResponse(result)) {
                return EXIT_FAILURE;
            }
        }
    }
    return EXIT_SUCCESS;
}

using namespace NJson;

namespace {
    void PrintJson(const TString &jsonString) {
        auto stringBuf = TStringBuf(jsonString);

        TJsonValue jsonValue;
        try {
            ReadJsonTree(stringBuf, &jsonValue, true/*throw on error*/);
        }
        catch (const TJsonException &ex) {
            Cerr << "Can't parse json: " << ex.what();
        }
        NJsonWriter::TBuf w;
        w.SetIndentSpaces(2);
        w.WriteJsonValue(&jsonValue);
        Cout << w.Str() << Endl;
    }
}

bool TCommandSql::PrintResponse(NQuery::TExecuteQueryIterator& result) {
    TMaybe<TString> stats;
    TMaybe<TString> plan;
    {
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (streamPart.EOS()) {
                    break;
                }
                ThrowOnError(streamPart);
            }

            if (streamPart.HasResultSet() && !ExplainAnalyzeMode) {
                printer.Print(streamPart.GetResultSet());
            }

            if (!streamPart.GetStats().Empty()) {
                if (OutputFormat == EOutputFormat::JsonUnicode) {
                    if (streamPart.GetStats()->GetPlan().Defined()) {
                        Cout << "Plan:" << Endl;
                        PrintJson(streamPart.GetStats()->GetPlan().GetRef());
                    }
                    if (streamPart.GetStats()->GetAst().Defined()) {
                        Cout << "Ast:" << Endl << streamPart.GetStats()->GetAst().GetRef() << Endl;
                    }
                } else if (OutputFormat == EOutputFormat::JsonBase64) {
                    auto& proto = NYdb::TProtoAccessor::GetProto(*streamPart.GetStats());
                    Cout << "Stats proto:" << Endl << proto.DebugString() << Endl;
                } else {
                    const auto& queryStats = *streamPart.GetStats();
                    stats = queryStats.ToString();

                    if (queryStats.GetPlan()) {
                        plan = queryStats.GetPlan();
                    }
                }
            }
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (stats && !ExplainMode) {
        Cout << Endl << "Statistics:" << Endl << *stats;
    }

    if (plan) {
        if (!ExplainMode) {
            Cout << Endl << "Query plan:" << Endl;
        }
        // TODO: get rid of pretty-table format, refactor TQueryPrinter to reflect that
        EOutputFormat format = (OutputFormat == EOutputFormat::Default || OutputFormat == EOutputFormat::Pretty)
            && !ExecutionPlan ? EOutputFormat::PrettyTable : OutputFormat;
        TQueryPlanPrinter queryPlanPrinter(format, /* show actual costs */ !ExplainMode);
        queryPlanPrinter.Print(*plan);
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return false;
    }
    return true;
}

bool TCommandSql::PrintResponse(NQuery::TScriptExecutionOperation& result) {
    Cout << "Operation info:" << Endl << result.ToString();
    return true;
}

}
}
