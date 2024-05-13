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
    config.Opts->AddLongOption("explain", "Execute explain request for the query. Shows query plan. "
            "The query is not actually executed, thus does not affect the database.")
        .StoreTrue(&ExplainMode);
    config.Opts->AddLongOption("explain-analyze", "Execute query in explain-analyze mode. Shows query plan only. "
            "Query results are ignored.\n"
            "Important! The query is actually executed, so any changes will be applied in the database.")
        .StoreTrue(&ExplainAnalyzeMode);
    config.Opts->AddLongOption("stats", "Statistics collection mode [none, basic, full, profile]\n"
            "Has no effect with analyze mode.")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption('q', "query", "Text of query to execute").RequiredArgument("[String]")
        .StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to file with query text").RequiredArgument("PATH")
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
        if (ExplainAnalyzeMode && (CollectStatsMode == "")) {
            throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
                << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
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

    NQuery::TExecuteQuerySettings settings;

    SetInterruptHandlers();

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
            NQuery::TTxControl::BeginTx().CommitTx(),
            settings
        );

        auto result = asyncResult.GetValueSync();
        ThrowOnError(result);
        if (!PrintResponse(result)) {
            return EXIT_FAILURE;
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
            Cout << Endl << "Full statistics:" << Endl;
        }

        TQueryPlanPrinter queryPlanPrinter(OutputFormat, /* analyzeMode */ true);
        queryPlanPrinter.Print(*plan);
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return false;
    }
    return true;
}

}
}
