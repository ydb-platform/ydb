#include "ydb_sql.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <util/generic/queue.h>

namespace NYdb {
namespace NConsoleClient {

TCommandSql::TCommandSql()
    : TYdbOperationCommand("sql", {}, "Execute SQL query")
{}

TCommandSql::TCommandSql(TString query, TString collectStatsMode)
    : TYdbOperationCommand("sql", {}, "Execute SQL query")
{
    Query = std::move(query);
    CollectStatsMode = std::move(collectStatsMode);
}

void TCommandSql::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    AddExamplesOption(config);
    config.Opts->AddLongOption("explain", "Execute query in explain mode. Shows query plan")
        .StoreTrue(&ExplainMode);
    config.Opts->AddLongOption("stats", "Collect statistics mode [none, basic, full, profile]")
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
    settings.StatsMode(ParseQueryStatsModeOrThrow(CollectStatsMode, NQuery::EStatsMode::None));
    settings.ExecMode(ExplainMode ? NQuery::EExecMode::Explain : NQuery::EExecMode::Execute);

    SetInterruptHandlers();

    if (!Parameters.empty() || !IsStdinInteractive()) {
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

            if (streamPart.HasResultSet()) {
                printer.Print(streamPart.GetResultSet());
            }

            if (!streamPart.GetStats().Empty()) {
                const auto& queryStats = *streamPart.GetStats();
                stats = queryStats.ToString();

                if (queryStats.GetPlan()) {
                    plan = queryStats.GetPlan();
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
