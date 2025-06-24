#include "ydb_sql.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_prettifier.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/plan2svg.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/progress_indication.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/waiting_bar.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <util/generic/queue.h>
#include <util/string/escape.h>
#include <google/protobuf/text_format.h>

namespace NYdb {
namespace NConsoleClient {

using namespace NKikimr::NOperationId;

TCommandSql::TCommandSql()
    : TYdbCommand("sql", {}, "Execute SQL query")
{}

void TCommandSql::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption('s', "script", "Script (query) text to execute").RequiredArgument("[String]")
        .StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to a file containing the query text to execute. "
            "The path '-' means reading the query text from stdin.").RequiredArgument("PATH")
        .StoreResult(&QueryFile);
    config.Opts->AddLongOption("explain", "Execute explain request for the query. Shows query logical plan. "
            "The query is not actually executed, thus does not affect the database.")
        .StoreTrue(&ExplainMode);
    config.Opts->AddLongOption("explain-ast", "In addition to the query logical plan, you can get an AST (abstract syntax tree). "
            "The AST section contains a representation in the internal miniKQL language.")
        .StoreTrue(&ExplainAst);
    config.Opts->AddLongOption("explain-analyze", "Execute query in explain-analyze mode. Shows query execution plan. "
            "Query results are ignored.\n"
            "Important note: The query is actually executed, so any changes will be applied to the database.")
        .StoreTrue(&ExplainAnalyzeMode);
    config.Opts->AddLongOption("stats", "Execution statistics collection mode [none, basic, full, profile]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);

    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    TStringStream description;
    description << "Print progress of query execution. Requires non-none statistics collection mode. Available options: ";
    description << "\n  " << colors.BoldColor() << "tty" << colors.OldColor()
            << "\n    " << "Print progress to the terminal";
    description << "\n  " << colors.BoldColor() << "none" << colors.OldColor()
            << "\n    " << "Disables progress printing";
    description << "\nDefault: " << colors.CyanColor() << "\"none\"" << colors.OldColor() << ".";

    config.Opts->AddLongOption("progress", description.Str())
        .RequiredArgument("[String]").Hidden().DefaultValue("none").StoreResult(&Progress);
    config.Opts->AddLongOption("diagnostics-file", "Path to file where the diagnostics will be saved.")
        .RequiredArgument("[String]").StoreResult(&DiagnosticsFile);
    config.Opts->AddLongOption("syntax", "Query syntax [yql, pg]")
        .RequiredArgument("[String]")
        .Hidden()
        .GetOpt().Handler1T<TString>("yql", [this](const TString& arg) {
            SetSyntax(arg);
        });

    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::JsonUnicode,
        EDataFormat::JsonUnicodeArray,
        EDataFormat::JsonBase64,
        EDataFormat::JsonBase64Array,
        EDataFormat::Csv,
        EDataFormat::Tsv,
        EDataFormat::Parquet,
    });

    AddParametersOption(config);

    AddDefaultParamFormats(config);

    AddBatchParametersOptions(config, "script");

    CheckExamples(config);

    config.SetFreeArgsNum(0);
}

void TCommandSql::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseInputFormats();
    ParseOutputFormats();
    if (Query && QueryFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of query\" (\"--query\", \"-q\") "
            << "and \"Path to file with query text\" (\"--file\", \"-f\") were provided.";
    }
    if (ExplainMode && ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExplainMode && ExplainAst) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-AST mode\" (\"--explain-ast\") were provided.";
    }
    if (ExplainAst && ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain-AST mode\" (\"--explain-ast\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExplainAnalyzeMode && !CollectStatsMode.empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain-analyze mode. "
            "Relevant for execution mode only.";
    }
    if (ExplainMode && !CollectStatsMode.empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain mode"
            "Relevant for execution mode only.";
    }
    if (QueryFile) {
        if (QueryFile == "-") {
            if (IsStdinInteractive()) {
                throw TMisuseException() << "Path to script file is \"-\", meaning that script text should be read "
                    "from stdin. This is only available in non-interactive mode";
            }
            if (ReadingSomethingFromStdin) {
                throw TMisuseException() << "Can't read both script file and parameters from stdin";
            }
            ReadingSomethingFromStdin = true;
            Query = Cin.ReadAll();
        } else {
            Query = ReadFromFile(QueryFile, "query");
        }
    }
    if (Query.empty()) {
        Cerr << "Neither text of script (\"--script\", \"-s\") "
            << "nor path to file with script text (\"--file\", \"-f\") were provided." << Endl;
        config.PrintHelpAndExit();
    }
    if (Progress && Progress != "tty" && Progress != "none") {
        throw TMisuseException() << "Unknow progress option \"" << Progress << "\".";
    }
    // Should be called after setting ReadingSomethingFromStdin
    ParseParameters(config);
}

int TCommandSql::Run(TConfig& config) {
    return RunCommand(config);
}

int TCommandSql::RunCommand(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NQuery::TQueryClient client(driver);
    SetInterruptHandlers();
    // Single stream execution
    NQuery::TExecuteQuerySettings settings;

    if (ExplainMode || ExplainAst) {
        // Execute explain request for the query
        settings.ExecMode(NQuery::EExecMode::Explain);
    } else {
        // Execute query
        settings.ExecMode(NQuery::EExecMode::Execute);
        auto defaultStatsMode = ExplainAnalyzeMode ? NQuery::EStatsMode::Full : NQuery::EStatsMode::None;
        auto statsMode = ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode);
        settings.StatsMode(statsMode);
        if (Progress == "tty") {
            if (statsMode == NQuery::EStatsMode::None) {
                throw TMisuseException() << "Non-none statistics collection mode are required to print progress.";
            }
            if (statsMode >= NQuery::EStatsMode::Full) {
                settings.StatsCollectPeriod(std::chrono::milliseconds(3000));
            } else {
                settings.StatsCollectPeriod(std::chrono::milliseconds(500));
            }
        }
    }

    settings.Syntax(SyntaxType);

    if (!Parameters.empty() || InputParamStream) {
        // Execute query with parameters
        THolder<TParamsBuilder> paramBuilder;
        while (!IsInterrupted() && GetNextParams(driver, Query, paramBuilder, config.IsVerbose())) {
            auto asyncResult = client.StreamExecuteQuery(
                    Query,
                    NQuery::TTxControl::NoTx(),
                    paramBuilder->Build(),
                    settings
                );

            auto result = asyncResult.GetValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
            int printResult = PrintResponse(result);
            if (printResult != EXIT_SUCCESS) {
                return printResult;
            }
        }
    } else {
        // Execute query without parameters
        auto asyncResult = client.StreamExecuteQuery(
            Query,
            NQuery::TTxControl::NoTx(),
            settings
        );

        auto result = asyncResult.GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        return PrintResponse(result);
    }
    return EXIT_SUCCESS;
}

int TCommandSql::PrintResponse(NQuery::TExecuteQueryIterator& result) {
    std::optional<std::string> stats;
    std::optional<std::string> plan;
    std::optional<std::string> ast;
    std::optional<std::string> meta;
    {
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        TProgressIndication progressIndication;
        TMaybe<NQuery::TExecStats> execStats;

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().ExtractValueSync();
            if (ThrowOnErrorAndCheckEOS(streamPart)) {
                break;
            }

            if (streamPart.HasStats()) {
                execStats = streamPart.ExtractStats();

                const auto& protoStats = TProtoAccessor::GetProto(execStats.GetRef());
                for (const auto& queryPhase : protoStats.query_phases()) {
                    for (const auto& tableAccessStats : queryPhase.table_access()) {
                        progressIndication.UpdateProgress({tableAccessStats.reads().rows(), tableAccessStats.reads().bytes()});
                    }
                }
                progressIndication.SetDurationUs(protoStats.total_duration_us());


                progressIndication.Render();
            }

            if (streamPart.HasResultSet() && !ExplainAnalyzeMode) {
                progressIndication.Finish();
                printer.Print(streamPart.GetResultSet());
            }
        }

        if (execStats) {
            stats = execStats->ToString();
            plan = execStats->GetPlan();
            ast = execStats->GetAst();
            meta = execStats->GetMeta();
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (ExplainAst) {
        Cout << "Query AST:" << Endl << ast << Endl;

        if (IsInterrupted()) {
            Cerr << "<INTERRUPTED>" << Endl;
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    if (stats && !ExplainMode && !ExplainAnalyzeMode) {
        Cout << Endl << "Statistics:" << Endl << *stats;
    }

    if (plan) {
        if (!ExplainMode && !ExplainAnalyzeMode
                && (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty)) {
            Cout << Endl << "Execution plan:" << Endl;
        }
        // TODO: get rid of pretty-table format, refactor TQueryPrinter to reflect that
        EDataFormat format = (OutputFormat == EDataFormat::Default || OutputFormat == EDataFormat::Pretty)
            && (ExplainMode || ExplainAnalyzeMode)
            ? EDataFormat::PrettyTable : OutputFormat;
        TQueryPlanPrinter queryPlanPrinter(format, /* show actual costs */ !ExplainMode);
        queryPlanPrinter.Print(TString{*plan});
    }

    if (!DiagnosticsFile.empty()) {
        TFileOutput file(DiagnosticsFile);

        NJson::TJsonValue diagnosticsJson(NJson::JSON_MAP);

        if (stats) {
            diagnosticsJson.InsertValue("stats", *stats);
        }
        if (ast) {
            diagnosticsJson.InsertValue("ast", *ast);
        }
        if (plan) {
            NJson::TJsonValue planJson;
            NJson::ReadJsonTree(*plan, &planJson, true);
            diagnosticsJson.InsertValue("plan", planJson);
        }
        if (meta) {
            NJson::TJsonValue metaJson;
            NJson::ReadJsonTree(*meta, &metaJson, true);
            metaJson.InsertValue("query_text", EscapeC(Query));
            diagnosticsJson.InsertValue("meta", metaJson);
        }
        file << NJson::PrettifyJson(NJson::WriteJson(diagnosticsJson, true), false);
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

void TCommandSql::SetScript(TString&& script) {
    Query = std::move(script);
}

void TCommandSql::SetCollectStatsMode(TString&& collectStatsMode) {
    CollectStatsMode = std::move(collectStatsMode);
}

void TCommandSql::SetSyntax(const TString& syntax) {
    if (syntax == "yql") {
        SyntaxType = NYdb::NQuery::ESyntax::YqlV1;
    } else if (syntax == "pg") {
        SyntaxType = NYdb::NQuery::ESyntax::Pg;
    } else {
        throw TMisuseException() << "Unknown syntax option \"" << syntax << "\"";
    }
}

}
}
