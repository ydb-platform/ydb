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
    config.Opts->AddLongOption('f', "file", "Path to file with script (query) text."
            " Path \"-\" means reading query text from stdin.").RequiredArgument("PATH")
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
    config.Opts->AddLongOption("diagnostics-file", "Path to file where the diagnostics will be saved.")
        .RequiredArgument("[String]").StoreResult(&DiagnosticsFile);
    config.Opts->AddLongOption("syntax", "Query syntax [yql, pg]")
        .RequiredArgument("[String]").DefaultValue("yql").StoreResult(&Syntax)
        .Hidden();

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
    if (PrintProgress && (CollectStatsMode.empty() || CollectStatsMode == "none")) {
        throw TMisuseException() << "Option \"--print-progress\" requires non-none statistics collection mode.";
    }
    if (FullStatsFileName && (CollectStatsMode.empty() || CollectStatsMode == "none" || CollectStatsMode == "basic")) {
        throw TMisuseException() << "Option \"--full-stats\" requires full or profile statistics collection mode.";
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
    if (PrintProgress) {
        settings.StatsCollectPeriod(std::chrono::milliseconds(500));
    }

    if (ExplainMode || ExplainAst) {
        // Execute explain request for the query
        settings.ExecMode(NQuery::EExecMode::Explain);
    } else {
        // Execute query
        settings.ExecMode(NQuery::EExecMode::Execute);
        auto defaultStatsMode = ExplainAnalyzeMode ? NQuery::EStatsMode::Full : NQuery::EStatsMode::None;
        settings.StatsMode(ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode));
    }
    if (Syntax == "yql") {
        settings.Syntax(NQuery::ESyntax::YqlV1);
    } else if (Syntax == "pg") {
        settings.Syntax(NQuery::ESyntax::Pg);
    } else {
        throw TMisuseException() << "Unknow syntax option \"" << Syntax << "\"";
    }

    if (!Parameters.empty() || InputParamStream) {
        // Execute query with parameters
        THolder<TParamsBuilder> paramBuilder;
        while (!IsInterrupted() && GetNextParams(driver, Query, paramBuilder)) {
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

        TString currentStatsFileName;
        TString currentPlanWithStatsFileName;
        TString currentPlanWithStatsFileNameJson;
        if (FullStatsFileName) {
            currentStatsFileName = TStringBuilder() << FullStatsFileName << ".stats";
            currentPlanWithStatsFileName = TStringBuilder() << FullStatsFileName << ".plan.svg";
            currentPlanWithStatsFileNameJson = TStringBuilder() << FullStatsFileName << ".plan.json";
        }

        TMaybe<NQuery::TExecStats> execStats;

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().ExtractValueSync();
            if (ThrowOnErrorAndCheckEOS(streamPart)) {
                break;
            }

            if (streamPart.HasStats()) {
                execStats = streamPart.ExtractStats();

                if (FullStatsFileName) {
                    TFileOutput out(currentStatsFileName);
                    out << execStats->ToString();
                    {
                        auto plan = execStats->GetPlan();
                        if (plan) {
                            {
                                TPlanVisualizer pv;
                                TFileOutput out(currentPlanWithStatsFileName);
                                try {
                                    pv.LoadPlans(*execStats->GetPlan());
                                    out << pv.PrintSvg();
                                } catch (std::exception& e) {
                                    out << "<svg width='1024' height='256' xmlns='http://www.w3.org/2000/svg'><text>" << e.what() << "<text></svg>";
                                }
                            }
                            {
                                TFileOutput out(currentPlanWithStatsFileNameJson);
                                TQueryPlanPrinter queryPlanPrinter(EDataFormat::JsonBase64, true, out, 120);
                                queryPlanPrinter.Print(*execStats->GetPlan());
                            }
                        }
                    }
                }

                const auto& protoStats = TProtoAccessor::GetProto(execStats.GetRef());
                // for (const auto& queryPhase : protoStats.query_phases()) {
                //     for (const auto& tableAccessStats : queryPhase.table_access()) {
                //         progressIndication.UpdateProgress({tableAccessStats.reads().rows(), tableAccessStats.reads().bytes(),
                //             tableAccessStats.updates().rows(), tableAccessStats.updates().bytes(),
                //             tableAccessStats.deletes().rows(), tableAccessStats.deletes().bytes()});
                //     }
                // }
                progressIndication.UpdateProgress({protoStats.total_read_rows(), protoStats.total_read_bytes(),
                    0, 0, 0, 0, protoStats.total_duration_us()});


                progressIndication.Render();
            }

            if (streamPart.HasResultSet() && !ExplainAnalyzeMode) {
                progressIndication.Finish();
                printer.Print(streamPart.GetResultSet());
            }
        }
        stats = execStats->ToString();
        ast = execStats->GetAst();

<<<<<<< HEAD
            if (streamPart.GetStats().has_value()) {
                const auto& queryStats = *streamPart.GetStats();
                stats = queryStats.ToString();
                ast = queryStats.GetAst();

                if (queryStats.GetPlan()) {
                    plan = queryStats.GetPlan();
                }
                if (queryStats.GetMeta()) {
                    meta = queryStats.GetMeta();
                }
            }
=======
        if (execStats->GetPlan()) {
            plan = execStats->GetPlan();
>>>>>>> Fixes
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

void TCommandSql::SetSyntax(TString&& syntax) {
    Syntax = std::move(syntax);
}

}
}
