#include "ydb_sql.h"

#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>

namespace NYdb::NConsoleClient {

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
        .StoreTrue(&ExecSettings.ExplainMode);
    config.Opts->AddLongOption("explain-ast", "In addition to the query logical plan, you can get an AST (abstract syntax tree). "
            "The AST section contains a representation in the internal miniKQL language.")
        .StoreTrue(&ExecSettings.ExplainAst);
    config.Opts->AddLongOption("explain-analyze", "Execute query in explain-analyze mode. Shows query execution plan. "
            "Query results are ignored.\n"
            "Important note: The query is actually executed, so any changes will be applied to the database.")
        .StoreTrue(&ExecSettings.ExplainAnalyzeMode);
    config.Opts->AddLongOption("stats", "Execution statistics collection mode [none, basic, full, profile]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);

    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
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
        .RequiredArgument("[String]").StoreResult(&ExecSettings.DiagnosticsFile);
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
    if (ExecSettings.ExplainMode && ExecSettings.ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExecSettings.ExplainMode && ExecSettings.ExplainAst) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain mode\" (\"--explain\") "
            << "and \"Explain-AST mode\" (\"--explain-ast\") were provided.";
    }
    if (ExecSettings.ExplainAst && ExecSettings.ExplainAnalyzeMode) {
        throw TMisuseException() << "Both mutually exclusive options \"Explain-AST mode\" (\"--explain-ast\") "
            << "and \"Explain-analyze mode\" (\"--explain-analyze\") were provided.";
    }
    if (ExecSettings.ExplainAnalyzeMode && !CollectStatsMode.empty()) {
        throw TMisuseException() << "Statistics collection mode option \"--stats\" has no effect in explain-analyze mode. "
            "Relevant for execution mode only.";
    }
    if (ExecSettings.ExplainMode && !CollectStatsMode.empty()) {
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
    ExecSettings.OutputFormat = OutputFormat;
}

int TCommandSql::Run(TConfig& config) {
    return RunCommand(config);
}

int TCommandSql::RunCommand(TConfig& config) {
    TDriver driver = CreateDriver(config);
    TExecuteGenericQuery executor(driver);
    SetInterruptHandlers();

    if (ExecSettings.ExplainMode || ExecSettings.ExplainAst) {
        // Execute explain request for the query
        ExecSettings.Settings.ExecMode(NQuery::EExecMode::Explain);
    } else {
        // Execute query
        ExecSettings.Settings.ExecMode(NQuery::EExecMode::Execute);
        auto defaultStatsMode = ExecSettings.ExplainAnalyzeMode ? NQuery::EStatsMode::Full : NQuery::EStatsMode::None;
        auto statsMode = ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode);
        ExecSettings.Settings.StatsMode(statsMode);
        if (Progress == "tty") {
            if (statsMode == NQuery::EStatsMode::None) {
                throw TMisuseException() << "Non-none statistics collection mode are required to print progress.";
            }
            if (statsMode >= NQuery::EStatsMode::Full) {
                ExecSettings.Settings.StatsCollectPeriod(std::chrono::milliseconds(3000));
            } else {
                ExecSettings.Settings.StatsCollectPeriod(std::chrono::milliseconds(500));
            }
        }
    }

    ExecSettings.Settings.Syntax(SyntaxType);

    if (!Parameters.empty() || InputParamStream) {
        // Execute query with parameters
        THolder<TParamsBuilder> paramBuilder;
        while (!IsInterrupted() && GetNextParams(driver, Query, paramBuilder, config.IsVerbose())) {
            ExecSettings.Parameters = paramBuilder->Build();

            if (const auto result = executor.Execute(Query, ExecSettings); result != EXIT_SUCCESS) {
                return result;
            }
        }
    } else {
        return executor.Execute(Query, ExecSettings);
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

} // namespace NYdb::NConsoleClient
