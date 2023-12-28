#include "ydb_service_scripting.h"

#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/stat_visualization/flame_graph_builder.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>

namespace NYdb {
namespace NConsoleClient {

TCommandScripting::TCommandScripting()
: TClientCommandTree("scripting", {}, "Scripting service operations")
{
    AddCommand(std::make_unique<TCommandExecuteYqlScript>());
}

TCommandExecuteYqlScript::TCommandExecuteYqlScript()
    : TYdbOperationCommand("yql", {}, "Execute YQL script")
{}

void TCommandExecuteYqlScript::Config(TConfig& config) {
    AddExamplesOption(config);
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption("stats", "Collect statistics mode [none, basic, full]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption("flame-graph", "Path for statistics flame graph image, works only with full stats")
            .RequiredArgument("[Path]").StoreResult(&FlameGraphPath);
    config.Opts->AddLongOption('s', "script", "Text of script to execute").RequiredArgument("[String]").StoreResult(&Script);
    config.Opts->AddLongOption('f', "file", "[Required] Script file").RequiredArgument("PATH").StoreResult(&ScriptFile);
    config.Opts->AddLongOption("explain", "Explain query").Optional().StoreTrue(&Explain);
    config.Opts->AddLongOption("show-response-metadata", ResponseHeadersHelp).Optional().StoreTrue(&ShowHeaders);

    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array,
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

    AddParametersStdinOption(config, "script");

    config.SetFreeArgsNum(0);

    AddCommandExamples(
        TExampleSetBuilder()
            .BeginExample()
                .Title("Execute script text")
                .Text("ydb scripting yql -s \"SELECT 1\"")
            .EndExample()
            .BeginExample()
                .Title("Execute script from file")
                .Text("ydb scripting yql -f script_file.sql")
            .EndExample()
        .Build()
    );

    CheckExamples(config);
}

void TCommandExecuteYqlScript::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    if (!Script && !ScriptFile) {
        throw TMisuseException() << "Neither \"Text of script\" (\"--script\", \"-s\") "
            << "nor \"Path to file with script text\" (\"--file\", \"-f\") were provided.";
    }
    if (Script && ScriptFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of script\" (\"--script\", \"-s\") "
            << "and \"Path to file with script text\" (\"--file\", \"-f\") were provided.";
    }
    if (ScriptFile) {
        Script = ReadFromFile(ScriptFile, "script");
    }
    if(FlameGraphPath && FlameGraphPath->Empty())
    {
        throw TMisuseException() << "FlameGraph path can not be empty.";
    }
    ParseParameters(config);
}

int TCommandExecuteYqlScript::Run(TConfig& config) {
    NScripting::TScriptingClient client(CreateDriver(config));

    if (Explain) {
        NScripting::TExplainYqlRequestSettings settings;
        settings.Mode(NScripting::ExplainYqlRequestMode::Plan);

        auto result = client.ExplainYqlScript(Script, settings).GetValueSync();

        ThrowOnError(result);
        PrintExplainResult(result);
    } else {
        NScripting::TExecuteYqlRequestSettings settings;
        settings.CollectQueryStats(ParseQueryStatsModeOrThrow(CollectStatsMode, NTable::ECollectQueryStatsMode::None));

        if (FlameGraphPath && (settings.CollectQueryStats_ != NTable::ECollectQueryStatsMode::Full
                               && settings.CollectQueryStats_ != NTable::ECollectQueryStatsMode::Profile)) {
            throw TMisuseException() << "Flame graph is available for full or profile stats. Current: "
                                        + (CollectStatsMode.Empty() ? "none" : CollectStatsMode) + '.';
        }

        if (!Parameters.empty() || !IsStdinInteractive()) {
            ValidateResult = MakeHolder<NScripting::TExplainYqlResult>(
                ExplainQuery(config, Script, NScripting::ExplainYqlRequestMode::Validate));
            THolder<TParamsBuilder> paramBuilder;
            while (GetNextParams(paramBuilder)) {
                auto asyncResult = client.ExecuteYqlScript(
                        Script,
                        paramBuilder->Build(),
                        FillSettings(settings)
                );

                auto result = asyncResult.GetValueSync();
                ThrowOnError(result);
                PrintResponseHeader(result);
                PrintResponse(result);
            }
        } else {
            auto asyncResult = client.ExecuteYqlScript(
                    Script,
                    FillSettings(settings)
            );
            auto result = asyncResult.GetValueSync();
            ThrowOnError(result);
            PrintResponseHeader(result);
            PrintResponse(result);
        }
    }

    return EXIT_SUCCESS;
}

void TCommandExecuteYqlScript::PrintResponse(NScripting::TExecuteYqlResult& result) {
    {
        TResultSetPrinter printer(OutputFormat);
        const TVector<TResultSet>& resultSets = result.GetResultSets();
        for (auto resultSetIt = resultSets.begin(); resultSetIt != resultSets.end(); ++resultSetIt) {
            if (resultSetIt != resultSets.begin()) {
                printer.Reset();
            }
            printer.Print(*resultSetIt);
        }
    } // TResultSetPrinter destructor should be called before printing stats

    const TMaybe<NTable::TQueryStats>& stats = result.GetStats();
    if (stats.Defined()) {
        Cout << Endl << "Statistics:" << Endl << stats->ToString();

        auto fullStats = stats->GetPlan();
        if (fullStats) {
            Cout << Endl << "Full statistics:" << Endl;

            TQueryPlanPrinter queryPlanPrinter(OutputFormat, /* analyzeMode */ true);
            queryPlanPrinter.Print(*fullStats);

            if (FlameGraphPath) {
                try {
                    NKikimr::NVisual::GenerateFlameGraphSvg(*FlameGraphPath, *fullStats);
                    Cout << "Resource usage flame graph is successfully saved to " << *FlameGraphPath << Endl;
                }
                catch (const yexception& ex) {
                    Cout << "Can't save resource usage flame graph, error: " << ex.what() << Endl;
                }
            }
        }
    }
}

void TCommandExecuteYqlScript::PrintExplainResult(NScripting::TExplainYqlResult& result) {
    TQueryPlanPrinter queryPlanPrinter(OutputFormat);
    queryPlanPrinter.Print(result.GetPlan());
}

}
}
