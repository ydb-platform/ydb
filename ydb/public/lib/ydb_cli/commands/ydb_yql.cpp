#include "ydb_yql.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/stat_visualization/flame_graph_builder.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <util/generic/queue.h>

namespace NYdb {
namespace NConsoleClient {

TCommandYql::TCommandYql()
    : TYdbOperationCommand("yql", {}, "Execute YQL script (streaming)")
{}

TCommandYql::TCommandYql(TString script, TString collectStatsMode)
    : TYdbOperationCommand("yql", {}, "Execute YQL script (streaming)")
{
    Script = std::move(script);
    CollectStatsMode = std::move(collectStatsMode);
}

void TCommandYql::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    AddExamplesOption(config);
    config.Opts->AddLongOption("stats", "Collect statistics mode [none, basic, full]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption("flame-graph", "Path for statistics flame graph image, works only with full stats")
            .RequiredArgument("[Path]").StoreResult(&FlameGraphPath);
    config.Opts->AddLongOption('s', "script", "Text of script to execute").RequiredArgument("[String]").StoreResult(&Script);
    config.Opts->AddLongOption('f', "file", "Script file").RequiredArgument("PATH").StoreResult(&ScriptFile);

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

    AddParametersStdinOption(config, "script");

    CheckExamples(config);

    config.SetFreeArgsNum(0);
}

void TCommandYql::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
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

int TCommandYql::Run(TConfig& config) {
    return RunCommand(config, Script);
}

int TCommandYql::RunCommand(TConfig& config, const TString& script) {
    TDriver driver = CreateDriver(config);
    NScripting::TScriptingClient client(driver);

    NScripting::TExecuteYqlRequestSettings settings;
    settings.CollectQueryStats(ParseQueryStatsModeOrThrow(CollectStatsMode, NTable::ECollectQueryStatsMode::None));

    if (FlameGraphPath && (settings.CollectQueryStats_ != NTable::ECollectQueryStatsMode::Full
                           && settings.CollectQueryStats_ != NTable::ECollectQueryStatsMode::Profile)) {
        throw TMisuseException() << "Flame graph is available for full or profile stats. Current: "
                                    + (CollectStatsMode.Empty() ? "none" : CollectStatsMode) + '.';
    }

    SetInterruptHandlers();

    if (!Parameters.empty() || !IsStdinInteractive()) {
        ValidateResult = MakeHolder<NScripting::TExplainYqlResult>(
            ExplainQuery(config, Script, NScripting::ExplainYqlRequestMode::Validate));
        THolder<TParamsBuilder> paramBuilder;
        while (!IsInterrupted() && GetNextParams(paramBuilder)) {
            auto asyncResult = client.StreamExecuteYqlScript(
                    script,
                    paramBuilder->Build(),
                    FillSettings(settings)
            );

            auto result = asyncResult.GetValueSync();
            ThrowOnError(result);
            if (!PrintResponse(result)) {
                return EXIT_FAILURE;
            }
        }
    } else {
        auto asyncResult = client.StreamExecuteYqlScript(
            script,
            FillSettings(settings)
        );

        auto result = asyncResult.GetValueSync();
        ThrowOnError(result);
        if (!PrintResponse(result)) {
            return EXIT_FAILURE;
        }
    }
    return EXIT_SUCCESS;
}

bool TCommandYql::PrintResponse(NScripting::TYqlResultPartIterator& result) {
    TStringStream statsStr;
    TMaybe<TString> fullStats;
    {
        ui32 currentIndex = 0;
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (streamPart.EOS()) {
                    break;
                }
                ThrowOnError(streamPart);
            }

            if (streamPart.HasPartialResult()) {
                const auto& partialResult = streamPart.GetPartialResult();

                ui32 resultSetIndex = partialResult.GetResultSetIndex();
                if (currentIndex != resultSetIndex) {
                    currentIndex = resultSetIndex;
                    printer.Reset();
                }

                printer.Print(partialResult.GetResultSet());
            }

            if (streamPart.HasQueryStats()) {
                const auto& queryStats = streamPart.GetQueryStats();
                statsStr << Endl << queryStats.ToString() << Endl;

                if (queryStats.GetPlan()) {
                    fullStats = queryStats.GetPlan();
                }
            }
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (statsStr.Size()) {
        Cout << Endl << "Statistics:" << statsStr.Str();
    }

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

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return false;
    }
    return true;
}

}
}
