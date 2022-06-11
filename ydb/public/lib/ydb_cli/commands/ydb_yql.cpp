#include "ydb_yql.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>

namespace NYdb {
namespace NConsoleClient {

TCommandYql::TCommandYql()
    : TYdbOperationCommand("yql", {}, "Execute YQL script (streaming)")
{}

void TCommandYql::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    config.Opts->AddLongOption("stats", "Collect statistics mode [none, basic, full]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption('s', "script", "Text of script to execute").RequiredArgument("[String]").StoreResult(&Script);
    config.Opts->AddLongOption('f', "file", "[Required] Script file").RequiredArgument("PATH").StoreResult(&ScriptFile);

    AddParametersOption(config);

    AddInputFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64
        });

    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array
        });

    config.SetFreeArgsNum(0);
}

void TCommandYql::Parse(TConfig& config) {
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
    ParseParameters();
}

int TCommandYql::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    NScripting::TScriptingClient client(driver);
    NTable::TTableClient tableClient(driver);

    NScripting::TExecuteYqlRequestSettings settings;
    settings.CollectQueryStats(ParseQueryStatsMode(CollectStatsMode, NTable::ECollectQueryStatsMode::None));

    NScripting::TAsyncYqlResultPartIterator asyncResult;
    if (Parameters.size()) {
        auto validateResult = ExplainQuery(config, Script, NScripting::ExplainYqlRequestMode::Validate);
        asyncResult = client.StreamExecuteYqlScript(
            Script,
            BuildParams(validateResult.GetParameterTypes(), InputFormat),
            FillSettings(settings)
        );
    } else {
        asyncResult = client.StreamExecuteYqlScript(
            Script,
            FillSettings(settings)
        );
    }
    NScripting::TYqlResultPartIterator result = asyncResult.GetValueSync();

    ThrowOnError(result);
    PrintResponse(result);
    return EXIT_SUCCESS;
}

void TCommandYql::PrintResponse(NScripting::TYqlResultPartIterator& result) {
    SetInterruptHandlers();
    TStringStream statsStr;
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
            }
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (statsStr.Size()) {
        Cout << Endl << "Statistics:" << statsStr.Str();
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
    }
}

}
}

