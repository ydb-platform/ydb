#include "ydb_yql.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/interactive_cli.h>
#include <util/generic/queue.h>

namespace NYdb {
namespace NConsoleClient {

TCommandYql::TCommandYql()
    : TYdbOperationCommand("yql", {}, "Execute YQL script (streaming)")
{}

void TCommandYql::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    AddExamplesOption(config);
    config.Opts->AddLongOption("stats", "Collect statistics mode [none, basic, full]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption('s', "script", "Text of script to execute").RequiredArgument("[String]").StoreResult(&Script);
    config.Opts->AddLongOption('f', "file", "Script file").RequiredArgument("PATH").StoreResult(&ScriptFile);

    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array,
        EOutputFormat::Csv,
        EOutputFormat::Tsv
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
    if (!Script && !ScriptFile) {
#ifdef _win32_
        throw TMisuseException() << "Neither \"Text of script\" (\"--script\", \"-s\") "
            << "nor \"Path to file with script text\" (\"--file\", \"-f\") were provided. "
            << "Interactive CLI is not supported on Windows.";
#else
        Interactive = true;
#endif
    }
    if (Script && ScriptFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of script\" (\"--script\", \"-s\") "
            << "and \"Path to file with script text\" (\"--file\", \"-f\") were provided.";
    }
    if (ScriptFile) {
        Script = ReadFromFile(ScriptFile, "script");
    }
    ParseParameters(config);
}

int TCommandYql::Run(TConfig& config) {
    if (Interactive) {
        // run interactive cli

        class TLogic : public TInteractiveCli::ILogic {
        public:
            TLogic(TCommandYql *base, TConfig& config)
                : Base(base)
                , Config(config)
            {}

            virtual bool Ready(const std::string &text) override {
                bool done = text.find(';') != std::string::npos;
                return done;
            }

            virtual void Run(const std::string &text, const std::string &stat) override {
                Y_UNUSED(stat);
                try {
                    Base->RunCommand(Config, TString(text.c_str()));
                } catch (TYdbErrorException &error) {
                    Cerr << error;
                }
                TTerminalOutput::Print("\n");
            }

        private:
            TCommandYql* Base;
            TConfig& Config;
        };

    
        TTerminalOutput::Print("\033[32mYDB Interactive CLI \033[0m");
        TTerminalOutput::Print("\033[31m(experimental, no compatibility guarantees)\033[0m\n\n");
        TInteractiveCli::TConfig interactiveCfg;
        interactiveCfg.Prompt = "\033[32m=> \033[0m";
        TInteractiveCli cli(std::make_shared<TLogic>(this, config), std::move(interactiveCfg));
        cli.Run();
        while(true) {
            // blocking until input
            auto action = GetKeyboardAction();
            cli.HandleInput(action);
        }

        return EXIT_SUCCESS;
    } else {
        return RunCommand(config, Script);    
    }
}

int TCommandYql::RunCommand(TConfig& config, const TString &script) {
    TDriver driver = CreateDriver(config);
    NScripting::TScriptingClient client(driver);

    NScripting::TExecuteYqlRequestSettings settings;
    settings.CollectQueryStats(ParseQueryStatsMode(CollectStatsMode, NTable::ECollectQueryStatsMode::None));

    if (!Interactive) {
        SetInterruptHandlers();
    }

    if (!Parameters.empty() || !IsStdinInteractive()) {
        ValidateResult = MakeHolder<NScripting::TExplainYqlResult>(
            ExplainQuery(config, Script, NScripting::ExplainYqlRequestMode::Validate));
        THolder<TParamsBuilder> paramBuilder;
        while (!IsInterrupted() && 
            GetNextParams(ValidateResult->GetParameterTypes(), InputFormat, StdinFormat, FramingFormat, paramBuilder)) {
            
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
        return false;
    }
    return true;
}

}
}

