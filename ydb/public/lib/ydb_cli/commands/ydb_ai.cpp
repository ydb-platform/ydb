#include "ydb_ai.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_ai/line_reader.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

void PrintExitMessage() {
    Cout << "\nBye" << Endl;
}

} // anonymous namespace

TCommandAi::TCommandAi()
    : TBase("ai", {}, "AI-TODO: KIKIMR-24198 -- description")
{}

void TCommandAi::Config(TConfig& config) {
    TClientCommand::Config(config);
    config.Opts->SetTitle("AI-TODO: KIKIMR-24198 -- title");
    config.Opts->SetFreeArgsNum(0);
}

int TCommandAi::Run(TConfig& config) {
    Y_UNUSED(config);

    Cout << "AI-TODO: KIKIMR-24198 -- welcome message" << Endl;

    // AI-TODO: KIKIMR-24202 - robust file creation
    NAi::TLineReader lineReader("ydb-ai> ", (TFsPath(HomeDir) / ".ydb-ai/history").GetPath());

    while (const auto& maybeLine = lineReader.ReadLine()) {
        const auto& input = *maybeLine;
        if (input.empty()) {
            continue;
        }

        if (IsIn({"quit", "exit"}, to_lower(input))) {
            PrintExitMessage();
            return EXIT_SUCCESS;
        }

        Cout << "Input value: " << input << Endl;
    }

    PrintExitMessage();
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient