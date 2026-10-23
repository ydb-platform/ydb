#include "ydb_echo.h"

namespace NYdb::NConsoleClient {

TCommandEcho::TCommandEcho()
    : TClientCommand("echo", {}, "Print text to standard output")
{}

void TCommandEcho::Config(TConfig& config) {
    TClientCommand::Config(config);

    config.NeedToConnect = false;
    config.NeedToCheckForUpdate = false;
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<text>", "Text to print");
}

int TCommandEcho::Run(TConfig& config) {
    Cout << config.ParseResult->GetFreeArgs()[0] << Endl;
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
