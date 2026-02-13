#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb::NConsoleClient {

bool TLeafCommand::Prompt(TConfig& config) {
    if (Dangerous && !config.AssumeYes) {
        return AskYesOrNo("This command may damage your cluster, do you want to continue?", /* defaultAnswer */ false);
    }

    return true;
}

TYdbCommand::TYdbCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TLeafCommand(name, aliases, description)
{}

TDriver TYdbCommand::CreateDriver(TConfig& config) {
    return TDriver(config.CreateDriverConfig());
}

TDriver TYdbCommand::CreateDriver(TConfig& config, std::unique_ptr<TLogBackend>&& loggingBackend) {
    auto driverConfig = config.CreateDriverConfig();
    driverConfig.SetLog(std::move(loggingBackend));

    return TDriver(driverConfig);
}

bool TYdbReadOnlyCommand::Prompt(TConfig& config) {
    Y_UNUSED(config);

    return true;
}

TYdbSimpleCommand::TYdbSimpleCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbSimpleCommand::Config(TConfig& config) {
    TClientCommand::Config(config);

    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption("timeout", "Client timeout. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds.")
        .RequiredArgument("DURATION").StoreResult(&ClientTimeout);
}

TYdbOperationCommand::TYdbOperationCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbOperationCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);

    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption("timeout", "Operation timeout. Supports time units (e.g., '5s', '1m'). Plain number interpreted as milliseconds. "
            "There could also be a delay up to 200ms to receive timeout error from server.")
        .RequiredArgument("DURATION").StoreResult(&OperationTimeout);
}

} // namespace NYdb::NConsoleClient
