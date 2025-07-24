#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb {
namespace NConsoleClient {

bool TLeafCommand::Prompt(TConfig& config) {
    Y_UNUSED(config);
    if (Dangerous && !config.AssumeYes) {
        return AskPrompt("This command may damage your cluster, do you want to continue?", false);
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
    opts.AddLongOption("timeout", "Client timeout. There is no point waiting for the result after this long.")
        .RequiredArgument("ms").StoreResult(&ClientTimeout);
}

TYdbOperationCommand::TYdbOperationCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    :TYdbCommand(name, aliases, description)
{}

void TYdbOperationCommand::Config(TConfig& config) {
    TYdbCommand::Config(config);

    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption("timeout", "Operation timeout. Operation should be executed on server within this timeout. "
            "There could also be a delay up to 200ms to receive timeout error from server.")
        .RequiredArgument("ms").StoreResult(&OperationTimeout);
}

}
}
