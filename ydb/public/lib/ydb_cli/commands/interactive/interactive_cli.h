#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/command.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>

#include <util/generic/string.h>

namespace NYdb::NConsoleClient {

class TInteractiveCLI {
    inline const static NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

public:
    explicit TInteractiveCLI(const TString& profileName);

    int Run(TClientCommand::TConfig& config);

private:
    int PrintWelcomeMessage(const TClientCommand::TConfig& config, const TDriver& driver, TInteractiveConfigurationManager::TPtr configManager) const;

    const TString Profile;
};

} // namespace NYdb::NConsoleClient
