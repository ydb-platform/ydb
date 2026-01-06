#pragma once

#include "session_runner_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient {

struct TAiSessionSettings {
    TInteractiveConfigurationManager::TPtr ConfigurationManager;
    TString Database;
    TDriver Driver;
    TString ConnectionString;
};

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient
