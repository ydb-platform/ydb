#pragma once

#include "session_runner_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

namespace NYdb::NConsoleClient {

struct TAiSessionSettings {
    TInteractiveConfigurationManager::TPtr ConfigurationManager;
    TString Database;
    // Lazy driver dedicated to AI tool calls; initialized at the start of
    // every HandleLine and stopped on return.
    TLazyDriver::TPtr AiLazyDriver;
    TString ConnectionString;
    TClientCommand::TConfig::TUsageInfoGetter UsageInfoGetter;
};

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings);

} // namespace NYdb::NConsoleClient
