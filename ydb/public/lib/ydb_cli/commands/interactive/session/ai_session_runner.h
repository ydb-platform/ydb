#pragma once

#include "session_runner_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient {

struct TAiSessionSettings {
};

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient
