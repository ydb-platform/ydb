#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient::NAi {

struct TExecQueryToolSettings {
    TString Prompt; // Current interactive CLI prompt
    TString Database;
    TDriver Driver;
};

ITool::TPtr CreateExecQueryTool(const TExecQueryToolSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient::NAi
