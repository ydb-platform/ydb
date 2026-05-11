#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb::NConsoleClient::NAi {

struct TExecShellToolSettings {
    TString Prompt; // Current interactive CLI prompt
};

ITool::TPtr CreateExecShellTool(const TExecShellToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
