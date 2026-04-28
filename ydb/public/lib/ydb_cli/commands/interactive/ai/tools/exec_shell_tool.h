#pragma once

#include "tool_interface.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb::NConsoleClient::NAi {

struct TExecShellToolSettings {
    TString Prompt; // Current interactive CLI prompt
    TDriver Driver;
};

ITool::TPtr CreateExecShellTool(const TExecShellToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
