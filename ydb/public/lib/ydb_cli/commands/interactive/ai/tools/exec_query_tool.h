#pragma once

#include "tool_interface.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TExecQueryToolSettings {
    TString Prompt; // Current interactive CLI prompt
    TString Database;
    TDriver Driver;
};

ITool::TPtr CreateExecQueryTool(const TExecQueryToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
