#pragma once

#include "tool_interface.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TExplainQueryToolSettings {
    TDriver Driver;
};

ITool::TPtr CreateExplainQueryTool(const TExplainQueryToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
