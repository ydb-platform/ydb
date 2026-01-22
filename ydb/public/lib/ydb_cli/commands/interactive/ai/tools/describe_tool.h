#pragma once

#include "tool_interface.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TDescribeToolSettings {
    TString Database;
    TDriver Driver;
};

ITool::TPtr CreateDescribeTool(const TDescribeToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
