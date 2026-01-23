#pragma once

#include "tool_interface.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TListDirectoryToolSettings {
    TString Database;
    TDriver Driver;
};

ITool::TPtr CreateListDirectoryTool(const TListDirectoryToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
