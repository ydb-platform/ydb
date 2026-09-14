#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TListDirectoryToolSettings {
    TString Database;
    TLazyDriver::TPtr LazyDriver;
};

ITool::TPtr CreateListDirectoryTool(const TListDirectoryToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
