#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TDescribeToolSettings {
    TString Database;
    TLazyDriver::TPtr LazyDriver;
};

ITool::TPtr CreateDescribeTool(const TDescribeToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
