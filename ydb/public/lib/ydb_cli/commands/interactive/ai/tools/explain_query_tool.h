#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TExplainQueryToolSettings {
    TLazyDriver::TPtr LazyDriver;
};

ITool::TPtr CreateExplainQueryTool(const TExplainQueryToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
