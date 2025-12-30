#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient::NAi {

struct TDescribeToolSettings {
    TString Database;
    TDriver Driver;
};

ITool::TPtr CreateDescribeTool(const TDescribeToolSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient::NAi

