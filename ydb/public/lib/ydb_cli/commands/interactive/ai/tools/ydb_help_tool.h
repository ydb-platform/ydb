#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

struct TYdbHelpToolSettings {
    TClientCommand::TConfig::TUsageInfoGetter UsageInfoGetter;
};

ITool::TPtr CreateYdbHelpTool(const TYdbHelpToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
