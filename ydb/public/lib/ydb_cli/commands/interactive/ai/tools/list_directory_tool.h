#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

namespace NYdb::NConsoleClient::NAi {

struct TListDirectoryToolSettings {
    TString Database;
    TDriver Driver;
};

ITool::TPtr CreateListDirectoryTool(const TListDirectoryToolSettings& settings, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient::NAi
