#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient::NAi {

ITool::TPtr CreateExecQueryTool(TClientCommand::TConfig& config);

} // namespace NYdb::NConsoleClient::NAi
