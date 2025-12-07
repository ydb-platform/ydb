#pragma once

#include "tool_interface.h"

namespace NYdb::NConsoleClient::NAi {

ITool::TPtr CreateExecQueryTool(TClientCommand::TConfig& config);

} // namespace NYdb::NConsoleClient::NAi
