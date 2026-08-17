#pragma once

#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

namespace NYdb::NConsoleClient::NAi {

struct TExecQueryToolSettings {
    TString Prompt; // Current interactive CLI prompt
    TString Database;
    TLazyDriver::TPtr LazyDriver;
};

ITool::TPtr CreateExecQueryTool(const TExecQueryToolSettings& settings);

} // namespace NYdb::NConsoleClient::NAi
