#pragma once

#include "runner.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

class TLog;

namespace NYdb::NTPCC {

void InitSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

} // namespace NYdb::NTPCC
