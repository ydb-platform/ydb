#pragma once

#include "runner.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NTPCC {

void CheckSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

} // namespace NYdb::NTPCC
