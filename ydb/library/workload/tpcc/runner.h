#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <library/cpp/logger/priority.h>

namespace NYdb::NTPCC {

struct TRunConfig {
    TRunConfig() = default;

    int WarehouseCount = 0;
    int WarmupSeconds = 0;
    int RunSeconds = 0;

    int MaxInflight = 0;

    TString Path;

    // advanced settings (normally, used by developer only)

    int ThreadCount = 0;
    int DriverCount = 0;
    ELogPriority LogPriority = ELogPriority::TLOG_INFO;
    bool NoSleep = false;
    bool Developer = false;

    // instead of actual transaction just async sleep and return SUCCESS
    int SimulateTransactionMs = 0;
    int SimulateTransactionSelect1Count = 0;
};

void RunSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

} // namespace NYdb::NTPCC
