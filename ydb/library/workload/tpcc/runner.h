#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <library/cpp/logger/priority.h>

namespace NYdb::NTPCC {

struct TRunConfig {
    enum class EDisplayMode {
        None = 0,
        Text,
        Htop,
    };

    TRunConfig() = default;
    void SetFullPath(const NConsoleClient::TClientCommand::TConfig& connectionConfig) {
        if (Path.empty()) {
            Path = connectionConfig.Database;
            return;
        }

        if (Path[0] == '/') {
            return;
        }

        Path = connectionConfig.Database + '/' + Path;
    }

    int WarehouseCount = 0;
    int WarmupMinutes = 0;
    int RunMinutes = 0;

    int MaxInflight = 0;

    TString Path;

    TString JsonResultPath;

    // advanced settings (normally, used by developer only)

    int ThreadCount = 0;
    int DriverCount = 0;
    ELogPriority LogPriority = ELogPriority::TLOG_INFO;
    bool NoDelays = false;
    bool ExtendedStats = false;
    EDisplayMode DisplayMode = EDisplayMode::None;

    // instead of actual transaction just async sleep and return SUCCESS
    int SimulateTransactionMs = 0;
    int SimulateTransactionSelect1Count = 0;
};

void RunSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

} // namespace NYdb::NTPCC
