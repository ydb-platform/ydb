#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <library/cpp/logger/priority.h>

namespace NYdb::NTPCC {

constexpr int DEFAULT_WAREHOUSE_COUNT = 1;
constexpr int DEFAULT_WARMUP_MINUTES = 1; // TODO
constexpr int DEFAULT_RUN_MINUTES = 2; // TODO
constexpr int DEFAULT_MAX_SESSIONS = 100; // TODO
constexpr int DEFAULT_LOG_LEVEL = 6; // TODO: properly use enum

struct TRunConfig {
    enum class EDisplayMode {
        None = 0,
        Text,
        Tui,
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

    void SetDisplayUpdateInterval() {
        switch (DisplayMode) {
        case EDisplayMode::None:
            return;
        case EDisplayMode::Text:
            DisplayUpdateInterval = DisplayUpdateTextInterval;
            return;
        case EDisplayMode::Tui:
            DisplayUpdateInterval = DisplayUpdateTuiInterval;
            return;
          break;
        }
    }

    int WarehouseCount = DEFAULT_WAREHOUSE_COUNT;
    int WarmupMinutes = DEFAULT_WARMUP_MINUTES;
    int RunMinutes = DEFAULT_RUN_MINUTES;

    int MaxInflight = DEFAULT_MAX_SESSIONS;

    TString Path;

    TString JsonResultPath;

    // advanced settings (normally, used by developer only)

    int ThreadCount = 0;
    int DriverCount = 0;
    ELogPriority LogPriority = static_cast<ELogPriority>(DEFAULT_LOG_LEVEL);
    bool NoDelays = false;
    bool ExtendedStats = false;
    EDisplayMode DisplayMode = EDisplayMode::None;

    // instead of actual transaction just async sleep and return SUCCESS
    int SimulateTransactionMs = 0;
    int SimulateTransactionSelect1Count = 0;

    std::chrono::duration<long long> DisplayUpdateInterval;

    static constexpr auto SleepMsEveryIterationMainLoop = std::chrono::milliseconds(50);
    static constexpr auto DisplayUpdateTextInterval = std::chrono::seconds(5);
    static constexpr auto DisplayUpdateTuiInterval = std::chrono::seconds(1);
};

void RunSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

} // namespace NYdb::NTPCC
