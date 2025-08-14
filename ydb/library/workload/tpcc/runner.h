#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <library/cpp/logger/priority.h>

#include <util/datetime/base.h>

namespace NYdb::NTPCC {

constexpr int DEFAULT_WAREHOUSE_COUNT = 10;
constexpr TDuration DEFAULT_RUN_DURATION = TDuration::Minutes(120);

constexpr int DEFAULT_MAX_SESSIONS = 100; // TODO

constexpr int DEFAULT_THREAD_COUNT = 0; // autodetect based on WAREHOUSES_PER_CPU_CORE
constexpr int DEFAULT_LOAD_THREAD_COUNT = 10; // TODO: autodetect

constexpr ELogPriority DEFAULT_LOG_LEVEL = TLOG_INFO;

struct TRunConfig {
    enum class EDisplayMode {
        None = 0,
        Text,
        Tui,
    };

    enum class EFormat {
        Pretty = 0,
        Json,
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

    void SetDisplay();

    int WarehouseCount = DEFAULT_WAREHOUSE_COUNT;
    TDuration WarmupDuration = {};
    TDuration RunDuration = DEFAULT_RUN_DURATION;

    int MaxInflight = DEFAULT_MAX_SESSIONS;

    TString Path;

    EFormat Format = EFormat::Pretty;

    TString JsonResultPath;

    // advanced settings (normally, used by developer only)

    int ThreadCount = DEFAULT_THREAD_COUNT;
    int LoadThreadCount = DEFAULT_LOAD_THREAD_COUNT;
    int DriverCount = 0;
    ELogPriority LogPriority = static_cast<ELogPriority>(DEFAULT_LOG_LEVEL);
    bool NoDelays = false;
    bool ExtendedStats = false;
    bool NoTui = false;
    EDisplayMode DisplayMode = EDisplayMode::None;

    // instead of actual transaction just async sleep and return SUCCESS
    int SimulateTransactionMs = 0;
    int SimulateTransactionSelect1Count = 0;

    std::chrono::duration<long long> DisplayUpdateInterval;

    // used by check command only
    bool JustImported = false;

    static constexpr auto SleepMsEveryIterationMainLoop = std::chrono::milliseconds(50);
    static constexpr auto DisplayUpdateTextInterval = std::chrono::seconds(5);
    static constexpr auto DisplayUpdateTuiInterval = std::chrono::seconds(1);
};

void RunSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

} // namespace NYdb::NTPCC
