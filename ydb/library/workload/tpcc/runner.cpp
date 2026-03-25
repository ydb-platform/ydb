#include "runner.h"

#include "constants.h"
#include "log.h"
#include "log_backend.h"
#include "path_checker.h"
#include "runner_display_data.h"
#include "runner_tui.h"
#include "task_queue.h"
#include "terminal.h"
#include "transactions.h"
#include "util.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/log.h>

#include <util/string/cast.h>

#include <atomic>
#include <stop_token>
#include <thread>
#include <vector>
#include <iomanip>
#include <sstream>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr auto GracefulShutdownTimeout = std::chrono::seconds(20);
constexpr auto MinWarmupPerTerminalMs = std::chrono::milliseconds(1);

constexpr auto MaxPerTerminalTransactionsInflight = 1;

const TDuration SaturatedThreadsWindowDuration = TDuration::Minutes(5);

//-----------------------------------------------------------------------------

void InterruptHandler(int) {
    GetGlobalInterruptSource().request_stop();
}

//-----------------------------------------------------------------------------

class TPCCRunner {
public:
    // we suppose that both constructor and destructor are called in a single "main" thread
    TPCCRunner(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);

    TPCCRunner(const TPCCRunner&) = delete;
    TPCCRunner& operator=(const TPCCRunner&) = delete;
    TPCCRunner(TPCCRunner&&) = delete;
    TPCCRunner& operator=(TPCCRunner&&) = delete;

    ~TPCCRunner();

    void RunSync();

private:
    void Join();

    void UpdateDisplayIfNeeded(Clock::time_point now);

    void CollectDataToDisplay(Clock::time_point now);
    void CollectStatistics(TAllStatistics& statistics);
    void CalculateStatusData(Clock::time_point now, TRunDisplayData& data);

    void UpdateDisplayTextMode();

    ftxui::Element BuildTuiLayout();
    void ExitTuiMode();

    void PrintTransactionStatisticsPretty(IOutputStream& os);
    void PrintFinalResultPretty();
    void PrintFinalResultJson();

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;

    // XXX Log instance owns LogBackend (unfortunately, it accepts THolder with LogBackend)
    TLogBackendWithCapture* LogBackend;
    std::shared_ptr<TLog> Log;

    std::vector<TDriver> Drivers;

    std::stop_source TerminalsStopSource;

    std::atomic<bool> StopWarmup{false};

    // 1. Terminals are pinned to own threads.
    // 2. Many terminals are executed on a single thread.
    // 3. Stats are shared between terminals and are aggregation.
    std::vector<std::shared_ptr<TTerminalStats>> PerThreadTerminalStats;
    std::vector<std::unique_ptr<TTerminal>> Terminals;

    std::unique_ptr<ITaskQueue> TaskQueue;

    Clock::time_point WarmupStartTs;
    Clock::time_point WarmupStopDeadline;

    TInstant MeasurementsStartTsWall;
    Clock::time_point MeasurementsStartTs;
    Clock::time_point StopDeadline;

    std::shared_ptr<TRunDisplayData> DataToDisplay;

    std::unique_ptr<TRunnerTui> Tui;

    Clock::time_point SaturationWindowStartTs{};
    size_t SaturationWindowMaxSaturatedThreads = 0;
};

//-----------------------------------------------------------------------------

TPCCRunner::TPCCRunner(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig)
    : ConnectionConfig(connectionConfig)
    , Config(runConfig)
    , LogBackend(new TLogBackendWithCapture("cerr", runConfig.LogPriority, TUI_LOG_LINES))
    , Log(std::make_shared<TLog>(THolder(static_cast<TLogBackend*>(LogBackend))))
{
    ConnectionConfig.IsNetworkIntensive = true;
    ConnectionConfig.UsePerChannelTcpConnection = true;
    ConnectionConfig.UseAllNodes = true;

    const size_t cpuCount = NumberOfMyCpus();

    if (Config.WarehouseCount == 0) {
        std::cerr << "Specified zero warehouses" << std::endl;
        std::exit(1);
    }

    if (Config.SimulateTransactionMs == 0 && Config.SimulateTransactionSelect1Count == 0) {
        CheckPathForRun(connectionConfig, Config.Path, Config.WarehouseCount);
    }

    const size_t terminalsCount = Config.WarehouseCount * TERMINALS_PER_WAREHOUSE;

    // Currently, terminal threads busy wait (for extra efficiency).
    // That is why we want to avoid thread oversubscription.
    //
    // However, note, that we have a very efficient implementation,
    // and don't need many threads to execute terminals.

    // we don't want to share CPU with SDK threads
    const size_t networkThreadCount = ConnectionConfig.GetNetworkThreadNum();
    const size_t maxTerminalThreadCountAvailable = cpuCount > networkThreadCount ? cpuCount - networkThreadCount : 1;

    // even with high number of terminals, this value is usually low
    const size_t recommendedThreadCount =
        (Config.WarehouseCount + WAREHOUSES_PER_CPU_CORE - 1) / WAREHOUSES_PER_CPU_CORE;

    size_t threadCount = 0;
    if (Config.ThreadCount == 0) {
        threadCount = std::min(maxTerminalThreadCountAvailable, terminalsCount);
        threadCount = std::min(threadCount, recommendedThreadCount);

        // in TUI even number of threads looks cute: increase number of threads
        // if we have enough CPU cores
        if (threadCount % 2 != 0 && threadCount < maxTerminalThreadCountAvailable) {
            ++threadCount;
        }
    } else {
        // user provided value: don't give us a chance to break things:
        // with too many threads, we get poor result
        threadCount = Config.ThreadCount;
        if (threadCount > maxTerminalThreadCountAvailable) {
            LOG_I("User provided thread count " << threadCount << " is above max available thread count "
                << maxTerminalThreadCountAvailable << " (cpu count " << cpuCount << ", network threads "
                << networkThreadCount << "). Recommended thread count for " << Config.WarehouseCount
                << " warehouses is " << recommendedThreadCount
                << ". Setting thread count to " << maxTerminalThreadCountAvailable);
            threadCount = maxTerminalThreadCountAvailable;
        }
    }

    if (threadCount < recommendedThreadCount) {
        LOG_W("Thread count " << threadCount << " is lower than recommended " << recommendedThreadCount
            << ". It might affect benchmark results");
    }

    // The number of terminals might be hundreds of thousands.
    // For now, we don't have more than 32 network threads (check TClientCommand::TConfig::GetNetworkThreadNum()),
    // so that maxTerminalThreads will be around more or less around 100.
    const size_t driverCount = Config.DriverCount == 0 ? threadCount : Config.DriverCount;
    Drivers.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        Drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(ConnectionConfig));
    }

    if (Config.MaxInflight == 0) {
        int32_t computeCores = 0;
        std::string reason;
        try {
            computeCores = NumberOfComputeCpus(Drivers[0]);
        } catch (const std::exception& ex) {
            reason = ex.what();
        }

        if (computeCores == 0) {
            std::cerr << "Failed to autodetect max number of sessions";
            if (!reason.empty()) {
                std::cerr << ": " << reason;
            }

            std::cerr << ". Please specify '-m' manually." << std::endl;
            std::exit(1);
        }

        Config.MaxInflight = std::min(terminalsCount, computeCores * SESSIONS_PER_COMPUTE_CORE);
        LOG_I("Set max sessions to " << Config.MaxInflight << ", feel free to manually adjust if needed");
    }

    const size_t maxSessionsPerClient = (Config.MaxInflight + driverCount - 1) / driverCount;
    NQuery::TSessionPoolSettings sessionPoolSettings;
    sessionPoolSettings.MaxActiveSessions(maxSessionsPerClient);
    sessionPoolSettings.MinPoolSize(maxSessionsPerClient);

    NQuery::TClientSettings clientSettings;
    clientSettings.SessionPoolSettings(sessionPoolSettings);

    std::vector<std::shared_ptr<NQuery::TQueryClient>> clients;
    clients.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        clients.emplace_back(std::make_shared<NQuery::TQueryClient>(Drivers[i], clientSettings));
    }

    PerThreadTerminalStats.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        PerThreadTerminalStats.emplace_back(std::make_shared<TTerminalStats>(Config.HighResHistogram));
    }

    const size_t maxTerminalsPerThread = (terminalsCount + threadCount - 1) / threadCount;
    const size_t maxReadyTransactions = maxTerminalsPerThread * MaxPerTerminalTransactionsInflight;

    TaskQueue = CreateTaskQueue(
        threadCount,
        Config.MaxInflight,
        maxTerminalsPerThread,
        maxReadyTransactions,
        Log);

    LOG_I("Creating " << terminalsCount << " terminals and " << threadCount
        << " workers on " << cpuCount << " cpus, " << driverCount << " query clients with at most "
        << maxSessionsPerClient << " sessions per client");

    Terminals.reserve(terminalsCount);
    for (size_t i = 0; i < terminalsCount; ++i) {
        size_t warehouseID = i / TERMINALS_PER_WAREHOUSE + 1;
        auto terminalPtr = std::make_unique<TTerminal>(
            i,
            warehouseID,
            Config.WarehouseCount,
            *TaskQueue,
            clients[i % Drivers.size()],
            Config.Path,
            Config.NoDelays,
            Config.SimulateTransactionMs,
            Config.SimulateTransactionSelect1Count,
            Config.TxMode,
            TerminalsStopSource.get_token(),
            StopWarmup,
            PerThreadTerminalStats[i % threadCount],
            Log);

        Terminals.emplace_back(std::move(terminalPtr));
    }

    // we set handler as late as possible to overwrite
    // any handlers set deeply inside
    signal(SIGINT, InterruptHandler);
    signal(SIGTERM, InterruptHandler);

    // we want to have even load distribution and not start terminals from first INFLIGHT warehouses,
    // then next, etc. Long warmup resolves this, but we want to have a good start even without warmup
    std::random_shuffle(Terminals.begin(), Terminals.end());
}

TPCCRunner::~TPCCRunner() {
    Join();
}

void TPCCRunner::Join() {
    if (TerminalsStopSource.stop_requested()) {
        // already stopped
        return;
    }

    if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
        ExitTuiMode();
    }

    LOG_I("Stopping the terminals...");

    // to gracefully shutdown, we must wait for all termianls
    // and especially running queries to finish
    TerminalsStopSource.request_stop();
    TaskQueue->WakeupAndNeverSleep();

    auto shutdownTs = Clock::now();
    for (const auto& terminal: Terminals) {
        // terminals should wait for their query coroutines to finish
        if (!terminal->IsDone()) {
            LOG_T("Waiting for terminal " << terminal->GetID());
        }
        while (!terminal->IsDone()) {
            std::this_thread::sleep_for(TRunConfig::SleepMsEveryIterationMainLoop);
            auto now = Clock::now();
            auto delta = now - shutdownTs;
            if (delta >= GracefulShutdownTimeout) {
                LOG_W("Graceful shutdown timeout on terminal " << terminal->GetID());
                break;
            }
        }
        auto now = Clock::now();
        auto delta = now - shutdownTs;
        if (delta >= GracefulShutdownTimeout) {
            break;
        }
    }

    LOG_I("Stopping task queue");
    TaskQueue->Join();

    LOG_I("Stopping YDB drivers");
    for (auto& driver: Drivers) {
        driver.Stop(true);
    }

    LOG_I("Runner stopped");
}

void TPCCRunner::RunSync() {
    Config.SetDisplay();

    Clock::time_point now = Clock::now();

    TaskQueue->Run();

    // We don't want to start all terminals at the same time, because then there will be
    // a huge queue of ready terminals, which we can't handle
    bool forcedWarmup = false;
    uint32_t minWarmupSeconds = Terminals.size() * MinWarmupPerTerminalMs.count() / 1000 + 1;

    uint32_t warmupSeconds = minWarmupSeconds;
    if (Config.WarmupDuration == TDuration()) {
        // adaptive, a very simple heuristic
        if (Config.WarehouseCount <= 10) {
            warmupSeconds = 30;
        } else if (Config.WarehouseCount <= 100) {
            warmupSeconds = 5 * 60;
        } else if (Config.WarehouseCount <= 1000) {
            warmupSeconds = 10 * 60;
        } else if (Config.WarehouseCount <= 1000) {
            warmupSeconds = 30 * 60;
        }
        warmupSeconds = std::max(warmupSeconds, minWarmupSeconds);
    } else {
        // user specified
        warmupSeconds = Config.WarmupDuration.Seconds();
        if (warmupSeconds < minWarmupSeconds) {
            forcedWarmup = true; // we must print log message later after display update
            warmupSeconds = minWarmupSeconds;
        }
    }

    WarmupStartTs = Clock::now();
    WarmupStopDeadline = WarmupStartTs + std::chrono::seconds(warmupSeconds);

    // not precise, but quite good: needed for TUI/text status updates to calculated remaining time
    StopDeadline = WarmupStopDeadline + std::chrono::seconds(Config.RunDuration.Seconds());

    CollectDataToDisplay(WarmupStartTs);

    // we want to switch buffers and draw UI ASAP to properly display logs
    // produced after this point and before the first screen update
    if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
        LogBackend->StartCapture(); // start earlier?
        Tui = std::make_unique<TRunnerTui>(Log, *LogBackend, DataToDisplay);
    }

#ifndef NDEBUG
    LOG_W("You're running a CLI binary built without NDEBUG defined, results will be much worse than expected");
#endif

    if (forcedWarmup) {
        LOG_I("Forced minimal warmup time: " << TDuration::Seconds(warmupSeconds));
    }

    LOG_I("Starting warmup for " << TDuration::Seconds(warmupSeconds));

    size_t startedTerminalId = 0;
    for (; startedTerminalId < Terminals.size() && !GetGlobalInterruptSource().stop_requested(); ++startedTerminalId) {
        Terminals[startedTerminalId]->Start();

        std::this_thread::sleep_for(MinWarmupPerTerminalMs);
        now = Clock::now();
        UpdateDisplayIfNeeded(now);
    }
    ++startedTerminalId;

    // start the rest of terminals (if any)
    for (; startedTerminalId < Terminals.size() && !GetGlobalInterruptSource().stop_requested(); ++startedTerminalId) {
        Terminals[startedTerminalId]->Start();
    }

    while (!GetGlobalInterruptSource().stop_requested()) {
        now = Clock::now();
        if (now >= WarmupStopDeadline) {
            break;
        }

        UpdateDisplayIfNeeded(Clock::now());
    }

    StopWarmup.store(true, std::memory_order_relaxed);

    // If warmup was interrupted we don't want to print INFO log about measurements.
    // However, we still initialize all displayed variables on the screen and used in
    // final calculations

    if (!GetGlobalInterruptSource().stop_requested()) {
        LOG_I("Measuring during " << Config.RunDuration);
    }

    MeasurementsStartTs = Clock::now();
    MeasurementsStartTsWall = TInstant::Now();

    // update to be precise, but actually it is not required
    StopDeadline = MeasurementsStartTs + std::chrono::seconds(Config.RunDuration.Seconds());

    // reset statistics
    DataToDisplay = std::make_shared<TRunDisplayData>(
        PerThreadTerminalStats.size(),
        MeasurementsStartTs,
        Config.HighResHistogram);

    while (!GetGlobalInterruptSource().stop_requested()) {
        if (now >= StopDeadline) {
            break;
        }
        std::this_thread::sleep_for(TRunConfig::SleepMsEveryIterationMainLoop);
        now = Clock::now();
        UpdateDisplayIfNeeded(now);
    }

    if (GetGlobalErrorVariable().load()) {
        LOG_D("Stopped by error, joining");
    } else {
        LOG_D("Finished measurements, joining");
    }

    Join();

    if (GetGlobalErrorVariable().load()) {
        ythrow yexception() << "critical error, see the logs";
    }

    switch (Config.Format) {
    case TRunConfig::EFormat::Pretty:
        PrintFinalResultPretty();
        break;
    case TRunConfig::EFormat::Json:
        PrintFinalResultJson();
        break;
    }
}

void TPCCRunner::UpdateDisplayIfNeeded(Clock::time_point now) {
    static bool firstTime = true;
    auto delta = now - DataToDisplay->Statistics.Ts;
    if (!firstTime && delta < Config.DisplayUpdateInterval) {
        return;
    }
    firstTime = false;

    CollectDataToDisplay(now);

    // Maintain tumbling window for saturated threads
    if (SaturationWindowStartTs == Clock::time_point{}) {
        SaturationWindowStartTs = now;
        SaturationWindowMaxSaturatedThreads = 0;
    }

    size_t currentSaturated = DataToDisplay->Statistics.SaturatedThreads;
    if (currentSaturated > SaturationWindowMaxSaturatedThreads) {
        SaturationWindowMaxSaturatedThreads = currentSaturated;
    }

    auto windowElapsedSec = duration_cast<std::chrono::seconds>(now - SaturationWindowStartTs).count();
    if (windowElapsedSec >= static_cast<long long>(SaturatedThreadsWindowDuration.Seconds())) {
        if (SaturationWindowMaxSaturatedThreads > 0) {
            LOG_W("Observed " << SaturationWindowMaxSaturatedThreads << " saturated threads within last "
                << SaturatedThreadsWindowDuration);
        }
        SaturationWindowStartTs = now;
        SaturationWindowMaxSaturatedThreads = 0;
    }

    switch (Config.DisplayMode) {
    case TRunConfig::EDisplayMode::Text:
        UpdateDisplayTextMode();
        break;
    case TRunConfig::EDisplayMode::Tui:
        Tui->Update(DataToDisplay);
        break;
    default:
        ;
    }
}

void TPCCRunner::UpdateDisplayTextMode() {
    TStringStream transactionsSs;
    PrintTransactionStatisticsPretty(transactionsSs);

    std::stringstream ss;
    ss << DataToDisplay->StatusData.ElapsedMinutesTotal << ":"
       << std::setfill('0') << std::setw(2) << DataToDisplay->StatusData.ElapsedSecondsTotal << " elapsed"
       << std::fixed << std::setprecision(1) << " (" << DataToDisplay->StatusData.ProgressPercentTotal << "%, "
       << DataToDisplay->StatusData.RemainingMinutesTotal << ":" << std::setfill('0') << std::setw(2)
       << DataToDisplay->StatusData.RemainingSecondsTotal << " remaining)";

    ss << " | Efficiency: " << std::setprecision(1) << DataToDisplay->StatusData.Efficiency << "% | "
       << "tpmC: " << std::setprecision(1) << DataToDisplay->StatusData.Tpmc;

    LOG_I(ss.str());

    // Per thread statistics (two columns)

    std::stringstream debugSs;

    debugSs << transactionsSs.Str();

    debugSs << "\nPer thread statistics:" << std::endl;

    size_t threadCount = DataToDisplay->Statistics.StatVec.size();
    size_t halfCount = (threadCount + 1) / 2;

    // Headers for both columns
    std::stringstream threadsHeader;
    threadsHeader << std::left
               << std::setw(5) << "Thr"
               << std::setw(5) << "Load"
               << std::setw(10) << "QPS"
               << std::setw(10) << "Queue"
               << std::setw(15) << "queue p90, ms";

    // Print headers side by side
    debugSs << threadsHeader.str() << " | " << threadsHeader.str() << std::endl;

    size_t totalWidth = threadsHeader.str().length() * 2 + 3;
    debugSs << std::string(totalWidth, '-') << std::endl;

    // Print thread data in two columns
    for (size_t i = 0; i < halfCount; ++i) {
        // Left column
        std::stringstream leftLine;
        if (i < threadCount) {
            const auto& stats = DataToDisplay->Statistics.StatVec[i];
            double load = stats.Load;
            leftLine << std::left
                     << std::setw(5) << (i + 1)
                     << std::setw(5) << std::fixed << std::setprecision(2) << load
                     << std::setw(10) << std::fixed << stats.QueriesPerSecond
                     << std::setw(10) << stats.TaskThreadStats->InternalTasksWaitingInflight
                     << std::setw(15) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);
        } else {
            leftLine << std::string(threadsHeader.str().length(), ' ');
        }

        // Right column
        std::stringstream rightLine;
        size_t rightIndex = i + halfCount;
        if (rightIndex < threadCount) {
            const auto& stats = DataToDisplay->Statistics.StatVec[rightIndex];
            double load = stats.Load;
            rightLine << std::left
                      << std::setw(5) << (rightIndex + 1)
                      << std::setw(5) << std::fixed << std::setprecision(2) << load
                      << std::setw(10) << std::fixed << stats.QueriesPerSecond
                      << std::setw(10) << stats.TaskThreadStats->InternalTasksWaitingInflight
                      << std::setw(15) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);
        } else {
            rightLine << std::string(threadsHeader.str().length(), ' ');
        }

        debugSs << leftLine.str() << " | " << rightLine.str() << std::endl;
    }
    debugSs << std::string(totalWidth, '-') << std::endl;

    // Transaction statistics
    debugSs << "\n";

    LOG_D(debugSs.str());
}

void TPCCRunner::CollectDataToDisplay(Clock::time_point now) {
    auto newDisplayData = std::make_shared<TRunDisplayData>(PerThreadTerminalStats.size(), now, Config.HighResHistogram);
    newDisplayData->WarehouseCount = Config.WarehouseCount;

    // order makes sense here
    CollectStatistics(newDisplayData->Statistics);
    CalculateStatusData(now, *newDisplayData);

    DataToDisplay.swap(newDisplayData);
}

void TPCCRunner::CollectStatistics(TAllStatistics& statistics) {
    for (size_t i = 0; i < PerThreadTerminalStats.size(); ++i) {
        PerThreadTerminalStats[i]->Collect(*statistics.StatVec[i].TerminalStats);
        TaskQueue->CollectStats(i, *statistics.StatVec[i].TaskThreadStats);
    }

    if (DataToDisplay) {
        // DataToDisplay contains current Statistics, while statistics is the new one
        statistics.CalculateDerivativeAndTotal(DataToDisplay->Statistics);
    }
}

void TPCCRunner::CalculateStatusData(Clock::time_point now, TRunDisplayData& data) {
    // calculate time and estimation
    Clock::time_point currentPhaseStartTs;

    std::chrono::duration<long long> warmupDuration;
    std::chrono::duration<long long> totalDuration;

    totalDuration = std::chrono::duration_cast<std::chrono::seconds>(StopDeadline - WarmupStartTs);
    warmupDuration = std::chrono::duration_cast<std::chrono::seconds>(WarmupStopDeadline - WarmupStartTs);

    auto elapsedTotal = std::chrono::duration_cast<std::chrono::seconds>(now - WarmupStartTs);
    auto remainingTotal = std::chrono::duration_cast<std::chrono::seconds>(StopDeadline - now);

    data.StatusData.ElapsedMinutesTotal = static_cast<int>(elapsedTotal.count() / 60);
    data.StatusData.ElapsedSecondsTotal = static_cast<int>(elapsedTotal.count() % 60);
    data.StatusData.RemainingMinutesTotal = std::max(0LL, remainingTotal.count() / 60);
    data.StatusData.RemainingSecondsTotal = std::max(0LL, remainingTotal.count() % 60);

    if (totalDuration.count() != 0) {
        data.StatusData.ProgressPercentTotal = (static_cast<double>(elapsedTotal.count()) / totalDuration.count()) * 100.0;
    }

    if (totalDuration.count() > 0) {
        data.StatusData.WarmupPercent = (static_cast<double>(warmupDuration.count()) / totalDuration.count()) * 100.0;
    }

    // Phase-specific calculations (for display)
    if (MeasurementsStartTs == Clock::time_point{}) {
        data.StatusData.Phase = "Warming up";
        currentPhaseStartTs = WarmupStartTs;
    } else {
        data.StatusData.Phase = "Measuring";
        currentPhaseStartTs = MeasurementsStartTs;
    }

    auto currentPhaseElapsed = std::chrono::duration_cast<std::chrono::seconds>(now - currentPhaseStartTs);

    // Calculate tpmC and efficiency based on currentPhaseElapsed

    double maxPossibleTpmc = 0;
    data.StatusData.Efficiency = 0;
    if (currentPhaseElapsed.count() == 0) {
        data.StatusData.Tpmc = 0;
    } else {
        // approximate
        const auto& newOrderStats = data.Statistics.TotalTerminalStats.GetStats(ETransactionType::NewOrder);
        auto newOrdersCount = newOrderStats.OK.load(std::memory_order_relaxed);
        double tpmc = newOrdersCount * 60 / currentPhaseElapsed.count();

        // there are two errors: rounding + approximation, we might overshoot
        // 100% efficiency very slightly because of errors and it's OK to "round down"
        maxPossibleTpmc = Config.WarehouseCount * MAX_TPMC_PER_WAREHOUSE;
        data.StatusData.Tpmc = std::min(maxPossibleTpmc, tpmc);
    }

    if (maxPossibleTpmc != 0) {
        data.StatusData.Efficiency = (data.StatusData.Tpmc * 100.0) / maxPossibleTpmc;
        // avoid slight rounding errors
        data.StatusData.Efficiency = std::min(data.StatusData.Efficiency, 100.0);
    }

    // Get running counts
    data.StatusData.RunningTerminals = TaskQueue->GetRunningCount();
    data.StatusData.RunningTransactions = TransactionsInflight.load(std::memory_order_relaxed);
}

void TPCCRunner::ExitTuiMode() {
    Tui.reset();
    LogBackend->StopCaptureAndFlush(Cerr);
}

void TPCCRunner::PrintTransactionStatisticsPretty(IOutputStream& os) {
    size_t totalOK = 0;
    size_t totalFailed = 0;
    size_t totalUserAborted = 0;

    TVector<TString> columnNames = {"Transaction", "OK", "Failed"};
    if (Config.ExtendedStats) {
        columnNames.emplace_back("UserAborted");
    }
    columnNames.emplace_back("p50, ms");
    columnNames.emplace_back("p90, ms");
    columnNames.emplace_back("p99, ms");

    NConsoleClient::TPrettyTable table(columnNames);

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        auto type = static_cast<ETransactionType>(i);
        auto typeStr = ToString(type);
        const auto& stats = DataToDisplay->Statistics.TotalTerminalStats.GetStats(type);
        auto ok = stats.OK.load(std::memory_order_relaxed);
        auto failed = stats.Failed.load(std::memory_order_relaxed);
        auto aborted = stats.UserAborted.load(std::memory_order_relaxed);

        totalOK += ok;
        totalFailed += failed;
        totalUserAborted += aborted;

        auto& row = table.AddRow();
        size_t columnIndex = 0;
        row.Column(columnIndex++, typeStr);
        row.Column(columnIndex++, ToString(ok));
        row.Column(columnIndex++, ToString(failed));
        if (Config.ExtendedStats) {
            row.Column(columnIndex++, ToString(aborted));
        }
        row.Column(columnIndex++, ToString(stats.LatencyHistogramFullMs.GetValueAtPercentile(50)));
        row.Column(columnIndex++, ToString(stats.LatencyHistogramFullMs.GetValueAtPercentile(90)));
        row.Column(columnIndex++, ToString(stats.LatencyHistogramFullMs.GetValueAtPercentile(99)));
    }

    auto& row = table.AddRow();
    size_t columnIndex = 0;
    row.Column(columnIndex++, "TOTAL");
    row.Column(columnIndex++, ToString(totalOK));
    row.Column(columnIndex++, ToString(totalFailed));
    if (Config.ExtendedStats) {
        row.Column(columnIndex++, ToString(totalUserAborted));
    }

    table.Print(os);
    os << "\n";
}

void TPCCRunner::PrintFinalResultPretty() {
    if (MeasurementsStartTs == Clock::time_point{}) {
        Cout << "Stopped before measurements" << Endl;
        return;
    }

    auto now = Clock::now();
    double secondsPassed = duration_cast<std::chrono::duration<double>>(now - MeasurementsStartTs).count();
    auto minutesPassed = secondsPassed / 60;

    CollectDataToDisplay(now);

    Cout << "\n\n";
    PrintTransactionStatisticsPretty(Cout);

    if (minutesPassed >= 1) {
        std::cout << "warehouses: " << Config.WarehouseCount << std::endl;
        std::cout << "tpmC: " << DataToDisplay->StatusData.Tpmc << "*" << std::endl;
        std::cout << "efficiency: " << std::fixed << std::setprecision(2) << DataToDisplay->StatusData.Efficiency << "%" << std::endl;
        std::cout << "* These results are not officially recognized TPC results "
                  << "and are not comparable with other TPC-C test results published on the TPC website" << std::endl;
    } else {
        std::cout << "Less than minute passed, tpmC calculation skipped" << std::endl;
    }
}

void TPCCRunner::PrintFinalResultJson() {
    if (MeasurementsStartTs == Clock::time_point{}) {
        Cout << "{\"error\": \"Stopped before measurements\"}" << Endl;
        return;
    }

    auto now = Clock::now();
    double secondsPassed = duration_cast<std::chrono::duration<double>>(now - MeasurementsStartTs).count();

    CollectDataToDisplay(now);

    const auto& newOrderStats = DataToDisplay->Statistics.TotalTerminalStats.GetStats(ETransactionType::NewOrder);
    auto newOrdersCount = newOrderStats.OK.load(std::memory_order_relaxed);

    NJson::TJsonValue root;
    root.SetType(NJson::JSON_MAP);

    // Summary section

    NJson::TJsonValue summary;
    summary.SetType(NJson::JSON_MAP);
    summary.InsertValue("name", "Total");
    summary.InsertValue("time_seconds", static_cast<long long>(secondsPassed));
    summary.InsertValue("measure_start_ts", MeasurementsStartTsWall.Seconds());
    summary.InsertValue("warehouses", static_cast<long long>(Config.WarehouseCount));
    summary.InsertValue("new_orders", static_cast<long long>(newOrdersCount));
    summary.InsertValue("tpmc", DataToDisplay->StatusData.Tpmc);
    summary.InsertValue("efficiency", DataToDisplay->StatusData.Efficiency);

    root.InsertValue("summary", std::move(summary));

    // Transactions section

    NJson::TJsonValue transactions;
    transactions.SetType(NJson::JSON_MAP);

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        auto type = static_cast<ETransactionType>(i);
        auto typeStr = ToString(type);
        const auto& stats = DataToDisplay->Statistics.TotalTerminalStats.GetStats(type);
        auto ok = stats.OK.load(std::memory_order_relaxed);
        auto failed = stats.Failed.load(std::memory_order_relaxed);

        NJson::TJsonValue txData;
        txData.SetType(NJson::JSON_MAP);
        txData.InsertValue("ok_count", static_cast<long long>(ok));
        txData.InsertValue("failed_count", static_cast<long long>(failed));

        NJson::TJsonValue percentiles;
        percentiles.SetType(NJson::JSON_MAP);
        percentiles.InsertValue("50", stats.LatencyHistogramFullMs.GetValueAtPercentile(50));
        percentiles.InsertValue("90", stats.LatencyHistogramFullMs.GetValueAtPercentile(90));
        percentiles.InsertValue("95", stats.LatencyHistogramFullMs.GetValueAtPercentile(95));
        percentiles.InsertValue("99", stats.LatencyHistogramFullMs.GetValueAtPercentile(99));
        percentiles.InsertValue("99.9", stats.LatencyHistogramFullMs.GetValueAtPercentile(99.9));

        txData.InsertValue("percentiles", std::move(percentiles));
        transactions.InsertValue(typeStr, std::move(txData));
    }

    root.InsertValue("transactions", std::move(transactions));

    NJson::WriteJson(&Cout, &root, false, false, false);
}

} // anonymous

//-----------------------------------------------------------------------------

void TRunConfig::SetDisplay() {
    if (NoTui) {
        DisplayMode = EDisplayMode::Text;
    } else {
        if (NConsoleClient::IsStdoutInteractive()) {
            DisplayMode = EDisplayMode::Tui;
        } else {
            DisplayMode = EDisplayMode::Text;
        }
    }

    switch (DisplayMode) {
    case EDisplayMode::None:
        return;
    case EDisplayMode::Text:
        DisplayUpdateInterval = DisplayUpdateTextInterval;
        return;
    case EDisplayMode::Tui:
        DisplayUpdateInterval = DisplayUpdateTuiInterval;
        return;
    }
}

//-----------------------------------------------------------------------------

void RunSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    try {
        TPCCRunner runner(connectionConfig, runConfig);
        runner.RunSync();
    } catch (const std::exception& ex) {
        std::cerr << "Exception while execution: " << ex.what() << std::endl;
        throw NConsoleClient::TNeedToExitWithCode(EXIT_FAILURE);
    }
}

} // namespace NYdb::NTPCC
