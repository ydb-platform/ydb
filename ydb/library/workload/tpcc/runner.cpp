#include "runner.h"

#include "constants.h"
#include "log.h"
#include "stderr_capture.h"
#include "task_queue.h"
#include "terminal.h"
#include "transactions.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

#include <util/string/cast.h>
#include <util/system/info.h>

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <array>
#include <atomic>
#include <stop_token>
#include <thread>
#include <vector>
#include <iomanip>
#include <sstream>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr auto GracefulShutdownTimeout = std::chrono::seconds(5);
constexpr auto MinWarmupPerTerminal = std::chrono::milliseconds(1);

constexpr auto MaxPerTerminalTransactionsInflight = 1;

//-----------------------------------------------------------------------------

struct TAllStatistics {
    struct TThreadStatistics {
        struct TStatsDerivative {
            double OkPerSecond = 0;
            double FailedPerSecond = 0;
            double UserAbortedPerSecond = 0;
            THistogram LatencyHistogramMs{256, 32768};
            THistogram LatencyHistogramFullMs{256, 32768};
            THistogram LatencyHistogramPure{256, 32768};
        };

        TThreadStatistics() {
            TaskThreadStats = std::make_unique<ITaskQueue::TThreadStats>();
            TerminalStats = std::make_unique<TTerminalStats>();
        }

        void CalculateDerivative(const TThreadStatistics& prev, size_t seconds) {
            TerminalsPerSecond =
                (TaskThreadStats->InternalTasksResumed.load(std::memory_order_relaxed) -
                    prev.TaskThreadStats->InternalTasksResumed.load(std::memory_order_relaxed)) / seconds;

            QueriesPerSecond =
                (TaskThreadStats->ExternalTasksResumed.load(std::memory_order_relaxed) -
                    prev.TaskThreadStats->ExternalTasksResumed.load(std::memory_order_relaxed)) / seconds;

            // Calculate derivatives for all transaction types
            for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
                auto type = static_cast<ETransactionType>(i);
                const auto& currentStats = TerminalStats->GetStats(type);
                const auto& prevStats = prev.TerminalStats->GetStats(type);

                Stats[i].OkPerSecond = 1.0 *
                    (currentStats.OK.load(std::memory_order_relaxed) -
                        prevStats.OK.load(std::memory_order_relaxed)) / seconds;

                Stats[i].FailedPerSecond = 1.0 *
                    (currentStats.Failed.load(std::memory_order_relaxed) -
                        prevStats.Failed.load(std::memory_order_relaxed)) / seconds;

                Stats[i].UserAbortedPerSecond = 1.0 *
                    (currentStats.UserAborted.load(std::memory_order_relaxed) -
                        prevStats.UserAborted.load(std::memory_order_relaxed)) / seconds;

                // Calculate histogram deltas directly
                Stats[i].LatencyHistogramMs = currentStats.LatencyHistogramMs;
                Stats[i].LatencyHistogramMs.Sub(prevStats.LatencyHistogramMs);

                Stats[i].LatencyHistogramFullMs = currentStats.LatencyHistogramFullMs;
                Stats[i].LatencyHistogramFullMs.Sub(prevStats.LatencyHistogramFullMs);

                Stats[i].LatencyHistogramPure = currentStats.LatencyHistogramPure;
                Stats[i].LatencyHistogramPure.Sub(prevStats.LatencyHistogramPure);
            }

            ExecutingTime = TaskThreadStats->ExecutingTime.load(std::memory_order_relaxed) -
                prev.TaskThreadStats->ExecutingTime.load(std::memory_order_relaxed);

            TotalTime = TaskThreadStats->TotalTime.load(std::memory_order_relaxed) -
                prev.TaskThreadStats->TotalTime.load(std::memory_order_relaxed);

            InternalInflightWaitTimeMs = TaskThreadStats->InternalInflightWaitTimeMs;
            InternalInflightWaitTimeMs.Sub(prev.TaskThreadStats->InternalInflightWaitTimeMs);

            ExternalQueueTimeMs = TaskThreadStats->ExternalQueueTimeMs;
            ExternalQueueTimeMs.Sub(prev.TaskThreadStats->ExternalQueueTimeMs);
        }

        std::unique_ptr<ITaskQueue::TThreadStats> TaskThreadStats;
        std::unique_ptr<TTerminalStats> TerminalStats;

        size_t TerminalsPerSecond = 0;
        size_t QueriesPerSecond = 0;
        std::array<TStatsDerivative, GetEnumItemsCount<ETransactionType>()> Stats;
        double ExecutingTime = 0;
        double TotalTime = 0;
        THistogram InternalInflightWaitTimeMs{ITaskQueue::TThreadStats::BUCKET_COUNT, ITaskQueue::TThreadStats::MAX_HIST_VALUE};
        THistogram ExternalQueueTimeMs{ITaskQueue::TThreadStats::BUCKET_COUNT, ITaskQueue::TThreadStats::MAX_HIST_VALUE};
    };

    TAllStatistics(size_t threadCount)
        : StatVec(threadCount)
    {
    }

    void CalculateDerivative(const TAllStatistics& prev) {
        size_t seconds = duration_cast<std::chrono::duration<size_t>>(Ts - prev.Ts).count();

        // Calculate per-thread derivatives
        for (size_t i = 0; i < StatVec.size(); ++i) {
            StatVec[i].CalculateDerivative(prev.StatVec[i], seconds);
        }

        // Aggregate global statistics
        for (size_t txType = 0; txType < GetEnumItemsCount<ETransactionType>(); ++txType) {
            GlobalStats[txType].OkPerSecond = 0;
            GlobalStats[txType].FailedPerSecond = 0;
            GlobalStats[txType].UserAbortedPerSecond = 0;
            GlobalStats[txType].LatencyHistogramMs.Reset();
            GlobalStats[txType].LatencyHistogramFullMs.Reset();
            GlobalStats[txType].LatencyHistogramPure.Reset();

            for (size_t i = 0; i < StatVec.size(); ++i) {
                GlobalStats[txType].OkPerSecond += StatVec[i].Stats[txType].OkPerSecond;
                GlobalStats[txType].FailedPerSecond += StatVec[i].Stats[txType].FailedPerSecond;
                GlobalStats[txType].UserAbortedPerSecond += StatVec[i].Stats[txType].UserAbortedPerSecond;
                GlobalStats[txType].LatencyHistogramMs.Add(StatVec[i].Stats[txType].LatencyHistogramMs);
                GlobalStats[txType].LatencyHistogramFullMs.Add(StatVec[i].Stats[txType].LatencyHistogramFullMs);
                GlobalStats[txType].LatencyHistogramPure.Add(StatVec[i].Stats[txType].LatencyHistogramPure);
            }
        }

        // Calculate tpmC based on New Order transactions
        Tpmc = GlobalStats[static_cast<size_t>(ETransactionType::NewOrder)].OkPerSecond * 60.0;
    }

    Clock::time_point Ts;
    TVector<TThreadStatistics> StatVec;
    std::array<TThreadStatistics::TStatsDerivative, GetEnumItemsCount<ETransactionType>()> GlobalStats;
    double Tpmc = 0;
};

//-----------------------------------------------------------------------------

void InterruptHandler(int) {
    GetGlobalInterruptSource().request_stop();
}

//-----------------------------------------------------------------------------

class TPCCRunner {
public:
    struct TCalculatedStatusData {
        // Phase and timing (computed values only)
        std::string Phase;
        int ElapsedMinutes = 0;
        int ElapsedSeconds = 0;
        int RemainingMinutes = 0;
        int RemainingSeconds = 0;
        double ProgressPercent = 0.0;

        // Computed metrics not in LastStatisticsSnapshot
        double Efficiency = 0.0;
        size_t RunningTerminals = 0;
        size_t RunningTransactions = 0;
    };

    // we suppose that both constructor and destructor are called in a single "main" thread
    TPCCRunner(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);
    ~TPCCRunner();

    void RunSync();

private:
    void Join();

    void UpdateDisplayIfNeeded(Clock::time_point now);
    std::unique_ptr<TAllStatistics> CollectStatistics(Clock::time_point now);

    void UpdateDisplayTextMode(const TCalculatedStatusData& data);
    void UpdateDisplayTuiMode(const TCalculatedStatusData& data);
    TCalculatedStatusData CalculateStatusData(Clock::time_point now);
    void ExitTuiMode();
    void DumpFinalStats();

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;
    std::shared_ptr<TLog> Log;

    std::vector<TDriver> Drivers;

    std::stop_source TerminalsStopSource;

    std::atomic<bool> StopWarmup{false};
    std::vector<std::shared_ptr<TTerminalStats>> StatsVec;
    std::vector<std::unique_ptr<TTerminal>> Terminals;

    std::unique_ptr<ITaskQueue> TaskQueue;

    Clock::time_point WarmupStartTs;
    Clock::time_point WarmupStopDeadline;

    Clock::time_point MeasurementsStartTs;
    Clock::time_point StopDeadline;

    std::unique_ptr<TAllStatistics> LastStatisticsSnapshot;
    std::unique_ptr<TStdErrCapture> LogCapture;
};

//-----------------------------------------------------------------------------

TPCCRunner::TPCCRunner(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig)
    : ConnectionConfig(connectionConfig)
    , Config(runConfig)
    , Log(std::make_shared<TLog>(CreateLogBackend("cerr", Config.LogPriority, true)))
    , LogCapture(Config.DisplayMode == TRunConfig::EDisplayMode::Tui ?
                 std::make_unique<TStdErrCapture>(TUI_LOG_LINES) : nullptr)
{
    ConnectionConfig.IsNetworkIntensive = true;
    ConnectionConfig.UsePerChannelTcpConnection = true;

    const size_t cpuCount = NSystemInfo::CachedNumberOfCpus();
    if (cpuCount == 0) {
        // dump sanity check
        std::cerr << "No CPUs" << std::endl;
        std::exit(1);
    }

    const size_t networkThreadCount = NConsoleClient::TYdbCommand::GetNetworkThreadNum(ConnectionConfig);
    const size_t maxTerminalThreadCount = cpuCount > networkThreadCount ? cpuCount - networkThreadCount : 1;

    const size_t terminalsCount = Config.WarehouseCount * TERMINALS_PER_WAREHOUSE;

    // we might consider using less than maxTerminalThreads
    const size_t threadCount = Config.ThreadCount == 0 ?
        std::min(maxTerminalThreadCount, terminalsCount) : Config.ThreadCount;

    // The number of terminals might be hundreds of thousands.
    // For now, we don't have more than 32 network threads (check TYdbCommand::GetNetworkThreadNum()),
    // so that maxTerminalThreads will be around more or less around 100.
    const size_t driverCount = Config.DriverCount == 0 ? threadCount : Config.DriverCount;

    const size_t maxSessionsPerClient = (Config.MaxInflight + driverCount - 1) / driverCount;
    NQuery::TSessionPoolSettings sessionPoolSettings;
    sessionPoolSettings.MaxActiveSessions(maxSessionsPerClient);
    sessionPoolSettings.MinPoolSize(maxSessionsPerClient);

    NQuery::TClientSettings clientSettings;
    clientSettings.SessionPoolSettings(sessionPoolSettings);

    Drivers.reserve(driverCount);
    std::vector<std::shared_ptr<NQuery::TQueryClient>> clients;
    clients.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        auto& driver = Drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(ConnectionConfig));
        clients.emplace_back(std::make_shared<NQuery::TQueryClient>(driver, clientSettings));
    }

    StatsVec.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        StatsVec.emplace_back(std::make_shared<TTerminalStats>());
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
            TerminalsStopSource.get_token(),
            StopWarmup,
            StatsVec[i % threadCount],
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
                LOG_E("Graceful shutdown timeout on terminal " << terminal->GetID());
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
    Config.SetDisplayUpdateInterval();

    Clock::time_point now = Clock::now();

    TaskQueue->Run();

    LastStatisticsSnapshot = std::make_unique<TAllStatistics>(StatsVec.size());
    LastStatisticsSnapshot->Ts = {}; // to force display update

    LastStatisticsSnapshot = std::make_unique<TAllStatistics>(StatsVec.size());
    LastStatisticsSnapshot->Ts = {}; // to force display update

    // We don't want to start all terminals at the same time, because then there will be
    // a huge queue of ready terminals, which we can't handle
    bool forcedWarmup = false;
    int minWarmupSeconds = Terminals.size() * MinWarmupPerTerminal.count() / 1000 + 1;
    int minWarmupMinutes = (minWarmupSeconds + 59) / 60;
    int warmupMinutes;
    if (Config.WarmupMinutes < minWarmupMinutes) {
        forcedWarmup = true; // we must print log message later after display update
        warmupMinutes = minWarmupMinutes;
    } else {
        warmupMinutes = Config.WarmupMinutes;
    }

    WarmupStartTs = Clock::now();
    WarmupStopDeadline = WarmupStartTs + std::chrono::minutes(warmupMinutes);

    // we want to switch buffers and draw UI ASAP to properly display logs
    // produced after this point and before the first screen update
    if (Config.DisplayMode == TRunConfig::EDisplayMode::Tui) {
        UpdateDisplayIfNeeded(Clock::now());
    }

    if (forcedWarmup) {
        LOG_I("Forced minimal warmup time: " << minWarmupMinutes << " minutes");
    }

    LOG_I("Starting warmup for " << warmupMinutes << " minutes");

    size_t startedTerminalId = 0;
    for (; startedTerminalId < Terminals.size() && !GetGlobalInterruptSource().stop_requested(); ++startedTerminalId) {
        if (now >= WarmupStopDeadline) {
            break;
        }
        Terminals[startedTerminalId]->Start();

        std::this_thread::sleep_for(MinWarmupPerTerminal);
        now = Clock::now();
        UpdateDisplayIfNeeded(now);
    }
    ++startedTerminalId;

    // start the rest of terminals (if any)
    for (; startedTerminalId < Terminals.size() && !GetGlobalInterruptSource().stop_requested(); ++startedTerminalId) {
        Terminals[startedTerminalId]->Start();
    }

    // in case we were starting the rest of terminals for too long (doubtfully)
    UpdateDisplayIfNeeded(Clock::now());

    StopWarmup.store(true, std::memory_order_relaxed);

    LOG_I("Measuring during " << Config.RunMinutes << " minutes");

    MeasurementsStartTs = Clock::now();

    // reset statistics
    LastStatisticsSnapshot = std::make_unique<TAllStatistics>(StatsVec.size());
    LastStatisticsSnapshot->Ts = MeasurementsStartTs;

    StopDeadline = MeasurementsStartTs + std::chrono::minutes(Config.RunMinutes);
    while (!GetGlobalInterruptSource().stop_requested()) {
        if (now >= StopDeadline) {
            break;
        }
        std::this_thread::sleep_for(TRunConfig::SleepMsEveryIterationMainLoop);
        now = Clock::now();
        UpdateDisplayIfNeeded(now);
    }

    LOG_D("Finished measurements");
    Join();

    DumpFinalStats();
}

void TPCCRunner::UpdateDisplayIfNeeded(Clock::time_point now) {
    auto delta = now - LastStatisticsSnapshot->Ts;
    if (delta < Config.DisplayUpdateInterval) {
        return;
    }
    std::unique_ptr<TAllStatistics> newStatistics = CollectStatistics(now);
    LastStatisticsSnapshot = std::move(newStatistics);

    TCalculatedStatusData data = CalculateStatusData(now);

    switch (Config.DisplayMode) {
    case TRunConfig::EDisplayMode::Text:
        UpdateDisplayTextMode(data);
        break;
    case TRunConfig::EDisplayMode::Tui:
        UpdateDisplayTuiMode(data);
        break;
    default:
        ;
    }
}

void TPCCRunner::UpdateDisplayTextMode(const TCalculatedStatusData& data) {
    std::stringstream ss;
    ss << "\n\n\n" << data.Phase << " - " << data.ElapsedMinutes << ":"
       << std::setfill('0') << std::setw(2) << data.ElapsedSeconds << " elapsed";

    if (data.ProgressPercent > 0) {
        ss << std::fixed << std::setprecision(1) << " (" << data.ProgressPercent << "%, "
           << data.RemainingMinutes << ":" << std::setfill('0') << std::setw(2)
           << data.RemainingSeconds << " remaining)";
    }

    ss << std::endl << "Efficiency: " << std::setprecision(1) << data.Efficiency << "% | "
       << "tpmC: " << std::setprecision(1) << LastStatisticsSnapshot->Tpmc << " | "
       << "Running: " << data.RunningTransactions << " transactions";

    std::cout << ss.str();

    // Per thread statistics (two columns)
    std::cout << "\nPer thread statistics:" << std::endl;

    size_t threadCount = LastStatisticsSnapshot->StatVec.size();
    size_t halfCount = (threadCount + 1) / 2;

    // Headers for both columns
    std::stringstream leftHeader, rightHeader;
    leftHeader << std::left
               << std::setw(5) << "Thr"
               << std::setw(5) << "Load"
               << std::setw(10) << "QPS"
               << std::setw(10) << "Queue"
               << std::setw(15) << "queue p90, ms";

    rightHeader << std::left
                << std::setw(5) << "Thr"
                << std::setw(5) << "Load"
                << std::setw(10) << "QPS"
                << std::setw(10) << "Queue"
                << std::setw(15) << "queue p90, ms";

    // Print headers side by side
    std::cout << leftHeader.str() << " | " << rightHeader.str() << std::endl;

    size_t totalWidth = leftHeader.str().length() + 3 + rightHeader.str().length();
    std::cout << std::string(totalWidth, '-') << std::endl;

    // Print thread data in two columns
    for (size_t i = 0; i < halfCount; ++i) {
        // Left column
        std::stringstream leftLine;
        if (i < threadCount) {
            const auto& stats = LastStatisticsSnapshot->StatVec[i];
            double load = stats.ExecutingTime / stats.TotalTime;
            leftLine << std::left
                     << std::setw(5) << i
                     << std::setw(5) << std::fixed << std::setprecision(2) << load
                     << std::setw(10) << std::fixed << stats.QueriesPerSecond
                     << std::setw(10) << stats.TaskThreadStats->InternalTasksWaitingInflight
                     << std::setw(15) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);
        } else {
            leftLine << std::string(leftHeader.str().length(), ' ');
        }

        // Right column
        std::stringstream rightLine;
        size_t rightIndex = i + halfCount;
        if (rightIndex < threadCount) {
            const auto& stats = LastStatisticsSnapshot->StatVec[rightIndex];
            double load = stats.ExecutingTime / stats.TotalTime;
            rightLine << std::left
                      << std::setw(5) << rightIndex
                      << std::setw(5) << std::fixed << std::setprecision(2) << load
                      << std::setw(10) << std::fixed << stats.QueriesPerSecond
                      << std::setw(10) << stats.TaskThreadStats->InternalTasksWaitingInflight
                      << std::setw(15) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);
        } else {
            rightLine << std::string(rightHeader.str().length(), ' ');
        }

        std::cout << leftLine.str() << " | " << rightLine.str() << std::endl;
    }
    std::cout << std::string(totalWidth, '-') << std::endl;

    // Transaction statistics
    std::cout << "\n\nTransaction Statistics:\n";

    // Build header using stringstream to calculate width
    std::stringstream txHeaderStream;
    txHeaderStream << std::left
                   << std::setw(15) << "Transaction"
                   << std::setw(12) << "OK/s"
                   << std::setw(12) << "Fail/s"
                   << std::setw(12) << "p50 (ms)"
                   << std::setw(12) << "p90 (ms)"
                   << std::setw(12) << "p99 (ms)";

    std::string txHeader = txHeaderStream.str();
    size_t txTableWidth = txHeader.length();

    std::cout << std::string(txTableWidth, '-') << std::endl;
    std::cout << txHeader << std::endl;
    std::cout << std::string(txTableWidth, '-') << std::endl;

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        const auto& stats = LastStatisticsSnapshot->GlobalStats[i];
        std::cout << std::left
                  << std::setw(15) << std::string(ToString(static_cast<ETransactionType>(i)))
                  << std::setw(12) << std::fixed << std::setprecision(2) << stats.OkPerSecond
                  << std::setw(12) << std::fixed << std::setprecision(2) << stats.FailedPerSecond
                  << std::setw(12) << std::fixed << std::setprecision(1) << stats.LatencyHistogramFullMs.GetValueAtPercentile(50)
                  << std::setw(12) << std::fixed << std::setprecision(1) << stats.LatencyHistogramFullMs.GetValueAtPercentile(90)
                  << std::setw(12) << std::fixed << std::setprecision(1) << stats.LatencyHistogramFullMs.GetValueAtPercentile(99)
                  << std::endl;
    }
    std::cout << std::string(txTableWidth, '-') << std::endl;
}

void TPCCRunner::UpdateDisplayTuiMode(const TCalculatedStatusData& data) {
    using namespace ftxui;

    // First update is special: we switch buffers and capture stderr to display live logs
    static bool firstUpdate = true;
    if (firstUpdate) {
        if (LogCapture) {
            LogCapture->StartCapture();
        }

        // Switch to alternate screen buffer (like htop)
        std::cout << "\033[?1049h";
        std::cout << "\033[2J\033[H"; // Clear screen and move to top
        firstUpdate = false;
    }

    if (LogCapture) {
        LogCapture->UpdateCapture();
    }

    // Left side of header: runner info, efficiency, phase, progress

    std::stringstream headerSs;
    headerSs << "TPC-C Runner: " << Config.WarehouseCount << " warehouses, "
             << Config.ThreadCount << " threads";

    std::stringstream metricsSs;
    metricsSs << std::fixed << std::setprecision(1)
              << "Efficiency: " << data.Efficiency << "%   "
              << "tpmC: " << LastStatisticsSnapshot->Tpmc;

    std::stringstream runningSs;
    runningSs << "Running: " << data.RunningTransactions << " transactions";

    std::stringstream timingSs;
    timingSs << data.Phase << ": " << data.ElapsedMinutes << ":"
             << std::setfill('0') << std::setw(2) << data.ElapsedSeconds << " elapsed";
    if (data.ProgressPercent > 0) {
        timingSs << "   "
            << data.RemainingMinutes << ":" << std::setfill('0') << std::setw(2) << data.RemainingSeconds
            << " remaining";
    }

    // Calculate progress ratio for gauge
    float progressRatio = static_cast<float>(data.ProgressPercent / 100.0);

    auto leftHeader = vbox({
        text(headerSs.str()),
        text(metricsSs.str()),
        text(runningSs.str()),
        text(timingSs.str()),
        hbox({
            text("Progress: "),
            gauge(progressRatio) | size(WIDTH, EQUAL, 20),
            text(" " + std::to_string(static_cast<int>(data.ProgressPercent)) + "%")
        })
    });

    // Right side of header: Transaction statistics table (without header)

    Elements txRows;
    // Add header row for transaction table
    txRows.push_back(hbox({
        text("Transaction") | size(WIDTH, EQUAL, 15),
        text("OK/s") | size(WIDTH, EQUAL, 6),
        text("Fail/s") | size(WIDTH, EQUAL, 8),
        text("p50") | size(WIDTH, EQUAL, 5),
        text("p99") | size(WIDTH, EQUAL, 5)
    }));

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        const auto& stats = LastStatisticsSnapshot->GlobalStats[i];

        std::stringstream okSs, failSs, p50Ss, p99Ss;
        okSs << std::fixed << std::setprecision(0) << stats.OkPerSecond;
        failSs << std::fixed << std::setprecision(1) << stats.FailedPerSecond;
        p50Ss << std::fixed << std::setprecision(0) << stats.LatencyHistogramFullMs.GetValueAtPercentile(50);
        p99Ss << std::fixed << std::setprecision(0) << stats.LatencyHistogramFullMs.GetValueAtPercentile(99);

        txRows.push_back(hbox({
            text(std::string(ToString(static_cast<ETransactionType>(i)))) | size(WIDTH, EQUAL, 15),
            text(okSs.str()) | size(WIDTH, EQUAL, 6),
            text(failSs.str()) | size(WIDTH, EQUAL, 8),
            text(p50Ss.str()) | size(WIDTH, EQUAL, 5),
            text(p99Ss.str()) | size(WIDTH, EQUAL, 5)
        }));
    }

    auto rightHeader = vbox(txRows);

    // Top section: left header + right transaction table (50/50 split)

    auto topSection = hbox({
        leftHeader | flex,
        separator(),
        rightHeader | flex
    }) | border;

    // Per-thread statistics in two columns with header

    Elements leftThreadElements, rightThreadElements;
    size_t threadCount = LastStatisticsSnapshot->StatVec.size();
    size_t halfCount = (threadCount + 1) / 2;

    auto headerRow = hbox({
        text("Thr") | size(WIDTH, EQUAL, 4),
        text("    Load") | size(WIDTH, EQUAL, 24),
        text("QPS") | size(WIDTH, EQUAL, 8),
        text("Queue") | size(WIDTH, EQUAL, 10),
        text("Queue p90, ms") | size(WIDTH, EQUAL, 20)
    });

    auto headerRow2 = hbox({
        text("Thr") | size(WIDTH, EQUAL, 4),
        text("    Load") | size(WIDTH, EQUAL, 24),
        text("QPS") | size(WIDTH, EQUAL, 8),
        text("Queue") | size(WIDTH, EQUAL, 10),
        text("Queue p90, ms") | size(WIDTH, EQUAL, 20)
    });

    leftThreadElements.push_back(headerRow);
    rightThreadElements.push_back(headerRow2);

    for (size_t i = 0; i < threadCount; ++i) {
        const auto& stats = LastStatisticsSnapshot->StatVec[i];
        double load = stats.ExecutingTime / stats.TotalTime;

        // Create custom progress bar with individual "|" characters
        constexpr int barWidth = 10;
        int filledBars = static_cast<int>(load * barWidth);
        std::string barContent(filledBars, '|');
        barContent += std::string(barWidth - filledBars, ' ');

        auto loadBar = text(barContent);
        Color loadColor;
        if (load < 0.6) {
            loadBar = loadBar | color(Color::Green);
            loadColor = Color::Green;
        } else if (load < 0.8) {
            loadBar = loadBar | color(Color::Yellow);
            loadColor = Color::Yellow;
        } else {
            loadBar = loadBar | color(Color::Red);
            loadColor = Color::Red;
        }

        std::stringstream loadPercentSs, qpsSs, queueSizeSs, queueP90Ss;
        loadPercentSs << std::fixed << std::setprecision(1) << (load * 100) << "%";
        qpsSs << std::fixed << std::setprecision(0) << stats.QueriesPerSecond;
        queueSizeSs << stats.TaskThreadStats->InternalTasksWaitingInflight;
        queueP90Ss << std::fixed << std::setprecision(1) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);

        auto threadLine = hbox({
            text(std::to_string(i)) | size(WIDTH, EQUAL, 4),
            hbox({
                text("["),
                loadBar | size(WIDTH, EQUAL, 10),
                text("] "),
                text(loadPercentSs.str()) | color(loadColor)
            }) | size(WIDTH, EQUAL, 24),
            text(qpsSs.str()) | size(WIDTH, EQUAL, 8),
            text(queueSizeSs.str()) | size(WIDTH, EQUAL, 10),
            text(queueP90Ss.str()) | size(WIDTH, EQUAL, 20)
        });

        if (i < halfCount) {
            leftThreadElements.push_back(threadLine);
        } else {
            rightThreadElements.push_back(threadLine);
        }
    }

    // Pad the shorter column with empty lines
    while (leftThreadElements.size() < rightThreadElements.size()) {
        leftThreadElements.push_back(text(""));
    }
    while (rightThreadElements.size() < leftThreadElements.size()) {
        rightThreadElements.push_back(text(""));
    }

    auto threadSection = hbox({
        vbox(leftThreadElements) | flex,
        separator(),
        vbox(rightThreadElements) | flex
    }) | border;

    // Logs section (last 10 lines, full width)

    Elements logElements;
    logElements.push_back(text("Logs"));
    if (LogCapture) {
        const auto& capturedLines = LogCapture->GetLogLines();
        size_t truncatedCount = LogCapture->GetTruncatedCount();

        // Get last 10 lines
        size_t startIndex = 0;
        if (capturedLines.size() > 10) {
            startIndex = capturedLines.size() - 10;
        }

        if (truncatedCount > 0) {
            logElements.push_back(text("... logs truncated: " + std::to_string(truncatedCount) + " lines"));
        }

        for (size_t i = startIndex; i < capturedLines.size(); ++i) {
            logElements.push_back(text(capturedLines[i]));
        }
    }

    auto logsSection = vbox(logElements) | border | size(HEIGHT, EQUAL, 12);

    // Main layout

    auto layout = vbox({
        topSection,
        threadSection,
        logsSection
    });

    // Render full screen

    std::cout << "\033[H"; // Move cursor to top
    auto screen = Screen::Create(Dimension::Full(), Dimension::Full());
    Render(screen, layout);
    std::cout << screen.ToString();
}

std::unique_ptr<TAllStatistics> TPCCRunner::CollectStatistics(Clock::time_point now) {
    auto threadCount = StatsVec.size();
    auto snapshot = std::make_unique<TAllStatistics>(threadCount);
    snapshot->Ts = now;

    for (size_t i = 0; i < StatsVec.size(); ++i) {
        StatsVec[i]->Collect(*snapshot->StatVec[i].TerminalStats);
        TaskQueue->CollectStats(i, *snapshot->StatVec[i].TaskThreadStats);
    }

    snapshot->CalculateDerivative(*LastStatisticsSnapshot);

    return snapshot;
}

TPCCRunner::TCalculatedStatusData TPCCRunner::CalculateStatusData(Clock::time_point now) {
    TCalculatedStatusData data;

    // calculate time and estimation

    Clock::time_point startTs;
    Clock::time_point deadline;
    std::chrono::duration<long long> duration;
    if (MeasurementsStartTs == Clock::time_point{}) {
        data.Phase = "Warming up";
        startTs = WarmupStartTs;
        deadline = WarmupStopDeadline;
        duration = std::chrono::duration_cast<std::chrono::seconds>(WarmupStopDeadline - WarmupStartTs);
    } else {
        data.Phase = "Measuring";
        startTs = MeasurementsStartTs;
        deadline = StopDeadline;
        duration = std::chrono::duration_cast<std::chrono::seconds>(StopDeadline - MeasurementsStartTs);
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - startTs);
    auto remaining = std::chrono::duration_cast<std::chrono::seconds>(deadline - now);

    data.ElapsedMinutes = static_cast<int>(elapsed.count() / 60);
    data.ElapsedSeconds = static_cast<int>(elapsed.count() % 60);
    data.RemainingMinutes = std::max(0LL, remaining.count() / 60);
    data.RemainingSeconds = std::max(0LL, remaining.count() % 60);

    data.ProgressPercent = (static_cast<double>(elapsed.count()) / duration.count()) * 100.0;

    // Calculate efficiency
    data.Efficiency = (LastStatisticsSnapshot->Tpmc * 100.0) / (Config.WarehouseCount * MAX_TPMC_PER_WAREHOUSE);

    // Get running counts
    data.RunningTerminals = TaskQueue->GetRunningCount();
    data.RunningTransactions = TransactionsInflight.load(std::memory_order_relaxed);

    return data;
}

void TPCCRunner::ExitTuiMode() {
    // Restore stderr and flush captured logs
    if (LogCapture) {
        LogCapture->RestoreAndFlush();
    }

    // Switch back to main screen buffer (restore original content)
    std::cout << "\033[?1049l";
    std::cout.flush();
}

void TPCCRunner::DumpFinalStats() {
    if (MeasurementsStartTs == Clock::time_point{}) {
        std::cout << "Stopped before measurements" << std::endl;
    }

    auto now = Clock::now();
    double secondsPassed = duration_cast<std::chrono::duration<double>>(now - MeasurementsStartTs).count();
    auto minutesPassed = secondsPassed / 60;

    TTerminalStats stats;

    // Collect stats from all terminals
    for (const auto& srcStats : StatsVec) {
        srcStats->Collect(stats);
    }

    // Calculate total transactions
    size_t totalOK = 0;
    size_t totalFailed = 0;
    size_t totalUserAborted = 0;

    size_t tableWidth = Config.ExtendedStats ? 95 : 65;

    // Print header
    std::cout << "\n\nTransaction Statistics:\n";
    std::cout << std::string(tableWidth, '-') << std::endl;
    std::cout << std::setw(15) << "Transaction"
              << std::setw(10) << "OK"
              << std::setw(10) << "Failed"
              << std::setw(15) << "User Aborted"
              << std::setw(10) << "p90 (ms)";

    if (Config.ExtendedStats) {
        std::cout << std::setw(15) << "terminal p90 (ms)"
                  << std::setw(15) << "pure p90 (ms)";
    }

    std::cout << std::endl;
    std::cout << std::string(tableWidth, '-') << std::endl;

    size_t totalNewOrders = 0;

    // Print stats for each transaction type
    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        auto type = static_cast<ETransactionType>(i);
        const auto& txStats = stats.GetStats(type);

        if (type == ETransactionType::NewOrder) {
            totalNewOrders += txStats.OK;
        }

        totalOK += txStats.OK;
        totalFailed += txStats.Failed;
        totalUserAborted += txStats.UserAborted;

        std::cout << std::setw(15) << std::string(ToString(type))
                  << std::setw(10) << txStats.OK
                  << std::setw(10) << txStats.Failed
                  << std::setw(15) << txStats.UserAborted
                  << std::setw(10) << txStats.LatencyHistogramFullMs.GetValueAtPercentile(90);

        if (Config.ExtendedStats) {
            std::cout << std::setw(15) << txStats.LatencyHistogramMs.GetValueAtPercentile(90)
                << std::setw(15) << txStats.LatencyHistogramPure.GetValueAtPercentile(90);
        }

        std::cout << std::endl;
    }

    // Print totals
    std::cout << std::string(tableWidth, '-') << std::endl;
    std::cout << std::setw(15) << "TOTAL"
              << std::setw(10) << totalOK
              << std::setw(10) << totalFailed
              << std::setw(15) << totalUserAborted
              << std::endl;
    std::cout << std::string(tableWidth, '-') << std::endl;

    if (minutesPassed >= 1) {
        size_t tpmC = size_t(totalNewOrders / minutesPassed);
        double efficiency = 1.0 * tpmC * 100 / Config.WarehouseCount / MAX_TPMC_PER_WAREHOUSE;
        std::cout << "warehouses: " << Config.WarehouseCount << std::endl;
        std::cout << "tpmC: " << tpmC << std::endl;
        std::cout << "efficiency: " << std::setprecision(2) << efficiency << "%" << std::endl;
    } else {
        std::cout << "Less than minute passed, tpmC calculation skipped" << std::endl;
    }
}

} // anonymous

//-----------------------------------------------------------------------------

void RunSync(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig) {
    TPCCRunner runner(connectionConfig, runConfig);
    runner.RunSync();
}

std::stop_source GetGlobalInterruptSource() {
    static std::stop_source StopByInterrupt;
    return StopByInterrupt;
}

} // namespace NYdb::NTPCC
