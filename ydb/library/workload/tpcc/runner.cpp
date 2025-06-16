#include "runner.h"

#include "constants.h"
#include "log.h"
#include "log_backend.h"
#include "task_queue.h"
#include "terminal.h"
#include "transactions.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
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

// We have two sources of stats: terminal stats and per taskqueue thread stats.
// Note, that terminal stats are also per thread: multiple terminals are running on the same thread
// shared same stats object. Thus, terminal stats are aggregated values.
//
// Here we collect all the stats in a single place to easily display progress:
//  * terminal stats: TPC-C related statistics like per transaction type latencies, tpmC.
//  * thread stats: load, Queue
struct TAllStatistics {

    struct TThreadStatistics {
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

        double ExecutingTime = 0;
        double TotalTime = 0;
        THistogram InternalInflightWaitTimeMs{ITaskQueue::TThreadStats::BUCKET_COUNT, ITaskQueue::TThreadStats::MAX_HIST_VALUE};
        THistogram ExternalQueueTimeMs{ITaskQueue::TThreadStats::BUCKET_COUNT, ITaskQueue::TThreadStats::MAX_HIST_VALUE};
    };

    TAllStatistics(size_t threadCount, Clock::time_point ts)
        : StatVec(threadCount)
        , Ts(ts)
    {
    }

    void CalculateDerivativeAndTotal(const TAllStatistics& prev) {
        size_t seconds = duration_cast<std::chrono::duration<size_t>>(Ts - prev.Ts).count();

        // Calculate per-thread derivatives
        if (seconds != 0) {
            for (size_t i = 0; i < StatVec.size(); ++i) {
                StatVec[i].CalculateDerivative(prev.StatVec[i], seconds);
            }
        }

        // Aggregate total statistics
        for (const auto& stats: StatVec) {
            stats.TerminalStats->Collect(TotalTerminalStats);
        }
    }

    TVector<TThreadStatistics> StatVec;
    const Clock::time_point Ts;

    TTerminalStats TotalTerminalStats;
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

        // these ones are results preview, i.e. "total" tpmC at the moment
        double Tpmc = 0.0;
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
    void CollectStatistics(Clock::time_point now);
    TCalculatedStatusData CalculateStatusData(Clock::time_point now);

    void UpdateDisplayTextMode(const TCalculatedStatusData& data);
    void UpdateDisplayTuiMode(const TCalculatedStatusData& data);
    void ExitTuiMode();

    void PrintTransactionStatisticsPretty(std::ostream& os);
    void PrintFinalResultPretty();

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

    Clock::time_point MeasurementsStartTs;
    Clock::time_point StopDeadline;

    std::unique_ptr<TAllStatistics> LastStatisticsSnapshot;
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

    PerThreadTerminalStats.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        PerThreadTerminalStats.emplace_back(std::make_shared<TTerminalStats>());
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
    Config.SetDisplay();

    Clock::time_point now = Clock::now();

    TaskQueue->Run();

    // empty ts to enforce display update
    LastStatisticsSnapshot = std::make_unique<TAllStatistics>(PerThreadTerminalStats.size(), Clock::time_point{});

    // We don't want to start all terminals at the same time, because then there will be
    // a huge queue of ready terminals, which we can't handle
    bool forcedWarmup = false;
    uint32_t minWarmupSeconds = Terminals.size() * MinWarmupPerTerminal.count() / 1000 + 1;
    uint32_t minWarmupMinutes = (minWarmupSeconds + 59) / 60;
    uint32_t warmupMinutes;
    if (Config.WarmupDuration.Minutes() < minWarmupMinutes) {
        forcedWarmup = true; // we must print log message later after display update
        warmupMinutes = minWarmupMinutes;
    } else {
        warmupMinutes = Config.WarmupDuration.Minutes();
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

    LOG_I("Measuring during " << Config.RunDuration);

    MeasurementsStartTs = Clock::now();

    // reset statistics
    LastStatisticsSnapshot = std::make_unique<TAllStatistics>(PerThreadTerminalStats.size(), MeasurementsStartTs);

    StopDeadline = MeasurementsStartTs + std::chrono::minutes(Config.RunDuration.Minutes());
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

    PrintFinalResultPretty();
}

void TPCCRunner::UpdateDisplayIfNeeded(Clock::time_point now) {
    auto delta = now - LastStatisticsSnapshot->Ts;
    if (delta < Config.DisplayUpdateInterval) {
        return;
    }

    CollectStatistics(now);
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
       << "tpmC: " << std::setprecision(1) << data.Tpmc;

    std::cout << ss.str();

    // Per thread statistics (two columns)
    std::cout << "\n\nPer thread statistics:" << std::endl;

    size_t threadCount = LastStatisticsSnapshot->StatVec.size();
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
    std::cout << threadsHeader.str() << " | " << threadsHeader.str() << std::endl;

    size_t totalWidth = threadsHeader.str().length() * 2 + 3;
    std::cout << std::string(totalWidth, '-') << std::endl;

    // Print thread data in two columns
    for (size_t i = 0; i < halfCount; ++i) {
        // Left column
        std::stringstream leftLine;
        if (i < threadCount) {
            const auto& stats = LastStatisticsSnapshot->StatVec[i];
            double load = stats.ExecutingTime / stats.TotalTime;
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
            const auto& stats = LastStatisticsSnapshot->StatVec[rightIndex];
            double load = stats.ExecutingTime / stats.TotalTime;
            rightLine << std::left
                      << std::setw(5) << (rightIndex + 1)
                      << std::setw(5) << std::fixed << std::setprecision(2) << load
                      << std::setw(10) << std::fixed << stats.QueriesPerSecond
                      << std::setw(10) << stats.TaskThreadStats->InternalTasksWaitingInflight
                      << std::setw(15) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);
        } else {
            rightLine << std::string(threadsHeader.str().length(), ' ');
        }

        std::cout << leftLine.str() << " | " << rightLine.str() << std::endl;
    }
    std::cout << std::string(totalWidth, '-') << std::endl;

    // Transaction statistics
    std::cout << "\n";
    PrintTransactionStatisticsPretty(std::cout);
}

void TPCCRunner::UpdateDisplayTuiMode(const TCalculatedStatusData& data) {
    using namespace ftxui;

    // First update is special: we switch buffers and capture stderr to display live logs
    static bool firstUpdate = true;
    if (firstUpdate) {
        LogBackend->StartCapture();

        // Switch to alternate screen buffer (like htop)
        std::cout << "\033[?1049h";
        std::cout << "\033[2J\033[H"; // Clear screen and move to top
        firstUpdate = false;
    }

    // Left side of header: runner info, efficiency, phase, progress

    std::stringstream headerSs;
    headerSs << "Result preview";

    std::stringstream metricsSs;
    metricsSs << "Efficiency: " << std::setw(3) << std::fixed << std::setprecision(1) << data.Efficiency << "%   "
        << "tpmC: " << std::fixed << std::setprecision(0) << data.Tpmc;

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

    auto topLeftMainInfo = vbox({
        text(metricsSs.str()),
        text(timingSs.str()),
        hbox({
            text("Progress: ["),
            gauge(progressRatio) | size(WIDTH, EQUAL, 20),
            text("] " + std::to_string(static_cast<int>(data.ProgressPercent)) + "%")
        })
    });

    // Right side of header: Transaction statistics table (without header)

    Elements txRows;
    // Add header row for transaction table
    txRows.push_back(hbox({
        text("Transaction") | size(WIDTH, EQUAL, 12),
        text("p50, ms") | align_right | size(WIDTH, EQUAL, 9),
        text("p90, ms") | align_right | size(WIDTH, EQUAL, 9),
        text("p99, ms") | align_right | size(WIDTH, EQUAL, 9)
    }));

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        auto type = static_cast<ETransactionType>(i);
        const auto& totalForType = LastStatisticsSnapshot->TotalTerminalStats.GetStats(type);

        std::stringstream p50Ss, p90Ss, p99Ss;
        p50Ss << std::fixed << std::setprecision(0) << totalForType.LatencyHistogramFullMs.GetValueAtPercentile(50);
        p90Ss << std::fixed << std::setprecision(0) << totalForType.LatencyHistogramFullMs.GetValueAtPercentile(90);
        p99Ss << std::fixed << std::setprecision(0) << totalForType.LatencyHistogramFullMs.GetValueAtPercentile(99);

        txRows.push_back(hbox({
            text(std::string(ToString(type))) | size(WIDTH, EQUAL, 12),
            text(p50Ss.str()) | align_right | size(WIDTH, EQUAL, 9),
            text(p90Ss.str()) | align_right | size(WIDTH, EQUAL, 9),
            text(p99Ss.str()) | align_right | size(WIDTH, EQUAL, 9)
        }));
    }
    auto topRightTransactionStats = vbox(txRows);

    auto topSection = window(text(headerSs.str()), hbox({
        topLeftMainInfo | flex,
        separator(),
        topRightTransactionStats | flex
    }));

    // Per-thread statistics in two columns with header

    Elements leftThreadElements, rightThreadElements;
    size_t threadCount = LastStatisticsSnapshot->StatVec.size();
    size_t halfCount = (threadCount + 1) / 2;

    auto headerRow = hbox({
        text("Thr") | size(WIDTH, EQUAL, 4),
        text("Load") | center | size(WIDTH, EQUAL, 24),
        text("QPS") | align_right | size(WIDTH, EQUAL, 8),
        text("Queue") | align_right | size(WIDTH, EQUAL, 10),
        text("Queue p90, ms") | align_right | size(WIDTH, EQUAL, 20)
    });

    auto headerRow2 = hbox({
        text("Thr") | size(WIDTH, EQUAL, 4),
        text("Load") | center | size(WIDTH, EQUAL, 24),
        text("QPS") | align_right | size(WIDTH, EQUAL, 8),
        text("Queue") | align_right | size(WIDTH, EQUAL, 10),
        text("Queue p90, ms") | align_right | size(WIDTH, EQUAL, 20)
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
        loadPercentSs << std::fixed << std::setprecision(1) << std::setw(4) << std::right << (load * 100) << "%";
        qpsSs << std::fixed << std::setprecision(0) << stats.QueriesPerSecond;
        queueSizeSs << stats.TaskThreadStats->InternalTasksWaitingInflight;
        queueP90Ss << std::fixed << std::setprecision(1) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);

        auto threadLine = hbox({
            text(std::to_string(i + 1)) | size(WIDTH, EQUAL, 4),
            hbox({
                text("["),
                loadBar | size(WIDTH, EQUAL, 10),
                text("] "),
                text(loadPercentSs.str()) | color(loadColor)
            }) |  size(WIDTH, EQUAL, 24),
            text(qpsSs.str()) | align_right | size(WIDTH, EQUAL, 8),
            text(queueSizeSs.str()) | align_right | size(WIDTH, EQUAL, 10),
            text(queueP90Ss.str()) | align_right | size(WIDTH, EQUAL, 20)
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

    auto threadSection = window(text("TPC-C client state"), hbox({
        vbox(leftThreadElements) | flex,
        separator(),
        vbox(rightThreadElements) | flex
    }));

    // Logs section (last 10 lines, full width)

    Elements logElements;

    LogBackend->GetLogLines([&](const std::string& line) {
        logElements.push_back(paragraph(line));
    });

    auto logsSection = window(text("Logs"),
        vbox(logElements) | size(HEIGHT, EQUAL, 12));

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

void TPCCRunner::CollectStatistics(Clock::time_point now) {
    auto threadCount = PerThreadTerminalStats.size();
    auto snapshot = std::make_unique<TAllStatistics>(threadCount, now);

    for (size_t i = 0; i < PerThreadTerminalStats.size(); ++i) {
        PerThreadTerminalStats[i]->Collect(*snapshot->StatVec[i].TerminalStats);
        TaskQueue->CollectStats(i, *snapshot->StatVec[i].TaskThreadStats);
    }

    snapshot->CalculateDerivativeAndTotal(*LastStatisticsSnapshot);

    LastStatisticsSnapshot.swap(snapshot);
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

    if (duration.count() != 0) {
        data.ProgressPercent = (static_cast<double>(elapsed.count()) / duration.count()) * 100.0;
    }

    // Calculate tpmC and efficiency
    if (data.ElapsedMinutes == 0 && data.ElapsedSeconds == 0) {
        data.Tpmc = 0;
    } else {
        // approximate
        const auto& newOrderStats = LastStatisticsSnapshot->TotalTerminalStats.GetStats(ETransactionType::NewOrder);
        auto newOrdersCount = newOrderStats.OK.load(std::memory_order_relaxed);
        double tpmc = newOrdersCount * 60 / elapsed.count();

        // there are two errors: rounding + approximation, we might overshoot
        // 100% efficiency very slightly because of errors and it's OK to "round down"
        double maxPossibleTpmc = Config.WarehouseCount * MAX_TPMC_PER_WAREHOUSE * 60 / elapsed.count();
        data.Tpmc = std::min(maxPossibleTpmc, tpmc);
    }

    data.Efficiency = (data.Tpmc * 100.0) / (Config.WarehouseCount * MAX_TPMC_PER_WAREHOUSE);

    // Get running counts
    data.RunningTerminals = TaskQueue->GetRunningCount();
    data.RunningTransactions = TransactionsInflight.load(std::memory_order_relaxed);

    return data;
}

void TPCCRunner::ExitTuiMode() {
    LogBackend->StopCapture();

    // Switch back to main screen buffer (restore original content)
    std::cout << "\033[?1049l";
    std::cout.flush();
}

void TPCCRunner::PrintTransactionStatisticsPretty(std::ostream& os) {
    os << "Transaction Statistics:\n";

    // Build header using stringstream to calculate width
    std::stringstream txHeaderStream;
    txHeaderStream << std::left
                   << std::setw(12) << "Transaction"
                   << std::setw(12) << std::right << "OK"
                   << std::setw(10) << std::right << "Failed";
    if (Config.ExtendedStats) {
        txHeaderStream << std::setw(15) << std::right << "UserAborted";
    }
    txHeaderStream << std::setw(9) << std::right << "p50, ms"
                   << std::setw(9) << std::right << "p90, ms"
                   << std::setw(9) << std::right << "p99, ms";

    std::string header = txHeaderStream.str();
    size_t tableWidth = header.length();

    os << std::string(tableWidth, '-') << std::endl;
    os << header << std::endl;
    os << std::string(tableWidth, '-') << std::endl;

    size_t totalOK = 0;
    size_t totalFailed = 0;
    size_t totalUserAborted = 0;

    for (size_t i = 0; i < GetEnumItemsCount<ETransactionType>(); ++i) {
        auto type = static_cast<ETransactionType>(i);
        auto typeStr = ToString(type);
        const auto& stats = LastStatisticsSnapshot->TotalTerminalStats.GetStats(type);
        auto ok = stats.OK.load(std::memory_order_relaxed);
        auto failed = stats.Failed.load(std::memory_order_relaxed);
        auto aborted = stats.UserAborted.load(std::memory_order_relaxed);

        totalOK += ok;
        totalFailed += failed;
        totalUserAborted += aborted;

        os << std::left
           << std::setw(12) << std::string(typeStr)
           << std::fixed << std::setprecision(1) << std::setw(12) << std::right << ok
           << std::fixed << std::setprecision(1) << std::setw(10) << std::right << failed;
        if (Config.ExtendedStats) {
            os << std::fixed << std::setprecision(1) << std::setw(15) << std::right << aborted;
        }
        os << std::fixed << std::setprecision(1) << std::setw(9) << std::right << stats.LatencyHistogramFullMs.GetValueAtPercentile(50)
           << std::fixed << std::setprecision(1) << std::setw(9) << std::right << stats.LatencyHistogramFullMs.GetValueAtPercentile(90)
           << std::fixed << std::setprecision(1) << std::setw(9) << std::right << stats.LatencyHistogramFullMs.GetValueAtPercentile(99)
           << std::endl;
    }
    os << std::string(tableWidth, '-') << std::endl;

    os << std::left << std::setw(12) << "TOTAL"
        << std::setw(12) << std::fixed << std::setprecision(1) << std::right << totalOK
        << std::setw(10) << std::fixed << std::setprecision(1) << std::right << totalFailed;
    if (Config.ExtendedStats) {
        os << std::setw(15) << std::fixed << std::setprecision(1) << std::right << totalUserAborted;
    }
    os << std::endl;

    os << std::string(tableWidth, '-') << std::endl;
}

void TPCCRunner::PrintFinalResultPretty() {
    if (MeasurementsStartTs == Clock::time_point{}) {
        std::cout << "Stopped before measurements" << std::endl;
        return;
    }

    auto now = Clock::now();
    double secondsPassed = duration_cast<std::chrono::duration<double>>(now - MeasurementsStartTs).count();
    auto minutesPassed = secondsPassed / 60;

    CollectStatistics(now);
    TCalculatedStatusData data = CalculateStatusData(now);

    std::cout << "\n\n";
    PrintTransactionStatisticsPretty(std::cout);

    if (minutesPassed >= 1) {
        std::cout << "warehouses: " << Config.WarehouseCount << std::endl;
        std::cout << "tpmC*: " << data.Tpmc << std::endl;
        std::cout << "efficiency: " << std::setprecision(2) << data.Efficiency << "%" << std::endl;
    } else {
        std::cout << "Less than minute passed, tpmC calculation skipped" << std::endl;
    }

    std::cout << "* These results are not officially recognized TPC results "
              << "and are not comparable with other TPC-C test results published on the TPC website" << std::endl;
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
        break;
    }
}

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
