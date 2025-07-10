#include "runner.h"

#include "constants.h"
#include "log.h"
#include "log_backend.h"
#include "runner_display_data.h"
#include "task_queue.h"
#include "terminal.h"
#include "transactions.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/logger/log.h>

#include <util/string/cast.h>
#include <util/system/info.h>

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/component_base.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

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
    void CalculateStatusData(Clock::time_point now, TDisplayData& data);

    void UpdateDisplayTextMode();

    ftxui::Element BuildTuiLayout();
    void UpdateDisplayTuiMode();
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

    struct TScreenWrapper {
        TScreenWrapper()
            : Screen(ftxui::ScreenInteractive::Fullscreen())
        {}

        ftxui::ScreenInteractive Screen;
    };

    std::unique_ptr<TScreenWrapper> TuiScreen;
    std::optional<std::thread> TuiThread;

    // main controlling thread publish calculated data to the Tui thread to display
    std::shared_ptr<TDisplayData> DataToDisplay;
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

    if (Config.WarehouseCount == 0) {
        std::cerr << "Specified zero warehouses" << std::endl;
        std::exit(1);
    }

    const size_t terminalsCount = Config.WarehouseCount * TERMINALS_PER_WAREHOUSE;

    size_t threadCount = 0;
    if (Config.ThreadCount == 0) {
        // here we calculate max possible efficient thread number
        const size_t networkThreadCount = NConsoleClient::TYdbCommand::GetNetworkThreadNum(ConnectionConfig);
        const size_t maxTerminalThreadCount = cpuCount > networkThreadCount ? cpuCount - networkThreadCount : 1;
        threadCount = std::min(maxTerminalThreadCount, terminalsCount);

        // usually this allows to lower number of threads
        const size_t recommendedThreadCount =
            (Config.WarehouseCount + WAREHOUSES_PER_CPU_CORE - 1) / WAREHOUSES_PER_CPU_CORE;
        threadCount = std::min(threadCount, recommendedThreadCount);
    } else {
        threadCount = Config.ThreadCount;
    }

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

    // We don't want to start all terminals at the same time, because then there will be
    // a huge queue of ready terminals, which we can't handle
    bool forcedWarmup = false;
    uint32_t minWarmupSeconds = Terminals.size() * MinWarmupPerTerminal.count() / 1000 + 1;
    uint32_t warmupSeconds;
    if (Config.WarmupDuration.Seconds() < minWarmupSeconds) {
        forcedWarmup = true; // we must print log message later after display update
        warmupSeconds = minWarmupSeconds;
    } else {
        warmupSeconds = Config.WarmupDuration.Seconds();
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
        TuiScreen = std::make_unique<TScreenWrapper>();
        TuiThread = std::thread([&] {
            TuiScreen->Screen.Loop(ftxui::Renderer([&] {
                return BuildTuiLayout();
            }));

            // ftxui catches signals and breaks the loop above, but
            // we have to let know the rest of app
            GetGlobalInterruptSource().request_stop();
        });
    }

    if (forcedWarmup) {
        LOG_I("Forced minimal warmup time: " << TDuration::Seconds(warmupSeconds));
    }

    LOG_I("Starting warmup for " << TDuration::Seconds(warmupSeconds));

    size_t startedTerminalId = 0;
    for (; startedTerminalId < Terminals.size() && !GetGlobalInterruptSource().stop_requested(); ++startedTerminalId) {
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

    while (!GetGlobalInterruptSource().stop_requested()) {
        now = Clock::now();
        if (now >= WarmupStopDeadline) {
            break;
        }

        UpdateDisplayIfNeeded(Clock::now());
    }

    StopWarmup.store(true, std::memory_order_relaxed);

    LOG_I("Measuring during " << Config.RunDuration);

    MeasurementsStartTs = Clock::now();
    MeasurementsStartTsWall = TInstant::Now();

    // update to be precise, but actually it is not required
    StopDeadline = MeasurementsStartTs + std::chrono::seconds(Config.RunDuration.Seconds());

    // reset statistics
    auto newDisplayData = std::make_shared<TDisplayData>(PerThreadTerminalStats.size(), MeasurementsStartTs);
    std::atomic_store(&DataToDisplay, newDisplayData);

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

    switch (Config.DisplayMode) {
    case TRunConfig::EDisplayMode::Text:
        UpdateDisplayTextMode();
        break;
    case TRunConfig::EDisplayMode::Tui:
        UpdateDisplayTuiMode();
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
            const auto& stats = DataToDisplay->Statistics.StatVec[rightIndex];
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

        debugSs << leftLine.str() << " | " << rightLine.str() << std::endl;
    }
    debugSs << std::string(totalWidth, '-') << std::endl;

    // Transaction statistics
    debugSs << "\n";

    LOG_D(debugSs.str());
}

ftxui::Element TPCCRunner::BuildTuiLayout() {
    using namespace ftxui;

    // Get window width to determine which columns to show
    constexpr int MIN_WINDOW_WIDTH_FOR_EXTENDED_COLUMNS = 140;

    std::shared_ptr<TDisplayData> data = std::atomic_load(&DataToDisplay);
    if (data == nullptr) {
        // just sanity check, normally should never happen
        return filler();
    }

    auto screen = Screen::Create(Dimension::Full(), Dimension::Full());
    const int windowWidth = screen.dimx();
    const bool showExtendedColumns = windowWidth >= MIN_WINDOW_WIDTH_FOR_EXTENDED_COLUMNS;

    // First update is special: we switch buffers and capture stderr to display live logs
    static bool firstUpdate = true;
    if (firstUpdate) {
        // Switch to alternate screen buffer (like htop)
        std::cout << "\033[?1049h";
        std::cout << "\033[2J\033[H"; // Clear screen and move to top
        firstUpdate = false;
    }

    // Left side of header: runner info, efficiency, phase, progress

    std::stringstream headerSs;
    headerSs << "Result preview: " << data->StatusData.Phase;

    std::stringstream metricsSs;
    metricsSs << "Efficiency: " << std::setw(3) << std::fixed << std::setprecision(1) << data->StatusData.Efficiency << "%   "
        << "tpmC: " << std::fixed << std::setprecision(0) << data->StatusData.Tpmc;

    std::stringstream timingSs;
    timingSs << data->StatusData.ElapsedMinutesTotal << ":"
             << std::setfill('0') << std::setw(2) << data->StatusData.ElapsedSecondsTotal << " elapsed"
             << ", "
             << data->StatusData.RemainingMinutesTotal << ":" << std::setfill('0') << std::setw(2) << data->StatusData.RemainingSecondsTotal
             << " remaining";

    // Calculate progress ratio for gauge
    float progressRatio = static_cast<float>(data->StatusData.ProgressPercentTotal / 100.0);
    constexpr int progressBarWidth = 15;

    auto topLeftMainInfo = vbox({
        text(metricsSs.str()) | bold,
        text(timingSs.str()),
        hbox({
            text("Progress: ["),
            gauge(progressRatio) | size(WIDTH, EQUAL, progressBarWidth),
            text("] " + std::to_string(static_cast<int>(data->StatusData.ProgressPercentTotal)) + "%")
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
        const auto& totalForType = data->Statistics.TotalTerminalStats.GetStats(type);

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
    size_t threadCount = data->Statistics.StatVec.size();
    size_t halfCount = (threadCount + 1) / 2;

    // Create header row elements
    Elements headerLeftColumn;
    headerLeftColumn.push_back(text("Thr") | size(WIDTH, EQUAL, 4));
    headerLeftColumn.push_back(text("Load") | center | size(WIDTH, EQUAL, 24));
    headerLeftColumn.push_back(text("QPS") | align_right | size(WIDTH, EQUAL, 8));
    if (showExtendedColumns) {
        headerLeftColumn.push_back(text("Queue") | align_right | size(WIDTH, EQUAL, 10));
        headerLeftColumn.push_back(text("Queue p90, ms") | align_right | size(WIDTH, EQUAL, 20));
    }

    Elements headerRightColumn;
    headerRightColumn.push_back(text("Thr") | size(WIDTH, EQUAL, 4));
    headerRightColumn.push_back(text("Load") | center | size(WIDTH, EQUAL, 24));
    headerRightColumn.push_back(text("QPS") | align_right | size(WIDTH, EQUAL, 8));
    if (showExtendedColumns) {
        headerRightColumn.push_back(text("Queue") | align_right | size(WIDTH, EQUAL, 10));
        headerRightColumn.push_back(text("Queue p90, ms") | align_right | size(WIDTH, EQUAL, 20));
    }

    auto headerLeft = hbox(headerLeftColumn);
    auto headerRight = hbox(headerRightColumn);

    leftThreadElements.push_back(headerLeft);
    rightThreadElements.push_back(headerRight);

    for (size_t i = 0; i < threadCount; ++i) {
        const auto& stats = data->Statistics.StatVec[i];
        double load = stats.TotalTime != 0 ? stats.ExecutingTime / stats.TotalTime : 0;

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
        qpsSs << std::fixed << std::setprecision(0) << std::setw(8) << std::right << stats.QueriesPerSecond;
        queueSizeSs << stats.TaskThreadStats->InternalTasksWaitingInflight;
        queueP90Ss << std::fixed << std::setprecision(1) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90);

        // Create thread line elements
        Elements threadLineElements;
        threadLineElements.push_back(text(std::to_string(i + 1)) | size(WIDTH, EQUAL, 4));
        threadLineElements.push_back(hbox({
            text("["),
            loadBar | size(WIDTH, EQUAL, 10),
            text("] "),
            text(loadPercentSs.str()) | color(loadColor)
        }) | size(WIDTH, EQUAL, 24));
        threadLineElements.push_back(text(qpsSs.str()) | align_right | size(WIDTH, EQUAL, 8));
        if (showExtendedColumns) {
            threadLineElements.push_back(text(queueSizeSs.str()) | align_right | size(WIDTH, EQUAL, 10));
            threadLineElements.push_back(text(queueP90Ss.str()) | align_right | size(WIDTH, EQUAL, 20));
        }

        auto threadLine = hbox(threadLineElements);

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

    auto logsContent = vbox(logElements);

    auto logsSection = window(text("Logs"),
        logsContent | flex);

    // Main layout

    auto layout = vbox({
        topSection,
        threadSection,
        logsSection
    });

    return layout;
}

void TPCCRunner::UpdateDisplayTuiMode() {
    TuiScreen->Screen.PostEvent(ftxui::Event::Custom);
}

void TPCCRunner::CollectDataToDisplay(Clock::time_point now) {
    auto newDisplayData = std::make_shared<TDisplayData>(PerThreadTerminalStats.size(), now);

    // order makes sence here
    CollectStatistics(newDisplayData->Statistics);
    CalculateStatusData(now, *newDisplayData);

    std::atomic_store(&DataToDisplay, newDisplayData);
}

void TPCCRunner::CollectStatistics(TAllStatistics& statistics) {
    for (size_t i = 0; i < PerThreadTerminalStats.size(); ++i) {
        PerThreadTerminalStats[i]->Collect(*statistics.StatVec[i].TerminalStats);
        TaskQueue->CollectStats(i, *statistics.StatVec[i].TaskThreadStats);
    }

    std::shared_ptr<TDisplayData> currentData = std::atomic_load(&DataToDisplay);
    if (currentData) {
        statistics.CalculateDerivativeAndTotal(currentData->Statistics);
    }
}

void TPCCRunner::CalculateStatusData(Clock::time_point now, TDisplayData& data) {
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
        maxPossibleTpmc = Config.WarehouseCount * MAX_TPMC_PER_WAREHOUSE * 60 / currentPhaseElapsed.count();
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
    LogBackend->StopCapture();

    TuiScreen->Screen.Exit();
    TuiThread->join();

    // Switch back to main screen buffer (restore original content)
    std::cout << "\033[?1049l";
    std::cout.flush();
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
        std::cout << "tpmC*: " << DataToDisplay->StatusData.Tpmc << std::endl;
        std::cout << "efficiency: " << std::setprecision(2) << DataToDisplay->StatusData.Efficiency << "%" << std::endl;
    } else {
        std::cout << "Less than minute passed, tpmC calculation skipped" << std::endl;
    }

    std::cout << "* These results are not officially recognized TPC results "
              << "and are not comparable with other TPC-C test results published on the TPC website" << std::endl;
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
    TPCCRunner runner(connectionConfig, runConfig);
    runner.RunSync();
}

std::stop_source GetGlobalInterruptSource() {
    static std::stop_source StopByInterrupt;
    return StopByInterrupt;
}

} // namespace NYdb::NTPCC
