#include "runner.h"

#include "constants.h"
#include "log.h"
#include "task_queue.h"
#include "terminal.h"
#include "transactions.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

#include <util/system/info.h>

#include <atomic>
#include <stop_token>
#include <thread>
#include <vector>
#include <iomanip>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr auto SleepMsEveryIterationMainLoop = std::chrono::milliseconds(50);
constexpr auto DisplayUpdateInterval = std::chrono::seconds(15);
constexpr auto GracefulShutdownTimeout = std::chrono::seconds(5);
constexpr auto MinWarmupPerTerminal = std::chrono::milliseconds(1);

constexpr auto MaxPerTerminalTransactionsInflight = 1;

//-----------------------------------------------------------------------------

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

            NewOrderOkPerSecond = 1.0 *
                (TerminalStats->GetStats(TTerminalStats::E_NEW_ORDER).OK.load(std::memory_order_relaxed) -
                    prev.TerminalStats->GetStats(TTerminalStats::E_NEW_ORDER).OK.load(std::memory_order_relaxed)) / seconds;

            NewOrderFailPerSecond = 1.0 *
                (TerminalStats->GetStats(TTerminalStats::E_NEW_ORDER).Failed.load(std::memory_order_relaxed) -
                    prev.TerminalStats->GetStats(TTerminalStats::E_NEW_ORDER).Failed.load(std::memory_order_relaxed)) / seconds;

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
        double NewOrderOkPerSecond = 0;
        double NewOrderFailPerSecond = 0;
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

        size_t totalNewOrdersPrev = 0;
        size_t totalNewOrdersThis = 0;
        for (size_t i = 0; i < StatVec.size(); ++i) {
            totalNewOrdersPrev += prev.StatVec[i].TerminalStats->GetStats(TTerminalStats::E_NEW_ORDER).OK;
            totalNewOrdersThis += StatVec[i].TerminalStats->GetStats(TTerminalStats::E_NEW_ORDER).OK;
            StatVec[i].CalculateDerivative(prev.StatVec[i], seconds);
        }

        size_t tpmcDelta = totalNewOrdersThis - totalNewOrdersPrev;
        double minutesPassed = double(seconds) / 60.0;
        Tpmc = tpmcDelta / minutesPassed;
    }

    Clock::time_point Ts;
    TVector<TThreadStatistics> StatVec;
    double Tpmc = 0;
};

//-----------------------------------------------------------------------------

std::stop_source StopByInterrupt;

void InterruptHandler(int) {
    StopByInterrupt.request_stop();
}

//-----------------------------------------------------------------------------

class TPCCRunner {
public:
    // we suppose that both constructor and destructor are called in a single "main" thread
    TPCCRunner(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig);
    ~TPCCRunner();

    void RunSync();

private:
    void Join();

    void UpdateDisplayIfNeeded(Clock::time_point now);
    std::unique_ptr<TAllStatistics> CollectStatistics(Clock::time_point now);

    void UpdateDisplayDeveloperMode();
    void DumpFinalStats();

private:
    NConsoleClient::TClientCommand::TConfig ConnectionConfig;
    TRunConfig Config;

    std::shared_ptr<TLog> Log;

    std::stop_source TerminalsStopSource;
    std::stop_source ThreadsStopSource;

    std::atomic<bool> StopWarmup{false};
    std::vector<std::shared_ptr<TTerminalStats>> StatsVec;
    std::vector<std::unique_ptr<TTerminal>> Terminals;

    std::unique_ptr<ITaskQueue> TaskQueue;

    Clock::time_point MeasurementsStartTs;
    std::unique_ptr<TAllStatistics> LastStatisticsSnapshot;
};

//-----------------------------------------------------------------------------

TPCCRunner::TPCCRunner(const NConsoleClient::TClientCommand::TConfig& connectionConfig, const TRunConfig& runConfig)
    : ConnectionConfig(connectionConfig)
    , Config(runConfig)
    , Log(std::make_shared<TLog>(CreateLogBackend("cerr", Config.LogPriority, true)))
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

    std::vector<TDriver> drivers;
    drivers.reserve(driverCount);
    std::vector<std::shared_ptr<NQuery::TQueryClient>> clients;
    clients.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        auto& driver = drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(ConnectionConfig));
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
            clients[i % drivers.size()],
            Config.Path,
            Config.NoSleep,
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
    if (ThreadsStopSource.stop_requested()) {
        // already stopped
        return;
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
            std::this_thread::sleep_for(SleepMsEveryIterationMainLoop);
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
    LOG_I("Runner stopped");
}

void TPCCRunner::RunSync() {
    Clock::time_point now = Clock::now();

    TaskQueue->Run();

    // TODO: convert to minutes when needed

    // We don't want to start all terminals at the same time, because then there will be
    // a huge queue of ready terminals, which we can't handle

    int minWarmupSeconds = Terminals.size() * MinWarmupPerTerminal.count() / 1000 + 1;
    int warmupSeconds;
    if (Config.WarmupSeconds < minWarmupSeconds) {
        LOG_I("Forced minimal warmup time: " << minWarmupSeconds << " seconds");
        warmupSeconds = minWarmupSeconds;
    } else {
        warmupSeconds = Config.WarmupSeconds;
    }

    LOG_I("Starting warmup for " << warmupSeconds << " seconds");

    LastStatisticsSnapshot = std::make_unique<TAllStatistics>(StatsVec.size());
    LastStatisticsSnapshot->Ts = Clock::now();

    auto warmupStartTs = Clock::now();
    auto warmupStopDeadline = warmupStartTs + std::chrono::seconds(warmupSeconds);

    size_t startedTerminalId = 0;
    for (; startedTerminalId < Terminals.size() && !StopByInterrupt.stop_requested(); ++startedTerminalId) {
        if (now >= warmupStopDeadline) {
            break;
        }
        Terminals[startedTerminalId]->Start();

        std::this_thread::sleep_for(MinWarmupPerTerminal);
        now = Clock::now();
        UpdateDisplayIfNeeded(now);
    }
    ++startedTerminalId;

    // start the rest of terminals
    for (; startedTerminalId < Terminals.size() && !StopByInterrupt.stop_requested(); ++startedTerminalId) {
        Terminals[startedTerminalId]->Start();
    }

    StopWarmup.store(true, std::memory_order_relaxed);

    // TODO: convert to minutes when needed
    LOG_I("Measuring during " << Config.RunSeconds << " seconds");

    MeasurementsStartTs = Clock::now();
    auto stopDeadline = MeasurementsStartTs + std::chrono::seconds(Config.RunSeconds);
    while (!StopByInterrupt.stop_requested()) {
        if (now >= stopDeadline) {
            break;
        }
        std::this_thread::sleep_for(SleepMsEveryIterationMainLoop);
        now = Clock::now();
        UpdateDisplayIfNeeded(now);
    }

    LOG_D("Finished measurements");
    Join();

    DumpFinalStats();
}

void TPCCRunner::UpdateDisplayIfNeeded(Clock::time_point now) {
    auto delta = now - LastStatisticsSnapshot->Ts;
    if (delta >= DisplayUpdateInterval) {
        std::unique_ptr<TAllStatistics> newStatistics = CollectStatistics(now);
        LastStatisticsSnapshot = std::move(newStatistics);

        if (Config.Developer) {
           UpdateDisplayDeveloperMode();
        }
    }
}

void TPCCRunner::UpdateDisplayDeveloperMode() {
    std::cout << "\n\n\n";

    std::cout << std::left
              << std::setw(5) << "Thr"
              << std::setw(5) << "Load"
              << std::setw(15) << "terminals/s"
              << std::setw(15) << "queries/s"
              << std::setw(20) << "NewOrder Fail/s"
              << std::setw(20) << "inflight queue"
              << std::setw(20) << "wait p50, ms"
              << std::setw(20) << "wait p90, ms"
              << std::setw(20) << "ewait p50, ms"
              << std::setw(20) << "ewait p90, ms"
              << std::endl;

    std::cout << std::string(175, '-') << std::endl;

    for (size_t i = 0; i < LastStatisticsSnapshot->StatVec.size(); ++i) {
        const auto& stats = LastStatisticsSnapshot->StatVec[i];
        double load = LastStatisticsSnapshot->StatVec[i].ExecutingTime / LastStatisticsSnapshot->StatVec[i].TotalTime;
        std::cout << std::left
                  << std::setw(5) << i
                  << std::setw(5) << std::setprecision(2) << load
                  << std::setw(15) << std::fixed << stats.TerminalsPerSecond
                  << std::setw(15) << std::fixed << stats.QueriesPerSecond
                  << std::setw(20) << std::fixed << std::setprecision(2) << stats.NewOrderFailPerSecond
                  << std::setw(20) << stats.TaskThreadStats->InternalTasksWaitingInflight
                  << std::setw(20) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(50)
                  << std::setw(20) << std::setprecision(2) << stats.InternalInflightWaitTimeMs.GetValueAtPercentile(90)
                  << std::setw(20) << std::setprecision(2) << stats.ExternalQueueTimeMs.GetValueAtPercentile(50)
                  << std::setw(20) << std::setprecision(2) << stats.ExternalQueueTimeMs.GetValueAtPercentile(90)
                  << std::endl;
    }

    std::cout << "\nRunning terminals: " << TaskQueue->GetRunningCount();
    std::cout << "\nRunning transactions: " << TransactionsInflight.load(std::memory_order_relaxed);
    std::cout << "\nCurrent tpmC: " << std::fixed << std::setprecision(2) << LastStatisticsSnapshot->Tpmc << std::endl;
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

    size_t tableWidth = Config.Developer ? 95 : 65;

    // Print header
    std::cout << "\nTransaction Statistics:\n";
    std::cout << "----------------------\n";
    std::cout << std::setw(15) << "Transaction"
              << std::setw(10) << "OK"
              << std::setw(10) << "Failed"
              << std::setw(15) << "User Aborted"
              << std::setw(10) << "p90 (ms)";

    if (Config.Developer) {
        std::cout << std::setw(15) << "terminal p90 (ms)"
            << std::setw(15) << "pure p90 (ms)";
    }

    std::cout << std::endl;
    std::cout << std::string(tableWidth, '-') << std::endl;

    size_t totalNewOrders = 0;

    // Print stats for each transaction type
    const char* txNames[] = {"NewOrder", "Delivery", "OrderStatus", "Payment", "StockLevel"};
    for (size_t i = 0; i < 5; ++i) {
        auto type = static_cast<TTerminalStats::ETransactionType>(i);
        const auto& txStats = stats.GetStats(type);

        if (type == TTerminalStats::E_NEW_ORDER) {
            totalNewOrders += txStats.OK;
        }

        totalOK += txStats.OK;
        totalFailed += txStats.Failed;
        totalUserAborted += txStats.UserAborted;

        std::cout << std::setw(15) << txNames[i]
                  << std::setw(10) << txStats.OK
                  << std::setw(10) << txStats.Failed
                  << std::setw(15) << txStats.UserAborted
                  << std::setw(10) << txStats.LatencyHistogramFullMs.GetValueAtPercentile(90);

        if (Config.Developer) {
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

} // namespace NYdb::NTPCC
