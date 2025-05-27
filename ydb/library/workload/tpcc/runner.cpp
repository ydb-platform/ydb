#include "runner.h"

#include "constants.h"
#include "log.h"
#include "task_queue.h"
#include "terminal.h"

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
constexpr auto MaxPerTerminalTransactionsInflight = 1;

//-----------------------------------------------------------------------------

std::stop_source StopByInterrupt;

void InterruptHandler(int) {
    StopByInterrupt.request_stop();
}

//-----------------------------------------------------------------------------

class TPCCRunner {
public:
    // we suppose that both constructor and destructor are called in a single "main" thread
    TPCCRunner(const TRunConfig& config);
    ~TPCCRunner();

    void RunSync();

private:
    void Join();

    void DumpFinalStats();

private:
    TRunConfig Config;

    std::shared_ptr<TLog> Log;

    std::stop_source TerminalsStopSource;
    std::stop_source ThreadsStopSource;

    std::atomic<bool> StopWarmup{false};
    std::vector<std::shared_ptr<TTerminalStats>> StatsVec;
    std::vector<std::unique_ptr<TTerminal>> Terminals;

    std::unique_ptr<ITaskQueue> TaskQueue;

    Clock::time_point LastStatsDump;
};

//-----------------------------------------------------------------------------

TPCCRunner::TPCCRunner(const TRunConfig& config)
    : Config(config)
    , Log(std::make_shared<TLog>(CreateLogBackend("cerr", config.LogPriority, true)))
{
    signal(SIGINT, InterruptHandler);
    signal(SIGTERM, InterruptHandler);

    Config.ConnectionConfig.IsNetworkIntensive = true;
    Config.ConnectionConfig.UsePerChannelTcpConnection = true;

    const size_t cpuCount = NSystemInfo::CachedNumberOfCpus();
    if (cpuCount == 0) {
        // dump sanity check
        std::cerr << "No CPUs" << std::endl;
        std::exit(1);
    }

    const size_t networkThreadCount = NConsoleClient::TYdbCommand::GetNetworkThreadNum(Config.ConnectionConfig);
    const size_t maxTerminalThreadCount = cpuCount > networkThreadCount ? cpuCount - networkThreadCount : 1;

    const size_t terminalsCount = Config.WarehouseCount * TERMINALS_PER_WAREHOUSE;

    // we might consider using less than maxTerminalThreads
    const size_t threadCount = std::min(maxTerminalThreadCount, terminalsCount);

    // The number of terminals might be hundreds of thousands.
    // For now, we don't have more than 32 network threads (check TYdbCommand::GetNetworkThreadNum()),
    // so that maxTerminalThreads will be around more or less around 100.
    const size_t driverCount = threadCount;
    std::vector<TDriver> drivers;
    drivers.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(Config.ConnectionConfig));
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
        << " workers on " << cpuCount << " cpus");

    Terminals.reserve(terminalsCount);
    for (size_t i = 0; i < terminalsCount; ++i) {
        size_t warehouseID = i % TERMINALS_PER_WAREHOUSE + 1;
        auto terminalPtr = std::make_unique<TTerminal>(
            i,
            warehouseID,
            Config.WarehouseCount,
            *TaskQueue,
            drivers[i % drivers.size()],
            Config.Path,
            Config.NoSleep,
            TerminalsStopSource.get_token(),
            StopWarmup,
            StatsVec[i % drivers.size()],
            Log);

        Terminals.emplace_back(std::move(terminalPtr));
    }

    // we want to have even load distribution and not start terminals from first INFLIGHT warehouses,
    // then next, etc. Long warmup resolves this, but we want to have a good start even without warmup
    std::random_shuffle(Terminals.begin(), Terminals.end());
    for (auto& terminal: Terminals) {
        terminal->Start();
    }
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
    TerminalsStopSource.request_stop();

    // we don't have to wait terminals to finish suspended coroutines, just stop
    // threads executing coroutines
    LOG_I("Stopping task queue");
    TaskQueue->Join();
    LOG_I("Runner stopped");
}

void TPCCRunner::RunSync() {
    Clock::time_point now = Clock::now();

    TaskQueue->Run();

    // TODO: convert to minutes when needed
    LOG_D("Starting warmup");

    auto warmupStartTs = Clock::now();
    auto warmupStopDeadline = warmupStartTs + std::chrono::seconds(Config.WarmupSeconds);
    while (!StopByInterrupt.stop_requested()) {
        if (now >= warmupStopDeadline) {
            break;
        }
        std::this_thread::sleep_for(SleepMsEveryIterationMainLoop);
        now = Clock::now();
    }

    StopWarmup.store(true, std::memory_order_relaxed);

    // TODO: convert to minutes when needed
    LOG_I("Measuring during " << Config.RunSeconds << " seconds");

    auto startTs = Clock::now();
    auto stopDeadline = startTs + std::chrono::seconds(Config.RunSeconds);
    while (!StopByInterrupt.stop_requested()) {
        if (now >= stopDeadline) {
            break;
        }
        std::this_thread::sleep_for(SleepMsEveryIterationMainLoop);
        now = Clock::now();
    }

    LOG_D("Finished measurements");
    Join();

    DumpFinalStats();
}

void TPCCRunner::DumpFinalStats() {
    TTerminalStats stats;

    // Collect stats from all terminals
    for (const auto& srcStats : StatsVec) {
        srcStats->Collect(stats);
    }

    // Calculate total transactions
    size_t totalOK = 0;
    size_t totalFailed = 0;
    size_t totalUserAborted = 0;

    // Print header
    std::cout << "\nTransaction Statistics:\n";
    std::cout << "----------------------\n";
    std::cout << std::setw(15) << "Transaction"
              << std::setw(10) << "OK"
              << std::setw(10) << "Failed"
              << std::setw(15) << "User Aborted"
              << std::setw(20) << "Latency p90 (ms)"
              << std::endl;
    std::cout << std::string(65, '-') << std::endl;

    // Print stats for each transaction type
    const char* txNames[] = {"NewOrder", "Delivery", "OrderStatus", "Payment", "StockLevel"};
    for (size_t i = 0; i < 5; ++i) {
        auto type = static_cast<TTerminalStats::ETransactionType>(i);
        const auto& txStats = stats.GetStats(type);

        totalOK += txStats.OK;
        totalFailed += txStats.Failed;
        totalUserAborted += txStats.UserAborted;

        std::cout << std::setw(15) << txNames[i]
                  << std::setw(10) << txStats.OK
                  << std::setw(10) << txStats.Failed
                  << std::setw(15) << txStats.UserAborted
                  << std::setw(15) << txStats.LatencyHistogramMs.GetValueAtPercentile(90)
                  << std::endl;
    }

    // Print totals
    std::cout << std::string(65, '-') << std::endl;
    std::cout << std::setw(15) << "TOTAL"
              << std::setw(10) << totalOK
              << std::setw(10) << totalFailed
              << std::setw(15) << totalUserAborted
              << std::endl;
    std::cout << std::string(65, '-') << std::endl;
}

} // anonymous

//-----------------------------------------------------------------------------

TRunConfig::TRunConfig(const NConsoleClient::TClientCommand::TConfig& connectionConfig)
    : ConnectionConfig(connectionConfig)
{
}

void RunSync(const TRunConfig& config) {
    TPCCRunner runner(config);
    runner.RunSync();
}

} // namespace NYdb::NTPCC
