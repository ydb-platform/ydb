#include "runner.h"

#include "constants.h"
#include "log.h"
#include "task_queue.h"
#include "terminal.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/logger/log.h>

#include <util/system/info.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <stop_token>
#include <thread>
#include <vector>

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

private:
    TRunConfig Config;

    std::shared_ptr<TLog> Log;

    Clock::time_point StartTime;

    std::stop_source TerminalsStopSource;
    std::stop_source ThreadsStopSource;

    std::atomic<bool> StopWarmup{false};
    std::vector<TTerminal> Terminals;

    std::unique_ptr<ITaskQueue> TaskQueue;
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
        std::terminate();
    }

    const size_t networkThreadCount = NConsoleClient::TYdbCommand::GetNetworkThreadNum(Config.ConnectionConfig);
    const size_t maxTerminalThreadCount = cpuCount > networkThreadCount ? cpuCount - networkThreadCount : 1;

    const size_t terminalsCount = Config.WarehouseCount * TERMINALS_PER_WAREHOUSE;

    // we might consider using less than maxTerminalThreads
    const size_t treadCount = std::min(maxTerminalThreadCount, terminalsCount);

    // The number of terminals might be hundreds of thousands.
    // For now, we don't have more than 32 network threads (check TYdbCommand::GetNetworkThreadNum()),
    // so that maxTerminalThreads will be around more or less around 100.
    const size_t driverCount = treadCount;
    std::vector<TDriver> drivers;
    drivers.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(Config.ConnectionConfig));
    }

    const size_t maxTerminalsPerThread = (terminalsCount + treadCount - 1) / treadCount;
    const size_t maxReadyTransactions = maxTerminalsPerThread * MaxPerTerminalTransactionsInflight;

    TaskQueue = CreateTaskQueue(
        treadCount,
        maxTerminalsPerThread,
        maxReadyTransactions,
        Log);

    LOG_I("Creating " << terminalsCount << " terminals and " << treadCount
        << " workers on " << cpuCount << " cpus");

    Terminals.reserve(terminalsCount);
    for (size_t i = 0; i < terminalsCount; ++i) {
        // note, that terminal adds itself to ready terminals
        Terminals.emplace_back(
            i,
            *TaskQueue,
            drivers[i % drivers.size()],
            TerminalsStopSource.get_token(),
            StopWarmup,
            Log);
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
    for (const auto& terminal: Terminals) {
        // terminals should wait for their query coroutines to finish
        while (!terminal.IsDone()) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }

    // now, we can safely stop our threads, which have been executing coroutines
    LOG_I("Stopping task queue");
    TaskQueue->Join();
    LOG_I("Runner stopped");
}

void TPCCRunner::RunSync() {
    Clock::time_point now = Clock::now();

    TaskQueue->Run();

    // warmup loop
    // TODO
    //LOG_D("Starting ")
    //while (!StopByInterrupt.stop_requested()) {
    //}

    StopWarmup.store(true, std::memory_order_relaxed);

    // TODO: convert to minutes
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
