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

using Clock = std::chrono::steady_clock;

constexpr auto SleepMsEveryIterationMainLoop = std::chrono::milliseconds(50);
constexpr auto SleepUsecIfNoProgress = std::chrono::microseconds(100);
constexpr auto MaxPerTerminalQueriesInflight = 1;

//-----------------------------------------------------------------------------

std::stop_source StopByInterrupt;

void InterruptHandler(int) {
    StopByInterrupt.request_stop();
}

//-----------------------------------------------------------------------------

// TODO: it's unclear what is better to use here:
// * on one hand this requires lock (spinlock is just fine), but does no memory allocations
// * on another hand MPSC doesn't need lock, but allocates memory
template <typename T>
class TSimpleCircularQueue {
public:
    TSimpleCircularQueue()
        : FirstEmpty(0)
        , FirstUsed(0)
        , Size(0)
    {
    }

    void Resize(size_t capacity) {
        Queue.resize(capacity);
    }

    bool TryPush(T&& item) {
        if (Size == Queue.size()) {
            return false;
        }
        Queue[FirstEmpty] = std::move(item);
        FirstEmpty = (FirstEmpty + 1) % Queue.size();
        ++Size;
        return true;
    }

    bool TryPop(T& item) {
        if (Size == 0) {
            return false;
        }
        item = std::move(Queue[FirstUsed]);
        FirstUsed = (FirstUsed + 1) % Queue.size();
        --Size;
        return true;
    }

    size_t GetSize() const {
        return Size;
    }

    bool IsEmpty() const {
        return Size == 0;
    }

    bool IsFull() const {
        return Size == Queue.size();
    }

private:
    std::vector<T> Queue;
    size_t FirstEmpty;
    size_t FirstUsed;
    size_t Size;
};

//-----------------------------------------------------------------------------

struct alignas(64) TPerThreadContext {
    TPerThreadContext() = default;

    TSpinLock Lock;
    TSimpleCircularQueue<TTerminalTask::TCoroHandle> ReadyTerminals;
    TSimpleCircularQueue<TTransactionTask::TCoroHandle> ReadyTransactions;
};

class TPCCRunner : public IReadyTaskQueue {
public:
    // we suppose that both constructor and destructor are called in a single "main" thread
    TPCCRunner(const TRunConfig& config);
    ~TPCCRunner();

    void RunSync();

public:
    // IReadyTaskQueue
    void TaskReady(TTerminalTask::TCoroHandle handle, size_t terminalId) override;
    void TaskReady(TTransactionTask::TCoroHandle handle, size_t terminalId) override;

private:
    void Join();
    void RunThread(size_t threadId);

private:
    TRunConfig Config;

    std::shared_ptr<TLog> Log;

    Clock::time_point StartTime;

    std::stop_source TerminalsStopSource;
    std::stop_source ThreadsStopSource;

    std::vector<TTerminal> Terminals;

    std::vector<std::thread> Threads;
    std::vector<TPerThreadContext> PerThreadContext;
};

//-----------------------------------------------------------------------------

TPCCRunner::TPCCRunner(const TRunConfig& config)
    : Config(config)
    , Log(std::make_shared<TLog>(CreateLogBackend("cerr", config.LogPriority, true)))
{
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
    LOG_I("Joining runner's threads");
    ThreadsStopSource.request_stop();
    for (auto& thread: Threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    LOG_I("Runner stopped");
}

void TPCCRunner::RunSync() {
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

    // we might consider using less, than maxTerminalThreads
    const size_t treadsCount = std::min(maxTerminalThreadCount, terminalsCount);

    // The number of terminals might be hundreds of thousands.
    // For now, we don't have more than 32 network threads (check TYdbCommand::GetNetworkThreadNum()),
    // so that maxTerminalThreads will be around more or less around 100.
    const size_t driverCount = treadsCount;
    std::vector<TDriver> drivers;
    drivers.reserve(driverCount);
    for (size_t i = 0; i < driverCount; ++i) {
        drivers.emplace_back(NConsoleClient::TYdbCommand::CreateDriver(Config.ConnectionConfig));
    }

    PerThreadContext.resize(treadsCount); // note, it's resize(), not reserve()
    const size_t terminalsPerThread = (terminalsCount + treadsCount - 1) / treadsCount;
    const size_t maxReadyQueries = terminalsPerThread * MaxPerTerminalQueriesInflight;
    for (auto& context: PerThreadContext) {
        context.ReadyTerminals.Resize(terminalsPerThread);
        context.ReadyTransactions.Resize(maxReadyQueries);
    }

    LOG_I("Creating " << terminalsCount << " terminals and " << treadsCount
        << " workers on " << cpuCount << " cpus");

    std::atomic<bool> stopWarmap{false};
    Terminals.reserve(terminalsCount);
    for (size_t i = 0; i < terminalsCount; ++i) {
        // note, that terminal adds itself to ready terminals
        Terminals.emplace_back(
            i,
            *this,
            drivers[i % drivers.size()],
            TerminalsStopSource.get_token(),
            stopWarmap,
            Log);
    }

    Threads.reserve(treadsCount);
    for (size_t i = 0; i < treadsCount; ++i) {
        Threads.emplace_back([this, i]() {
            RunThread(i);
        });
    }

    Clock::time_point now = Clock::now();

    // warmup loop
    // TODO
    //LOG_D("Starting ")
    //while (!StopByInterrupt.stop_requested()) {
    //}

    stopWarmap.store(true, std::memory_order_relaxed);

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

void TPCCRunner::TaskReady(TTerminalTask::TCoroHandle handle, size_t terminalId) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    TGuard guard(context.Lock);
    if (!context.ReadyTerminals.TryPush(std::move(handle))) {
        // just a sanity check, should never happen
        std::cerr << "Error: failed to push ready terminal" << std::endl;
        std::terminate();
    }
}

void TPCCRunner::TaskReady(TTransactionTask::TCoroHandle handle, size_t terminalId) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    TGuard guard(context.Lock);
    if (!context.ReadyTransactions.TryPush(std::move(handle))) {
        // just a sanity check, should never happen
        std::cerr << "Error: failed to push ready query" << std::endl;
        std::terminate();
    }
}

void TPCCRunner::RunThread(size_t threadId) {
    auto& context = PerThreadContext[threadId];

    while (!ThreadsStopSource.stop_requested()) {
        bool hasProgress = false;
        std::optional<TTransactionTask::TCoroHandle> queryHandle;
        {
            TGuard guard(context.Lock);
            TTransactionTask::TCoroHandle h;
            if (context.ReadyTransactions.TryPop(h)) {
                queryHandle = std::move(h);
            }
        }

        if (queryHandle) {
            hasProgress = true;
            if (!queryHandle->done()) {
                LOG_D("Thread " << threadId << " resumed query");
                queryHandle->resume();
            }
        }

        // TODO: limit max number of active terminals (or queries, which is the same)
        std::optional<TTerminalTask::TCoroHandle> terminalHandle;
        {
            TGuard guard(context.Lock);
            TTerminalTask::TCoroHandle h;
            if (context.ReadyTerminals.TryPop(h)) {
                terminalHandle = std::move(h);
            }
        }

        if (terminalHandle) {
            hasProgress = true;
            if (!terminalHandle->done()) {
                LOG_D("Thread " << threadId << " resumed terminal");
                terminalHandle->resume();
            }
        }

        if (!hasProgress) {
            std::this_thread::sleep_for(SleepUsecIfNoProgress);
        }
    }
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

} // namesapce NYdb::NTPCC
