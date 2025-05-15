#include "task_queue.h"

#include "circular_queue.h"
#include "log.h"

#include <chrono>
#include <thread>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr auto SleepUsecIfNoProgress = std::chrono::microseconds(100);

//-----------------------------------------------------------------------------

struct alignas(64) TPerThreadContext {
    TPerThreadContext() = default;

    TSpinLock Lock;
    TCircularQueue<TTerminalTask::TCoroHandle> ReadyTerminals;
    TCircularQueue<TTransactionTask::TCoroHandle> ReadyTransactions;
};

//-----------------------------------------------------------------------------

class TTaskQueue : public ITaskQueue {
public:
    TTaskQueue(size_t threadCount,
               size_t maxReadyTerminals,
               size_t maxReadyTransactions,
               std::shared_ptr<TLog> log);

    ~TTaskQueue() {
        Join();
    }

    // ITaskQueue

    void Run() override;
    void Join() override;

    void TaskReady(TTerminalTask::TCoroHandle handle, size_t terminalId) override;
    void TaskReady(TTransactionTask::TCoroHandle handle, size_t terminalId) override;

private:
    void RunThread(size_t threadId);

private:
    size_t ThreadCount;
    size_t MaxReadyTerminals;
    size_t MaxReadyTransactions;

    std::shared_ptr<TLog> Log;

    std::stop_source ThreadsStopSource;
    std::vector<std::thread> Threads;
    std::vector<TPerThreadContext> PerThreadContext;
};

TTaskQueue::TTaskQueue(size_t threadCount,
            size_t maxReadyTerminals,
            size_t maxReadyTransactions,
            std::shared_ptr<TLog> log)
    : ThreadCount(threadCount)
    , MaxReadyTerminals(maxReadyTerminals)
    , MaxReadyTransactions(maxReadyTransactions)
    , Log(std::move(log))
{
    // should never happen
    if (ThreadCount == 0) {
        std::cerr << "zero task queue threads" << std::endl;
        std::terminate();
    }

    PerThreadContext.resize(ThreadCount);
    for (auto& context: PerThreadContext) {
        context.ReadyTerminals.Resize(MaxReadyTerminals);
        context.ReadyTransactions.Resize(MaxReadyTransactions);
    }
}

void TTaskQueue::Run() {
    Threads.reserve(ThreadCount);
    for (size_t i = 0; i < ThreadCount; ++i) {
        Threads.emplace_back([this, i]() {
            RunThread(i);
        });
    }
}

void TTaskQueue::Join() {
    if (ThreadsStopSource.stop_requested()) {
        // already stopped
        return;
    }

    ThreadsStopSource.request_stop();
    for (auto& thread: Threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TTaskQueue::TaskReady(TTerminalTask::TCoroHandle handle, size_t terminalId) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    TGuard guard(context.Lock);
    if (!context.ReadyTerminals.TryPush(std::move(handle))) {
        // just a sanity check, should never happen
        std::cerr << "Error: failed to push ready terminal" << std::endl;
        std::terminate();
    }
}

void TTaskQueue::TaskReady(TTransactionTask::TCoroHandle handle, size_t terminalId) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    TGuard guard(context.Lock);
    if (!context.ReadyTransactions.TryPush(std::move(handle))) {
        // just a sanity check, should never happen
        std::cerr << "Error: failed to push ready query" << std::endl;
        std::terminate();
    }
}

void TTaskQueue::RunThread(size_t threadId) {
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

std::unique_ptr<ITaskQueue> CreateTaskQueue(
    size_t threadCount,
    size_t maxReadyTerminals,
    size_t maxReadyTransactions,
    std::shared_ptr<TLog> log)
{
    return std::make_unique<TTaskQueue>(
        threadCount,
        maxReadyTerminals,
        maxReadyTransactions,
        std::move(log));
}

} // namesapce NYdb::NTPCC
