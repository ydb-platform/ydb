#include "task_queue.h"

#include "circular_queue.h"
#include "timer_queue.h"
#include "log.h"

#include <util/system/thread.h>

#include <chrono>
#include <thread>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr auto SleepUsecIfNoProgress = std::chrono::microseconds(100);

//-----------------------------------------------------------------------------

struct alignas(64) TPerThreadContext {
    TPerThreadContext() = default;

    // thread-safe because accessed by this thread only
    TBinnedTimerQueue<TTerminalTask::TCoroHandle> SleepingTerminals;
    TCircularQueue<TTerminalTask::TCoroHandle> ReadyTerminals;

    // accessed by other threads to add ready coroutine
    TSpinLock ReadyTransactionsLock;
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

    void AsyncSleep(TTerminalTask::TCoroHandle handle, size_t terminalId, std::chrono::milliseconds delay) override;

private:
    void RunThread(size_t threadId);
    void HandleQueueFull(const char* queueType);

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
    if (ThreadCount == 0) {
        LOG_E("Zero task queue threads");
        throw std::invalid_argument("Thread count must be greater than zero");
    }

    // usually almost all terminals sleep and we have a long timer queue
    const size_t maxSleepingTerminals = MaxReadyTerminals;
    constexpr size_t timerBucketSize = 100;
    const size_t timerBucketCount = (maxSleepingTerminals + timerBucketSize - 1) / timerBucketSize;

    PerThreadContext.resize(ThreadCount);
    for (auto& context: PerThreadContext) {
        context.SleepingTerminals.Resize(timerBucketCount, timerBucketSize);
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

void TTaskQueue::HandleQueueFull(const char* queueType) {
    LOG_E("Failed to push ready " << queueType << ", queue is full");
    throw std::runtime_error(std::string("Task queue is full: ") + queueType);
}

void TTaskQueue::TaskReady(TTerminalTask::TCoroHandle handle, size_t terminalId) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    if (!context.ReadyTerminals.TryPush(std::move(handle))) {
        HandleQueueFull("terminal");
    }
}

void TTaskQueue::TaskReady(TTransactionTask::TCoroHandle handle, size_t terminalId) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    TGuard guard(context.ReadyTransactionsLock);
    if (!context.ReadyTransactions.TryPush(std::move(handle))) {
        HandleQueueFull("transaction");
    }
}

void TTaskQueue::AsyncSleep(TTerminalTask::TCoroHandle handle, size_t terminalId, std::chrono::milliseconds delay) {
    auto index = terminalId % PerThreadContext.size();
    auto& context = PerThreadContext[index];
    context.SleepingTerminals.Add(delay, std::move(handle));
}

void TTaskQueue::RunThread(size_t threadId) {
    TThread::SetCurrentThreadName((TStringBuilder() << "task_queue_" << threadId).c_str());

    auto& context = PerThreadContext[threadId];

    while (!ThreadsStopSource.stop_requested()) {
        auto now = Clock::now();

        while (context.SleepingTerminals.GetNextDeadline() <= now) {
            auto handle = context.SleepingTerminals.PopFront().Value;
            if (!context.ReadyTerminals.TryPush(std::move(handle))) {
                HandleQueueFull("awakened terminal");
            }
        }

        bool hasProgress = false;
        std::optional<TTransactionTask::TCoroHandle> queryHandle;
        {
            TGuard guard(context.ReadyTransactionsLock);
            TTransactionTask::TCoroHandle h;
            if (context.ReadyTransactions.TryPop(h)) {
                queryHandle = std::move(h);
            }
        }

        if (queryHandle && *queryHandle && !queryHandle->done()) {
            hasProgress = true;
            LOG_D("Thread " << threadId << " resumed transaction");
            queryHandle->resume();
        }

        // TODO: limit max number of active terminals (or queries, which is the same)
        TTerminalTask::TCoroHandle terminalHandle;
        if (context.ReadyTerminals.TryPop(terminalHandle)) {
            hasProgress = true;
            if (terminalHandle && !terminalHandle.done()) {
                LOG_D("Thread " << threadId << " resumed terminal");
                terminalHandle.resume();
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

} // namespace NYdb::NTPCC
