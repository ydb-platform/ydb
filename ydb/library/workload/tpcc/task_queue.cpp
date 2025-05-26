#include "task_queue.h"

#include "circular_queue.h"
#include "timer_queue.h"
#include "log.h"

#include <util/system/spinlock.h>
#include <util/system/thread.h>

#include <chrono>
#include <thread>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

constexpr auto SleepUsecIfNoProgress = std::chrono::microseconds(100);

// Initialize thread-local variable
thread_local int TaskQueueThreadId = -1;

//-----------------------------------------------------------------------------

struct alignas(64) TPerThreadContext {
    TPerThreadContext() = default;

    // thread-safe because accessed by this thread only

    TBinnedTimerQueue<std::coroutine_handle<>> SleepingTasks;
    TCircularQueue<std::coroutine_handle<>> ReadyTasksInternal;
    TCircularQueue<std::coroutine_handle<>> InflightWaitingTasksInternal;

    // accessed by other threads to add ready coroutine
    TSpinLock ReadyTasksLock;
    TCircularQueue<std::coroutine_handle<>> ReadyTasksExternal;
};

//-----------------------------------------------------------------------------

class TTaskQueue : public ITaskQueue {
public:
    TTaskQueue(size_t threadCount,
               size_t maxRunningInternal,
               size_t maxReadyInternal,
               size_t maxReadyExternal,
               std::shared_ptr<TLog> log);

    ~TTaskQueue() {
        Join();
    }

    // ITaskQueue

    void Run() override;
    void Join() override;

    void TaskReady(std::coroutine_handle<>, size_t threadHint) override;
    void AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::milliseconds delay) override;
    bool IncInflight(std::coroutine_handle<> handle, size_t threadHint) override;
    void DecInflight() override;

    void TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) override;

    bool CheckCurrentThread() const override;

private:
    void RunThread(size_t threadId);
    void HandleQueueFull(const char* queueType);

private:
    size_t ThreadCount;
    size_t MaxRunningInternal;
    size_t MaxReadyInternal;
    size_t MaxReadyExternal;
    std::shared_ptr<TLog> Log;

    std::atomic<size_t> RunningInternalCount{0};

    std::stop_source ThreadsStopSource;
    std::vector<std::thread> Threads;
    std::vector<TPerThreadContext> PerThreadContext;
};

TTaskQueue::TTaskQueue(size_t threadCount,
            size_t maxRunningInternal,
            size_t maxReadyInternal,
            size_t maxReadyExternal,
            std::shared_ptr<TLog> log)
    : ThreadCount(threadCount)
    , MaxRunningInternal(maxRunningInternal)
    , MaxReadyInternal(maxReadyInternal)
    , MaxReadyExternal(maxReadyExternal)
    , Log(std::move(log))
{
    Y_UNUSED(MaxRunningInternal);
    if (ThreadCount == 0) {
        LOG_E("Zero TaskQueue threads");
        throw std::invalid_argument("Thread count must be greater than zero");
    }

    // usually almost all internal tasks (at leat in TPC-C) sleep and we have a long timer queue
    const size_t maxSleepingInternal = MaxReadyInternal;
    constexpr size_t timerBucketSize = 100;
    const size_t timerBucketCount = (maxSleepingInternal + timerBucketSize - 1) / timerBucketSize;

    PerThreadContext.resize(ThreadCount);
    for (auto& context: PerThreadContext) {
        context.SleepingTasks.Resize(timerBucketCount, timerBucketSize);
        context.ReadyTasksInternal.Resize(MaxReadyInternal);
        context.InflightWaitingTasksInternal.Resize(MaxReadyInternal);
        context.ReadyTasksExternal.Resize(MaxReadyExternal);
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

void TTaskQueue::TaskReady(std::coroutine_handle<> handle, size_t threadHint) {
    auto index = threadHint % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    if (!context.ReadyTasksInternal.TryPush(std::move(handle))) {
        HandleQueueFull("internal");
    }
}

void TTaskQueue::AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::milliseconds delay) {
    auto index = threadHint % PerThreadContext.size();
    auto& context = PerThreadContext[index];
    context.SleepingTasks.Add(delay, std::move(handle));
}

bool TTaskQueue::IncInflight(std::coroutine_handle<> handle, size_t threadHint) {
    if (MaxRunningInternal == 0) {
        return false;
    }

    auto runningCount = RunningInternalCount.fetch_add(1, std::memory_order_relaxed);
    if (runningCount < MaxRunningInternal) {
        return false; // do not suspend
    }

    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);

    auto index = threadHint % PerThreadContext.size();
    auto& context = PerThreadContext[index];
    if (!context.InflightWaitingTasksInternal.TryPush(std::move(handle))) {
        HandleQueueFull("inflight-waiting");
    }

    return true;
}

void TTaskQueue::DecInflight() {
    if (MaxRunningInternal == 0) {
        return;
    }
    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);
}

void TTaskQueue::TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) {
    auto index = threadHint % PerThreadContext.size();
    auto& context = PerThreadContext[index];

    TGuard guard(context.ReadyTasksLock);
    if (!context.ReadyTasksExternal.TryPush(std::move(handle))) {
        HandleQueueFull("external");
    }
}

void TTaskQueue::RunThread(size_t threadId) {
    TaskQueueThreadId = static_cast<int>(threadId);
    TThread::SetCurrentThreadName((TStringBuilder() << "task_queue_" << threadId).c_str());

    // TODO: just set seed? Or use random generator per internal task?

    auto& context = PerThreadContext[threadId];

    while (!ThreadsStopSource.stop_requested()) {
        auto now = Clock::now();

        bool hasProgress = false;
        if (MaxRunningInternal != 0) {
            while (!context.InflightWaitingTasksInternal.Empty()
                    && RunningInternalCount.load(std::memory_order_relaxed) < MaxRunningInternal) {
                auto runningCount = RunningInternalCount.fetch_add(1, std::memory_order_relaxed);
                if (runningCount >= MaxRunningInternal) {
                    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);
                    break;
                }

                std::coroutine_handle<> handleInternal;
                if (context.InflightWaitingTasksInternal.TryPop(handleInternal)) {
                    hasProgress = true;
                    if (handleInternal && !handleInternal.done()) {
                        LOG_D("Thread " << threadId << " resumed task waited for inflight (internal)");
                        handleInternal.resume();
                    }
                }
            }
        }

        while (!context.SleepingTasks.Empty() && context.SleepingTasks.GetNextDeadline() <= now) {
            auto handle = context.SleepingTasks.PopFront().Value;
            if (!context.ReadyTasksInternal.TryPush(std::move(handle))) {
                HandleQueueFull("internal (awakened)");
            }
        }

        std::optional<std::coroutine_handle<>> handleExternal;
        {
            TGuard guard(context.ReadyTasksLock);
            std::coroutine_handle<> h;
            if (context.ReadyTasksExternal.TryPop(h)) {
                handleExternal = std::move(h);
            }
        }

        if (handleExternal && *handleExternal && !handleExternal->done()) {
            hasProgress = true;
            LOG_T("Thread " << threadId << " resumed task (external)");
            handleExternal->resume();
        }

        std::coroutine_handle<> handleInternal;
        if (context.ReadyTasksInternal.TryPop(handleInternal)) {
            hasProgress = true;
            if (handleInternal && !handleInternal.done()) {
                LOG_D("Thread " << threadId << " resumed task (internal)");
                handleInternal.resume();
            }
        }

        if (!hasProgress) {
            std::this_thread::sleep_for(SleepUsecIfNoProgress);
        }
    }
}

bool TTaskQueue::CheckCurrentThread() const {
    return TaskQueueThreadId >= 0;
}

} // anonymous

//-----------------------------------------------------------------------------

std::unique_ptr<ITaskQueue> CreateTaskQueue(
    size_t threadCount,
    size_t maxRunningInternal,
    size_t maxReadyInternal,
    size_t maxReadyExternal,
    std::shared_ptr<TLog> log)
{
    return std::make_unique<TTaskQueue>(
        threadCount,
        maxRunningInternal,
        maxReadyInternal,
        maxReadyExternal,
        std::move(log));
}

} // namespace NYdb::NTPCC
