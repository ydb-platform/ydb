#include "task_queue.h"

#include "circular_queue.h"
#include "timer_queue.h"
#include "log.h"

#include <util/system/hp_timer.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

#include <chrono>
#include <thread>

namespace NYdb::NTPCC {

namespace {

//-----------------------------------------------------------------------------

// Initialize thread-local variable
thread_local int TaskQueueThreadId = -1;

//-----------------------------------------------------------------------------

struct THandleWithTs {
    std::coroutine_handle<> Handle;
    THPTimer Timer;
};

struct alignas(64) TPerThreadContext {
    TPerThreadContext() = default;

    TPerThreadContext(const TPerThreadContext&) = delete;
    TPerThreadContext& operator=(const TPerThreadContext&) = delete;
    TPerThreadContext(TPerThreadContext&&) = delete;
    TPerThreadContext& operator=(TPerThreadContext&&) = delete;

    // thread-safe because accessed by task queue thread only

    TBinnedTimerQueue<std::coroutine_handle<>> SleepingTasks;
    TCircularQueue<THandleWithTs> ReadyTasksInternal;
    TCircularQueue<THandleWithTs> InflightWaitingTasksInternal;

    // accessed by other threads to add ready coroutine
    TSpinLock ReadyTasksLock;
    TCircularQueue<THandleWithTs> ReadyTasksExternal;

    ITaskQueue::TThreadStats Stats;
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
    void WakeupAndNeverSleep() override;

    void TaskReady(std::coroutine_handle<>, size_t threadHint) override;
    void AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::milliseconds delay) override;
    bool IncInflight(std::coroutine_handle<> handle, size_t threadHint) override;
    void DecInflight() override;

    void TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) override;

    bool CheckCurrentThread() const override;

    void CollectStats(size_t threadIndex, TThreadStats& dst) override;

    size_t GetRunningCount() const override {
        return RunningInternalCount.load(std::memory_order_relaxed);
    }

private:
    void RunThread(size_t threadId);
    void ProcessSleepingTasks(size_t threadId, TPerThreadContext& context, Clock::time_point now);
    void ProcessInflightQueue(size_t threadId, TPerThreadContext& context, std::optional<ui64>& internalInflightWaitTimeMs);

    void HandleQueueFull(const char* queueType);

private:
    size_t ThreadCount;
    size_t MaxRunningInternal;
    size_t MaxReadyInternal;
    size_t MaxReadyExternal;
    std::shared_ptr<TLog> Log;

    std::atomic<size_t> RunningInternalCount{0};

    std::stop_source ThreadsStopSource;
    std::atomic_flag WakeupAll;
    std::vector<std::thread> Threads;
    std::vector<std::unique_ptr<TPerThreadContext>> PerThreadContext;
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
    for (auto& context : PerThreadContext) {
        context = std::make_unique<TPerThreadContext>();
        context->SleepingTasks.Resize(timerBucketCount, timerBucketSize);
        context->ReadyTasksInternal.Resize(MaxReadyInternal);
        context->InflightWaitingTasksInternal.Resize(MaxReadyInternal);
        context->ReadyTasksExternal.Resize(MaxReadyExternal);
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

void TTaskQueue::WakeupAndNeverSleep() {
    WakeupAll.test_and_set(std::memory_order_relaxed);
}

void TTaskQueue::HandleQueueFull(const char* queueType) {
    LOG_E("Failed to push ready " << queueType << ", queue is full");
    throw std::runtime_error(std::string("Task queue is full: ") + queueType);
}

void TTaskQueue::TaskReady(std::coroutine_handle<> handle, size_t threadHint) {
    auto contextIndex = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[contextIndex];

    if (!context.ReadyTasksInternal.TryPush({std::move(handle), THPTimer()})) {
        HandleQueueFull("internal");
    }
}

void TTaskQueue::AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::milliseconds delay) {
    auto contextIndex = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[contextIndex];
    context.SleepingTasks.Add(delay, std::move(handle));
}

bool TTaskQueue::IncInflight(std::coroutine_handle<> handle, size_t threadHint) {
    auto prevRunningCount = RunningInternalCount.fetch_add(1, std::memory_order_relaxed);

    // no limit
    if (MaxRunningInternal == 0) {
        return false;
    }

    // if we are within the limit, do not suspend
    if (prevRunningCount < MaxRunningInternal) {
        return false;
    }

    // blocked by inflight limit, decrese back and put task to the queue

    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);

    auto index = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[index];
    if (!context.InflightWaitingTasksInternal.TryPush({std::move(handle), THPTimer()})) {
        HandleQueueFull("inflight-waiting");
    }

    return true;
}

void TTaskQueue::DecInflight() {
    RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);
}

void TTaskQueue::TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) {
    auto index = threadHint % PerThreadContext.size();
    auto& context = *PerThreadContext[index];

    TGuard guard(context.ReadyTasksLock);
    if (!context.ReadyTasksExternal.TryPush({std::move(handle), THPTimer()})) {
        HandleQueueFull("external");
    }
}

void TTaskQueue::ProcessSleepingTasks(size_t, TPerThreadContext& context, Clock::time_point now) {
    while (!context.SleepingTasks.Empty() &&
            (context.SleepingTasks.GetNextDeadline() <= now || WakeupAll.test(std::memory_order_relaxed) )) {
        auto handle = context.SleepingTasks.PopFront().Value;
        if (!context.ReadyTasksInternal.TryPush({std::move(handle), THPTimer()})) {
            HandleQueueFull("internal (awakened)");
        }
    }
}

void TTaskQueue::ProcessInflightQueue(
    size_t threadId,
    TPerThreadContext& context,
    std::optional<ui64>& internalInflightWaitTimeMs)
{
    if (MaxRunningInternal == 0) {
        return;
    }

    while (!context.InflightWaitingTasksInternal.Empty()
            && RunningInternalCount.load(std::memory_order_relaxed) < MaxRunningInternal) {
        auto runningCount = RunningInternalCount.fetch_add(1, std::memory_order_relaxed);
        if (runningCount >= MaxRunningInternal) {
            RunningInternalCount.fetch_sub(1, std::memory_order_relaxed);
            break;
        }

        THandleWithTs internalTask;
        if (context.InflightWaitingTasksInternal.TryPop(internalTask)) {
            if (internalTask.Handle && !internalTask.Handle.done()) {
                LOG_D("Thread " << threadId << " marked ready task waited for inflight (internal)");
                internalInflightWaitTimeMs = int(internalTask.Timer.PassedReset() * 1000);
                context.ReadyTasksInternal.TryPush(std::move(internalTask));
            }
        }
    }
}

void TTaskQueue::RunThread(size_t threadId) {
    TaskQueueThreadId = static_cast<int>(threadId);
    TThread::SetCurrentThreadName((TStringBuilder() << "task_queue_" << threadId).c_str());

    // TODO: just set seed? Or use random generator per internal task?

    auto& context = *PerThreadContext[threadId];
    auto& stats = context.Stats;

    THPTimer totalTimer;
    while (!ThreadsStopSource.stop_requested()) {
        double executingTime = 0;

        std::optional<ui64> internalInflightWaitTimeMs;
        std::optional<ui64> internalQueueTimeMs;

        std::vector<ui64> externalQueueTimeLatencies;

        auto now = Clock::now();

        // External tasks are processed first (that's our inflight tasks)

        std::vector<THandleWithTs> externalTasks;
        {
            TGuard guard(context.ReadyTasksLock);
            externalTasks.reserve(context.ReadyTasksExternal.Size());
            THandleWithTs task;
            while (context.ReadyTasksExternal.TryPop(task)) {
                externalTasks.emplace_back(std::move(task));
            }
        }
        stats.ExternalTasksReady.store(externalTasks.size(), std::memory_order_relaxed);

        for (auto& handleWithTs: externalTasks) {
            if (ThreadsStopSource.stop_requested()) {
                break;
            }
            if (handleWithTs.Handle && !handleWithTs.Handle.done()) {
                LOG_T("Thread " << threadId << " resumed task (external)");
                stats.ExternalTasksResumed.fetch_add(1, std::memory_order_relaxed);
                externalQueueTimeLatencies.emplace_back(int(handleWithTs.Timer.Passed() * 1000));

                THPTimer timer;
                handleWithTs.Handle.resume();
                executingTime += timer.Passed();
            }
        }

        ProcessInflightQueue(threadId, context, internalInflightWaitTimeMs);
        ProcessSleepingTasks(threadId, context, now);

        // Internal task

        THandleWithTs internalTask;
        if (context.ReadyTasksInternal.TryPop(internalTask)) {
            if (internalTask.Handle && !internalTask.Handle.done()) {
                LOG_D("Thread " << threadId << " resumed task (internal)");
                stats.InternalTasksResumed.fetch_add(1, std::memory_order_relaxed);
                internalQueueTimeMs = int(internalTask.Timer.Passed() * 1000);
                THPTimer timer;
                internalTask.Handle.resume();
                executingTime += timer.Passed();
            }
        }


        // Update remaining stats

        stats.InternalTasksSleeping.store(context.SleepingTasks.Size(), std::memory_order_relaxed);
        stats.InternalTasksWaitingInflight.store(context.InflightWaitingTasksInternal.Size(), std::memory_order_relaxed);
        stats.InternalTasksReady.store(context.ReadyTasksInternal.Size(), std::memory_order_relaxed);
        stats.ExecutingTime.fetch_add(executingTime, std::memory_order_relaxed);
        stats.TotalTime.fetch_add(totalTimer.PassedReset());
        {
            TGuard guard(stats.HistLock);
            if (internalInflightWaitTimeMs) {
                stats.InternalInflightWaitTimeMs.RecordValue(*internalInflightWaitTimeMs);
            }
            if (internalQueueTimeMs) {
                stats.InternalQueueTimeMs.RecordValue(*internalQueueTimeMs);
            }

            for (auto latency: externalQueueTimeLatencies) {
                stats.ExternalQueueTimeMs.RecordValue(latency);
            }
        }
    }
}

bool TTaskQueue::CheckCurrentThread() const {
    return TaskQueueThreadId >= 0;
}

void TTaskQueue::CollectStats(size_t threadIndex, TThreadStats& dst) {
    if (threadIndex >= PerThreadContext.size()) {
        throw std::runtime_error("Invalid thread index in stats collection");
    }

    auto& context = *PerThreadContext[threadIndex];
    auto& srcStats = context.Stats;
    srcStats.Collect(dst);
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
