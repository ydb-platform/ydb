#pragma once

#include "histogram.h"

#include <library/cpp/threading/future/future.h>

#include <exception>
#include <coroutine>
#include <memory>
#include <utility>

class TLog;

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

using Clock = std::chrono::steady_clock;

//-----------------------------------------------------------------------------

// We have two types of coroutines:
// * Outter or Internal (in TPC-C this is a terminal task). It can sleep (co_awaiting) or wait for another "inner" task.
// * Inner or External (in TPC-C this is a transaction task). E.g. it might perform async requests to YDB.
// when it is finished, terminal task continues. It is called external, because usually it has to use
// thread-safe interface of ITaskQueue (future used to co_await is normally set by another thread).

template <typename T>
struct TTask {
    struct TPromiseType;
    using TCoroHandle = std::coroutine_handle<TPromiseType>;

    struct TPromiseType {
        TTask get_return_object() {
            return TTask{std::coroutine_handle<TPromiseType>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() { return {}; }

        auto final_suspend() noexcept {
            struct TFinalAwaiter {
                bool await_ready() noexcept { return false; }
                std::coroutine_handle<> await_suspend(TCoroHandle h) noexcept {
                    if (h.promise().Continuation) {
                        return h.promise().Continuation;
                    }
                    return std::noop_coroutine();
                }
                void await_resume() noexcept {}
            };
            return TFinalAwaiter{};
        }

        template <typename U>
        void return_value(U&& v) {
            Value = std::forward<U>(v);
        }

        void unhandled_exception() {
            Exception = std::current_exception();
        }

        T Value;
        std::exception_ptr Exception;
        std::coroutine_handle<> Continuation;
    };

    using promise_type = TPromiseType;

    TTask(TCoroHandle h)
        : Handle(h)
    {
    }

    // note, default move constructors doesn't null Handle,
    // so that we need to add our own
    TTask(TTask&& other) noexcept
        : Handle(std::exchange(other.Handle, nullptr))
    {}

    TTask& operator=(TTask&& other) noexcept {
        if (this != &other) {
            if (Handle) {
                Handle.destroy();
            }
            Handle = std::exchange(other.Handle, nullptr);
        }
        return *this;
    }

    TTask(const TTask&) = delete;
    TTask& operator=(const TTask&) = delete;

    ~TTask() {
        if (Handle) {
            Handle.destroy();
        }
    }

    // awaitable task

    bool await_ready() const noexcept {
        return Handle.done();
    }

    void await_suspend(std::coroutine_handle<> awaiting) {
        Handle.promise().Continuation = awaiting;
        Handle.resume();  // start inner task
    }

    T&& await_resume() {
        if (Handle.promise().Exception) {
            std::rethrow_exception(Handle.promise().Exception);
        }

        return std::move(Handle.promise().Value);
    }

    TCoroHandle Handle;
};

template <>
struct TTask<void> {
    struct TPromiseType;
    using TCoroHandle = std::coroutine_handle<TPromiseType>;

    struct TPromiseType {
        TTask get_return_object() {
            return TTask{std::coroutine_handle<TPromiseType>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() { return {}; }

        auto final_suspend() noexcept {
            struct TFinalAwaiter {
                bool await_ready() noexcept { return false; }
                std::coroutine_handle<> await_suspend(TCoroHandle h) noexcept {
                    if (h.promise().Continuation) {
                        return h.promise().Continuation;
                    }
                    return std::noop_coroutine();
                }
                void await_resume() noexcept {}
            };
            return TFinalAwaiter{};
        }

        void return_void() {}

        void unhandled_exception() {
            Exception = std::current_exception();
        }

        std::exception_ptr Exception;
        std::coroutine_handle<> Continuation;
    };

    using promise_type = TPromiseType;

    TTask(TCoroHandle h)
        : Handle(h)
    {
    }

    // note, default move constructors doesn't null Handle,
    // so that we need to add our own
    TTask(TTask&& other) noexcept
        : Handle(std::exchange(other.Handle, nullptr))
    {}

    TTask& operator=(TTask&& other) noexcept {
        if (this != &other) {
            if (Handle) {
                Handle.destroy();
            }
            Handle = std::exchange(other.Handle, nullptr);
        }
        return *this;
    }

    TTask(const TTask&) = delete;
    TTask& operator=(const TTask&) = delete;

    ~TTask() {
        if (Handle) {
            Handle.destroy();
        }
    }

    // awaitable task

    bool await_ready() const noexcept {
        return Handle.done();
    }

    void await_suspend(std::coroutine_handle<> awaiting) {
        Handle.promise().Continuation = awaiting;
        Handle.resume();  // start inner task
    }

    void await_resume() {
        if (Handle.promise().Exception) {
            std::rethrow_exception(Handle.promise().Exception);
        }
    }

    TCoroHandle Handle;
};

//-----------------------------------------------------------------------------

class ITaskQueue {
public:
    struct TThreadStats {
        constexpr static size_t BUCKET_COUNT = 4096;
        constexpr static size_t MAX_HIST_VALUE = 32768;

        TThreadStats() = default;

        TThreadStats(const TThreadStats& other) = delete;
        TThreadStats(TThreadStats&& other) = delete;
        TThreadStats& operator=(const TThreadStats& other) = delete;
        TThreadStats& operator=(TThreadStats&& other) = delete;

        void Collect(TThreadStats& dst) {
            dst.InternalTasksSleeping.fetch_add(
                InternalTasksSleeping.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.InternalTasksWaitingInflight.fetch_add(
                InternalTasksWaitingInflight.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.InternalTasksReady.fetch_add(
                InternalTasksReady.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.ExternalTasksReady.fetch_add(
                ExternalTasksReady.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.InternalTasksResumed.fetch_add(
                InternalTasksResumed.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.ExternalTasksResumed.fetch_add(
                ExternalTasksResumed.load(std::memory_order_relaxed), std::memory_order_relaxed);

            dst.ExecutingTime.fetch_add(ExecutingTime.load(std::memory_order_relaxed), std::memory_order_relaxed);
            dst.TotalTime.fetch_add(TotalTime.load(std::memory_order_relaxed), std::memory_order_relaxed);

            TGuard guard(HistLock);
            dst.InternalInflightWaitTimeMs.Add(InternalInflightWaitTimeMs);
            dst.InternalQueueTimeMs.Add(InternalQueueTimeMs);
            dst.ExternalQueueTimeMs.Add(ExternalQueueTimeMs);
        }

        std::atomic<ui64> InternalTasksSleeping{0};
        std::atomic<ui64> InternalTasksWaitingInflight{0};

        std::atomic<ui64> InternalTasksReady{0};
        std::atomic<ui64> ExternalTasksReady{0};

        std::atomic<ui64> InternalTasksResumed{0};
        std::atomic<ui64> ExternalTasksResumed{0};

        std::atomic<double> ExecutingTime{0};
        std::atomic<double> TotalTime{0};

        TSpinLock HistLock;
        THistogram InternalInflightWaitTimeMs{BUCKET_COUNT, MAX_HIST_VALUE};
        THistogram InternalQueueTimeMs{BUCKET_COUNT, MAX_HIST_VALUE};
        THistogram ExternalQueueTimeMs{BUCKET_COUNT, MAX_HIST_VALUE};
    };

    ITaskQueue() = default;
    virtual ~ITaskQueue() = default;

    // non copyable / non moveable
    ITaskQueue(const ITaskQueue&) = delete;
    ITaskQueue& operator=(const ITaskQueue&) = delete;
    ITaskQueue(ITaskQueue&&) = delete;
    ITaskQueue& operator=(ITaskQueue&&) = delete;

    virtual void Run() = 0;
    virtual void Join() = 0;
    virtual void WakeupAndNeverSleep() = 0;

    // functions are called from the thread executing the coroutine, no locks needed

    virtual void TaskReady(std::coroutine_handle<> handle, size_t threadHint) = 0;
    virtual void AsyncSleep(std::coroutine_handle<> handle, size_t threadHint, std::chrono::milliseconds delay) = 0;

    // returns true if task must be suspeded
    virtual bool IncInflight(std::coroutine_handle<> handle, size_t threadHint) = 0;
    virtual void DecInflight() = 0;

    // functions called by other threads

    virtual void TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t threadHint) = 0;

    // Check if current thread is one of the task queue threads
    virtual bool CheckCurrentThread() const = 0;

    virtual void CollectStats(size_t threadIndex, TThreadStats& dst) = 0;

    virtual size_t GetRunningCount() const = 0;
};

std::unique_ptr<ITaskQueue> CreateTaskQueue(
    size_t threadCount,
    size_t maxRunningInternal,
    size_t maxReadyInternal,
    size_t maxReadyExternal,
    std::shared_ptr<TLog> log);

//-----------------------------------------------------------------------------

// used to implement AsyncSleep()
struct TSuspend {
    TSuspend(ITaskQueue& taskQueue, size_t threadHint, std::chrono::milliseconds delay)
        : TaskQueue(taskQueue)
        , ThreadHint(threadHint)
        , Delay(delay)
    {
    }

    bool await_ready() {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        TaskQueue.AsyncSleep(handle, ThreadHint, Delay);
    }

    void await_resume() {}

    ITaskQueue& TaskQueue;
    size_t ThreadHint;
    std::chrono::milliseconds Delay;
};

//-----------------------------------------------------------------------------

// we don't use the one from library/cpp/threading/future/core/coroutine_traits.h, because we
// want to resume terminal in its thread from IReadyTaskQueue and not to resume
// by SDK thread who set the promise value.
template <typename T>
struct TSuspendWithFuture {
    TSuspendWithFuture(NThreading::TFuture<T>& future, ITaskQueue& taskQueue, size_t threadHint)
        : Future(future)
        , TaskQueue(taskQueue)
        , ThreadHint(threadHint)
    {}

    TSuspendWithFuture() = delete;
    TSuspendWithFuture(const TSuspendWithFuture&) = delete;
    TSuspendWithFuture& operator=(const TSuspendWithFuture&) = delete;

    bool await_ready() {
        return Future.HasValue();
    }

    void await_suspend(std::coroutine_handle<> handle) {
        // we use subscribe as async wait and don't handle result here: resumed task will
        Future.NoexceptSubscribe([this, handle](const NThreading::TFuture<T>&) {
            TaskQueue.TaskReadyThreadSafe(handle, ThreadHint);
        });
    }

    T await_resume() {
        return Future.GetValue();
    }

    NThreading::TFuture<T>& Future;
    ITaskQueue& TaskQueue;
    size_t ThreadHint;
};

template <>
struct TSuspendWithFuture<void> {
    TSuspendWithFuture(NThreading::TFuture<void>& future, ITaskQueue& taskQueue, size_t threadHint)
        : Future(future)
        , TaskQueue(taskQueue)
        , ThreadHint(threadHint)
    {}

    TSuspendWithFuture() = delete;
    TSuspendWithFuture(const TSuspendWithFuture&) = delete;
    TSuspendWithFuture& operator=(const TSuspendWithFuture&) = delete;

    bool await_ready() {
        return Future.HasValue();
    }

    void await_suspend(std::coroutine_handle<> handle) {
        // we use subscribe as async wait and don't handle result here: resumed task will
        Future.NoexceptSubscribe([this, handle](const NThreading::TFuture<void>&) {
            TaskQueue.TaskReadyThreadSafe(handle, ThreadHint);
        });
    }

    void await_resume() {
        Future.GetValue();
    }

    NThreading::TFuture<void>& Future;
    ITaskQueue& TaskQueue;
    size_t ThreadHint;
};

// used by coroutine which might be started outside TaskQueue to await, when it is in TaskQueue
struct TTaskReady {
    TTaskReady(ITaskQueue& taskQueue, size_t threadHint)
        : TaskQueue(taskQueue)
        , ThreadHint(threadHint)
    {}

    bool await_ready() { return false; }

    bool await_suspend(std::coroutine_handle<> h) {
        if (TaskQueue.CheckCurrentThread()) {
            return false;
        }
        TaskQueue.TaskReadyThreadSafe(h, ThreadHint);
        return true;
    }

    void await_resume() {}

    ITaskQueue& TaskQueue;
    size_t ThreadHint;
};

struct TTaskHasInflight {
    TTaskHasInflight(ITaskQueue& taskQueue, size_t threadHint)
        : TaskQueue(taskQueue)
        , ThreadHint(threadHint)
    {}

    bool await_ready() { return false; }

    bool await_suspend(std::coroutine_handle<> h) {
        return TaskQueue.IncInflight(h, ThreadHint);
    }

    void await_resume() {}

    ITaskQueue& TaskQueue;
    size_t ThreadHint;
};

} // namespace NYdb::NTPCC
