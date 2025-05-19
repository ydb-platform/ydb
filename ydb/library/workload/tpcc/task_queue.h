#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

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
// * Terminal task. It can sleep (co_awaiting) or wait for another task representing transaction.
// * Transaction task. It performs async requests to YDB. When it is finished, it should continue terminal's task.
// For now we allocate transaction coroutine each time we need it. However, we could try to reuse the coroutine and
// avoid extra allocation / deallocation (in SDK/gRPC there will be allocations anyway).

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

using TTerminalTask = TTask<void>;

// TODO: can we move it?
struct TTransactionResult {
};

using TTransactionTask = TTask<TTransactionResult>;

//-----------------------------------------------------------------------------

class ITaskQueue {
public:
    ITaskQueue() = default;
    virtual ~ITaskQueue() = default;

    // non copyable / non moveable
    ITaskQueue(const ITaskQueue&) = delete;
    ITaskQueue& operator=(const ITaskQueue&) = delete;
    ITaskQueue(ITaskQueue&&) = delete;
    ITaskQueue& operator=(ITaskQueue&&) = delete;

    virtual void Run() = 0;
    virtual void Join() = 0;

    // functions are called from the thread executing the coroutine, no locks needed

    virtual void TaskReady(std::coroutine_handle<> handle, size_t terminalId) = 0;
    virtual void AsyncSleep(std::coroutine_handle<> handle, size_t terminalId, std::chrono::milliseconds delay) = 0;

    // functions called by other threads

    virtual void TaskReadyThreadSafe(std::coroutine_handle<> handle, size_t terminalId) = 0;
};

std::unique_ptr<ITaskQueue> CreateTaskQueue(
    size_t threadCount,
    size_t maxReadyTerminals,
    size_t maxReadyTransactions,
    std::shared_ptr<TLog> log);

//-----------------------------------------------------------------------------

// used to implement AsyncSleep()
struct TSuspend {
    TSuspend(ITaskQueue& taskQueue, size_t terminalId, std::chrono::milliseconds delay)
        : TaskQueue(taskQueue)
        , TerminalId(terminalId)
        , Delay(delay)
    {
    }

    bool await_ready() {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        TaskQueue.AsyncSleep(handle, TerminalId, Delay);
    }

    void await_resume() {}

    ITaskQueue& TaskQueue;
    size_t TerminalId;
    std::chrono::milliseconds Delay;
};

//-----------------------------------------------------------------------------

// we don't use library/cpp/threading/future/core/coroutine_traits.h, because we
// want to resume terminal in its thread from IReadyTaskQueue and not to resume
// by SDK thread who set the promise value.

template <typename T>
struct TSuspendWithFuture {
    TSuspendWithFuture(NThreading::TFuture<T>& future, ITaskQueue& taskQueue, size_t terminalId)
        : Future(future)
        , TaskQueue(taskQueue)
        , TerminalId(terminalId)
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
            TaskQueue.TaskReadyThreadSafe(handle, TerminalId);
        });
    }

    T await_resume() {
        return Future.GetValue();
    }

    NThreading::TFuture<T>& Future;
    ITaskQueue& TaskQueue;
    size_t TerminalId;
};

template <>
struct TSuspendWithFuture<void> {
    TSuspendWithFuture(NThreading::TFuture<void>& future, ITaskQueue& taskQueue, size_t terminalId)
        : Future(future)
        , TaskQueue(taskQueue)
        , TerminalId(terminalId)
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
            TaskQueue.TaskReadyThreadSafe(handle, TerminalId);
        });
    }

    void await_resume() {
        Future.GetValue();
    }

    NThreading::TFuture<void>& Future;
    ITaskQueue& TaskQueue;
    size_t TerminalId;
};

} // namespace NYdb::NTPCC
