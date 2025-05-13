#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/threading/future/future.h>

#include <exception>
#include <coroutine>
#include <utility>

namespace NYdb::NTPCC {

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
                void await_suspend(TCoroHandle h) noexcept {
                    if (h.promise().Continuation) {
                        h.promise().Continuation.resume(); // resume outer task (terminal)
                    }
                }
                void await_resume() noexcept {}
            };
            return TFinalAwaiter{};
        }

        template <typename U>
        void return_value(U&& v) {
            Value = std::forward<U>(v);
        }

        // TODO
        void unhandled_exception() { std::terminate(); }

        T Value;
        std::coroutine_handle<> Continuation;
    };

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

    // TODO: maybe Release() with move?
    T& Get() {
        return Handle.promise().Value;
    }

    // avaitable task

    bool await_ready() const noexcept {
        return Handle.done();
    }

    void await_suspend(std::coroutine_handle<> awaiting) {
        Handle.promise().Continuation = awaiting;
        Handle.resume();  // start inner task
    }

    T& await_resume() {
        return Get();
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
                void await_suspend(TCoroHandle h) noexcept {
                    if (h.promise().Continuation) {
                        h.promise().Continuation.resume(); // resume outer task (terminal)
                    }
                }
                void await_resume() noexcept {}
            };
            return TFinalAwaiter{};
        }

        void return_void() {}

        // TODO
        void unhandled_exception() { std::terminate(); }

        std::coroutine_handle<> Continuation;
    };

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

    // avaitable task

    bool await_ready() const noexcept {
        return Handle.done();
    }

    void await_suspend(std::coroutine_handle<> awaiting) {
        Handle.promise().Continuation = awaiting;
        Handle.resume();  // start inner task
    }

    void await_resume() {}

    TCoroHandle Handle;
};

using TTerminalTask = TTask<void>;

// TODO: can we move it?
struct TTransactionResult {
};

using TTransactionTask = TTask<TTransactionResult>;

//-----------------------------------------------------------------------------

class IReadyTaskQueue {
public:
    virtual ~IReadyTaskQueue() = default;

    virtual void TaskReady(TTerminalTask::TCoroHandle handle, size_t terminalId) = 0;
    virtual void TaskReady(TTransactionTask::TCoroHandle handle, size_t terminalId) = 0;
};

//-----------------------------------------------------------------------------

// we don't use library/cpp/threading/future/core/coroutine_traits.h, because we
// want to resume terminal in its thread from IReadyTaskQueue and not to resume
// by SDK thread who set the promise value.

template <typename T>
struct TSuspendWithFuture {
    TSuspendWithFuture(NThreading::TFuture<T>& future, IReadyTaskQueue& taskQueue, size_t terminalId)
        : Future(future)
        , TaskQueue(taskQueue)
        , TerminalId(terminalId)
    {}

    TSuspendWithFuture() = delete;
    TSuspendWithFuture(const TSuspendWithFuture&) = delete;
    TSuspendWithFuture& operator=(const TSuspendWithFuture&) = delete;

    bool await_ready() {
        return false;
    }

    void await_suspend(TTransactionTask::TCoroHandle handle) {
        // we use subscribe as async wait and don't handle result here: resumed task will
        if constexpr (std::is_void_v<T>) {
            Future.NoexceptSubscribe([this, handle](const NThreading::TFuture<T>&) {
                TaskQueue.TaskReady(handle, TerminalId);
            });
        } else {
            Future.NoexceptSubscribe([this, handle](const NThreading::TFuture<T>&) {
                TaskQueue.TaskReady(handle, TerminalId);
            });
        }
    }

    void await_resume() {}

    NThreading::TFuture<T>& Future;
    IReadyTaskQueue& TaskQueue;
    size_t TerminalId;
};

} // namesapce NYdb::NTPCC
