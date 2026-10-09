#pragma once

#include "async_abi.h"

#include <coroutine>
#include <optional>
#include <utility>

namespace NYdb::NWasm::NAsync {

template <class T> class [[nodiscard]] TTask {
public:
    struct promise_type {
        std::optional<T> Value;
        std::coroutine_handle<> Continuation = std::noop_coroutine();

        TTask get_return_object() {
            return TTask(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        std::suspend_always initial_suspend() noexcept {
            return {};
        }

        struct TFinalAwaiter {
            bool await_ready() noexcept {
                return false;
            }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                return h.promise().Continuation;
            }
            void await_resume() noexcept {}
        };

        TFinalAwaiter final_suspend() noexcept {
            return {};
        }
        void return_value(T value) {
            Value.emplace(std::move(value));
        }
        void unhandled_exception() noexcept {
            __builtin_trap();
        }
    };

    using TCoroutine = std::coroutine_handle<promise_type>;

    explicit TTask(TCoroutine coroutine)
        : Coroutine_(coroutine)
    {}

    TTask(TTask&& other) noexcept
        : Coroutine_(std::exchange(other.Coroutine_, {}))
    {}

    TTask(const TTask&) = delete;
    TTask& operator=(const TTask&) = delete;
    TTask& operator=(TTask&&) = delete;

    ~TTask() {
        if (Coroutine_) {
            Coroutine_.destroy();
        }
    }

    bool Done() const {
        return Coroutine_.done();
    }
    TCoroutine Handle() const {
        return Coroutine_;
    }
    T TakeResult() {
        return std::move(*Coroutine_.promise().Value);
    }

    struct TAwaiter {
        TCoroutine Coroutine;
        bool await_ready() noexcept {
            return Coroutine.done();
        }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
            Coroutine.promise().Continuation = caller;
            return Coroutine;
        }
        T await_resume() {
            return std::move(*Coroutine.promise().Value);
        }
    };

    TAwaiter operator co_await() const {
        return {Coroutine_};
    }

private:
    TCoroutine Coroutine_;
};

struct TCallContext {
    // A nested Task may suspend: resume the leaf, not its suspended parent.
    std::coroutine_handle<> Runnable;

    void Resume() {
        auto runnable = std::exchange(Runnable, {});
        if (!runnable || runnable.done()) {
            __builtin_trap();
        }
        runnable.resume();
    }
};

class TOperation {
public:
    TOperation(const void* request, uint64_t size)
        : Handle_(WasmAsyncOperationStart(request, size))
    {}

    TOperation(TOperation&& other) noexcept
        : Handle_(std::exchange(other.Handle_, 0))
    {}

    TOperation(const TOperation&) = delete;
    TOperation& operator=(const TOperation&) = delete;
    TOperation& operator=(TOperation&&) = delete;

    ~TOperation() {
        if (Handle_) {
            WasmAsyncOperationDrop(Handle_);
        }
    }

    THandle Handle() const {
        return Handle_;
    }

    static TOperation Timer(uint64_t delayMicroseconds) {
        return TOperation(WasmAsyncTimerStart(delayMicroseconds));
    }

    struct TAwaiter {
        THandle Handle;
        TCallContext& Context;

        bool await_ready() const {
            return WasmAsyncOperationPoll(Handle) != static_cast<uint32_t>(EOperationStatus::Pending);
        }

        void await_suspend(std::coroutine_handle<> coroutine) {
            Context.Runnable = coroutine;
            WasmAsyncCallWait(Handle);
        }

        EOperationStatus await_resume() const {
            return static_cast<EOperationStatus>(WasmAsyncOperationPoll(Handle));
        }
    };

    TAwaiter Wait(TCallContext& context) const {
        return {Handle_, context};
    }

    uint64_t Size() const {
        return WasmAsyncOperationSize(Handle_);
    }
    void Read(void* output, uint64_t size) const {
        WasmAsyncOperationRead(Handle_, output, size);
    }
    void Cancel() const {
        WasmAsyncOperationCancel(Handle_);
    }

private:
    explicit TOperation(THandle handle)
        : Handle_(handle)
    {}

    THandle Handle_;
};

} // namespace NYdb::NWasm::NAsync
