#pragma once
#include "task_result.h"
#include <util/system/yassert.h>
#include <coroutine>
#include <utility>

namespace NActors {

    template<class T>
    class TTask;

    /**
     * This exception is commonly thrown when task is cancelled
     */
    class TTaskCancelled : public std::exception {
    public:
        const char* what() const noexcept {
            return "Task cancelled";
        }
    };

    namespace NDetail {

        template<class T>
        class TTaskPromise;

        template<class T>
        using TTaskHandle = std::coroutine_handle<TTaskPromise<T>>;

        template<class T>
        class TTaskAwaiter {
        public:
            explicit TTaskAwaiter(TTaskHandle<T> handle)
                : Handle(handle)
            {
                Y_DEBUG_ABORT_UNLESS(Handle);
            }

            TTaskAwaiter(TTaskAwaiter&& rhs)
                : Handle(std::exchange(rhs.Handle, {}))
            {}

            TTaskAwaiter& operator=(const TTaskAwaiter&) = delete;
            TTaskAwaiter& operator=(TTaskAwaiter&&) = delete;

            ~TTaskAwaiter() noexcept {
                if (Handle) {
                    Handle.destroy();
                }
            }

            // We can only await a task that has not started yet
            static bool await_ready() noexcept { return false; }

            // Some arbitrary continuation c suspended and awaits the task
            TTaskHandle<T> await_suspend(std::coroutine_handle<> c) noexcept {
                Y_DEBUG_ABORT_UNLESS(Handle);
                Handle.promise().SetContinuation(c);
                return Handle;
            }

            TTaskResult<T>&& await_resume() noexcept {
                Y_DEBUG_ABORT_UNLESS(Handle);
                return std::move(Handle.promise().Result);
            }

        private:
            TTaskHandle<T> Handle;
        };

        template<class T>
        class TTaskResultAwaiter final : public TTaskAwaiter<T> {
        public:
            using TTaskAwaiter<T>::TTaskAwaiter;

            T&& await_resume() {
                return TTaskAwaiter<T>::await_resume().Value();
            }
        };

        template<>
        class TTaskResultAwaiter<void> final : public TTaskAwaiter<void> {
        public:
            using TTaskAwaiter<void>::TTaskAwaiter;

            void await_resume() {
                TTaskAwaiter<void>::await_resume().Value();
            }
        };

        template<class T>
        class TTaskResultHandlerBase {
        public:
            void unhandled_exception() noexcept {
                Result.SetException(std::current_exception());
            }

        protected:
            TTaskResult<T> Result;
        };

        template<class T>
        class TTaskResultHandler : public TTaskResultHandlerBase<T> {
        public:
            template<class TResult>
            void return_value(TResult&& value) {
                this->Result.SetValue(std::forward<TResult>(value));
            }
        };

        template<>
        class TTaskResultHandler<void> : public TTaskResultHandlerBase<void> {
        public:
            void return_void() noexcept {
                this->Result.SetValue();
            }
        };

        template<class T>
        class TTaskPromise final
            : public TTaskResultHandler<T>
        {
            friend class TTaskAwaiter<T>;

        public:
            TTask<T> get_return_object() noexcept;

            static auto initial_suspend() noexcept { return std::suspend_always{}; }

            struct TFinalSuspend {
                static bool await_ready() noexcept { return false; }
                static void await_resume() noexcept { Y_ABORT("unexpected coroutine resume"); }

                static std::coroutine_handle<> await_suspend(std::coroutine_handle<TTaskPromise<T>> h) noexcept {
                    auto next = std::exchange(h.promise().Continuation, std::noop_coroutine());
                    Y_DEBUG_ABORT_UNLESS(next, "Task finished without a continuation");
                    return next;
                }
            };

            static auto final_suspend() noexcept { return TFinalSuspend{}; }

        private:
            void SetContinuation(std::coroutine_handle<> continuation) noexcept {
                Y_DEBUG_ABORT_UNLESS(!Continuation, "Task can only be awaited once");
                Continuation = continuation;
            }

        private:
            std::coroutine_handle<> Continuation;
        };

    } // namespace NDetail

    /**
     * Represents a task that has not been started yet
     */
    template<class T>
    class TTask final {
    public:
        using promise_type = NDetail::TTaskPromise<T>;
        using value_type = T;

    public:
        TTask() noexcept = default;

        explicit TTask(NDetail::TTaskHandle<T> handle) noexcept
            : Handle(handle)
        {}

        TTask(TTask&& rhs) noexcept
            : Handle(std::exchange(rhs.Handle, {}))
        {}

        ~TTask() {
            if (Handle) {
                Handle.destroy();
            }
        }

        TTask& operator=(TTask&& rhs) noexcept {
            if (Y_LIKELY(this != &rhs)) {
                auto handle = std::exchange(Handle, {});
                Handle = std::exchange(rhs.Handle, {});
                if (handle) {
                    handle.destroy();
                }
            }
            return *this;
        }

        /**
         * Returns true for a valid task object
         */
        explicit operator bool() const noexcept {
            return bool(Handle);
        }

        /**
         * Starts task and returns TTaskResult<T> when it completes
         */
        auto WhenDone() && noexcept {
            Y_DEBUG_ABORT_UNLESS(Handle, "Cannot await an empty task");
            return NDetail::TTaskAwaiter<T>(std::exchange(Handle, {}));
        }

        /**
         * Starts task and returns its result when it completes
         */
        auto operator co_await() && noexcept {
            Y_DEBUG_ABORT_UNLESS(Handle, "Cannot await an empty task");
            return NDetail::TTaskResultAwaiter<T>(std::exchange(Handle, {}));
        }

    private:
        NDetail::TTaskHandle<T> Handle;
    };

    namespace NDetail {

        template<class T>
        inline TTask<T> TTaskPromise<T>::get_return_object() noexcept {
            return TTask<T>(TTaskHandle<T>::from_promise(*this));
        }

    } // namespace NDetail

} // namespace NActors
