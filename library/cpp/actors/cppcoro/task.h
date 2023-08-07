#pragma once
#include <util/system/compiler.h>
#include <util/system/yassert.h>
#include <coroutine>
#include <exception>
#include <variant>

namespace NActors {

    template<class T>
    class TTask;

    namespace NDetail {

        class TTaskPromiseBase {
        public:
            static auto initial_suspend() noexcept {
                return std::suspend_always{};
            }

            struct TFinalSuspend {
                static bool await_ready() noexcept { return false; }
                static void await_resume() noexcept { std::terminate(); }

                template<class TPromise>
                static std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> h) noexcept {
                    TTaskPromiseBase& promise = h.promise();
                    return std::exchange(promise.Continuation_, {});
                }
            };

            static auto final_suspend() noexcept {
                return TFinalSuspend{};
            }

            bool HasStarted() const noexcept {
                return Flags_ & 1;
            }

            void SetStarted() noexcept {
                Flags_ |= 1;
            }

            bool HasContinuation() const noexcept {
                return Flags_ & 2;
            }

            void SetContinuation(std::coroutine_handle<> continuation) noexcept {
                Y_VERIFY_DEBUG(continuation, "Attempt to set an invalid continuation");
                Y_VERIFY_DEBUG(!HasContinuation(), "Attempt to set multiple continuations");
                Continuation_ = continuation;
                Flags_ |= 2;
            }

        private:
            // Default is used when task is resumed without a continuation
            std::coroutine_handle<> Continuation_ = std::noop_coroutine();
            unsigned char Flags_ = 0;
        };

        template<class T>
        class TTaskPromise;

        template<class T>
        using TTaskHandle = std::coroutine_handle<TTaskPromise<T>>;

        template<class T>
        class TTaskPromise final : public TTaskPromiseBase {
        public:
            TTask<T> get_return_object() noexcept;

            std::coroutine_handle<> Start() noexcept {
                if (Y_LIKELY(!HasStarted())) {
                    SetStarted();
                    return TTaskHandle<T>::from_promise(*this);
                } else {
                    // After coroutine starts is cannot be safely resumed, because
                    // it is waiting for something and must be resumed via its
                    // continuation.
                    return std::noop_coroutine();
                }
            }

            void unhandled_exception() noexcept {
                Result_.template emplace<std::exception_ptr>(std::current_exception());
            }

            template<class TResult>
            void return_value(TResult&& result) {
                Result_.template emplace<T>(std::forward<TResult>(result));
            }

            T ExtractResult() {
                switch (Result_.index()) {
                    case 0: {
                        std::rethrow_exception(std::get<0>(std::move(Result_)));
                    }
                    case 1: {
                        return std::get<1>(std::move(Result_));
                    }
                }
                std::terminate();
            }

        private:
            std::variant<std::exception_ptr, T> Result_;
        };

        template<>
        class TTaskPromise<void> final : public TTaskPromiseBase {
        public:
            TTask<void> get_return_object() noexcept;

            std::coroutine_handle<> Start() noexcept {
                if (Y_LIKELY(!HasStarted())) {
                    SetStarted();
                    return TTaskHandle<void>::from_promise(*this);
                } else {
                    // After coroutine starts is cannot be safely resumed, because
                    // it is waiting for something and must be resumed via its
                    // continuation.
                    return std::noop_coroutine();
                }
            }

            void unhandled_exception() noexcept {
                Exception_ = std::current_exception();
            }

            void return_void() noexcept {
                Exception_ = nullptr;
            }

            void ExtractResult() {
                if (Exception_) {
                    std::rethrow_exception(std::move(Exception_));
                }
            }

        private:
            std::exception_ptr Exception_;
        };

    } // namespace NDetail

    /**
     * A bare-bones lazy task implementation
     *
     * This task is not thread safe and assumes external synchronization, e.g.
     * races between destructor and await resume are not allowed and not safe.
     */
    template<class T>
    class TTask final {
    public:
        using promise_type = NDetail::TTaskPromise<T>;
        using value_type = T;

    public:
        TTask() noexcept = default;

        explicit TTask(NDetail::TTaskHandle<T> handle) noexcept
            : Handle_(handle)
        {}

        TTask(TTask&& rhs) noexcept
            : Handle_(std::exchange(rhs.Handle_, {}))
        {}

        ~TTask() {
            if (Handle_) {
                Handle_.destroy();
            }
        }

        TTask& operator=(TTask&& rhs) noexcept {
            if (Y_LIKELY(this != &rhs)) {
                auto handle = std::exchange(Handle_, {});
                Handle_ = std::exchange(rhs.Handle_, {});
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
            return bool(Handle_);
        }

        /**
         * Returns true if the task finished executing (produced a result)
         */
        bool Done() const {
            Y_VERIFY_DEBUG(Handle_);
            return Handle_.done();
        }

        /**
         * Manually start the task, only possible once
         */
        void Start() const {
            Y_VERIFY_DEBUG(!Done());
            Y_VERIFY_DEBUG(!Promise().HasStarted());
            Y_VERIFY_DEBUG(!Promise().HasContinuation());
            Promise().Start().resume();
        }

        /**
         * Implementation of awaiter for WhenDone
         */
        class TWhenDoneAwaiter {
        public:
            TWhenDoneAwaiter(NDetail::TTaskHandle<T> handle) noexcept
                : Handle_(handle)
            {}

            bool await_ready() const noexcept {
                return Handle_.done();
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) const noexcept {
                Handle_.promise().SetContinuation(continuation);
                return Handle_.promise().Start();
            }

            void await_resume() const noexcept {
                // nothing
            }

        private:
            NDetail::TTaskHandle<T> Handle_;
        };

        /**
         * Returns an awaitable that completes when task finishes executing
         *
         * Note the result of the task is not consumed.
         */
        auto WhenDone() const noexcept {
            return TWhenDoneAwaiter(Handle_);
        }

        /**
         * Extracts result of the task
         */
        T ExtractResult() {
            Y_VERIFY_DEBUG(Done());
            return Promise().ExtractResult();
        }

        /**
         * Implementation of awaiter for co_await
         */
        class TAwaiter {
        public:
            TAwaiter(TTask&& task) noexcept
                : Task_(std::move(task))
            {}

            bool await_ready() const noexcept {
                return Task_.Done();
            }

            std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) const noexcept {
                Task_.Promise().SetContinuation(continuation);
                return Task_.Promise().Start();
            }

            T await_resume() {
                // We destroy task state before we return
                TTask task(std::move(Task_));
                return task.ExtractResult();
            }

        private:
            TTask Task_;
        };

        /**
         * Returns the task result when it finishes
         */
        auto operator co_await() && noexcept {
            return TAwaiter(std::move(*this));
        }

    private:
        NDetail::TTaskPromise<T>& Promise() const noexcept {
            Y_VERIFY_DEBUG(Handle_);
            return Handle_.promise();
        }

    private:
        NDetail::TTaskHandle<T> Handle_;
    };

    namespace NDetail {

        template<class T>
        inline TTask<T> TTaskPromise<T>::get_return_object() noexcept {
            return TTask<T>(TTaskHandle<T>::from_promise(*this));
        }

        inline TTask<void> TTaskPromise<void>::get_return_object() noexcept {
            return TTask<void>(TTaskHandle<void>::from_promise(*this));
        }

    } // namespace NDetail

} // namespace NActors
