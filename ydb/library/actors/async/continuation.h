#pragma once
#include "async.h"

namespace NActors {

    namespace NDetail {
        template<class T>
        class TAsyncContinuationAwaiter;
    }

    template<class T>
    class TAsyncContinuation {
        friend NDetail::TAsyncContinuationAwaiter<T>;

    public:
        TAsyncContinuation() noexcept
            : Awaiter(nullptr)
        {}

        TAsyncContinuation(TAsyncContinuation&& rhs) noexcept
            : Awaiter(rhs.Awaiter)
        {
            if (Awaiter) {
                Awaiter->Link = this;
                rhs.Awaiter = nullptr;
            }
        }

        TAsyncContinuation& operator=(TAsyncContinuation&& rhs) {
            if (this != &rhs) {
                Detach();
                Awaiter = rhs.Awaiter;
                if (Awaiter) {
                    Awaiter->Link = this;
                    rhs.Awaiter = nullptr;
                }
            }
            return *this;
        }

        ~TAsyncContinuation() {
            Detach();
        }

    public:
        explicit operator bool() const {
            return bool(Awaiter);
        }

        void Resume()
            requires (std::is_void_v<T>)
        {
            if (!Awaiter) [[unlikely]] {
                throw std::logic_error("coroutine unavailable");
            }

            Awaiter->Result.SetValue();
            Awaiter->Resume();
            Awaiter = nullptr;
        }

        template<class U>
        void Resume(U&& value)
            requires (!std::is_void_v<T> && std::is_convertible_v<U&&, T>)
        {
            if (!Awaiter) [[unlikely]] {
                throw std::logic_error("coroutine unavailable");
            }

            Awaiter->Result.SetValue(std::forward<U>(value));
            Awaiter->Resume();
            Awaiter = nullptr;
        }

        void Throw(std::exception_ptr e) {
            if (!Awaiter) [[unlikely]] {
                throw std::logic_error("coroutine unavailable");
            }

            Awaiter->Result.SetException(std::move(e));
            Awaiter->Resume();
            Awaiter = nullptr;
        }

    private:
        TAsyncContinuation(NDetail::TAsyncContinuationAwaiter<T>* awaiter) noexcept
            : Awaiter(awaiter)
        {
            Awaiter->Link = this;
        }

        void Detach() {
            if (Awaiter) {
                Awaiter->Result.SetException(std::make_exception_ptr(std::logic_error("continuation object was destroyed")));
                Awaiter->Resume();
                Awaiter = nullptr;
            }
        }

    private:
        NDetail::TAsyncContinuationAwaiter<T>* Awaiter;
    };

    namespace NDetail {

        template<class T>
        class TAsyncContinuationAwaiter
            : private TActorRunnableItem::TImpl<TAsyncContinuationAwaiter<T>>
        {
            friend TActorRunnableItem::TImpl<TAsyncContinuationAwaiter<T>>;
            friend TAsyncContinuation<T>;

        public:
            TAsyncContinuationAwaiter() = default;

            TAsyncContinuationAwaiter(const TAsyncContinuationAwaiter&) = delete;
            TAsyncContinuationAwaiter& operator=(const TAsyncContinuationAwaiter&) = delete;

            ~TAsyncContinuationAwaiter() {
                Detach();
            }

        protected:
            TAsyncContinuation<T> CreateContinuation() {
                return TAsyncContinuation<T>(this);
            }

            bool Detach() {
                if (Link) {
                    Link->Awaiter = nullptr;
                    Link = nullptr;
                    return true;
                }
                return false;
            }

        private:
            void Resume() {
                Y_ASSERT(Link);
                Link = nullptr;
                // Note: when continuation is empty we are resuming from within the callback
                if (Continuation) {
                    // Never resume recursively
                    TActorRunnableQueue::Schedule(this);
                }
            }

            void DoRun(IActor*) noexcept {
                Continuation.resume();
            }

        protected:
            std::coroutine_handle<> Continuation;
            TAsyncContinuation<T>* Link = nullptr;
            TAsyncResult<T> Result;
        };

        template<class T, class TCallback>
        class [[nodiscard]] TAsyncContinuationAwaiterWithCallback final
            : public TAsyncContinuationAwaiter<T>
        {
        public:
            static constexpr bool IsActorAwareAwaiter = true;

            TAsyncContinuationAwaiterWithCallback(TCallback&& callback)
                : Callback(std::forward<TCallback>(callback))
            {}

            TAsyncContinuationAwaiterWithCallback& CoAwaitByValue() && noexcept {
                return *this;
            }

            bool await_ready() const noexcept {
                return false;
            }

            bool await_suspend(std::coroutine_handle<> continuation) {
                std::forward<TCallback>(Callback)(this->CreateContinuation());
                if (!this->Link) {
                    // Resumed from within the callback
                    return false;
                }
                this->Continuation = continuation;
                return true;
            }

            std::coroutine_handle<> await_cancel(std::coroutine_handle<> cancellation) noexcept {
                if (this->Detach()) {
                    return cancellation;
                }
                return {};
            }

            T await_resume() {
                return this->Result.ExtractValue();
            }

        private:
            TCallback&& Callback;
        };

    } // namespace NDetail

    template<class T, class TCallback>
    NDetail::TAsyncContinuationAwaiterWithCallback<T, TCallback> WithAsyncContinuation(TCallback&& callback) {
        return NDetail::TAsyncContinuationAwaiterWithCallback<T, TCallback>(std::forward<TCallback>(callback));
    }

} // namespace NActors
