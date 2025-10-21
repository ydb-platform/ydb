#pragma once
#include "async.h"
#include "decorator.h"
#include <ydb/library/actors/core/events.h>

namespace NActors::NDetail {

    template<class TWhen, class R>
    class [[nodiscard]] TWithTimeoutAwaiter
        : public TAsyncDecoratorAwaiter<R, TWithTimeoutAwaiter<TWhen, R>>
    {
        friend TAsyncDecoratorAwaiter<R, TWithTimeoutAwaiter<TWhen, R>>;
        using TBase = TAsyncDecoratorAwaiter<R, TWithTimeoutAwaiter<TWhen, R>>;

    public:
        template<class TCallback, class... TArgs>
        TWithTimeoutAwaiter(TWhen when, TCallback&& callback, TArgs&&... args)
            : TBase(std::forward<TCallback>(callback), std::forward<TArgs>(args)...)
            , When(when)
        {}

        ~TWithTimeoutAwaiter() {
            // Handle emergency cancellation (actor system shutdown)
            Detach();
        }

        TWithTimeoutAwaiter& CoAwaitByValue() && noexcept {
            return *this;
        }

    private:
        std::coroutine_handle<> Bypass() noexcept {
            // When caller is cancelled already or the timeout is infinite we bypass everything
            if (this->GetCancellation() || IsInfinite()) {
                return this->GetAsyncBody();
            }
            return nullptr;
        }

        std::coroutine_handle<> Start() {
            // Start a timer
            auto selfId = this->GetActor().SelfId();
            Bridge.Reset(new TBridge(this));
            Bridge->Ref(); // extra reference used by the event
            selfId.Schedule(When, new TEvents::TEvResumeRunnable(Bridge.Get()));

            return TBase::Start();
        }

        std::coroutine_handle<> OnCancel() noexcept {
            if (Detach()) {
                // Propagate cancellation, since timer was still active,
                // which means async body was not cancelled yet. We also
                // don't need to intercept unwind in that case.
                return TBase::CancelWithBypass();
            }

            return nullptr;
        }

        void OnTimeout() noexcept {
            Y_ABORT_UNLESS(Bridge, "Unexpected bridge timer callback after Detach()");
            Bridge->Self = nullptr;
            Bridge.Reset();
            // Cancel async body, since we're in an event handler resume inline
            if (auto h = TBase::Cancel()) {
                h.resume();
            }
        }

        std::coroutine_handle<> OnUnwind(std::coroutine_handle<>) noexcept {
            // Nested coroutine confirmed cancellation after a timeout
            Y_ABORT_UNLESS(!Bridge, "Unexpected cancellation confirmation before Detach()");

            if (this->GetCancellation()) {
                // Unwind caller since it is cancelled
                return this->GetCancellation();
            } else {
                // We resume normally to indicate a timeout
                return this->GetContinuation();
            }
        }

        bool Return() requires (std::is_void_v<R>) {
            // Make sure timer is cancelled
            Detach();

            if (this->DidUnwind()) {
                return false;
            }

            TBase::Return();
            return true;
        }

        std::optional<R> Return() requires (!std::is_void_v<R>) {
            // Make sure timer is cancelled
            Detach();

            if (this->DidUnwind()) {
                return std::nullopt;
            }

            struct TImplicitConverter {
                TBase& base;

                operator R() const {
                    return base.Return();
                }
            };

            return TImplicitConverter{ *this };
        }

    private:
        bool IsInfinite() const {
            if constexpr (std::is_same_v<TWhen, TDuration>) {
                return When == TDuration::Max();
            } else if constexpr (std::is_same_v<TWhen, TMonotonic>) {
                return When == TMonotonic::Max();
            } else if constexpr (std::is_same_v<TWhen, TInstant>) {
                return When == TInstant::Max();
            } else {
                static_assert(false, "Unsupported type");
            }
        }

        bool Detach() noexcept {
            if (Bridge) {
                // Not resumed yet, but the event is in the scheduler and may
                // concurrently run with actor == nullptr. Make sure it is
                // disarmed in case it is eventually delivered.
                // TODO: need a way to deterministically cancel long timers.
                Bridge->Self = nullptr;
                Bridge.Reset();
                return true;
            }
            if (IsInfinite()) {
                return true;
            }
            return false;
        }

    private:
        class TBridge : public TThrRefBase, public TActorRunnableItem::TImpl<TBridge> {
        public:
            TBridge(TWithTimeoutAwaiter* self)
                : Self(self)
            {}

            void DoRun(IActor* actor) noexcept {
                // Note: awaiter may set this->Self to nullptr concurrently with
                // scheduler shutdown, but in case of any possible concurrency
                // we will have actor == nullptr.
                if (actor && Self) {
                    // We are running within the actor
                    Self->OnTimeout();
                }
                // Event was holding an extra reference
                UnRef();
            }

        public:
            TWithTimeoutAwaiter* Self;
        };

    private:
        const TWhen When;
        TIntrusivePtr<TBridge> Bridge;
    };

} // namespace NActors::NDetail

namespace NActors {

    template<class TCallback, class... TArgs>
    inline auto WithTimeout(TDuration duration, TCallback&& callback, TArgs&&... args)
        requires (IsAsyncCoroutineCallable<TCallback, TArgs...>)
    {
        using TCallbackResult = std::invoke_result_t<TCallback, TArgs...>;
        using R = typename TCallbackResult::result_type;
        return NDetail::TWithTimeoutAwaiter<TDuration, R>(duration, std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
    }

    template<class R>
    inline auto WithTimeout(TDuration duration, async<R> wrapped) {
        return NDetail::TWithTimeoutAwaiter<TDuration, R>(duration, [&wrapped]{ return wrapped.UnsafeMove(); });
    }

    template<class TCallback, class... TArgs>
    inline auto WithDeadline(TMonotonic deadline, TCallback&& callback, TArgs&&... args)
        requires (IsAsyncCoroutineCallable<TCallback, TArgs...>)
    {
        using TCallbackResult = std::invoke_result_t<TCallback, TArgs...>;
        using R = typename TCallbackResult::result_type;
        return NDetail::TWithTimeoutAwaiter<TMonotonic, R>(deadline, std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
    }

    template<class R>
    inline auto WithDeadline(TMonotonic deadline, async<R> wrapped) {
        return NDetail::TWithTimeoutAwaiter<TMonotonic, R>(deadline, [&wrapped]{ return wrapped.UnsafeMove(); });
    }

    template<class TCallback, class... TArgs>
    inline auto WithDeadline(TInstant deadline, TCallback&& callback, TArgs&&... args)
        requires (IsAsyncCoroutineCallable<TCallback, TArgs...>)
    {
        using TCallbackResult = std::invoke_result_t<TCallback, TArgs...>;
        using R = typename TCallbackResult::result_type;
        return NDetail::TWithTimeoutAwaiter<TInstant, R>(deadline, std::forward<TCallback>(callback), std::forward<TArgs>(args)...);
    }

    template<class R>
    inline auto WithDeadline(TInstant deadline, async<R> wrapped) {
        return NDetail::TWithTimeoutAwaiter<TInstant, R>(deadline, [&wrapped]{ return wrapped.UnsafeMove(); });
    }

} // namespace NActors
