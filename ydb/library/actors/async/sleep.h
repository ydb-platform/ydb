#pragma once
#include "async.h"
#include "symmetric_proxy.h"
#include <ydb/library/actors/core/events.h>

namespace NActors {

    namespace NDetail {

        template<class TWhen>
        class [[nodiscard]] TActorSleepAwaiter final
            : private TActorRunnableItem::TImpl<TActorSleepAwaiter<TWhen>>
        {
            friend TActorRunnableItem::TImpl<TActorSleepAwaiter<TWhen>>;

        public:
            explicit TActorSleepAwaiter(TWhen when)
                : When{ when }
            {}

            TActorSleepAwaiter(const TActorSleepAwaiter&) = delete;
            TActorSleepAwaiter& operator=(const TActorSleepAwaiter&) = delete;

            ~TActorSleepAwaiter() {
                // Handle emergency cancellation (actor system shutdown)
                Detach();
            }

            bool AwaitReady() const noexcept {
                return false;
            }

            template<class TPromise>
            void AwaitSuspend(std::coroutine_handle<TPromise> parent) {
                IActor& actor = parent.promise().GetActor();
                Continuation = parent;
                auto selfId = actor.SelfId();
                if (IsImmediate()) {
                    // Use a simple Send, everything is synchronized to the mailbox
                    bool ok = selfId.Send(selfId, (Event = new TEvents::TEvResumeRunnable(this)));
                    if (!ok) [[unlikely]] {
                        throw std::runtime_error("unexpected failure to send an event to SelfId");
                    }
                } else if (!IsInfinite()) {
                    // Event may be concurrently destroyed on scheduler shutdown
                    // We need to allocate a bridge that will outlive both actor and event
                    Bridge.Reset(new TBridge(this));
                    // Extra reference will be used by the event
                    Bridge->Ref();
                    selfId.Schedule(When, new TEvents::TEvResumeRunnable(Bridge.Get()));
                }
            }

            void AwaitResume() noexcept {
                // nothing
            }

            std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> c) noexcept {
                if (Detach()) {
                    // Not resumed yet, schedule cancellation instead
                    return c;
                }
                return {};
            }

        private:
            bool IsImmediate() const {
                return !When;
            }

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

        private:
            bool Detach() noexcept {
                if (Event) {
                    // Not resumed yet, but the event is still in our mailbox, make
                    // sure it does nothing when it is eventually delivered.
                    Event->Item = nullptr;
                    Event = nullptr;
                    return true;
                }
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

            void Resume() {
                // We resume recursively, since we're in an event handler
                Continuation.resume();
            }

        private:
            void DoRun(IActor* actor) noexcept {
                // Note: this method will be called in one of these cases:
                // - Send failed unexpectedly (actor == nullptr)
                // - Event destroyed due to mailbox cleanup (actor == nullptr)
                // - Event is delivered before cancellation (actor != nullptr)
                // In all cases Event will be a dangling pointer soon
                Event = nullptr;
                // When actor != nullptr we are in an event handler
                if (actor) {
                    Resume();
                }
            }

        private:
            class TBridge : public TThrRefBase, public TActorRunnableItem::TImpl<TBridge> {
            public:
                TBridge(TActorSleepAwaiter* self)
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
                TActorSleepAwaiter* Self;
            };

            void OnTimeout() {
                Y_ABORT_UNLESS(Bridge, "Unexpected bridge timer callback after Detach()");
                Bridge->Self = nullptr;
                Bridge.Reset();
                Resume();
            }

        private:
            const TWhen When;
            std::coroutine_handle<> Continuation;
            TEvents::TEvResumeRunnable* Event = nullptr;
            TIntrusivePtr<TBridge> Bridge;
        };

    } // namespace NDetail

    inline NDetail::TActorSleepAwaiter<TDuration> ActorYield() {
        return NDetail::TActorSleepAwaiter<TDuration>(TDuration::Zero());
    }

    inline NDetail::TActorSleepAwaiter<TDuration> ActorSleepFor(TDuration duration) {
        return NDetail::TActorSleepAwaiter<TDuration>(duration);
    }

    inline NDetail::TActorSleepAwaiter<TMonotonic> ActorSleepUntil(TMonotonic deadline) {
        return NDetail::TActorSleepAwaiter<TMonotonic>(deadline);
    }

    inline NDetail::TActorSleepAwaiter<TInstant> ActorSleepUntil(TInstant deadline) {
        return NDetail::TActorSleepAwaiter<TInstant>(deadline);
    }

    class TActorTimeoutException : public TAsyncCancellation {};

    namespace NDetail {

        template<class TWhen, class T>
        class [[nodiscard]] TActorAsyncWithTimeoutAwaiter
            : public TActorAwareAwaiter
            , private TAwaitCancelSource
            , private TSymmetricTransferCallback<TActorAsyncWithTimeoutAwaiter<TWhen, T>>
        {
            friend TSymmetricTransferCallback<TActorAsyncWithTimeoutAwaiter<TWhen, T>>;

        public:
            template<class TCallback>
            TActorAsyncWithTimeoutAwaiter(TWhen when, TCallback&& callback)
                : When{ when }
                , NestedCoroutine{ std::forward<TCallback>(callback)() }
            {}

            TActorAsyncWithTimeoutAwaiter(const TActorAsyncWithTimeoutAwaiter&) = delete;
            TActorAsyncWithTimeoutAwaiter& operator=(const TActorAsyncWithTimeoutAwaiter&) = delete;

            ~TActorAsyncWithTimeoutAwaiter() {
                // Handle emergency cancellation (actor system shutdown)
                Detach();
            }

            bool await_ready() const noexcept {
                return false;
            }

            template<class TPromise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<TPromise> parent) {
                auto self = NestedCoroutine.GetHandle();

                IActor& actor = parent.promise().GetActor();
                TAwaitCancelSource& source = parent.promise().GetAwaitCancelSource();

                // When source is cancelled already or the timeout is infite we bypass everything
                if (source.GetCancellation() || IsInfinite()) {
                    self.promise().SetContinuation(parent, actor, source);
                    return self;
                }

                // Otherwise we start a timer
                auto selfId = actor.SelfId();
                Bridge.Reset(new TBridge(this));
                Bridge->Ref(); // extra reference used by the event
                selfId.Schedule(When, new TEvents::TEvResumeRunnable(Bridge.Get()));

                // And intercept cancellation so we can cancel the timer
                TimeoutContinuation = parent;
                self.promise().SetContinuation(parent, actor, *this);
                Cleanup = source.SetAwaiter(*this);
                return self;
            }

            T await_resume() {
                Cleanup();
                Detach();

                if (ThrowException) {
                    throw TActorTimeoutException() << "operation timed out";
                }

                return NestedCoroutine.GetHandle().promise().ExtractValue();
            }

            std::coroutine_handle<> AwaitCancel(std::coroutine_handle<> h) noexcept {
                // Downstream cancellation will propagate cancellation
                TimeoutContinuation = h;
                if (Detach()) {
                    // Timer was still pending and now cancelled, pass upstream
                    // cancellation directly to downstream subscribers. It will
                    // either resume normally with a result, or confirm
                    // cancellation upstream.
                    return TAwaitCancelSource::Cancel(h);
                }
                return {};
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
                TBridge(TActorAsyncWithTimeoutAwaiter* self)
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
                TActorAsyncWithTimeoutAwaiter* Self;
            };

            void OnTimeout() noexcept {
                Y_ABORT_UNLESS(Bridge, "Unexpected bridge timer callback after Detach()");
                Bridge->Self = nullptr;
                Bridge.Reset();
                // We want downstream to resume our proxy on cancellation
                // We can then resume normally with an exception instead
                if (auto next = TAwaitCancelSource::Cancel(this->ToCoroutineHandle())) {
                    next.resume();
                }
            }

            std::coroutine_handle<> OnResume() noexcept {
                // Nested coroutine confirmed cancellation after a timeout
                Y_ABORT_UNLESS(!Bridge, "Unexpected cancellation confirmation before Detach()");
                // We will throw exception on normal resume
                ThrowException = true;
                return TimeoutContinuation;
            }

        private:
            const TWhen When;
            async<T> NestedCoroutine;
            TAwaitCancelCleanup Cleanup;
            TIntrusivePtr<TBridge> Bridge;
            std::coroutine_handle<> TimeoutContinuation;
            bool ThrowException = false;
        };
    }

    template<IsAsyncCoroutineCallback TCallback>
    inline auto ActorWithTimeout(TDuration duration, TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)());
        using T = TAsyncCoroutineResult<TCallbackResult>;
        return NDetail::TActorAsyncWithTimeoutAwaiter<TDuration, T>(duration, std::forward<TCallback>(callback));
    }

    template<IsAsyncCoroutineCallback TCallback>
    inline auto ActorWithDeadline(TMonotonic deadline, TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)());
        using T = TAsyncCoroutineResult<TCallbackResult>;
        return NDetail::TActorAsyncWithTimeoutAwaiter<TMonotonic, T>(deadline, std::forward<TCallback>(callback));
    }

    template<IsAsyncCoroutineCallback TCallback>
    inline auto ActorWithDeadline(TInstant deadline, TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)());
        using T = TAsyncCoroutineResult<TCallbackResult>;
        return NDetail::TActorAsyncWithTimeoutAwaiter<TInstant, T>(deadline, std::forward<TCallback>(callback));
    }

} // namespace NActors
