#pragma once
#include "async.h"
#include <ydb/library/actors/core/events.h>

namespace NActors::NDetail {

    template<class TWhen>
    class [[nodiscard]] TAsyncSleepAwaiter final
        : private TActorRunnableItem::TImpl<TAsyncSleepAwaiter<TWhen>>
    {
        friend TActorRunnableItem::TImpl<TAsyncSleepAwaiter<TWhen>>;

    public:
        static constexpr bool IsActorAwareAwaiter = true;

        explicit TAsyncSleepAwaiter(TWhen when)
            : When(when)
        {}

        TAsyncSleepAwaiter(const TAsyncSleepAwaiter&) = delete;
        TAsyncSleepAwaiter& operator=(const TAsyncSleepAwaiter&) = delete;

        ~TAsyncSleepAwaiter() {
            // Handle emergency cancellation (actor system shutdown)
            Detach();
        }

        TAsyncSleepAwaiter& CoAwaitByValue() && noexcept {
            return *this;
        }

        bool await_ready() const noexcept {
            return false;
        }

        template<class TPromise>
        void await_suspend(std::coroutine_handle<TPromise> parent) {
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

        void await_resume() noexcept {
            // nothing
        }

        std::coroutine_handle<> await_cancel(std::coroutine_handle<> c) noexcept {
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
            TBridge(TAsyncSleepAwaiter* self)
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
            TAsyncSleepAwaiter* Self;
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

} // namespace NActors::NDetail

namespace NActors {

    inline NDetail::TAsyncSleepAwaiter<TDuration> AsyncSleepFor(TDuration duration) {
        return NDetail::TAsyncSleepAwaiter<TDuration>(duration);
    }

    inline NDetail::TAsyncSleepAwaiter<TMonotonic> AsyncSleepUntil(TMonotonic deadline) {
        return NDetail::TAsyncSleepAwaiter<TMonotonic>(deadline);
    }

    inline NDetail::TAsyncSleepAwaiter<TInstant> AsyncSleepUntil(TInstant deadline) {
        return NDetail::TAsyncSleepAwaiter<TInstant>(deadline);
    }

} // namespace NActors
