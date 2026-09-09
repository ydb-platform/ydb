#pragma once
#include "async.h"

#include <util/random/random.h>

#include <atomic>
#include <functional>

namespace NActors {

    namespace NDetail {

        template<class TEvent>
        class [[nodiscard]] TActorSpecificEventAwaiter
            : private TActorEventAwaiter::TImpl<TActorSpecificEventAwaiter<TEvent>>
        {
            friend TActorEventAwaiter::TImpl<TActorSpecificEventAwaiter<TEvent>>;

        public:
            static constexpr bool IsActorAwareAwaiter = true;

            TActorSpecificEventAwaiter(ui64 cookie)
                : Cookie(cookie)
            {}

            TActorSpecificEventAwaiter(const TActorSpecificEventAwaiter&) = delete;
            TActorSpecificEventAwaiter& operator=(const TActorSpecificEventAwaiter&) = delete;

            ~TActorSpecificEventAwaiter() {
                Detach();
            }

            TActorSpecificEventAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

        public:
            bool await_ready() const noexcept {
                return false;
            }

            template<class TPromise>
            void await_suspend(std::coroutine_handle<TPromise> parent) {
                IActor& actor = parent.promise().GetActor();
                actor.RegisterEventAwaiter(Cookie, this);
                Actor = &actor;
                Continuation = parent;
            }

            typename TEvent::TPtr await_resume() noexcept {
                return std::move(Result);
            }

            std::coroutine_handle<> await_cancel(std::coroutine_handle<> c) noexcept {
                // Perform cancellation only when still attached (not resuming)
                if (Detach()) {
                    return c;
                }
                return {};
            }

        private:
            bool Detach() {
                if (Actor) {
                    Actor->UnregisterEventAwaiter(Cookie, this);
                    Actor = nullptr;
                    return true;
                }
                return false;
            }

            bool Matches(TAutoPtr<IEventHandle>& ev) {
                if constexpr (std::is_same_v<TEvent, IEventHandle>) {
                    return true;
                } else {
                    return ev->GetTypeRewrite() == TEvent::EventType;
                }
            }

            bool DoHandle(TAutoPtr<IEventHandle>& ev) {
                Y_ABORT_UNLESS(Actor, "Unexpected Handle call after Detach()");
                if (Matches(ev)) {
                    Result = std::move(reinterpret_cast<typename TEvent::TPtr&>(ev));
                    Detach();
                    // Resume recursively since it's an event handler
                    Continuation.resume();
                    return true;
                }
                return false;
            }

        protected:
            const ui64 Cookie;
            TEvent::TPtr Result;
            IActor* Actor = nullptr;
            std::coroutine_handle<> Continuation;
        };

        /**
         * Waits for an event with the given cookie which is produced by a request started only after
         * the waiter has been registered. The starter is called from await_suspend as
         * starter(IActor& self, ui64 cookie): whatever it triggers (a Send, an external callback that
         * sends back to self) cannot deliver the reply before the coroutine is ready to receive it,
         * so there is no window in which the reply could fall through to the state function.
         */
        template<class TEvent, class TStarter>
        class [[nodiscard]] TActorStartedEventAwaiter : public TActorSpecificEventAwaiter<TEvent> {
            using TBase = TActorSpecificEventAwaiter<TEvent>;

        public:
            TActorStartedEventAwaiter(ui64 cookie, TStarter&& starter)
                : TBase(cookie)
                , Starter(std::forward<TStarter>(starter))
            {}

            TActorStartedEventAwaiter& CoAwaitByValue() && noexcept {
                return *this;
            }

            template<class TPromise>
            void await_suspend(std::coroutine_handle<TPromise> parent) {
                TBase::await_suspend(parent);
                // registered: from now on a matching reply resumes this coroutine
                std::invoke(Starter, *this->Actor, this->Cookie);
            }

        private:
            std::decay_t<TStarter> Starter;
        };

    } // namespace NDetail

    template<class TEvent>
    inline auto ActorWaitForEvent(ui64 cookie) {
        return NDetail::TActorSpecificEventAwaiter<TEvent>{ cookie };
    }

    /**
     * Allocates a cookie for ActorWaitForEvent that is unique among all live waits in the process
     * and never zero. The high bit and a per-process random prefix make a clash with
     * application-chosen cookies (tx ids, sequence numbers, cookies echoed by remote peers)
     * unlikely, not impossible: a clash only matters for two live waits of the same actor and
     * the same event type.
     */
    inline ui64 AllocateWaitCookie() noexcept {
        static std::atomic<ui64> counter{ (ui64(1) << 63) | ((RandomNumber<ui64>() & 0xFFFFF) << 42) };
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * Registers a waiter for TEvent with a fresh unique cookie and then calls
     * starter(IActor& self, ui64 cookie) to start the request. Use it whenever the request is
     * started by something other than a plain Send in the same turn (an external callback, a
     * future subscription, a helper that may reply synchronously).
     */
    template<class TEvent, class TStarter>
    inline auto ActorWaitForEvent(TStarter&& starter)
        requires std::invocable<TStarter&, IActor&, ui64>
    {
        return NDetail::TActorStartedEventAwaiter<TEvent, TStarter>(AllocateWaitCookie(), std::forward<TStarter>(starter));
    }

    /**
     * Request/reply in one step: allocates a unique cookie, registers the waiter for TEvent, sends
     * the request to the recipient with that cookie and resumes with the reply (TEvent::TPtr).
     * Combine with WithTimeout / WithDeadline for a bounded wait; a late reply after a timeout is
     * no longer intercepted and reaches the state function.
     */
    template<class TEvent>
    inline auto ActorRequest(const TActorId& recipient, IEventBase* request, ui32 flags = 0) {
        THolder<IEventBase> holder(request);
        return ActorWaitForEvent<TEvent>([recipient, holder = std::move(holder), flags](IActor& self, ui64 cookie) mutable {
            TActivationContext::Send(new IEventHandle(recipient, self.SelfId(), holder.Release(), flags, cookie));
        });
    }

} // namespace NActors
