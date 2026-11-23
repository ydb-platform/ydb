#pragma once
#include "async.h"

#include <util/random/random.h>

#include <atomic>

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

        private:
            const ui64 Cookie;
            TEvent::TPtr Result;
            IActor* Actor = nullptr;
            std::coroutine_handle<> Continuation;
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
     * Request/reply in one step: allocates a unique cookie, sends the request to the recipient with
     * that cookie and returns a wait for the reply (TEvent::TPtr). The request is sent right away,
     * which is safe in an actor turn: the reply is a mailbox event and cannot be handled before
     * this turn ends, so it is still intercepted by the wait.
     *
     * The result is an awaiter, not an async<T>, so to bound the wait wrap it in a coroutine:
     *
     *     auto reply = co_await WithTimeout(timeout, [&]() -> async<TEvReply::TPtr> {
     *         co_return co_await ActorRequest<TEvReply>(recipient, new TEvRequest);
     *     });
     *
     * A reply that arrives after the timeout is no longer intercepted and reaches the state function.
     */
    template<class TEvent>
    inline auto ActorRequest(const TActorId& recipient, IEventBase* request, ui32 flags = 0) {
        const TActorId selfId = TActivationContext::AsActorContext().SelfID;
        const ui64 cookie = AllocateWaitCookie();
        TActivationContext::Send(new IEventHandle(recipient, selfId, request, flags, cookie));
        return ActorWaitForEvent<TEvent>(cookie);
    }

} // namespace NActors
