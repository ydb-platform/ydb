#pragma once
#include "async.h"

namespace NActors {

    template<class TEvent>
    class [[nodiscard]] TActorSpecificEventAwaiter
        : private TActorEventAwaiter::TImpl<TActorSpecificEventAwaiter<TEvent>>
        , private TActorRunnableItem::TImpl<TActorSpecificEventAwaiter<TEvent>>
    {
        friend TActorEventAwaiter::TImpl<TActorSpecificEventAwaiter<TEvent>>;
        friend TActorRunnableItem::TImpl<TActorSpecificEventAwaiter<TEvent>>;

    public:
        TActorSpecificEventAwaiter(ui64 cookie)
            : Cookie(cookie)
        {}

        TActorSpecificEventAwaiter(const TActorSpecificEventAwaiter&) = delete;
        TActorSpecificEventAwaiter& operator=(const TActorSpecificEventAwaiter&) = delete;

        ~TActorSpecificEventAwaiter() {
            Detach();
        }

    public:
        bool AwaitReady() const noexcept {
            return false;
        }

        template<class TPromise>
        void AwaitSuspend(std::coroutine_handle<TPromise> parent) {
            IActor& actor = parent.promise().GetActor();
            actor.RegisterEventAwaiter(Cookie, this);
            Actor = &actor;
            Continuation = parent;
        }

        typename TEvent::TPtr AwaitResume() noexcept {
            return std::move(Result);
        }

        void AwaitCancel(std::coroutine_handle<> c) noexcept {
            if (Actor) {
                // Perform cancellation only when still attached (not resuming)
                Detach();
                // Schedule cancellation
                Continuation = c;
                TActorRunnableQueue::Schedule(this);
            }
        }

    private:
        void Detach() {
            if (Actor) {
                Actor->UnregisterEventAwaiter(Cookie, this);
                Actor = nullptr;
            }
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

        void DoRun(IActor*) noexcept {
            Continuation.resume();
        }

    private:
        const ui64 Cookie;
        TEvent::TPtr Result;
        IActor* Actor = nullptr;
        std::coroutine_handle<> Continuation;
    };

    template<class TEvent>
    TActorSpecificEventAwaiter<TEvent> ActorWaitForEvent(ui64 cookie) {
        return TActorSpecificEventAwaiter<TEvent>{ cookie };
    }

} // namespace NActors
