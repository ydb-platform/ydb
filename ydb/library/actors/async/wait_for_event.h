#pragma once
#include "async.h"

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

} // namespace NActors
