#include "async.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NActors::NDetail {

    void TActorAsyncHandlerPromise::unhandled_exception() noexcept {
        try {
            std::rethrow_exception(std::current_exception());
        } catch (const TAsyncCancellation&) {
            // ignore
        } catch (const std::exception& e) {
            if (!Actor.OnUnhandledExceptionSafe(e)) {
                std::terminate();
            }
        } catch (...) {
            std::terminate();
        }
    }

    class TBridgeCoroutine final
        : private TCustomCoroutineCallbacks<TBridgeCoroutine>
        , private TActorRunnableItem::TImpl<TBridgeCoroutine>
    {
        friend TCustomCoroutineCallbacks<TBridgeCoroutine>;
        friend TActorRunnableItem::TImpl<TBridgeCoroutine>;

    public:
        TBridgeCoroutine(IActor& actor, TActorRunnableItem& item)
            : Actor(actor)
            , SelfId(actor.SelfId())
            , Item(item)
        {
            TActivationContext* ctx = TlsActivationContext;
            Y_ABORT_UNLESS(ctx, "Unexpected missing activation context");
            ActorSystem = ctx->ExecutorThread.ActorSystem;
            Mailbox = &ctx->Mailbox;
        }

        using TCustomCoroutineCallbacks<TBridgeCoroutine>::ToCoroutineHandle;

    private:
        void OnResume() {
            TActivationContext* ctx = TlsActivationContext;
            if (ctx && Mailbox == &ctx->Mailbox) {
                // We are currently running on the same mailbox
                TActorRunnableQueue::Schedule(this);
            } else {
                // Send an event using the actor system
                ActorSystem->Send(SelfId, new TEvents::TEvResumeRunnable(this));
            }
        }

        void OnDestroy() {
            delete this;
        }

        void DoRun(IActor* actor) noexcept {
            if (!actor) {
                // The event was destroyed before it was delivered
                // This means we must destroy ourselves
                delete this;
                return;
            }
            // Note: we have an actor argument, but this might be a different
            // actor on the same mailbox (due to recursion). We know however
            // that as long as mailbox is valid actor must be valid as well,
            // because awaiter must keep waiting until we resume, and actor
            // will not be destroyed. After we resume it'ss awaiter's
            // responsibility to destroy us.
            Item.Run(&Actor);
        }

    private:
        IActor& Actor;
        TActorId SelfId;
        TActorRunnableItem& Item;
        TActorSystem* ActorSystem;
        TMailbox* Mailbox;
    };

    std::coroutine_handle<> MakeBridgeCoroutine(IActor& actor, TActorRunnableItem& item) {
        TBridgeCoroutine* c = new TBridgeCoroutine(actor, item);
        return c->ToCoroutineHandle();
    }

    class TBridgeCoroutines final {
    public:
        TBridgeCoroutines(IActor& actor, TActorRunnableItem& item1, TActorRunnableItem& item2)
            : Actor(actor)
            , SelfId(actor.SelfId())
            , Trampoline1(*this, item1)
            , Trampoline2(*this, item2)
        {
            TActivationContext* ctx = TlsActivationContext;
            Y_ABORT_UNLESS(ctx, "Unexpected missing activation context");
            ActorSystem = ctx->ExecutorThread.ActorSystem;
            Mailbox = &ctx->Mailbox;
        }

        std::pair<std::coroutine_handle<>, std::coroutine_handle<>> ToCoroutineHandles() {
            return { Trampoline1.ToCoroutineHandle(), Trampoline2.ToCoroutineHandle() };
        }

    private:
        class TBridgeTrampoline final
            : private TCustomCoroutineCallbacks<TBridgeTrampoline>
            , private TActorRunnableItem::TImpl<TBridgeTrampoline>
        {
            friend TCustomCoroutineCallbacks<TBridgeTrampoline>;
            friend TActorRunnableItem::TImpl<TBridgeTrampoline>;
            friend class TBridgeCoroutines;

        public:
            TBridgeTrampoline(TBridgeCoroutines& self, TActorRunnableItem& item)
                : Self(self)
                , Item(item)
            {}

            using TCustomCoroutineCallbacks<TBridgeTrampoline>::ToCoroutineHandle;

        private:
            void OnResume() {
                TActivationContext* ctx = TlsActivationContext;
                if (ctx && Self.Mailbox == &ctx->Mailbox) {
                    // We are currently running on the same mailbox
                    TActorRunnableQueue::Schedule(this);
                } else {
                    // Send an event using the actor system
                    Self.ActorSystem->Send(Self.SelfId, new TEvents::TEvResumeRunnable(this));
                }
            }

            void OnDestroy() {
                Self.Destroy();
            }

            void DoRun(IActor* actor) noexcept {
                if (!actor) {
                    // The event was destroyed before it was delivered
                    // This means we must destroy ourselves
                    Self.Destroy();
                    return;
                }
                // Note: we have an actor argument, but this might be a different
                // actor on the same mailbox (due to recursion). We know however
                // that as long as mailbox is valid actor must be valid as well,
                // because awaiter must keep waiting until we resume, and actor
                // will not be destroyed. After we resume it'ss awaiter's
                // responsibility to destroy us.
                Item.Run(&Self.Actor);
            }

        private:
            TBridgeCoroutines& Self;
            TActorRunnableItem& Item;
        };

        void Destroy() noexcept {
            delete this;
        }

    private:
        IActor& Actor;
        TActorId SelfId;
        TBridgeTrampoline Trampoline1;
        TBridgeTrampoline Trampoline2;
        TActorSystem* ActorSystem;
        TMailbox* Mailbox;
    };

    std::pair<std::coroutine_handle<>, std::coroutine_handle<>> MakeBridgeCoroutines(
        IActor& actor, TActorRunnableItem& item1, TActorRunnableItem& item2)
    {
        TBridgeCoroutines* c = new TBridgeCoroutines(actor, item1, item2);
        return c->ToCoroutineHandles();
    }

} // namespace NActors::NDetail
