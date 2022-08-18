#pragma once

#include <util/system/context.h>
#include <util/system/filemap.h>

#include "actor_bootstrapped.h"
#include "executor_thread.h"
#include "event_local.h"

namespace NActors {

    class TActorCoro;

    class TActorCoroImpl : public ITrampoLine {
        TMappedAllocation Stack;
        bool AllowUnhandledPoisonPill;
        bool AllowUnhandledDtor;
        TContClosure FiberClosure;
        TExceptionSafeContext FiberContext;
        TExceptionSafeContext* ActorSystemContext = nullptr;
        THolder<IEventHandle> PendingEvent;
        bool Finished = false;
        ui64 WaitCookie = 0;
        TActorContext *ActorContext = nullptr;

    protected:
        TActorIdentity SelfActorId = TActorIdentity(TActorId());
        TActorId ParentActorId;

    private:
        template <typename TFirstEvent, typename... TOtherEvents>
        struct TIsOneOf: public TIsOneOf<TOtherEvents...> {
            bool operator()(IEventHandle& ev) const {
                return ev.GetTypeRewrite() == TFirstEvent::EventType || TIsOneOf<TOtherEvents...>()(ev);
            }
        };

        template <typename TSingleEvent>
        struct TIsOneOf<TSingleEvent> {
            bool operator()(IEventHandle& ev) const {
                return ev.GetTypeRewrite() == TSingleEvent::EventType;
            }
        };

        struct TEvCoroTimeout : TEventLocal<TEvCoroTimeout, TEvents::TSystem::CoroTimeout> {};

    protected:
        struct TPoisonPillException : yexception {};
        struct TDtorException : yexception {};

    public:
        TActorCoroImpl(size_t stackSize, bool allowUnhandledPoisonPill = false, bool allowUnhandledDtor = false);
        // specify stackSize explicitly for each actor; don't forget about overflow control gap

        virtual ~TActorCoroImpl();

        virtual void Run() = 0;

        virtual void BeforeResume() {}

        // Handle all events that are not expected in wait loops.
        virtual void ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) = 0;

        // Release execution ownership and wait for some event to arrive. When PoisonPill event is received, then
        // TPoisonPillException is thrown.
        THolder<IEventHandle> WaitForEvent(TInstant deadline = TInstant::Max());

        // Wait for specific event set by filter functor. Function returns first event that matches filter. On any other
        // kind of event ProcessUnexpectedEvent() is called.
        //
        // Example: WaitForSpecificEvent([](IEventHandle& ev) { return ev.Cookie == 42; });
        template <typename TFunc>
        THolder<IEventHandle> WaitForSpecificEvent(TFunc&& filter, TInstant deadline = TInstant::Max()) {
            for (;;) {
                if (THolder<IEventHandle> event = WaitForEvent(deadline); !event) {
                    return nullptr;
                } else if (filter(*event)) {
                    return event;
                } else {
                    ProcessUnexpectedEvent(event);
                }
            }
        }

        // Wait for specific event or set of events. Function returns first event that matches enlisted type. On any other
        // kind of event ProcessUnexpectedEvent() is called.
        //
        // Example: WaitForSpecificEvent<TEvReadResult, TEvFinished>();
        template <typename TFirstEvent, typename TSecondEvent, typename... TOtherEvents>
        THolder<IEventHandle> WaitForSpecificEvent(TInstant deadline = TInstant::Max()) {
            TIsOneOf<TFirstEvent, TSecondEvent, TOtherEvents...> filter;
            return WaitForSpecificEvent(filter, deadline);
        }

        // Wait for single specific event.
        template <typename TEventType>
        THolder<typename TEventType::THandle> WaitForSpecificEvent(TInstant deadline = TInstant::Max()) {
            auto filter = [](IEventHandle& ev) {
                return ev.GetTypeRewrite() == TEventType::EventType;
            };
            THolder<IEventHandle> event = WaitForSpecificEvent(filter, deadline);
            return THolder<typename TEventType::THandle>(static_cast<typename TEventType::THandle*>(event ? event.Release() : nullptr));
        }

    protected: // Actor System compatibility section
        const TActorContext& GetActorContext() const;
        TActorSystem *GetActorSystem() const { return GetActorContext().ExecutorThread.ActorSystem; }
        TInstant Now() const { return GetActorContext().Now(); }

        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
            return GetActorContext().Send(recipient, ev, flags, cookie, std::move(traceId));
        }

        template <typename TEvent>
        bool Send(const TActorId& recipient, THolder<TEvent> ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
            return GetActorContext().Send(recipient, ev.Release(), flags, cookie, std::move(traceId));
        }

        bool Send(TAutoPtr<IEventHandle> ev);

        void Schedule(TDuration delta, IEventBase* ev, ISchedulerCookie* cookie = nullptr) {
            return GetActorContext().Schedule(delta, ev, cookie);
        }

        void Schedule(TInstant deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) {
            return GetActorContext().Schedule(deadline, ev, cookie);
        }

        void Schedule(TMonotonic deadline, IEventBase* ev, ISchedulerCookie* cookie = nullptr) {
            return GetActorContext().Schedule(deadline, ev, cookie);
        }

        TActorId Register(IActor* actor, TMailboxType::EType mailboxType = TMailboxType::HTSwap, ui32 poolId = Max<ui32>()) {
            return GetActorContext().Register(actor, mailboxType, poolId);
        }

        TActorId RegisterWithSameMailbox(IActor* actor) {
            return GetActorContext().RegisterWithSameMailbox(actor);
        }

    private:
        friend class TActorCoro;
        bool ProcessEvent(THolder<IEventHandle> ev);

    private:
        /* Resume() function goes to actor coroutine context and continues (or starts) to execute it until actor finishes
         * his job or it is blocked on WaitForEvent. Then the function returns. */
        void Resume();
        void ReturnToActorSystem();
        void DoRun() override final;
    };

    class TActorCoro : public IActor {
        THolder<TActorCoroImpl> Impl;

    public:
        TActorCoro(THolder<TActorCoroImpl> impl, ui32 activityType = IActor::ACTOR_COROUTINE)
            : IActor(static_cast<TReceiveFunc>(&TActorCoro::StateFunc), activityType)
            , Impl(std::move(impl))
        {}

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parent) override {
            return new IEventHandle(TEvents::TSystem::Bootstrap, 0, self, parent, {}, 0);
        }

    private:
        STATEFN(StateFunc) {
            if (Impl->ProcessEvent(ev)) {
                PassAway();
            }
        }
    };

}
