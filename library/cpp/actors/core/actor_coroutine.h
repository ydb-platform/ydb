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
        bool AllowUnhandledDtor;
        bool Finished = false;
        bool InvokedFromDtor = false;
        TContClosure FiberClosure;
        TExceptionSafeContext FiberContext;
        TExceptionSafeContext* ActorSystemContext = nullptr;
        THolder<IEventHandle> PendingEvent;

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

    protected:
        struct TDtorException : yexception {};

    public:
        TActorCoroImpl(size_t stackSize, bool allowUnhandledDtor = false);
        // specify stackSize explicitly for each actor; don't forget about overflow control gap

        virtual ~TActorCoroImpl() = default;

        virtual void Run() = 0;

        virtual void BeforeResume() {}

        // Release execution ownership and wait for some event to arrive.
        THolder<IEventHandle> WaitForEvent(TMonotonic deadline = TMonotonic::Max());

        // Wait for specific event set by filter functor. Function returns first event that matches filter. On any other
        //
        // kind of event processUnexpectedEvent() is called.
        // Example: WaitForSpecificEvent([](IEventHandle& ev) { return ev.Cookie == 42; });
        template <typename TFunc, typename TCallback, typename = std::enable_if_t<std::is_invocable_v<TCallback, TAutoPtr<IEventHandle>>>>
        THolder<IEventHandle> WaitForSpecificEvent(TFunc&& filter, TCallback processUnexpectedEvent, TMonotonic deadline = TMonotonic::Max()) {
            for (;;) {
                if (THolder<IEventHandle> event = WaitForEvent(deadline); !event) {
                    return nullptr;
                } else if (filter(*event)) {
                    return event;
                } else {
                    processUnexpectedEvent(event);
                }
            }
        }

        template <typename TFunc, typename TDerived, typename = std::enable_if_t<std::is_base_of_v<TActorCoroImpl, TDerived>>>
        THolder<IEventHandle> WaitForSpecificEvent(TFunc&& filter, void (TDerived::*processUnexpectedEvent)(TAutoPtr<IEventHandle>),
                TMonotonic deadline = TMonotonic::Max()) {
            auto callback = [&](TAutoPtr<IEventHandle> ev) { (static_cast<TDerived&>(*this).*processUnexpectedEvent)(ev); };
            return WaitForSpecificEvent(std::forward<TFunc>(filter), callback, deadline);
        }

        // Wait for specific event or set of events. Function returns first event that matches enlisted type. On any other
        // kind of event processUnexpectedEvent() is called.
        //
        // Example: WaitForSpecificEvent<TEvReadResult, TEvFinished>();
        template <typename TFirstEvent, typename TSecondEvent, typename... TOtherEvents, typename TCallback>
        THolder<IEventHandle> WaitForSpecificEvent(TCallback&& callback, TMonotonic deadline = TMonotonic::Max()) {
            TIsOneOf<TFirstEvent, TSecondEvent, TOtherEvents...> filter;
            return WaitForSpecificEvent(filter, std::forward<TCallback>(callback), deadline);
        }

        // Wait for single specific event.
        template <typename TEventType, typename TCallback>
        THolder<typename TEventType::THandle> WaitForSpecificEvent(TCallback&& callback, TMonotonic deadline = TMonotonic::Max()) {
            auto filter = [](IEventHandle& ev) {
                return ev.GetTypeRewrite() == TEventType::EventType;
            };
            THolder<IEventHandle> event = WaitForSpecificEvent(filter, std::forward<TCallback>(callback), deadline);
            return THolder<typename TEventType::THandle>(static_cast<typename TEventType::THandle*>(event ? event.Release() : nullptr));
        }

    protected: // Actor System compatibility section
        const TActorContext& GetActorContext() const { return TActivationContext::AsActorContext(); }
        TActorSystem *GetActorSystem() const { return GetActorContext().ExecutorThread.ActorSystem; }
        TInstant Now() const { return GetActorContext().Now(); }
        TMonotonic Monotonic() const { return GetActorContext().Monotonic(); }

        bool Send(const TActorId& recipient, IEventBase* ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
            return GetActorContext().Send(recipient, ev, flags, cookie, std::move(traceId));
        }

        bool Send(const TActorId& recipient, THolder<IEventBase> ev, ui32 flags = 0, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
            return GetActorContext().Send(recipient, ev.Release(), flags, cookie, std::move(traceId));
        }

        bool Send(TAutoPtr<IEventHandle> ev);

        bool Forward(THolder<IEventHandle>& ev, const TActorId& recipient) {
            return Send(IEventHandle::Forward(ev, recipient).Release());
        }

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
        void Destroy();

    private:
        /* Resume() function goes to actor coroutine context and continues (or starts) to execute it until actor finishes
         * his job or it is blocked on WaitForEvent. Then the function returns. */
        void Resume();
        void ReturnToActorSystem();
        void DoRun() override final;
    };

    class TActorCoro : public IActorCallback {
        THolder<TActorCoroImpl> Impl;

    public:
        TActorCoro(THolder<TActorCoroImpl> impl, ui32 activityType = IActor::ACTOR_COROUTINE)
            : IActorCallback(static_cast<TReceiveFunc>(&TActorCoro::StateFunc), activityType)
            , Impl(std::move(impl))
        {}

        ~TActorCoro();

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parent) override {
            return new IEventHandle(TEvents::TSystem::Bootstrap, 0, self, parent, {}, 0);
        }

    private:
        STATEFN(StateFunc);
    };

}
