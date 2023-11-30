#pragma once

#include <util/system/context.h>
#include <util/system/filemap.h>

#include "actor_bootstrapped.h"
#include "executor_thread.h"
#include "event_local.h"

#include <thread>

namespace NActors {

    class TActorCoro;

#ifndef CORO_THROUGH_THREADS
#   ifdef _tsan_enabled_
#       define CORO_THROUGH_THREADS 1
#   else
#       define CORO_THROUGH_THREADS 0
#   endif
#endif

    class TActorCoroImpl : public ITrampoLine {
        const bool AllowUnhandledDtor;
        bool Finished = false;
        bool InvokedFromDtor = false;
#if CORO_THROUGH_THREADS
        TAutoEvent InEvent;
        TAutoEvent OutEvent;
        TActivationContext *ActivationContext = nullptr;
        std::thread WorkerThread;
#else
        TMappedAllocation Stack;
        TContClosure FiberClosure;
        TExceptionSafeContext FiberContext;
        TExceptionSafeContext* ActorSystemContext = nullptr;
#endif
        THolder<IEventHandle> PendingEvent;

    protected:
        TActorIdentity SelfActorId = TActorIdentity(TActorId());
        TActorId ParentActorId;

        // Pre-leave and pre-enter hook functions are called by coroutine actor code to conserve the state of required TLS
        // variables.
        //
        // They are called in the following order:
        //
        // 1. coroutine executes WaitForEvent
        // 2. StoreTlsState() is called
        // 3. control is returned to the actor system
        // 4. some event is received, handler called (now in different thread, unconserved TLS variables are changed!)
        // 5. handler transfers control to the coroutine
        // 6. RestoreTlsState() is called
        //
        // These hooks may be used in the following way:
        //
        // thread_local TMyClass *MyObject = nullptr;
        //
        // class TMyCoroImpl : public TActorCoroImpl {
        //     TMyClass *SavedMyObject;
        //     ...
        // public:
        //     TMyCoroImpl()
        //         : TActorCoroImpl(...)
        //     {
        //         StoreTlsState = RestoreTlsState = &TMyCoroImpl::ConserveState;
        //     }
        //
        //     static void ConserveState(TActorCoroImpl *p) {
        //         TMyCoroImpl *my = static_cast<TMyCoroImpl*>(p);
        //         std::swap(my->SavedMyObject, MyObject);
        //     }
        //
        //     ...
        // }
        void (*StoreTlsState)(TActorCoroImpl*) = nullptr;
        void (*RestoreTlsState)(TActorCoroImpl*) = nullptr;


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
        void Resume(THolder<IEventHandle> ev);
        THolder<IEventHandle> ReturnToActorSystem();
        void DoRun() override final;
    };

    class TActorCoro : public IActorCallback {
        THolder<TActorCoroImpl> Impl;

    public:
        template <class TEnumActivityType = IActor::EActivityType>
        TActorCoro(THolder<TActorCoroImpl> impl, const TEnumActivityType activityType = IActor::EActivityType::ACTOR_COROUTINE)
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
