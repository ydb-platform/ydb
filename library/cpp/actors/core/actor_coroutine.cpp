#include "actor_coroutine.h"
#include "executor_thread.h"

#include <util/system/sanitizers.h>
#include <util/system/type_name.h>

namespace NActors {
    static constexpr size_t StackOverflowGap = 4096;
    static char GoodStack[StackOverflowGap];

    static struct TInitGoodStack {
        TInitGoodStack() {
            // fill stack with some pseudo-random pattern
            for (size_t k = 0; k < StackOverflowGap; ++k) {
                GoodStack[k] = k + k * 91;
            }
        }
    } initGoodStack;

    TActorCoroImpl::TActorCoroImpl(size_t stackSize, bool allowUnhandledPoisonPill, bool allowUnhandledDtor)
        : Stack(stackSize)
        , AllowUnhandledPoisonPill(allowUnhandledPoisonPill)
        , AllowUnhandledDtor(allowUnhandledDtor)
        , FiberClosure{this, TArrayRef(Stack.Begin(), Stack.End())} 
        , FiberContext(FiberClosure)
    {
#ifndef NDEBUG
        char* p;
#if STACK_GROW_DOWN
        p = Stack.Begin();
#else
        p = Stack.End() - StackOverflowGap;
#endif
        memcpy(p, GoodStack, StackOverflowGap);
#endif
    }

    TActorCoroImpl::~TActorCoroImpl() {
        if (!Finished && !NSan::TSanIsOn()) { // only resume when we have bootstrapped and Run() was entered and not yet finished; in other case simply terminate
            Y_VERIFY(!PendingEvent);
            Resume();
        }
    }

    bool TActorCoroImpl::Send(TAutoPtr<IEventHandle> ev) {
        return GetActorContext().ExecutorThread.Send(ev);
    }

    THolder<IEventHandle> TActorCoroImpl::WaitForEvent(TInstant deadline) {
        const ui64 cookie = ++WaitCookie;
        if (deadline != TInstant::Max()) {
            ActorContext->ExecutorThread.Schedule(deadline - Now(), new IEventHandle(SelfActorId, {}, new TEvCoroTimeout,
                0, cookie));
        }

        // ensure we have no unprocessed event and return back to actor system to receive one
        Y_VERIFY(!PendingEvent);
        ReturnToActorSystem();

        // obtain pending event and ensure we've got one
        while (THolder<IEventHandle> event = std::exchange(PendingEvent, {})) {
            if (event->GetTypeRewrite() != TEvents::TSystem::CoroTimeout) {
                // special handling for poison pill -- we throw exception
                if (event->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType) {
                    throw TPoisonPillException();
                }

                // otherwise just return received event
                return event;
            } else if (event->Cookie == cookie) {
                return nullptr; // it is not a race -- we've got timeout exactly for our current wait
            } else {
                ReturnToActorSystem(); // drop this event and wait for the next one
            }
        }
        Y_FAIL("no pending event");
    }

    const TActorContext& TActorCoroImpl::GetActorContext() const {
        Y_VERIFY(ActorContext);
        return *ActorContext;
    }

    bool TActorCoroImpl::ProcessEvent(THolder<IEventHandle> ev) {
        Y_VERIFY(!PendingEvent);
        if (!SelfActorId) { // process bootstrap message, extract actor ids
            Y_VERIFY(ev->GetTypeRewrite() == TEvents::TSystem::Bootstrap);
            SelfActorId = ev->Recipient;
            ParentActorId = ev->Sender;
        } else { // process further messages
            PendingEvent = std::move(ev);
        }

        // prepare actor context for in-coroutine use
        TActivationContext *ac = TlsActivationContext;
        TlsActivationContext = nullptr;
        TActorContext ctx(ac->Mailbox, ac->ExecutorThread, ac->EventStart, SelfActorId);
        ActorContext = &ctx;

        Resume();

        // drop actor context
        TlsActivationContext = ac;
        ActorContext = nullptr;

        return Finished;
    }

    void TActorCoroImpl::Resume() {
        // save caller context for a later return
        Y_VERIFY(!ActorSystemContext);
        TExceptionSafeContext actorSystemContext;
        ActorSystemContext = &actorSystemContext;

        // go to actor coroutine
        BeforeResume();
        ActorSystemContext->SwitchTo(&FiberContext);

        // check for stack overflow
#ifndef NDEBUG
        const char* p;
#if STACK_GROW_DOWN
        p = Stack.Begin();
#else
        p = Stack.End() - StackOverflowGap;
#endif
        Y_VERIFY_DEBUG(memcmp(p, GoodStack, StackOverflowGap) == 0);
#endif
    }

    void TActorCoroImpl::DoRun() {
        try {
            if (ActorContext) { // ActorContext may be nullptr here if the destructor was invoked before bootstrapping
                Y_VERIFY(!PendingEvent);
                Run();
            }
        } catch (const TPoisonPillException& /*ex*/) {
            if (!AllowUnhandledPoisonPill) {
                Y_FAIL("unhandled TPoisonPillException");
            }
        } catch (const TDtorException& /*ex*/) {
            if (!AllowUnhandledDtor) {
                Y_FAIL("unhandled TDtorException");
            }
        } catch (const std::exception& ex) {
            Y_FAIL("unhandled exception of type %s", TypeName(ex).data());
        } catch (...) {
            Y_FAIL("unhandled exception of type not derived from std::exception");
        }
        Finished = true;
        ReturnToActorSystem();
    }

    void TActorCoroImpl::ReturnToActorSystem() {
        TExceptionSafeContext* returnContext = std::exchange(ActorSystemContext, nullptr);
        Y_VERIFY(returnContext);
        FiberContext.SwitchTo(returnContext);
        if (!PendingEvent) {
            // we have returned from the actor system and it kindly asks us to terminate the coroutine as it is being
            // stopped
            throw TDtorException();
        }
    }

}
