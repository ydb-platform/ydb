#include "actor_coroutine.h"
#include "executor_thread.h"

#include <util/system/sanitizers.h>
#include <util/system/type_name.h>
#include <util/system/info.h>
#include <util/system/protect.h>

namespace NActors {
    static const size_t PageSize = NSystemInfo::GetPageSize();

    static size_t AlignStackSize(size_t size) {
        size += PageSize - (size & PageSize - 1) & PageSize - 1;
#ifndef NDEBUG
        size += PageSize;
#endif
        return size;
    }

    TActorCoroImpl::TActorCoroImpl(size_t stackSize, bool allowUnhandledDtor)
        : Stack(AlignStackSize(stackSize))
        , AllowUnhandledDtor(allowUnhandledDtor)
        , FiberClosure{this, TArrayRef(Stack.Begin(), Stack.End())}
        , FiberContext(FiberClosure)
    {
#ifndef NDEBUG
        ProtectMemory(STACK_GROW_DOWN ? Stack.Begin() : Stack.End() - PageSize, PageSize, EProtectMemoryMode::PM_NONE);
#endif
    }

    void TActorCoroImpl::Destroy() {
        if (!Finished && !NSan::TSanIsOn()) { // only resume when we have bootstrapped and Run() was entered and not yet finished; in other case simply terminate
            Y_VERIFY(!PendingEvent);
            InvokedFromDtor = true;
            Resume();
        }
    }

    bool TActorCoroImpl::Send(TAutoPtr<IEventHandle> ev) {
        return GetActorContext().ExecutorThread.Send(ev);
    }

    THolder<IEventHandle> TActorCoroImpl::WaitForEvent(TMonotonic deadline) {
        IEventHandle *timeoutEv = nullptr;
        if (deadline != TMonotonic::Max()) {
            TActivationContext::Schedule(deadline, timeoutEv = new IEventHandle(TEvents::TSystem::CoroTimeout, 0,
                SelfActorId, {}, nullptr, 0));
        }

        // ensure we have no unprocessed event and return back to actor system to receive one
        Y_VERIFY(!PendingEvent);
        ReturnToActorSystem();

        // obtain pending event and ensure we've got one
        while (THolder<IEventHandle> event = std::exchange(PendingEvent, {})) {
            if (event->GetTypeRewrite() != TEvents::TSystem::CoroTimeout) {
                return event;
            } else if (event.Get() == timeoutEv) {
                return nullptr; // it is not a race -- we've got timeout exactly for our current wait
            } else {
                ReturnToActorSystem(); // drop this event and wait for the next one
            }
        }
        Y_FAIL("no pending event");
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
        TActorContext actorContext(ac->Mailbox, ac->ExecutorThread, ac->EventStart, SelfActorId);
        TlsActivationContext = &actorContext;

        Resume();

        // drop actor context
        TlsActivationContext = ac;

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
    }

    void TActorCoroImpl::DoRun() {
        try {
            if (!InvokedFromDtor) { // ActorContext may be nullptr here if the destructor was invoked before bootstrapping
                Y_VERIFY(!PendingEvent);
                Run();
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
        if (StoreTlsState) {
            StoreTlsState(this);
        }
        FiberContext.SwitchTo(returnContext);
        if (RestoreTlsState) {
            RestoreTlsState(this);
        }
        if (!PendingEvent) {
            // we have returned from the actor system and it kindly asks us to terminate the coroutine as it is being
            // stopped
            throw TDtorException();
        }
    }

    TActorCoro::~TActorCoro() {
        Impl->Destroy();
    }

    STATEFN(TActorCoro::StateFunc) {
        if (Impl->ProcessEvent(ev)) {
            PassAway();
        }
    }
}
