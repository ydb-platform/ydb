#include "actor_coroutine.h"
#include "executor_thread.h"

#include <util/system/sanitizers.h>
#include <util/system/type_name.h>
#include <util/system/info.h>
#include <util/system/protect.h>

namespace NActors {
    static const size_t PageSize = NSystemInfo::GetPageSize();

#if !CORO_THROUGH_THREADS
    static size_t AlignStackSize(size_t size) {
        size += PageSize - (size & PageSize - 1) & PageSize - 1;
#ifndef NDEBUG
        size += PageSize;
#endif
        return size;
    }
#endif

    TActorCoroImpl::TActorCoroImpl(size_t stackSize, bool allowUnhandledDtor)
        : AllowUnhandledDtor(allowUnhandledDtor)
#if !CORO_THROUGH_THREADS
        , Stack(AlignStackSize(stackSize))
        , FiberClosure{this, TArrayRef(Stack.Begin(), Stack.End())}
        , FiberContext(FiberClosure)
#endif
    {
        Y_UNUSED(stackSize);
#if !CORO_THROUGH_THREADS && !defined(NDEBUG)
        ProtectMemory(STACK_GROW_DOWN ? Stack.Begin() : Stack.End() - PageSize, PageSize, EProtectMemoryMode::PM_NONE);
#endif
    }

    void TActorCoroImpl::Destroy() {
        if (!Finished) { // only resume when we have bootstrapped and Run() was entered and not yet finished; in other case simply terminate
            InvokedFromDtor = true;
            Resume(nullptr);
        }
#if CORO_THROUGH_THREADS
        if (WorkerThread.joinable()) {
            WorkerThread.join();
        }
#endif
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
        Y_ABORT_UNLESS(!Finished);

        // obtain pending event and ensure we've got one
        while (THolder<IEventHandle> event = ReturnToActorSystem()) {
            if (event->GetTypeRewrite() != TEvents::TSystem::CoroTimeout) {
                return event;
            } else if (event.Get() == timeoutEv) {
                return nullptr; // it is not a race -- we've got timeout exactly for our current wait
            }
        }
        Y_ABORT("no pending event");
    }

    bool TActorCoroImpl::ProcessEvent(THolder<IEventHandle> ev) {
        if (!SelfActorId) { // process bootstrap message, extract actor ids
            Y_ABORT_UNLESS(ev->GetTypeRewrite() == TEvents::TSystem::Bootstrap);
            SelfActorId = ev->Recipient;
            ParentActorId = ev->Sender;
            ev.Reset();
#if CORO_THROUGH_THREADS
            WorkerThread = std::thread(std::bind(&TActorCoroImpl::DoRun, this));
#endif
        }

        // prepare actor context for in-coroutine use
        TActivationContext *ac = TlsActivationContext;
        TActorContext actorContext(ac->Mailbox, ac->ExecutorThread, ac->EventStart, SelfActorId);
        TlsActivationContext = &actorContext;

        Resume(std::move(ev));

        // drop actor context
        TlsActivationContext = ac;

        return Finished;
    }

    void TActorCoroImpl::Resume(THolder<IEventHandle> ev) {
        BeforeResume();

        Y_ABORT_UNLESS(!PendingEvent);
        PendingEvent.Swap(ev);

#if CORO_THROUGH_THREADS
        ActivationContext = TlsActivationContext;
        InEvent.Signal();
        OutEvent.Wait();
#else
        // save caller context for a later return
        Y_ABORT_UNLESS(!ActorSystemContext);
        TExceptionSafeContext actorSystemContext;
        ActorSystemContext = &actorSystemContext;

        // go to actor coroutine
        ActorSystemContext->SwitchTo(&FiberContext);
#endif

        Y_ABORT_UNLESS(!PendingEvent);
    }

    void TActorCoroImpl::DoRun() {
#if CORO_THROUGH_THREADS
        InEvent.Wait();
        TlsActivationContext = ActivationContext;
#endif
        if (!InvokedFromDtor) {
            try {
                Run();
            } catch (const TDtorException& /*ex*/) {
                if (!AllowUnhandledDtor) {
                    Y_ABORT("unhandled TDtorException");
                }
            } catch (const std::exception& ex) {
                Y_ABORT("unhandled exception of type %s", TypeName(ex).data());
            } catch (...) {
                Y_ABORT("unhandled exception of type not derived from std::exception");
            }
        }
        Finished = true;
        ReturnToActorSystem();
    }

    THolder<IEventHandle> TActorCoroImpl::ReturnToActorSystem() {
#if CORO_THROUGH_THREADS
        OutEvent.Signal();
        if (Finished) {
            return nullptr;
        } else {
            InEvent.Wait(); // wait for reentry
            TlsActivationContext = ActivationContext;
        }
#else
        TExceptionSafeContext* returnContext = std::exchange(ActorSystemContext, nullptr);
        Y_ABORT_UNLESS(returnContext);
        if (StoreTlsState) {
            StoreTlsState(this);
        }
        FiberContext.SwitchTo(returnContext);
        if (RestoreTlsState) {
            RestoreTlsState(this);
        }
#endif

        if (THolder<IEventHandle> ev = std::exchange(PendingEvent, nullptr)) {
            return ev;
        } else {
            // we have returned from the actor system and it kindly asks us to terminate the coroutine as it is being
            // stopped
            Y_ABORT_UNLESS(InvokedFromDtor);
            throw TDtorException();
        }
    }

    TActorSystem *TActorCoroImpl::GetActorSystem() const {
        return GetActorContext().ExecutorThread.ActorSystem;
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
