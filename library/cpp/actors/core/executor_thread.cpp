#include "executor_thread.h"
#include "actorsystem.h"
#include "actor.h"
#include "callstack.h"
#include "mailbox.h"
#include "event.h"
#include "events.h"

#include <library/cpp/actors/prof/tag.h>
#include <library/cpp/actors/util/affinity.h>
#include <library/cpp/actors/util/datetime.h>
#include <library/cpp/actors/util/thread.h>

#ifdef BALLOC
#include <library/cpp/balloc/optional/operators.h>
#endif

#ifdef _linux_
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <util/system/type_name.h>
#include <util/system/datetime.h>

LWTRACE_USING(ACTORLIB_PROVIDER)

namespace NActors {
    constexpr TDuration TExecutorThread::DEFAULT_TIME_PER_MAILBOX;

    TExecutorThread::TExecutorThread(
            TWorkerId workerId,
            TWorkerId cpuId,
            TActorSystem* actorSystem,
            IExecutorPool* executorPool,
            TMailboxTable* mailboxTable,
            const TString& threadName,
            TDuration timePerMailbox,
            ui32 eventsPerMailbox)
        : ActorSystem(actorSystem)
        , ExecutorPool(executorPool)
        , Ctx(workerId, cpuId, actorSystem ? actorSystem->GetMaxActivityType() : 1)
        , ThreadName(threadName)
        , IsUnitedWorker(true)
    {
        Ctx.Switch(
            ExecutorPool,
            mailboxTable,
            NHPTimer::GetClockRate() * timePerMailbox.SecondsFloat(),
            eventsPerMailbox,
            ui64(-1), // infinite soft deadline
            &Ctx.WorkerStats);
    }


    TExecutorThread::~TExecutorThread()
    { }

    void TExecutorThread::UnregisterActor(TMailboxHeader* mailbox, TActorId actorId) {
        Y_VERIFY_DEBUG(IsUnitedWorker || actorId.PoolID() == ExecutorPool->PoolId && ExecutorPool->ResolveMailbox(actorId.Hint()) == mailbox);
        IActor* actor = mailbox->DetachActor(actorId.LocalId());
        Ctx.DecrementActorsAliveByActivity(actor->GetActivityType());
        DyingActors.push_back(THolder(actor));
    }

    void TExecutorThread::DropUnregistered() {
        DyingActors.clear(); // here is actual destruction of actors
    }

    void TExecutorThread::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        Ctx.Executor->Schedule(deadline, ev, cookie, Ctx.WorkerId);
    }

    void TExecutorThread::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        Ctx.Executor->Schedule(deadline, ev, cookie, Ctx.WorkerId);
    }

    void TExecutorThread::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        Ctx.Executor->Schedule(delta, ev, cookie, Ctx.WorkerId);
    }

    template <class T>
    inline TString SafeTypeName(const T* t) {
        if (t == nullptr) {
            return "nullptr";
        }
        try {
            return TypeName(*t);
        } catch (...) {
            return "unknown-type";
        }
    }

    inline void LwTraceSlowDelivery(IEventHandle* ev, const std::type_info* actorType, ui32 poolId, const TActorId& currentRecipient,
                                    double delivMs, double sinceActivationMs, ui32 eventsExecutedBefore) {
        LWPROBE(EventSlowDelivery,
                poolId,
                delivMs,
                sinceActivationMs,
                eventsExecutedBefore,
                ev && ev->HasEvent() ? ev->GetTypeName() : (ev ? ToString(ev->Type) : TString("nullptr")),
                currentRecipient.ToString(),
                SafeTypeName(actorType));
    }

    inline void LwTraceSlowEvent(IEventHandle* ev, ui32 evTypeForTracing, const std::type_info* actorType, ui32 poolId,
                                 const TActorId& currentRecipient, double eventMs) {
        // Event could have been destroyed by actor->Receive();
        LWPROBE(SlowEvent,
                poolId,
                eventMs,
                ev && ev->HasEvent() ? ev->GetTypeName() : ToString(evTypeForTracing),
                currentRecipient.ToString(),
                SafeTypeName(actorType));
    }

    template <typename TMailbox, bool IsTailExecution>
    bool TExecutorThread::Execute(TMailbox* mailbox, ui32 hint) {
        Y_VERIFY_DEBUG(DyingActors.empty());

        bool reclaimAsFree = false;

        if constexpr (!IsTailExecution) {
            Ctx.HPStart = GetCycleCountFast();
            Ctx.ExecutedEvents = 0;
        }
        NHPTimer::STime hpprev = Ctx.HPStart;

        IActor* actor = nullptr;
        const std::type_info* actorType = nullptr;
        ui32 prevActivityType = std::numeric_limits<ui32>::max();
        TActorId recipient;
        bool firstEvent = true;
        bool preempted = false;
        for (; Ctx.ExecutedEvents < Ctx.EventsPerMailbox; ++Ctx.ExecutedEvents) {
            TAutoPtr<IEventHandle> ev(mailbox->Pop());
            if (!!ev) {
                NHPTimer::STime hpnow;
                recipient = ev->GetRecipientRewrite();
                if (actor = mailbox->FindActor(recipient.LocalId())) {
                    TActorContext ctx(*mailbox, *this, hpprev, recipient);
                    TlsActivationContext = &ctx;

                    // Since actor is not null there should be no exceptions
                    actorType = &typeid(*actor);

#ifdef USE_ACTOR_CALLSTACK
                    TCallstack::GetTlsCallstack() = ev->Callstack;
                    TCallstack::GetTlsCallstack().SetLinesToSkip();
#endif
                    CurrentRecipient = recipient;
                    CurrentActorScheduledEventsCounter = 0;

                    if (firstEvent) {
                        double usec = Ctx.AddActivationStats(AtomicLoad(&mailbox->ScheduleMoment), hpprev);
                        if (usec > 500) {
                            GLOBAL_LWPROBE(ACTORLIB_PROVIDER, SlowActivation, Ctx.PoolId, usec / 1000.0);
                        }
                        firstEvent = false;
                    }

                    i64 usecDeliv = Ctx.AddEventDeliveryStats(ev->SendTime, hpprev);
                    if (usecDeliv > 5000) {
                        double sinceActivationMs = NHPTimer::GetSeconds(hpprev - Ctx.HPStart) * 1000.0;
                        LwTraceSlowDelivery(ev.Get(), actorType, Ctx.PoolId, CurrentRecipient, NHPTimer::GetSeconds(hpprev - ev->SendTime) * 1000.0, sinceActivationMs, Ctx.ExecutedEvents);
                    }

                    ui32 evTypeForTracing = ev->Type;

                    ui32 activityType = actor->GetActivityType();
                    if (activityType != prevActivityType) {
                        prevActivityType = activityType;
                        NProfiling::TMemoryTagScope::Reset(ActorSystem->MemProfActivityBase + activityType);
                    }

                    actor->Receive(ev);

                    size_t dyingActorsCnt = DyingActors.size();
                    Ctx.UpdateActorsStats(dyingActorsCnt);
                    if (dyingActorsCnt) {
                        DropUnregistered();
                        actor = nullptr;
                    }

                    if (mailbox->IsEmpty()) // was not-free and become free, we must reclaim mailbox
                        reclaimAsFree = true;

                    hpnow = GetCycleCountFast();
                    NHPTimer::STime elapsed = Ctx.AddEventProcessingStats(hpprev, hpnow, activityType, CurrentActorScheduledEventsCounter);
                    if (elapsed > 1000000) {
                        LwTraceSlowEvent(ev.Get(), evTypeForTracing, actorType, Ctx.PoolId, CurrentRecipient, NHPTimer::GetSeconds(elapsed) * 1000.0);
                    }

                    // The actor might have been destroyed
                    if (actor)
                        actor->AddElapsedTicks(elapsed);

                    CurrentRecipient = TActorId();
                } else {
                    actorType = nullptr;

                    TAutoPtr<IEventHandle> nonDelivered = IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::ReasonActorUnknown);
                    if (nonDelivered.Get()) {
                        ActorSystem->Send(nonDelivered);
                    } else {
                        Ctx.IncrementNonDeliveredEvents();
                    }
                    hpnow = GetCycleCountFast();
                }

                hpprev = hpnow;

                if (TlsThreadContext->CapturedType == ESendingType::Tail) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutByTailSending();
                    LWTRACK(MailboxPushedOutByTailSending,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            Ctx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - Ctx.HPStart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    break;
                }

                // Soft preemption in united pool
                if (Ctx.SoftDeadlineTs < (ui64)hpnow) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutBySoftPreemption();
                    LWTRACK(MailboxPushedOutBySoftPreemption,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            Ctx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - Ctx.HPStart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preempted = true;
                    break;
                }

                // time limit inside one mailbox passed, let others do some work
                if (hpnow - Ctx.HPStart > (i64)Ctx.TimePerMailboxTs) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutByTime();
                    LWTRACK(MailboxPushedOutByTime,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            Ctx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - Ctx.HPStart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preempted = true;
                    break;
                }

                if (Ctx.ExecutedEvents + 1 == Ctx.EventsPerMailbox) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutByEventCount();
                    LWTRACK(MailboxPushedOutByEventCount,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            Ctx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - Ctx.HPStart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preempted = true;
                    break;
                }
            } else {
                if (Ctx.ExecutedEvents == 0)
                    Ctx.IncrementEmptyMailboxActivation();
                LWTRACK(MailboxEmpty,
                        Ctx.Orbit,
                        Ctx.PoolId,
                        Ctx.Executor->GetName(),
                        Ctx.ExecutedEvents,
                        CyclesToDuration(GetCycleCountFast() - Ctx.HPStart),
                        Ctx.WorkerId,
                        recipient.ToString(),
                        SafeTypeName(actor));
                break; // empty queue, leave
            }
        }

        NProfiling::TMemoryTagScope::Reset(0);
        TlsActivationContext = nullptr;
        UnlockFromExecution(mailbox, Ctx.Executor, reclaimAsFree, hint, Ctx.WorkerId, RevolvingWriteCounter);
        return preempted;
    }

    TThreadId TExecutorThread::GetThreadId() const {
#ifdef _linux_
        while (AtomicLoad(&ThreadId) == UnknownThreadId) {
            NanoSleep(1000);
        }
#endif
        return ThreadId;
    }

    void* TExecutorThread::ThreadProc() {
#ifdef _linux_
        pid_t tid = syscall(SYS_gettid);
        AtomicSet(ThreadId, (ui64)tid);
#endif

#ifdef BALLOC
        ThreadDisableBalloc();
#endif

        if (ThreadName) {
            ::SetCurrentThreadName(ThreadName);
        }
        TThreadContext threadCtx;
        TlsThreadContext = &threadCtx;
        TlsThreadContext->Pool = static_cast<IExecutorPool*>(ExecutorPool);

        ExecutorPool->SetRealTimeMode();
        TAffinityGuard affinity(ExecutorPool->Affinity());

        NHPTimer::STime hpnow = GetCycleCountFast();
        NHPTimer::STime hpprev = hpnow;
        ui64 execCount = 0;
        ui64 readyActivationCount = 0;
        i64 execCycles = 0;
        i64 nonExecCycles = 0;

        auto executeActivation = [&]<bool IsTailExecution>(ui32 activation) {
            LWTRACK(ActivationBegin, Ctx.Orbit, Ctx.CpuId, Ctx.PoolId, Ctx.WorkerId, NHPTimer::GetSeconds(Ctx.Lease.GetPreciseExpireTs()) * 1e3);
            readyActivationCount++;
            if (TMailboxHeader* header = Ctx.MailboxTable->Get(activation)) {
                if (header->LockForExecution()) {
                    hpnow = GetCycleCountFast();
                    nonExecCycles += hpnow - hpprev;
                    hpprev = hpnow;
#define EXECUTE_MAILBOX(type) \
    case TMailboxType:: type: \
        { \
            using TMailBox = TMailboxTable:: T ## type ## Mailbox ; \
            if (Execute<TMailBox, IsTailExecution>(static_cast<TMailBox*>(header), activation)) { \
                TlsThreadContext->CapturedType = ESendingType::Lazy; \
            } \
        } \
        break \
// EXECUTE_MAILBOX
                    switch (header->Type) {
                        EXECUTE_MAILBOX(Simple);
                        EXECUTE_MAILBOX(Revolving);
                        EXECUTE_MAILBOX(HTSwap);
                        EXECUTE_MAILBOX(ReadAsFilled);
                        EXECUTE_MAILBOX(TinyReadAsFilled);
                    }
#undef EXECUTE_MAILBOX
                    hpnow = GetCycleCountFast();
                    i64 currentExecCycles = hpnow - hpprev;
                    execCycles += currentExecCycles;
                    hpprev = hpnow;
                    execCount++;
                    if (execCycles + nonExecCycles > 39000000) { // every 15 ms at 2.6GHz, so 1000 items is 15 sec (solomon interval)
                        LWPROBE(ExecutorThreadStats, ExecutorPool->PoolId, ExecutorPool->GetName(), Ctx.WorkerId,
                                execCount, readyActivationCount,
                                NHPTimer::GetSeconds(execCycles) * 1000.0, NHPTimer::GetSeconds(nonExecCycles) * 1000.0);
                        execCount = 0;
                        readyActivationCount = 0;
                        execCycles = 0;
                        nonExecCycles = 0;
                        Ctx.UpdateThreadTime();
                    }

                    if (!TlsThreadContext->IsEnoughCpu) {
                        Ctx.IncreaseNotEnoughCpuExecutions();
                        TlsThreadContext->IsEnoughCpu = true;
                    }
                }
            }
            LWTRACK(ActivationEnd, Ctx.Orbit, Ctx.CpuId, Ctx.PoolId, Ctx.WorkerId);
            Ctx.Orbit.Reset();
        };

        for (;;) {
            if (TlsThreadContext->CapturedType == ESendingType::Tail) {
                TlsThreadContext->CapturedType = ESendingType::Lazy;
                ui32 activation = std::exchange(TlsThreadContext->CapturedActivation, 0);
                executeActivation.operator()<true>(activation);
                continue;
            }
            Ctx.HasCapturedMessageBox = TlsThreadContext->CapturedActivation;
            ui32 activation = ExecutorPool->GetReadyActivation(Ctx, ++RevolvingReadCounter);
            if (!activation) {
                activation = std::exchange(TlsThreadContext->CapturedActivation, 0);
            } else if (TlsThreadContext->CapturedActivation) {
                ui32 capturedActivation = std::exchange(TlsThreadContext->CapturedActivation, 0);
                ExecutorPool->ScheduleActivation(capturedActivation);
            }
            if (!activation) {
                break;
            }
            executeActivation.operator()<false>(activation);
        }
        return nullptr;
    }

    // there must be barrier and check-read with following cas
    // or just cas w/o read.
    // or queue unlocks must be performed with exchange and not generic write
    // TODO: check performance of those options under contention

    // placed here in hope for better compiler optimization

    bool TMailboxHeader::MarkForSchedule() {
        AtomicBarrier();
        for (;;) {
            const ui32 state = AtomicLoad(&ExecutionState);
            switch (state) {
                case TExecutionState::Inactive:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::Scheduled, TExecutionState::Inactive))
                        return true;
                    break;
                case TExecutionState::Scheduled:
                    return false;
                case TExecutionState::Leaving:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::LeavingMarked, TExecutionState::Leaving))
                        return true;
                    break;
                case TExecutionState::Executing:
                case TExecutionState::LeavingMarked:
                    return false;
                case TExecutionState::Free:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::Free))
                        return true;
                    break;
                case TExecutionState::FreeScheduled:
                    return false;
                case TExecutionState::FreeLeaving:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeLeavingMarked, TExecutionState::FreeLeaving))
                        return true;
                    break;
                case TExecutionState::FreeExecuting:
                case TExecutionState::FreeLeavingMarked:
                    return false;
                default:
                    Y_FAIL();
            }
        }
    }

    bool TMailboxHeader::LockForExecution() {
        AtomicBarrier(); // strictly speaking here should be AtomicBarrier, but as we got mailboxes from queue - this barrier is already set implicitly and could be removed
        for (;;) {
            const ui32 state = AtomicLoad(&ExecutionState);
            switch (state) {
                case TExecutionState::Inactive:
                    return false;
                case TExecutionState::Scheduled:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::Executing, TExecutionState::Scheduled))
                        return true;
                    break;
                case TExecutionState::Leaving:
                case TExecutionState::Executing:
                case TExecutionState::LeavingMarked:
                    return false;
                case TExecutionState::Free:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeExecuting, TExecutionState::Free))
                        return true;
                    break;
                case TExecutionState::FreeScheduled:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeExecuting, TExecutionState::FreeScheduled))
                        return true;
                    break;
                case TExecutionState::FreeLeaving:
                case TExecutionState::FreeExecuting:
                case TExecutionState::FreeLeavingMarked:
                    return false;
                default:
                    Y_FAIL();
            }
        }
    }

    bool TMailboxHeader::LockFromFree() {
        AtomicBarrier();
        for (;;) {
            const ui32 state = AtomicLoad(&ExecutionState);
            switch (state) {
                case TExecutionState::Inactive:
                case TExecutionState::Scheduled:
                case TExecutionState::Leaving:
                case TExecutionState::Executing:
                case TExecutionState::LeavingMarked:
                    Y_FAIL();
                case TExecutionState::Free:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::Executing, TExecutionState::Free))
                        return true;
                    break;
                case TExecutionState::FreeScheduled:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::Executing, TExecutionState::FreeScheduled))
                        return true;
                    break;
                case TExecutionState::FreeLeaving:
                case TExecutionState::FreeExecuting:
                case TExecutionState::FreeLeavingMarked:
                    return false;
                default:
                    Y_FAIL();
            }
        }
    }

    void TMailboxHeader::UnlockFromExecution1() {
        const ui32 state = AtomicLoad(&ExecutionState);
        if (state == TExecutionState::Executing)
            AtomicStore(&ExecutionState, (ui32)TExecutionState::Leaving);
        else if (state == TExecutionState::FreeExecuting)
            AtomicStore(&ExecutionState, (ui32)TExecutionState::FreeLeaving);
        else
            Y_FAIL();
        AtomicBarrier();
    }

    bool TMailboxHeader::UnlockFromExecution2(bool wouldReschedule) {
        AtomicBarrier();
        for (;;) {
            const ui32 state = AtomicLoad(&ExecutionState);
            switch (state) {
                case TExecutionState::Inactive:
                case TExecutionState::Scheduled:
                    Y_FAIL();
                case TExecutionState::Leaving:
                    if (!wouldReschedule) {
                        if (AtomicUi32Cas(&ExecutionState, TExecutionState::Inactive, TExecutionState::Leaving))
                            return false;
                    } else {
                        if (AtomicUi32Cas(&ExecutionState, TExecutionState::Scheduled, TExecutionState::Leaving))
                            return true;
                    }
                    break;
                case TExecutionState::Executing:
                    Y_FAIL();
                case TExecutionState::LeavingMarked:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::Scheduled, TExecutionState::LeavingMarked))
                        return true;
                    break;
                case TExecutionState::Free:
                case TExecutionState::FreeScheduled:
                    Y_FAIL();
                case TExecutionState::FreeLeaving:
                    if (!wouldReschedule) {
                        if (AtomicUi32Cas(&ExecutionState, TExecutionState::Free, TExecutionState::FreeLeaving))
                            return false;
                    } else {
                        if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::FreeLeaving))
                            return true;
                    }
                    break;
                case TExecutionState::FreeExecuting:
                    Y_FAIL();
                case TExecutionState::FreeLeavingMarked:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::FreeLeavingMarked))
                        return true;
                    break;
                default:
                    Y_FAIL();
            }
        }
    }

    bool TMailboxHeader::UnlockAsFree(bool wouldReschedule) {
        AtomicBarrier();
        for (;;) {
            const ui32 state = AtomicLoad(&ExecutionState);
            switch (state) {
                case TExecutionState::Inactive:
                case TExecutionState::Scheduled:
                    Y_FAIL();
                case TExecutionState::Leaving:
                    if (!wouldReschedule) {
                        if (AtomicUi32Cas(&ExecutionState, TExecutionState::Free, TExecutionState::Leaving))
                            return false;
                    } else {
                        if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::Leaving))
                            return true;
                    }
                    break;
                case TExecutionState::Executing:
                    Y_FAIL();
                case TExecutionState::LeavingMarked:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::LeavingMarked))
                        return true;
                    break;
                case TExecutionState::Free:
                case TExecutionState::FreeScheduled:
                case TExecutionState::FreeLeaving:
                case TExecutionState::FreeExecuting:
                case TExecutionState::FreeLeavingMarked:
                    Y_FAIL();
                default:
                    Y_FAIL();
            }
        }
    }

    void TExecutorThread::GetCurrentStats(TExecutorThreadStats& statsCopy) const {
        Ctx.GetCurrentStats(statsCopy);
    }

}
