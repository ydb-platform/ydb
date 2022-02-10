#include "executor_thread.h"
#include "actorsystem.h"
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
    {
        Ctx.Switch(
            ExecutorPool,
            mailboxTable,
            NHPTimer::GetClockRate() * timePerMailbox.SecondsFloat(),
            eventsPerMailbox,
            ui64(-1), // infinite soft deadline
            &Ctx.WorkerStats);
    }

    TActorId TExecutorThread::RegisterActor(IActor* actor, TMailboxType::EType mailboxType, ui32 poolId, const TActorId& parentId) {
        if (poolId == Max<ui32>())
            return Ctx.Executor->Register(actor, mailboxType, ++RevolvingWriteCounter, parentId ? parentId : CurrentRecipient);
        else
            return ActorSystem->Register(actor, mailboxType, poolId, ++RevolvingWriteCounter, parentId ? parentId : CurrentRecipient);
    }

    TActorId TExecutorThread::RegisterActor(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId) {
        return Ctx.Executor->Register(actor, mailbox, hint, parentId ? parentId : CurrentRecipient);
    }

    void TExecutorThread::UnregisterActor(TMailboxHeader* mailbox, ui64 localActorId) {
        IActor* actor = mailbox->DetachActor(localActorId);
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
    inline TString SafeTypeName(T* t) {
        if (t == nullptr) {
            return "nullptr";
        }
        try {
            return TypeName(*t);
        } catch (...) {
            return "unknown-type";
        }
    }

    inline TString ActorTypeName(const IActor* actor, ui32 activityType) {
        return actor ? SafeTypeName(actor) : ("activityType_" + ToString(activityType) + " (destroyed)");
    }

    inline void LwTraceSlowDelivery(IEventHandle* ev, const IActor* actor, ui32 poolId, const TActorId& currentRecipient,
                                    double delivMs, double sinceActivationMs, ui32 eventsExecutedBefore) {
        const auto baseEv = (ev && ev->HasEvent()) ? ev->GetBase() : nullptr;
        LWPROBE(EventSlowDelivery,
                poolId,
                delivMs,
                sinceActivationMs,
                eventsExecutedBefore,
                baseEv ? SafeTypeName(baseEv) : (ev ? ToString(ev->Type) : TString("nullptr")),
                currentRecipient.ToString(),
                SafeTypeName(actor));
    }

    inline void LwTraceSlowEvent(IEventHandle* ev, ui32 evTypeForTracing, const IActor* actor, ui32 poolId, ui32 activityType,
                                 const TActorId& currentRecipient, double eventMs) {
        // Event could have been destroyed by actor->Receive();
        const auto baseEv = (ev && ev->HasEvent()) ? ev->GetBase() : nullptr;
        LWPROBE(SlowEvent,
                poolId,
                eventMs,
                baseEv ? SafeTypeName(baseEv) : ToString(evTypeForTracing),
                currentRecipient.ToString(),
                ActorTypeName(actor, activityType));
    }

    template <typename TMailbox>
    void TExecutorThread::Execute(TMailbox* mailbox, ui32 hint) {
        Y_VERIFY_DEBUG(DyingActors.empty());

        bool reclaimAsFree = false;

        NHPTimer::STime hpstart = GetCycleCountFast();
        NHPTimer::STime hpprev = hpstart;

        IActor* actor = nullptr;
        ui32 prevActivityType = std::numeric_limits<ui32>::max();
        TActorId recipient;
        for (ui32 executed = 0; executed < Ctx.EventsPerMailbox; ++executed) {
            TAutoPtr<IEventHandle> ev(mailbox->Pop());
            if (!!ev) {
                NHPTimer::STime hpnow;
                recipient = ev->GetRecipientRewrite();
                if (actor = mailbox->FindActor(recipient.LocalId())) {
                    TActorContext ctx(*mailbox, *this, hpprev, recipient);
                    TlsActivationContext = &ctx;

#ifdef USE_ACTOR_CALLSTACK
                    TCallstack::GetTlsCallstack() = ev->Callstack;
                    TCallstack::GetTlsCallstack().SetLinesToSkip();
#endif
                    CurrentRecipient = recipient;
                    CurrentActorScheduledEventsCounter = 0;

                    if (executed == 0) {
                        double usec = Ctx.AddActivationStats(AtomicLoad(&mailbox->ScheduleMoment), hpprev);
                        if (usec > 500) {
                            GLOBAL_LWPROBE(ACTORLIB_PROVIDER, SlowActivation, Ctx.PoolId, usec / 1000.0);
                        }
                    }

                    i64 usecDeliv = Ctx.AddEventDeliveryStats(ev->SendTime, hpprev);
                    if (usecDeliv > 5000) {
                        double sinceActivationMs = NHPTimer::GetSeconds(hpprev - hpstart) * 1000.0;
                        LwTraceSlowDelivery(ev.Get(), actor, Ctx.PoolId, CurrentRecipient, NHPTimer::GetSeconds(hpprev - ev->SendTime) * 1000.0, sinceActivationMs, executed);
                    }

                    ui32 evTypeForTracing = ev->Type;

                    ui32 activityType = actor->GetActivityType();
                    if (activityType != prevActivityType) {
                        prevActivityType = activityType;
                        NProfiling::TMemoryTagScope::Reset(ActorSystem->MemProfActivityBase + activityType);
                    }

                    actor->Receive(ev, ctx);

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
                        LwTraceSlowEvent(ev.Get(), evTypeForTracing, actor, Ctx.PoolId, activityType, CurrentRecipient, NHPTimer::GetSeconds(elapsed) * 1000.0);
                    }

                    // The actor might have been destroyed
                    if (actor)
                        actor->AddElapsedTicks(elapsed);

                    CurrentRecipient = TActorId();
                } else {
                    TAutoPtr<IEventHandle> nonDelivered = ev->ForwardOnNondelivery(TEvents::TEvUndelivered::ReasonActorUnknown);
                    if (nonDelivered.Get()) {
                        ActorSystem->Send(nonDelivered);
                    } else {
                        Ctx.IncrementNonDeliveredEvents();
                    }
                    hpnow = GetCycleCountFast();
                }

                hpprev = hpnow;

                // Soft preemption in united pool
                if (Ctx.SoftDeadlineTs < (ui64)hpnow) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutBySoftPreemption();
                    LWTRACK(MailboxPushedOutBySoftPreemption,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            executed + 1,
                            CyclesToDuration(hpnow - hpstart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actor));
                    break;
                }

                // time limit inside one mailbox passed, let others do some work
                if (hpnow - hpstart > (i64)Ctx.TimePerMailboxTs) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutByTime();
                    LWTRACK(MailboxPushedOutByTime,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            executed + 1,
                            CyclesToDuration(hpnow - hpstart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actor));
                    break;
                }

                if (executed + 1 == Ctx.EventsPerMailbox) {
                    AtomicStore(&mailbox->ScheduleMoment, hpnow);
                    Ctx.IncrementMailboxPushedOutByEventCount();
                    LWTRACK(MailboxPushedOutByEventCount,
                            Ctx.Orbit,
                            Ctx.PoolId,
                            Ctx.Executor->GetName(),
                            executed + 1,
                            CyclesToDuration(hpnow - hpstart),
                            Ctx.WorkerId,
                            recipient.ToString(),
                            SafeTypeName(actor));
                    break;
                }
            } else {
                if (executed == 0)
                    Ctx.IncrementEmptyMailboxActivation();
                LWTRACK(MailboxEmpty,
                        Ctx.Orbit,
                        Ctx.PoolId,
                        Ctx.Executor->GetName(),
                        executed,
                        CyclesToDuration(GetCycleCountFast() - hpstart),
                        Ctx.WorkerId,
                        recipient.ToString(),
                        SafeTypeName(actor));
                break; // empty queue, leave
            }
        }

        NProfiling::TMemoryTagScope::Reset(0);
        TlsActivationContext = nullptr;
        UnlockFromExecution(mailbox, Ctx.Executor, reclaimAsFree, hint, Ctx.WorkerId, RevolvingWriteCounter);
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

        ExecutorPool->SetRealTimeMode();
        TAffinityGuard affinity(ExecutorPool->Affinity());

        NHPTimer::STime hpnow = GetCycleCountFast();
        NHPTimer::STime hpprev = hpnow;
        ui64 execCount = 0;
        ui64 readyActivationCount = 0;
        i64 execCycles = 0;
        i64 nonExecCycles = 0;

        for (;;) {
            if (ui32 activation = ExecutorPool->GetReadyActivation(Ctx, ++RevolvingReadCounter)) {
                LWTRACK(ActivationBegin, Ctx.Orbit, Ctx.CpuId, Ctx.PoolId, Ctx.WorkerId, NHPTimer::GetSeconds(Ctx.Lease.GetPreciseExpireTs()) * 1e3);
                readyActivationCount++;
                if (TMailboxHeader* header = Ctx.MailboxTable->Get(activation)) {
                    if (header->LockForExecution()) {
                        hpnow = GetCycleCountFast();
                        nonExecCycles += hpnow - hpprev;
                        hpprev = hpnow;
                        switch (header->Type) {
                            case TMailboxType::Simple:
                                Execute(static_cast<TMailboxTable::TSimpleMailbox*>(header), activation);
                                break;
                            case TMailboxType::Revolving:
                                Execute(static_cast<TMailboxTable::TRevolvingMailbox*>(header), activation);
                                break;
                            case TMailboxType::HTSwap:
                                Execute(static_cast<TMailboxTable::THTSwapMailbox*>(header), activation);
                                break;
                            case TMailboxType::ReadAsFilled:
                                Execute(static_cast<TMailboxTable::TReadAsFilledMailbox*>(header), activation);
                                break;
                            case TMailboxType::TinyReadAsFilled:
                                Execute(static_cast<TMailboxTable::TTinyReadAsFilledMailbox*>(header), activation);
                                break;
                        }
                        hpnow = GetCycleCountFast();
                        execCycles += hpnow - hpprev;
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
                    }
                }
                LWTRACK(ActivationEnd, Ctx.Orbit, Ctx.CpuId, Ctx.PoolId, Ctx.WorkerId);
                Ctx.Orbit.Reset();
            } else { // no activation means PrepareStop was called so thread must terminate
                break;
            }
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
}
