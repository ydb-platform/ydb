#include "executor_thread.h"
#include "actorsystem.h"
#include "actor.h"
#include "callstack.h"
#include "mailbox.h"
#include "event.h"
#include "events.h"
#include "executor_pool_base.h"
#include "executor_pool_basic.h"
#include "executor_thread_ctx.h"
#include "probes.h"

#include <atomic>
#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/thread.h>

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
    constexpr TDuration TGenericExecutorThread::DEFAULT_TIME_PER_MAILBOX;

    TGenericExecutorThread::TGenericExecutorThread(
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
        , Ctx(workerId, cpuId)
        , ThreadName(threadName)
        , TimePerMailbox(timePerMailbox)
        , EventsPerMailbox(eventsPerMailbox)
        , ActorSystemIndex(TActorTypeOperator::GetActorSystemIndex())
    {
        Ctx.Switch(
            ExecutorPool,
            mailboxTable,
            NHPTimer::GetClockRate() * timePerMailbox.SecondsFloat(),
            eventsPerMailbox,
            ui64(-1), // infinite soft deadline
            &Ctx.WorkerStats);
    }

    TGenericExecutorThread::TGenericExecutorThread(TWorkerId workerId,
            TActorSystem* actorSystem,
            IExecutorPool* executorPool,
            i16 poolCount,
            const TString& threadName,
            ui64 softProcessingDurationTs,
            TDuration timePerMailbox,
            ui32 eventsPerMailbox)
        : ActorSystem(actorSystem)
        , ExecutorPool(executorPool)
        , Ctx(workerId, 0)
        , ThreadName(threadName)
        , IsSharedThread(true)
        , TimePerMailbox(timePerMailbox)
        , EventsPerMailbox(eventsPerMailbox)
        , SoftProcessingDurationTs(softProcessingDurationTs)
        , SharedStats(poolCount)
        , ActorSystemIndex(TActorTypeOperator::GetActorSystemIndex())
    {
        Ctx.Switch(
            ExecutorPool,
            static_cast<TExecutorPoolBaseMailboxed*>(executorPool)->MailboxTable.Get(),
            NHPTimer::GetClockRate() * timePerMailbox.SecondsFloat(),
            eventsPerMailbox,
            ui64(-1), // infinite soft deadline
            &SharedStats[ExecutorPool->PoolId]);
    }

    TSharedExecutorThread::TSharedExecutorThread(TWorkerId workerId,
                TActorSystem* actorSystem,
                TSharedExecutorThreadCtx *threadCtx,
                i16 poolCount,
                const TString& threadName,
                ui64 softProcessingDurationTs,
                TDuration timePerMailbox,
                ui32 eventsPerMailbox)
        : TGenericExecutorThread(workerId, actorSystem, threadCtx->ExecutorPools[0].load(), poolCount, threadName, softProcessingDurationTs, timePerMailbox, eventsPerMailbox)
        , ThreadCtx(threadCtx)
    {
        Ctx.SharedThread = threadCtx;
    }

    TGenericExecutorThread::~TGenericExecutorThread()
    { }

    void TGenericExecutorThread::UnregisterActor(TMailboxHeader* mailbox, TActorId actorId) {
        Y_DEBUG_ABORT_UNLESS(actorId.PoolID() == ExecutorPool->PoolId && ExecutorPool->ResolveMailbox(actorId.Hint()) == mailbox);
        IActor* actor = mailbox->DetachActor(actorId.LocalId());
        Ctx.DecrementActorsAliveByActivity(actor->GetActivityType());
        DyingActors.push_back(THolder(actor));
    }

    void TGenericExecutorThread::DropUnregistered() {
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        if (ActorSystem->MonitorStuckActors()) {
            if (auto *pool = dynamic_cast<TExecutorPoolBaseMailboxed*>(ExecutorPool)) {
                with_lock (pool->StuckObserverMutex) {
                    for (const auto& actor : DyingActors) {
                        const size_t i = actor->StuckIndex;
                        auto& actorPtr = pool->Actors[i];
                        actorPtr = pool->Actors.back();
                        actorPtr->StuckIndex = i;
                        pool->Actors.pop_back();
                        pool->DeadActorsUsage.emplace_back(actor->GetActivityType(), actor->GetUsage(GetCycleCountFast()));
                    }
                }
            }
        }
#endif
        DyingActors.clear(); // here is actual destruction of actors
    }

    void TGenericExecutorThread::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        Ctx.Executor->Schedule(deadline, ev, cookie, Ctx.WorkerId);
    }

    void TGenericExecutorThread::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        Ctx.Executor->Schedule(deadline, ev, cookie, Ctx.WorkerId);
    }

    void TGenericExecutorThread::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
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

    template <typename TMailbox>
    TGenericExecutorThread::TProcessingResult TGenericExecutorThread::Execute(TMailbox* mailbox, ui32 hint, bool isTailExecution) {
        Y_DEBUG_ABORT_UNLESS(DyingActors.empty());

        bool reclaimAsFree = false;

        if (!isTailExecution) {
            Ctx.HPStart = GetCycleCountFast();
            Ctx.ExecutedEvents = 0;
        }

        IActor* actor = nullptr;
        const std::type_info* actorType = nullptr;
        ui32 prevActivityType = std::numeric_limits<ui32>::max();
        TActorId recipient;
        bool firstEvent = true;
        bool preempted = false;
        bool wasWorking = false;
        NHPTimer::STime hpnow = Ctx.HPStart;
        NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
        Ctx.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);
        NHPTimer::STime eventStart = Ctx.HPStart;
        TlsThreadContext->ActivationStartTS.store(Ctx.HPStart, std::memory_order_release);

        for (; Ctx.ExecutedEvents < Ctx.EventsPerMailbox; ++Ctx.ExecutedEvents) {
            if (TAutoPtr<IEventHandle> evExt = mailbox->Pop()) {
                mailbox->ProcessEvents(mailbox);
                recipient = evExt->GetRecipientRewrite();
                TActorContext ctx(*mailbox, *this, eventStart, recipient);
                TlsActivationContext = &ctx; // ensure dtor (if any) is called within actor system
                // move for destruct before ctx;
                auto ev = std::move(evExt);
                if (actor = mailbox->FindActor(recipient.LocalId())) {
                    wasWorking = true;
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
                        NProfiling::TMemoryTagScope::Reset(activityType);
                        TlsThreadContext->ElapsingActorActivity.store(activityType, std::memory_order_release);
                    }

                    actor->Receive(ev);

                    hpnow = GetCycleCountFast();
                    hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);

                    mailbox->ProcessEvents(mailbox);
                    actor->OnDequeueEvent();

                    size_t dyingActorsCnt = DyingActors.size();
                    Ctx.UpdateActorsStats(dyingActorsCnt);
                    if (dyingActorsCnt) {
                        DropUnregistered();
                        mailbox->ProcessEvents(mailbox);
                        actor = nullptr;
                    }

                    if (mailbox->IsEmpty()) // was not-free and become free, we must reclaim mailbox
                        reclaimAsFree = true;
                    
                    Ctx.AddElapsedCycles(activityType, hpnow - hpprev);
                    NHPTimer::STime elapsed = Ctx.AddEventProcessingStats(eventStart, hpnow, activityType, CurrentActorScheduledEventsCounter);
                    mailbox->AddElapsedCycles(elapsed);
                    if (elapsed > 1000000) {
                        LwTraceSlowEvent(ev.Get(), evTypeForTracing, actorType, Ctx.PoolId, CurrentRecipient, NHPTimer::GetSeconds(elapsed) * 1000.0);
                    }

                    // The actor might have been destroyed
                    if (actor)
                        actor->AddElapsedTicks(elapsed);

                    CurrentRecipient = TActorId();
                } else {
                    actorType = nullptr;

                    TAutoPtr<IEventHandle> nonDelivered = IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown);
                    if (nonDelivered.Get()) {
                        ActorSystem->Send(nonDelivered);
                    } else {
                        Ctx.IncrementNonDeliveredEvents();
                    }
                    hpnow = GetCycleCountFast();
                    hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
                    Ctx.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);
                }
                eventStart = hpnow;

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
        TlsThreadContext->ActivationStartTS.store(hpnow, std::memory_order_release);
        TlsThreadContext->ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);

        NProfiling::TMemoryTagScope::Reset(0);
        TlsActivationContext = nullptr;
        UnlockFromExecution(mailbox, Ctx.Executor, reclaimAsFree, hint, Ctx.WorkerId, RevolvingWriteCounter);
        return {preempted, wasWorking};
    }

    TThreadId TGenericExecutorThread::GetThreadId() const {
#ifdef _linux_
        while (AtomicLoad(&ThreadId) == UnknownThreadId) {
            NanoSleep(1000);
        }
#endif
        return ThreadId;
    }

    TWorkerId TGenericExecutorThread::GetWorkerId() const {
        return Ctx.WorkerId;
    }

    TGenericExecutorThread::TProcessingResult TGenericExecutorThread::ProcessExecutorPool(IExecutorPool *pool) {
        ExecutorPool = pool;
        TlsThreadContext->Pool = ExecutorPool;
        TlsThreadContext->WorkerId = Ctx.WorkerId;
        pool->Initialize(Ctx);

        ExecutorPool->SetRealTimeMode();
        TAffinityGuard affinity(ExecutorPool->Affinity());

        NHPTimer::STime hpnow = GetCycleCountFast();
        NHPTimer::STime hpprev = hpnow;
        ui64 execCount = 0;
        ui64 readyActivationCount = 0;
        i64 execCycles = 0;
        i64 nonExecCycles = 0;

        bool needToStop = false;
        bool wasWorking = false;

        auto executeActivation = [&](ui32 activation, bool isTailExecution) {
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
            auto result = Execute<TMailBox>(static_cast<TMailBox*>(header), activation, isTailExecution); \
            if (result.IsPreempted) { \
                TlsThreadContext->CapturedType = ESendingType::Lazy; \
            } \
            wasWorking |= result.WasWorking; \
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

                    if (IsSharedThread && (ui64)hpnow > Ctx.SoftDeadlineTs) {
                        needToStop = true;
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

        while (!needToStop && !StopFlag.load(std::memory_order_relaxed)) {
            if (TlsThreadContext->CapturedType == ESendingType::Tail) {
                TlsThreadContext->CapturedType = ESendingType::Lazy;
                ui32 activation = std::exchange(TlsThreadContext->CapturedActivation, 0);
                executeActivation(activation, true);
                continue;
            }
            Ctx.IsNeededToWaitNextActivation = !TlsThreadContext->CapturedActivation && !IsSharedThread;
            ui32 activation = ExecutorPool->GetReadyActivation(Ctx, ++RevolvingReadCounter);
            if (!activation) {
                activation = std::exchange(TlsThreadContext->CapturedActivation, 0);
            } else if (TlsThreadContext->CapturedActivation) {
                ui32 capturedActivation = std::exchange(TlsThreadContext->CapturedActivation, 0);
                ExecutorPool->ScheduleActivation(capturedActivation);
            }
            if (!activation) {
                return {IsSharedThread, wasWorking};
            }
            executeActivation(activation, false);
        }

        if (IsSharedThread) {
            Ctx.UpdateThreadTime();
        }

        return {IsSharedThread, wasWorking};
    }

    void* TExecutorThread::ThreadProc() {
#ifdef _linux_
        pid_t tid = syscall(SYS_gettid);
        AtomicSet(ThreadId, (ui64)tid);
#endif

#ifdef BALLOC
        ThreadDisableBalloc();
#endif

        TlsThreadCtx.WorkerCtx = &Ctx;
        TlsThreadCtx.ActorSystemIndex = ActorSystemIndex;
        TlsThreadCtx.ElapsingActorActivity = ActorSystemIndex;
        NHPTimer::STime now = GetCycleCountFast();
        TlsThreadCtx.StartOfProcessingEventTS = now;
        TlsThreadCtx.ActivationStartTS = now;
        TlsThreadContext = &TlsThreadCtx;
        if (ThreadName) {
            ::SetCurrentThreadName(ThreadName);
        }

        ProcessExecutorPool(ExecutorPool);
        return nullptr;
    }

    TGenericExecutorThread::TProcessingResult TSharedExecutorThread::ProcessSharedExecutorPool(TExecutorPoolBaseMailboxed *pool) {
        TWorkerId workerId = (pool == ThreadCtx->ExecutorPools[0].load(std::memory_order_relaxed) ? -1 : -2);
        Ctx.Switch(
            pool,
            pool->MailboxTable.Get(),
            NHPTimer::GetClockRate() * TimePerMailbox.SecondsFloat(),
            EventsPerMailbox,
            (workerId == -1 ? -1 : GetCycleCountFast() + SoftProcessingDurationTs),
            &SharedStats[pool->PoolId]);
        Ctx.WorkerId = workerId;
        return ProcessExecutorPool(pool);
    }

    void* TSharedExecutorThread::ThreadProc() {
#ifdef _linux_
        pid_t tid = syscall(SYS_gettid);
        AtomicSet(ThreadId, (ui64)tid);
#endif

#ifdef BALLOC
        ThreadDisableBalloc();
#endif

        TlsThreadCtx.WorkerCtx = &Ctx;
        TlsThreadCtx.ActorSystemIndex = ActorSystemIndex;
        TlsThreadCtx.ElapsingActorActivity = ActorSystemIndex;
        NHPTimer::STime now = GetCycleCountFast();
        TlsThreadCtx.StartOfProcessingEventTS = now;
        TlsThreadCtx.ActivationStartTS = now;
        TlsThreadContext = &TlsThreadCtx;
        if (ThreadName) {
            ::SetCurrentThreadName(ThreadName);
        }

        do {
            bool wasWorking = true;
            while (wasWorking && !StopFlag.load(std::memory_order_relaxed)) {
                wasWorking = false;

                for (ui32 poolIdx = 0; poolIdx < MaxPoolsForSharedThreads; ++poolIdx) {
                    TExecutorPoolBaseMailboxed *pool = dynamic_cast<TExecutorPoolBaseMailboxed*>(ThreadCtx->ExecutorPools[poolIdx].load(std::memory_order_acquire));
                    if (!pool) {
                        break;
                    }
                    TProcessingResult result = ProcessSharedExecutorPool(pool);
                    wasWorking |= result.WasWorking;
                }
            }

            if (!wasWorking && !StopFlag.load(std::memory_order_relaxed)) {
                ThreadCtx->UnsetWork();
                ThreadCtx->Wait(0, &StopFlag);
            }

        } while (!StopFlag.load(std::memory_order_acquire));

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
                    Y_ABORT();
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
                    Y_ABORT();
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
                    Y_ABORT();
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
                    Y_ABORT();
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
            Y_ABORT();
        AtomicBarrier();
    }

    bool TMailboxHeader::UnlockFromExecution2(bool wouldReschedule) {
        AtomicBarrier();
        for (;;) {
            const ui32 state = AtomicLoad(&ExecutionState);
            switch (state) {
                case TExecutionState::Inactive:
                case TExecutionState::Scheduled:
                    Y_ABORT();
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
                    Y_ABORT();
                case TExecutionState::LeavingMarked:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::Scheduled, TExecutionState::LeavingMarked))
                        return true;
                    break;
                case TExecutionState::Free:
                case TExecutionState::FreeScheduled:
                    Y_ABORT();
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
                    Y_ABORT();
                case TExecutionState::FreeLeavingMarked:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::FreeLeavingMarked))
                        return true;
                    break;
                default:
                    Y_ABORT();
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
                    Y_ABORT();
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
                    Y_ABORT();
                case TExecutionState::LeavingMarked:
                    if (AtomicUi32Cas(&ExecutionState, TExecutionState::FreeScheduled, TExecutionState::LeavingMarked))
                        return true;
                    break;
                case TExecutionState::Free:
                case TExecutionState::FreeScheduled:
                case TExecutionState::FreeLeaving:
                case TExecutionState::FreeExecuting:
                case TExecutionState::FreeLeavingMarked:
                    Y_ABORT();
                default:
                    Y_ABORT();
            }
        }
    }

    void TGenericExecutorThread::UpdateThreadStats() {
        NHPTimer::STime hpnow = GetCycleCountFast();
        ui64 activityType = TlsThreadCtx.ElapsingActorActivity.load(std::memory_order_acquire);
        NHPTimer::STime hpprev = TlsThreadCtx.UpdateStartOfProcessingEventTS(hpnow);
        if (activityType == Max<ui64>()) {
            Ctx.AddParkedCycles(hpnow - hpprev);
        } else {
            Ctx.AddElapsedCycles(activityType, hpnow - hpprev);
        }
        if (activityType != Max<ui64>()) {
            NHPTimer::STime activationStart = TlsThreadCtx.ActivationStartTS.load(std::memory_order_acquire);
            NHPTimer::STime passedTime = Max<i64>(hpnow - activationStart, 0);
            Ctx.SetCurrentActivationTime(activityType, Ts2Us(passedTime));
        } else {
            Ctx.SetCurrentActivationTime(0, 0);
        }
    }

    void TGenericExecutorThread::GetCurrentStats(TExecutorThreadStats& statsCopy) {
        UpdateThreadStats();
        Ctx.GetCurrentStats(statsCopy);
    }

    void TGenericExecutorThread::GetSharedStats(i16 poolId, TExecutorThreadStats &statsCopy) {
        UpdateThreadStats();
        statsCopy = TExecutorThreadStats();
        statsCopy.Aggregate(SharedStats[poolId]);
    }

    void TGenericExecutorThread::GetCurrentStatsForHarmonizer(TExecutorThreadStats& statsCopy) {
        statsCopy.SafeElapsedTicks = RelaxedLoad(&Ctx.Stats->SafeElapsedTicks);
        statsCopy.CpuUs = RelaxedLoad(&Ctx.Stats->CpuUs);
        statsCopy.NotEnoughCpuExecutions = RelaxedLoad(&Ctx.Stats->NotEnoughCpuExecutions);
    }

    void TGenericExecutorThread::GetSharedStatsForHarmonizer(i16 poolId, TExecutorThreadStats &stats) {
        stats.SafeElapsedTicks = RelaxedLoad(&SharedStats[poolId].SafeElapsedTicks);
        stats.CpuUs = RelaxedLoad(&SharedStats[poolId].CpuUs);
        stats.NotEnoughCpuExecutions = RelaxedLoad(&SharedStats[poolId].NotEnoughCpuExecutions);
    }


    TGenericExecutorThreadCtx::~TGenericExecutorThreadCtx()
    {}
}
