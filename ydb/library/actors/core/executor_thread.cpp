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
#include "debug.h"
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


#define POOL_ID() \
    (!TlsThreadContext ? "OUTSIDE" : \
    (TlsThreadContext->IsShared() ? "Shared[" + ToString(TlsThreadContext->OwnerPoolId()) + "]_" + ToString(TlsThreadContext->PoolId()) : \
    ("Pool_" + ToString(TlsThreadContext->PoolId()))))

#define WORKER_ID() ("Worker_" + ToString(TlsThreadContext ? TlsThreadContext->WorkerId() : Max<TWorkerId>()))

#define EXECUTOR_THREAD_DEBUG(level, ...) \
    ACTORLIB_DEBUG(level, POOL_ID(), " ", WORKER_ID(), " TExecutorThread::", __func__, ": ", __VA_ARGS__)


namespace NActors {
    constexpr TDuration TExecutorThread::DEFAULT_TIME_PER_MAILBOX;

    TExecutorThread::TExecutorThread(
            TWorkerId workerId,
            TActorSystem* actorSystem,
            IExecutorPool* executorPool,
            const TString& threadName)
        : ActorSystem(actorSystem)
        , Stats(1)
        , ThreadCtx(workerId, executorPool, nullptr)
        , ExecutionStats()
        , ThreadName(threadName)
        , ActorSystemIndex(TActorTypeOperator::GetActorSystemIndex())
    {
        ExecutionStats.Switch(&Stats[0]);
    }

    TExecutorThread::TExecutorThread(TWorkerId workerId,
            TActorSystem* actorSystem,
            IExecutorPool* sharedPool,
            IExecutorPool* executorPool,
            i16 poolCount,
            const TString& threadName,
            ui64 softProcessingDurationTs)
        : ActorSystem(actorSystem)
        , Stats(poolCount)
        , ThreadCtx(workerId, executorPool, sharedPool)
        , ExecutionStats()
        , ThreadName(threadName)
        , SoftProcessingDurationTs(softProcessingDurationTs)
        , ActorSystemIndex(TActorTypeOperator::GetActorSystemIndex())
    {
        Stats.resize(poolCount);
        ExecutionStats.Switch(&Stats[executorPool->PoolId]);
    }

    void TExecutorThread::SwitchPool(TExecutorPoolBaseMailboxed* pool) {
        Y_ABORT_UNLESS(ThreadCtx.IsShared());
        ExecutionStats.Switch(&Stats[pool->PoolId]);
        ThreadCtx.AssignPool(pool);
    }

    void TExecutorThread::UnregisterActor(TMailbox* mailbox, TActorId actorId) {
        Y_DEBUG_ABORT_UNLESS(actorId.PoolID() == ThreadCtx.PoolId() && ThreadCtx.Pool()->ResolveMailbox(actorId.Hint()) == mailbox);
        IActor* actor = mailbox->DetachActor(actorId.LocalId());
        ExecutionStats.DecrementActorsAliveByActivity(actor->GetActivityType());
        DyingActors.push_back(THolder(actor));
    }

    void TExecutorThread::DropUnregistered() {
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        if (ActorSystem->MonitorStuckActors()) {
            if (auto *pool = dynamic_cast<TExecutorPoolBaseMailboxed*>(ThreadCtx.Pool())) {
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
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "DyingActors.clear()");
        DyingActors.clear(); // here is actual destruction of actors
    }

    void TExecutorThread::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        ThreadCtx.Pool()->Schedule(deadline, ev, cookie, ThreadCtx.WorkerId());
    }

    void TExecutorThread::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        ThreadCtx.Pool()->Schedule(deadline, ev, cookie, ThreadCtx.WorkerId());
    }

    void TExecutorThread::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie) {
        ++CurrentActorScheduledEventsCounter;
        ThreadCtx.Pool()->Schedule(delta, ev, cookie, ThreadCtx.WorkerId());
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

    ui32 TExecutorThread::GetOverwrittenEventsPerMailbox() const {
        return ThreadCtx.OverwrittenEventsPerMailbox();
    }

    void TExecutorThread::SetOverwrittenEventsPerMailbox(ui32 value) {
        ThreadCtx.SetOverwrittenEventsPerMailbox(Max(value, ThreadCtx.EventsPerMailbox()));
    }

    ui64 TExecutorThread::GetOverwrittenTimePerMailboxTs() const {
        return ThreadCtx.OverwrittenTimePerMailboxTs();
    }

    void TExecutorThread::SetOverwrittenTimePerMailboxTs(ui64 value) {
        ThreadCtx.SetOverwrittenTimePerMailboxTs(Max(value, ThreadCtx.TimePerMailboxTs()));
    }

    void TExecutorThread::SubscribeToPreemption(TActorId actorId) {
        ThreadCtx.ExecutionContext.PreemptionSubscribed.push_back(actorId);
    }

    TExecutorThread::TProcessingResult TExecutorThread::Execute(TMailbox* mailbox, bool isTailExecution) {
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Activation, "Execute mailbox");
        Y_ABORT_UNLESS(mailbox, "mailbox must be not null");
        Y_DEBUG_ABORT_UNLESS(DyingActors.empty());

        TExecutionContext& execCtx = ThreadCtx.ExecutionContext;

        if (!isTailExecution) {
            execCtx.ExecutedEvents = 0;
            execCtx.HPStart = GetCycleCountFast();
        }

        IActor* actor = nullptr;
        const std::type_info* actorType = nullptr;
        ui32 prevActivityType = std::numeric_limits<ui32>::max();
        TActorId recipient;
        bool firstEvent = true;
        bool preemptedByEventCount = false;
        bool preemptedByCycles = false;
        bool preemptedByTailSend = false;
        bool wasWorking = false;
        NHPTimer::STime hpnow = execCtx.HPStart;
        NHPTimer::STime hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
        ExecutionStats.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);
        NHPTimer::STime eventStart = execCtx.HPStart;
        TlsThreadContext->ActivityContext.ActivationStartTS.store(execCtx.HPStart, std::memory_order_release);

        ThreadCtx.ResetOverwrittenEventsPerMailbox();
        ThreadCtx.ResetOverwrittenTimePerMailboxTs();
        bool drained = false;
        for (; execCtx.ExecutedEvents < ThreadCtx.OverwrittenEventsPerMailbox(); execCtx.ExecutedEvents++) {
            if (TAutoPtr<IEventHandle> evExt = mailbox->Pop()) {
                EXECUTOR_THREAD_DEBUG(EDebugLevel::Event, "mailbox->Pop()");
                recipient = evExt->GetRecipientRewrite();
                actor = mailbox->FindActor(recipient.LocalId());
                if (!actor) {
                    actor = mailbox->FindAlias(recipient.LocalId());
                    if (actor) {
                        // Work as if some alias actor rewrites events and delivers them to the real actor id
                        evExt->Rewrite(evExt->GetTypeRewrite(), actor->SelfId());
                        recipient = evExt->GetRecipientRewrite();
                    }
                }
                TActorContext ctx(*mailbox, *this, eventStart, recipient);
                TlsActivationContext = &ctx; // ensure dtor (if any) is called within actor system
                // move for destruct before ctx;
                auto ev = std::move(evExt);
                if (actor) {
                    EXECUTOR_THREAD_DEBUG(EDebugLevel::Event, "actor is not null");
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
                        double usec = ExecutionStats.AddActivationStats(mailbox->ScheduleMoment, hpprev);
                        if (usec > 500) {
                            GLOBAL_LWPROBE(ACTORLIB_PROVIDER, SlowActivation, TlsThreadContext->Pool()->PoolId, usec / 1000.0);
                        }
                        firstEvent = false;
                    }

                    i64 usecDeliv = ExecutionStats.AddEventDeliveryStats(ev->SendTime, hpprev);
                    if (usecDeliv > 5000) {
                        double sinceActivationMs = NHPTimer::GetSeconds(hpprev - execCtx.HPStart) * 1000.0;
                        LwTraceSlowDelivery(ev.Get(), actorType, ThreadCtx.PoolId(), CurrentRecipient, NHPTimer::GetSeconds(hpprev - ev->SendTime) * 1000.0, sinceActivationMs, execCtx.ExecutedEvents);
                    }

                    ui32 evTypeForTracing = ev->Type;

                    ui32 activityType = actor->GetActivityType();
                    if (activityType != prevActivityType) {
                        prevActivityType = activityType;
                        NProfiling::TMemoryTagScope::Reset(activityType);
                        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(activityType, std::memory_order_release);
                    }

                    actor->Receive(ev);

                    hpnow = GetCycleCountFast();
                    hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);

                    actor->OnDequeueEvent();

                    size_t dyingActorsCnt = DyingActors.size();
                    EXECUTOR_THREAD_DEBUG(EDebugLevel::Event, "dyingActorsCnt ", dyingActorsCnt);
                    ExecutionStats.UpdateActorsStats(dyingActorsCnt, ThreadCtx.Pool());
                    if (dyingActorsCnt) {
                        DropUnregistered();
                        actor = nullptr;
                    }

                    if (mailbox->IsEmpty()) {
                        // had actors and became empty, prepare to reclaim mailbox
                        mailbox->LockToFree();
                    }

                    ExecutionStats.AddElapsedCycles(activityType, hpnow - hpprev);
                    NHPTimer::STime elapsed = ExecutionStats.AddEventProcessingStats(eventStart, hpnow, activityType, CurrentActorScheduledEventsCounter);
                    mailbox->AddElapsedCycles(elapsed);
                    if (elapsed > 1000000) {
                        LwTraceSlowEvent(ev.Get(), evTypeForTracing, actorType, ThreadCtx.PoolId(), CurrentRecipient, NHPTimer::GetSeconds(elapsed) * 1000.0);
                    }

                    // The actor might have been destroyed
                    if (actor)
                        actor->AddElapsedTicks(elapsed);

                    CurrentRecipient = TActorId();
                } else {
                    EXECUTOR_THREAD_DEBUG(EDebugLevel::Event, "actor is null");
                    actorType = nullptr;

                    TAutoPtr<IEventHandle> nonDelivered = IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown);
                    if (nonDelivered.Get()) {
                        ActorSystem->Send(nonDelivered);
                    } else {
                        ExecutionStats.IncrementNonDeliveredEvents();
                    }
                    hpnow = GetCycleCountFast();
                    hpprev = TlsThreadContext->UpdateStartOfProcessingEventTS(hpnow);
                    ExecutionStats.AddElapsedCycles(ActorSystemIndex, hpnow - hpprev);
                }
                eventStart = hpnow;

                if (TlsThreadContext->CheckCapturedSendingType(ESendingType::Tail)) {
                    ExecutionStats.IncrementMailboxPushedOutByTailSending();
                    LWTRACK(MailboxPushedOutByTailSending,
                            ThreadCtx.ExecutionContext.Orbit,
                            ThreadCtx.PoolId(),
                            ThreadCtx.PoolName(),
                            execCtx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - execCtx.HPStart),
                            ThreadCtx.WorkerId(),
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preemptedByTailSend = true;
                    break;
                }

                // Soft preemption in united pool
                if (ThreadCtx.SoftDeadlineTs() < (ui64)hpnow) {
                    ExecutionStats.IncrementMailboxPushedOutBySoftPreemption();
                    LWTRACK(MailboxPushedOutBySoftPreemption,
                            ThreadCtx.ExecutionContext.Orbit,
                            ThreadCtx.PoolId(),
                            ThreadCtx.PoolName(),
                            execCtx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - execCtx.HPStart),
                            ThreadCtx.WorkerId(),
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preemptedByCycles = true;
                    break;
                }

                // time limit inside one mailbox passed, let others do some work
                if (hpnow - execCtx.HPStart > (i64)ThreadCtx.TimePerMailboxTs()) {
                    ExecutionStats.IncrementMailboxPushedOutByTime();
                    LWTRACK(MailboxPushedOutByTime,
                            ThreadCtx.ExecutionContext.Orbit,
                            ThreadCtx.PoolId(),
                            ThreadCtx.PoolName(),
                            execCtx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - execCtx.HPStart),
                            ThreadCtx.WorkerId(),
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preemptedByCycles = true;
                    break;
                }

                if (execCtx.ExecutedEvents + 1 == ThreadCtx.EventsPerMailbox()) {
                    ExecutionStats.IncrementMailboxPushedOutByEventCount();
                    LWTRACK(MailboxPushedOutByEventCount,
                            ThreadCtx.ExecutionContext.Orbit,
                            ThreadCtx.PoolId(),
                            ThreadCtx.PoolName(),
                            execCtx.ExecutedEvents + 1,
                            CyclesToDuration(hpnow - execCtx.HPStart),
                            ThreadCtx.WorkerId(),
                            recipient.ToString(),
                            SafeTypeName(actorType));
                    preemptedByEventCount = true;
                    break;
                }
            } else {
                if (execCtx.ExecutedEvents == 0)
                    ExecutionStats.IncrementEmptyMailboxActivation();
                LWTRACK(MailboxEmpty,
                        ThreadCtx.ExecutionContext.Orbit,
                        ThreadCtx.PoolId(),
                        ThreadCtx.PoolName(),
                        execCtx.ExecutedEvents,
                        CyclesToDuration(GetCycleCountFast() - execCtx.HPStart),
                        ThreadCtx.WorkerId(),
                        recipient.ToString(),
                        SafeTypeName(actor));
                drained = true;
                break; // empty queue, leave
            }
        }
        if (execCtx.PreemptionSubscribed.size()) {
            std::unique_ptr<TEvents::TEvPreemption> event = std::make_unique<TEvents::TEvPreemption>();
            event->ByEventCount = preemptedByEventCount;
            event->ByCycles = preemptedByCycles;
            event->ByTailSend = preemptedByTailSend;
            event->EventCount = execCtx.ExecutedEvents;
            event->Cycles = hpnow - execCtx.HPStart;
            TAutoPtr<IEventHandle> ev = new IEventHandle(TActorId(), TActorId(), event.release());
            for (const auto& actorId : execCtx.PreemptionSubscribed) {
                IActor *actor = mailbox->FindActor(actorId.LocalId());
                if (actor) {
                    actor->Receive(ev);
                }
            }
            execCtx.PreemptionSubscribed.clear();
        }
        TlsThreadContext->ActivityContext.ActivationStartTS.store(hpnow, std::memory_order_release);
        TlsThreadContext->ActivityContext.ElapsingActorActivity.store(ActorSystemIndex, std::memory_order_release);

        NProfiling::TMemoryTagScope::Reset(0);
        TlsActivationContext = nullptr;
        if (mailbox->IsEmpty() && drained) {
            ThreadCtx.FreeMailbox(mailbox);
        } else {
            mailbox->Unlock(ThreadCtx.Pool(), hpnow, RevolvingWriteCounter);
        }
        return {preemptedByEventCount || preemptedByCycles, wasWorking};
    }

    TThreadId TExecutorThread::GetThreadId() const {
#ifdef _linux_
        while (AtomicLoad(&ThreadId) == UnknownThreadId) {
            NanoSleep(1000);
        }
#endif
        return ThreadId;
    }

    TWorkerId TExecutorThread::GetWorkerId() const {
        return ThreadCtx.WorkerId();
    }

    void TExecutorThread::ProcessExecutorPool() {
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "start");
        IExecutorPool* initPool = ThreadCtx.IsShared() ? ThreadCtx.SharedPool() : ThreadCtx.Pool();
        initPool->Initialize();
        initPool->SetRealTimeMode();
        TAffinityGuard affinity(initPool->Affinity());

        NHPTimer::STime hpnow = GetCycleCountFast();
        NHPTimer::STime hpprev = hpnow;
        ui64 execCount = 0;
        ui64 readyActivationCount = 0;
        i64 execCycles = 0;
        i64 nonExecCycles = 0;

        auto executeActivation = [&](TMailbox* mailbox, bool isTailExecution) {
            EXECUTOR_THREAD_DEBUG(EDebugLevel::Activation, "executeActivation");
            LWTRACK(ActivationBegin, ThreadCtx.ExecutionContext.Orbit, ThreadCtx.PoolId(), ThreadCtx.WorkerId());
            readyActivationCount++;
            if (true /* already have pointer TMailbox* mailbox = ExecutionStats.MailboxTable->Get(activation) */) {
                if (true /* already locked header->LockForExecution() */) {
                    hpnow = GetCycleCountFast();
                    nonExecCycles += hpnow - hpprev;
                    hpprev = hpnow;
                    {
                        auto result = Execute(mailbox, isTailExecution);
                        if (result.IsPreempted) {
                            TlsThreadContext->ChangeCapturedSendingType(ESendingType::Lazy);
                        }
                    }
                    hpnow = GetCycleCountFast();
                    i64 currentExecCycles = hpnow - hpprev;
                    execCycles += currentExecCycles;
                    hpprev = hpnow;
                    execCount++;
                    if (execCycles + nonExecCycles > 39000000) { // every 15 ms at 2.6GHz, so 1000 items is 15 sec (solomon interval)
                        LWPROBE(ExecutorThreadStats, ThreadCtx.PoolId(), ThreadCtx.PoolName(), ThreadCtx.WorkerId(),
                                execCount, readyActivationCount,
                                NHPTimer::GetSeconds(execCycles) * 1000.0, NHPTimer::GetSeconds(nonExecCycles) * 1000.0);
                        execCount = 0;
                        readyActivationCount = 0;
                        execCycles = 0;
                        nonExecCycles = 0;
                        ExecutionStats.UpdateThreadTime();
                    }

                    if (!TlsThreadContext->IsEnoughCpu) {
                        ExecutionStats.IncreaseNotEnoughCpuExecutions();
                        TlsThreadContext->IsEnoughCpu = true;
                    }
                }
            }
            LWTRACK(ActivationEnd, ThreadCtx.ExecutionContext.Orbit, ThreadCtx.PoolId(), ThreadCtx.WorkerId());
            ThreadCtx.ExecutionContext.Orbit.Reset();
        };

        IExecutorPool* mainPool = ThreadCtx.IsShared() ? ThreadCtx.SharedPool() : ThreadCtx.Pool();

        while (!StopFlag.load(std::memory_order_relaxed)) {
            if (TlsThreadContext->CheckCapturedSendingType(ESendingType::Tail)) {
                TMailbox* mailbox = ThreadCtx.CaptureMailbox(nullptr);
                Y_ABORT_UNLESS(mailbox, "activation must be not null");
                executeActivation(mailbox, true);
                continue;
            }
            TMailbox* capturedMailbox = ThreadCtx.CaptureMailbox(nullptr);
            ThreadCtx.ExecutionContext.IsNeededToWaitNextActivation = !capturedMailbox;
            TMailbox* mailbox = mainPool->GetReadyActivation(++RevolvingReadCounter);
            if (!mailbox) {
                mailbox = capturedMailbox;
            } else if (capturedMailbox) {
                mainPool->ScheduleActivation(capturedMailbox);
            }
            if (!mailbox) {
                EXECUTOR_THREAD_DEBUG(EDebugLevel::Activation, "no activation");
                break;
            }
            executeActivation(mailbox, false);
        }
    }

    void* TExecutorThread::ThreadProc() {
#ifdef _linux_
        pid_t tid = syscall(SYS_gettid);
        AtomicSet(ThreadId, (ui64)tid);
#endif

#ifdef BALLOC
        ThreadDisableBalloc();
#endif

        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "start ", ThreadName);
        ThreadCtx.ExecutionStats = &ExecutionStats;
        ThreadCtx.ActivityContext.ActorSystemIndex = ActorSystemIndex;
        ThreadCtx.ActivityContext.ElapsingActorActivity = ActorSystemIndex;
        NHPTimer::STime now = GetCycleCountFast();
        ThreadCtx.ActivityContext.StartOfProcessingEventTS = now;
        ThreadCtx.ActivityContext.ActivationStartTS = now;
        TlsThreadContext = &ThreadCtx;
        if (ThreadName) {
            ::SetCurrentThreadName(ThreadName);
        }

        ProcessExecutorPool();
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "end ", ThreadName);
        TlsThreadContext = nullptr;
        return nullptr;
    }

    void TExecutorThread::UpdateThreadStats() {
        NHPTimer::STime hpnow = GetCycleCountFast();

        ui32 activityType = ThreadCtx.ActivityContext.ElapsingActorActivity.load(std::memory_order_acquire);
        NHPTimer::STime hpprev = ThreadCtx.UpdateStartOfProcessingEventTS(hpnow);
        if (activityType == SleepActivity) {
            ExecutionStats.AddParkedCycles(hpnow - hpprev);
            ExecutionStats.SetCurrentActivationTime(0, 0);
        } else {
            ExecutionStats.AddElapsedCycles(activityType, hpnow - hpprev);
            ExecutionStats.AddOveraddedCpuUs(Ts2Us(hpnow - hpprev));
            NHPTimer::STime activationStart = ThreadCtx.ActivityContext.ActivationStartTS.load(std::memory_order_acquire);
            NHPTimer::STime passedTime = Max<i64>(hpnow - activationStart, 0);
            ExecutionStats.SetCurrentActivationTime(activityType, Ts2Us(passedTime));
        }
        ExecutionStats.CopySafeTicks();
    }

    void TExecutorThread::GetCurrentStats(TExecutorThreadStats& statsCopy) {
        UpdateThreadStats();
        ExecutionStats.GetCurrentStats(statsCopy);
    }

    void TExecutorThread::GetSharedStats(i16 poolId, TExecutorThreadStats &statsCopy) {
        UpdateThreadStats();
        statsCopy = TExecutorThreadStats();
        statsCopy.Aggregate(Stats[poolId]);
    }

    void TExecutorThread::GetCurrentStatsForHarmonizer(TExecutorThreadStats& statsCopy) {
        UpdateThreadStats();
        statsCopy.SafeElapsedTicks = RelaxedLoad(&ExecutionStats.Stats->SafeElapsedTicks);
        statsCopy.SafeParkedTicks = RelaxedLoad(&ExecutionStats.Stats->SafeParkedTicks);
        statsCopy.CpuUs = RelaxedLoad(&ExecutionStats.Stats->CpuUs);
        statsCopy.NotEnoughCpuExecutions = RelaxedLoad(&ExecutionStats.Stats->NotEnoughCpuExecutions);
    }

    void TExecutorThread::GetSharedStatsForHarmonizer(i16 poolId, TExecutorThreadStats &stats) {
        UpdateThreadStats();
        stats.SafeElapsedTicks = RelaxedLoad(&Stats[poolId].SafeElapsedTicks);
        stats.SafeParkedTicks = RelaxedLoad(&Stats[poolId].SafeParkedTicks);
        stats.CpuUs = RelaxedLoad(&Stats[poolId].CpuUs);
        stats.NotEnoughCpuExecutions = RelaxedLoad(&Stats[poolId].NotEnoughCpuExecutions);
    }

    TExecutorThread::~TExecutorThread() {
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, ThreadName, ' ');
    }

    TGenericExecutorThreadCtx::~TGenericExecutorThreadCtx()
    {
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "dtor start");
        if (Thread) {
            Thread->Join();
            EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "join end");
        } else {
            EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "thread is null");
        }
        Thread.reset();
        EXECUTOR_THREAD_DEBUG(EDebugLevel::Executor, "end");
    }
}
