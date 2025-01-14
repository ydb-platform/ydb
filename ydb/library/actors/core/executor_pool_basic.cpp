#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_basic_sanitizer.h"
#include "executor_pool_shared.h"
#include "executor_pool_jail.h"
#include "actor.h"
#include "config.h"
#include "executor_thread_ctx.h"
#include "probes.h"
#include "mailbox.h"
#include "debug.h"
#include "thread_context.h"
#include <atomic>
#include <memory>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>

#ifdef _linux_
#include <pthread.h>
#endif

namespace NActors {

    namespace {
#ifdef ACTOR_SANITIZER
        constexpr bool DebugMode = true;
#else
        constexpr bool DebugMode = false;
#endif
    }


    LWTRACE_USING(ACTORLIB_PROVIDER);


    const double TWaitingStatsConstants::HistogramResolutionUs = MaxSpinThersholdUs / BucketCount;
    const ui64 TWaitingStatsConstants::HistogramResolution = NHPTimer::GetCyclesPerSecond() * 0.000001 * HistogramResolutionUs;

    constexpr TDuration TBasicExecutorPool::DEFAULT_TIME_PER_MAILBOX;

    TString GetCurrentThreadKind() { 
        if (TlsThreadContext) {
            return TlsThreadContext->WorkerId >= 0 ? "[common]" : "[shared]";
        }
        return "[outsider]";
    }

    TBasicExecutorPool::TBasicExecutorPool(
        ui32 poolId,
        ui32 threads,
        ui64 spinThreshold,
        const TString& poolName,
        IHarmonizer *harmonizer,
        TAffinity* affinity,
        TDuration timePerMailbox,
        ui32 eventsPerMailbox,
        int realtimePriority,
        ui32 /*maxActivityType*/,
        i16 minThreadCount,
        i16 maxThreadCount,
        i16 defaultThreadCount,
        i16 priority,
        bool hasOwnSharedThread,
        TExecutorPoolJail *jail
    )
        : TBasicExecutorPool(TBasicExecutorPoolConfig{
            .PoolId = poolId,
            .PoolName = poolName,
            .Threads = threads,
            .SpinThreshold = spinThreshold,
            .Affinity = (affinity ? static_cast<TCpuMask>(*affinity) : TCpuMask{}),
            .TimePerMailbox = timePerMailbox,
            .EventsPerMailbox = eventsPerMailbox,
            .RealtimePriority = realtimePriority,
            .MinThreadCount = minThreadCount,
            .MaxThreadCount = maxThreadCount,
            .DefaultThreadCount = defaultThreadCount,
            .Priority = priority,
            .HasSharedThread = hasOwnSharedThread,
        }, harmonizer, jail)
    {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::ctor ", poolName, ' ', threads);
        if (affinity != nullptr) {
            delete affinity;
        }
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::ctor end");
    }

    TBasicExecutorPool::TBasicExecutorPool(const TBasicExecutorPoolConfig& cfg, IHarmonizer *harmonizer, TExecutorPoolJail *jail)
        : TExecutorPoolBase(cfg.PoolId, cfg.Threads, new TAffinity(cfg.Affinity), cfg.UseRingQueue)
        , DefaultSpinThresholdCycles(cfg.SpinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , SpinThresholdCycles(DefaultSpinThresholdCycles)
        , SpinThresholdCyclesPerThread(new NThreading::TPadded<std::atomic<ui64>>[cfg.Threads])
        , WaitingStats(new TWaitingStats<ui64>[cfg.Threads])
        , PoolName(cfg.PoolName)
        , TimePerMailbox(cfg.TimePerMailbox)
        , EventsPerMailbox(cfg.EventsPerMailbox)
        , RealtimePriority(cfg.RealtimePriority)
        , ThreadCount(cfg.Threads)
        , MinFullThreadCount(cfg.MinThreadCount)
        , MaxFullThreadCount(cfg.MaxThreadCount)
        , DefaultFullThreadCount(cfg.DefaultThreadCount)
        , Harmonizer(harmonizer)
        , SoftProcessingDurationTs(cfg.SoftProcessingDurationTs)
        , HasOwnSharedThread(cfg.HasSharedThread)
        , Priority(cfg.Priority)
        , Jail(jail)
        , ActorSystemProfile(cfg.ActorSystemProfile)
    {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::ctor start ", cfg.PoolName, ' ', cfg.Threads);
        Y_UNUSED(Jail, SoftProcessingDurationTs);

        ui32 threads = ThreadCount;
        if (HasOwnSharedThread && threads) {
            threads = threads - 1;
        }

        if constexpr (NFeatures::IsLocalQueues()) {
            LocalQueues.Reset(new NThreading::TPadded<std::queue<ui32>>[threads]);
            if constexpr (NFeatures::TLocalQueuesFeatureFlags::FIXED_LOCAL_QUEUE_SIZE) {
                LocalQueueSize = *NFeatures::TLocalQueuesFeatureFlags::FIXED_LOCAL_QUEUE_SIZE;
            } else {
                LocalQueueSize = NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE;
            }
        }
        if constexpr (NFeatures::TSpinFeatureFlags::CalcPerThread) {
            for (ui32 idx = 0; idx < threads; ++idx) {
                SpinThresholdCyclesPerThread[idx].store(0);
            }
        }
        if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
            MovingWaitingStats.Reset(new TWaitingStats<double>[threads]);
        }

        i16 limit = Min(threads, (ui32)Max<i16>());
        if (DefaultFullThreadCount) {
            DefaultFullThreadCount = Min<i16>(DefaultFullThreadCount - HasOwnSharedThread, limit);
        } else {
            DefaultFullThreadCount = limit;
        }

        MaxFullThreadCount = Min(Max<i16>(MaxFullThreadCount - HasOwnSharedThread, DefaultFullThreadCount), limit);

        if (MinFullThreadCount) {
            MinFullThreadCount = Min<i16>(MinFullThreadCount - HasOwnSharedThread, DefaultFullThreadCount);
        } else {
            MinFullThreadCount = DefaultFullThreadCount;
        }

        ThreadCount = static_cast<i16>(MaxFullThreadCount);
        auto semaphore = TSemaphore();
        semaphore.CurrentThreadCount = ThreadCount;
        Semaphore = semaphore.ConvertToI64();

        DefaultThreadCount = DefaultFullThreadCount + HasOwnSharedThread;
        MinThreadCount = MinFullThreadCount + HasOwnSharedThread;
        MaxThreadCount = MaxFullThreadCount + HasOwnSharedThread;

        Threads.Reset(new NThreading::TPadded<TExecutorThreadCtx>[MaxFullThreadCount]);
        if constexpr (DebugMode) {
            Sanitizer.reset(new TBasicExecutorPoolSanitizer(this));
        }
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::ctor end");
    }

    TBasicExecutorPool::~TBasicExecutorPool() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::dtor start");
        Threads.Destroy();
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::dtor end");
    }

    void TBasicExecutorPool::AskToGoToSleep(bool *needToWait, bool *needToBlock) {
        TAtomic x = AtomicGet(Semaphore);
        do {
            i64 oldX = x;
            TSemaphore semaphore = TSemaphore::GetSemaphore(x);;
            if (semaphore.CurrentSleepThreadCount < 0) {
                semaphore.CurrentSleepThreadCount++;
                x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), x);
                if (x == oldX) {
                    *needToWait = true;
                    *needToBlock = true;
                    return;
                }
                continue;
            }

            if (semaphore.OldSemaphore == 0) {
                semaphore.CurrentSleepThreadCount++;
                if (semaphore.CurrentSleepThreadCount == AtomicLoad(&ThreadCount)) {
                    AllThreadsSleep.store(true);
                }
                x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), x);
                if (x == oldX) {
                    *needToWait = true;
                    *needToBlock = false;
                    return;
                }
                continue;
            }

            *needToWait = false;
            *needToBlock = false;
            return;
        } while (true);
    }

    TMailbox* TBasicExecutorPool::GetReadyActivationCommon(TWorkerContext& wctx, ui64 revolvingCounter) {
        ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivationCommon: start");
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        Threads[workerId].UnsetWork();
        if (Harmonizer) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivationCommon: try to harmonize");
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivationCommon: harmonize done");
        }

        TAtomic semaphoreRaw = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(semaphoreRaw);
        while (!StopFlag.load(std::memory_order_acquire)) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivationCommon: semaphore.OldSemaphore == ", semaphore.OldSemaphore, " semaphore.CurrentSleepThreadCount == ", semaphore.CurrentSleepThreadCount);
            if (!semaphore.OldSemaphore || workerId >= 0 && semaphore.CurrentSleepThreadCount < 0) {
                ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, '_', workerId, " TBasicExecutorPool::GetReadyActivationCommon: semaphore.OldSemaphore == 0 or workerId >= 0 && semaphore.CurrentSleepThreadCount < 0");
                if (!wctx.IsNeededToWaitNextActivation) {
                    ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, '_', workerId, " TBasicExecutorPool::GetReadyActivationCommon: wctx.IsNeededToWaitNextActivation == false");
                    return nullptr;
                }

                bool needToWait = false;
                bool needToBlock = false;
                AskToGoToSleep(&needToWait, &needToBlock);
                if (needToWait) {
                    ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, '_', workerId, " TBasicExecutorPool::GetReadyActivationCommon: go to sleep");
                    if (Threads[workerId].Wait(SpinThresholdCycles, &StopFlag)) {
                        ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, '_', workerId, " TBasicExecutorPool::GetReadyActivationCommon: sleep interrupted");
                        return nullptr;
                    }
                }
            } else {
                TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                if (const ui32 activation = std::visit([&revolvingCounter](auto &x) {return x.Pop(++revolvingCounter);}, Activations)) {
                    ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, '_', workerId, " TBasicExecutorPool::GetReadyActivationCommon: activation found");
                    Threads[workerId].SetWork();
                    AtomicDecrement(Semaphore);
                    return MailboxTable->Get(activation);
                }
            }

            SpinLockPause();
            semaphoreRaw = AtomicGet(Semaphore);
            semaphore = TSemaphore::GetSemaphore(semaphoreRaw);
        }

        return nullptr;
    }

    TMailbox* TBasicExecutorPool::GetReadyActivationLocalQueue(TWorkerContext& wctx, ui64 revolvingCounter) {
        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < static_cast<i32>(MaxFullThreadCount));

        if (workerId >= 0 && LocalQueues[workerId].size()) {
            ui32 activation = LocalQueues[workerId].front();
            LocalQueues[workerId].pop();
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivation: local queue: activation found");
            return MailboxTable->Get(activation);
        } else {
            TlsThreadContext->WriteTurn = 0;
            TlsThreadContext->LocalQueueSize = LocalQueueSize.load(std::memory_order_relaxed);
        }
        ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivation: local queue done; moving to common");
        return GetReadyActivationCommon(wctx, revolvingCounter);
    }

    TMailbox* TBasicExecutorPool::GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
        if constexpr (NFeatures::IsLocalQueues()) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivation: local queue");
            return GetReadyActivationLocalQueue(wctx, revolvingCounter);
        } else {
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, "_", wctx.WorkerId, " TBasicExecutorPool::GetReadyActivation");
            return GetReadyActivationCommon(wctx, revolvingCounter);
        }
        return nullptr;
    }

    inline void TBasicExecutorPool::WakeUpLoop(i16 currentThreadCount) {
        for (i16 i = 0;;) {
            if (Threads[i].WakeUp()) {
                if (i >= currentThreadCount) {
                    AtomicIncrement(WrongWakenedThreadCount);
                }
                return;
            }
            if (++i >= MaxFullThreadCount) {
                i = 0;
            }
        }
    }

    void TBasicExecutorPool::ScheduleActivationExCommon(TMailbox* mailbox, ui64 revolvingCounter, TAtomic x) {
        TWorkerId workerId = Max<TWorkerId>();
        if (TlsThreadContext && TlsThreadContext->Pool == this && TlsThreadContext->WorkerId >= 0) {
            workerId = TlsThreadContext->WorkerId;
        }
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, ' ', '_', workerId, " TBasicExecutorPool::ScheduleActivationExCommon: semaphore.OldSemaphore == ", semaphore.OldSemaphore, " semaphore.CurrentSleepThreadCount == ", semaphore.CurrentSleepThreadCount);
        std::visit([mailbox, revolvingCounter](auto &x) {
            x.Push(mailbox->Hint, revolvingCounter);
        }, Activations);
        bool needToWakeUp = false;
        bool needToChangeOldSemaphore = true;

        if (SharedPool) {
            x = AtomicIncrement(Semaphore);
            needToChangeOldSemaphore = false;
            if (SharedPool->WakeUpLocalThreads(PoolId)) {
                ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName,' ', '_', workerId, " TBasicExecutorPool::ScheduleActivationExCommon: shared pool wake up local threads");
                return;
            }
            semaphore = TSemaphore::GetSemaphore(x);
        }    

        i16 sleepThreads = 0;
        Y_UNUSED(sleepThreads);
        do {
            needToWakeUp = semaphore.CurrentSleepThreadCount > 0;
            i64 oldX = semaphore.ConvertToI64();
            if (needToChangeOldSemaphore) {
                semaphore.OldSemaphore++;
            }
            if (needToWakeUp) {
                sleepThreads = semaphore.CurrentSleepThreadCount--;
            }
            x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), oldX);
            if (x == oldX) {
                break;
            }
            semaphore = TSemaphore::GetSemaphore(x);
        } while (true);

        if (needToWakeUp) { // we must find someone to wake-up
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, ' ', '_', workerId, " TBasicExecutorPool::ScheduleActivationExCommon: need to wake up");
            WakeUpLoop(semaphore.CurrentThreadCount);
        } else if (SharedPool) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, PoolName, ' ', '_', workerId, " TBasicExecutorPool::ScheduleActivationExCommon: shared pool wake up global threads");
            SharedPool->WakeUpGlobalThreads(PoolId);
        }
    }

    void TBasicExecutorPool::ScheduleActivationExLocalQueue(TMailbox* mailbox, ui64 revolvingWriteCounter) {
        if (TlsThreadContext && TlsThreadContext->Pool == this && TlsThreadContext->WorkerId >= 0) {
            if (++TlsThreadContext->WriteTurn < TlsThreadContext->LocalQueueSize) {
                LocalQueues[TlsThreadContext->WorkerId].push(mailbox->Hint);
                return;
            }
            if (ActorSystemProfile != EASProfile::Default) {
                TAtomic x = AtomicGet(Semaphore);
                TSemaphore semaphore = TSemaphore::GetSemaphore(x);
                if constexpr (NFeatures::TLocalQueuesFeatureFlags::UseIfAllOtherThreadsAreSleeping) {
                    if (semaphore.CurrentSleepThreadCount == semaphore.CurrentThreadCount - 1 && semaphore.OldSemaphore == 0) {
                        if (LocalQueues[TlsThreadContext->WorkerId].empty()) {
                            LocalQueues[TlsThreadContext->WorkerId].push(mailbox->Hint);
                            return;
                        }
                    }
                }

                if constexpr (NFeatures::TLocalQueuesFeatureFlags::UseOnMicroburst) {
                    if (semaphore.OldSemaphore >= semaphore.CurrentThreadCount) {
                        if (LocalQueues[TlsThreadContext->WorkerId].empty() && TlsThreadContext->WriteTurn < 1) {
                            TlsThreadContext->WriteTurn++;
                            LocalQueues[TlsThreadContext->WorkerId].push(mailbox->Hint);
                            return;
                        }
                    }
                }
                ScheduleActivationExCommon(mailbox, revolvingWriteCounter, x);
                return;
            }
        }
        ScheduleActivationExCommon(mailbox, revolvingWriteCounter, AtomicGet(Semaphore));
    }

    void TBasicExecutorPool::ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) {
        if constexpr (NFeatures::IsLocalQueues()) {
            ScheduleActivationExLocalQueue(mailbox, revolvingCounter);
        } else {
            ScheduleActivationExCommon(mailbox, revolvingCounter, AtomicGet(Semaphore));
        }
    }

    void TBasicExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        poolStats.MaxUtilizationTime = RelaxedLoad(&MaxUtilizationAccumulator) / (i64)(NHPTimer::GetCyclesPerSecond() / 1000);
        poolStats.WrongWakenedThreadCount = RelaxedLoad(&WrongWakenedThreadCount);
        poolStats.CurrentThreadCount = GetThreadCount();
        poolStats.DefaultThreadCount = GetDefaultThreadCount();
        poolStats.MaxThreadCount = GetMaxThreadCount();
        poolStats.SpinningTimeUs = Ts2Us(SpinningTimeUs);
        poolStats.SpinThresholdUs = Ts2Us(SpinThresholdCycles);
        if (Harmonizer) {
            TPoolHarmonizerStats stats = Harmonizer->GetPoolStats(PoolId);
            poolStats.IsNeedy = stats.IsNeedy;
            poolStats.IsStarved = stats.IsStarved;
            poolStats.IsHoggish = stats.IsHoggish;
            poolStats.IncreasingThreadsByNeedyState = stats.IncreasingThreadsByNeedyState;
            poolStats.IncreasingThreadsByExchange = stats.IncreasingThreadsByExchange;
            poolStats.DecreasingThreadsByStarvedState = stats.DecreasingThreadsByStarvedState;
            poolStats.DecreasingThreadsByHoggishState = stats.DecreasingThreadsByHoggishState;
            poolStats.DecreasingThreadsByExchange = stats.DecreasingThreadsByExchange;
            poolStats.PotentialMaxThreadCount = stats.PotentialMaxThreadCount;
        }

        statsCopy.resize(MaxFullThreadCount + 1);
        // Save counters from the pool object
        statsCopy[0] = TExecutorThreadStats();
        statsCopy[0].Aggregate(Stats);
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        RecalculateStuckActors(statsCopy[0]);
#endif
        // Per-thread stats
        for (i16 i = 0; i < MaxFullThreadCount; ++i) {
            Threads[i].Thread->GetCurrentStats(statsCopy[i + 1]);
        }
    }

    void TBasicExecutorPool::GetExecutorPoolState(TExecutorPoolState &poolState) const {
        if (Harmonizer) {
            TPoolHarmonizerStats stats = Harmonizer->GetPoolStats(PoolId);
            poolState.ElapsedCpu = stats.AvgElapsedCpu;
            poolState.PossibleMaxLimit = stats.PotentialMaxThreadCount;
        } else {
            poolState.PossibleMaxLimit = poolState.MaxLimit;
        }
        poolState.CurrentLimit = GetThreadCount();
        poolState.MaxLimit = GetMaxThreadCount();
        poolState.MinLimit = GetDefaultThreadCount();
    }

    void TBasicExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Prepare: start");

        ActorSystem = actorSystem;

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[MaxFullThreadCount + 2]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[MaxFullThreadCount + 2]);


        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread.reset(
                new TExecutorThread(
                    i,
                    0, // CpuId is not used in BASIC pool
                    actorSystem,
                    this,
                    MailboxTable,
                    PoolName,
                    TimePerMailbox,
                    EventsPerMailbox));
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }

        ScheduleWriters[MaxFullThreadCount].Init(ScheduleReaders[MaxFullThreadCount]);
        ScheduleWriters[MaxFullThreadCount + 1].Init(ScheduleReaders[MaxFullThreadCount + 1]);

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = MaxFullThreadCount + 2;
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Prepare: end");
    }

    void TBasicExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Start: start ", PoolName, ' ', MaxFullThreadCount);
        ThreadUtilization = 0;
        AtomicAdd(MaxUtilizationCounter, -(i64)GetCycleCountFast());

        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread->Start();
        }

        if constexpr (DebugMode) {
            Sanitizer->Start();
        }
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Start: end");
    }

    void TBasicExecutorPool::PrepareStop() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::PrepareStop: start ", PoolName, ' ', MaxFullThreadCount);
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::PrepareStop: stop flag ", i);
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Interrupt();
        }
        if constexpr (DebugMode) {
            Sanitizer->Stop();
        }
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::PrepareStop: end");
    }

    void TBasicExecutorPool::Shutdown() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Shutdown: start ", PoolName, ' ', MaxFullThreadCount);
        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Shutdown: join ", i);
            Threads[i].Thread->Join();
        }
        if constexpr (DebugMode) {
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Shutdown: join sanitizer");
            Sanitizer->Join();
        }
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TBasicExecutorPool::Shutdown: end");
    }

    void TBasicExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TBasicExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        const auto current = ActorSystem->Monotonic();
        if (deadline < current)
            deadline = current;

        if (TlsThreadContext->WorkerCtx->SharedExecutor) {
            TlsThreadContext->WorkerCtx->SharedExecutor->Schedule(deadline, ev, cookie, workerId);
        } else {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TBasicExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        const auto deadline = ActorSystem->Monotonic() + delta;
        if (TlsThreadContext->WorkerCtx->SharedExecutor) {
            TlsThreadContext->WorkerCtx->SharedExecutor->Schedule(deadline, ev, cookie, workerId);
        } else {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TBasicExecutorPool::SetRealTimeMode() const {
// TODO: musl-libc version of `sched_param` struct is for some reason different from pthread
// version in Ubuntu 12.04
#if defined(_linux_) && !defined(_musl_)
        if (RealtimePriority != 0) {
            pthread_t threadSelf = pthread_self();
            sched_param param = {RealtimePriority};
            if (pthread_setschedparam(threadSelf, SCHED_FIFO, &param)) {
                Y_ABORT("Cannot set realtime priority");
            }
        }
#else
        Y_UNUSED(RealtimePriority);
#endif
    }

    float TBasicExecutorPool::GetThreadCount() const {
        return GetFullThreadCount();
    }

    i16 TBasicExecutorPool::GetFullThreadCount() const {
        return AtomicGet(ThreadCount);
    }

    void TBasicExecutorPool::SetFullThreadCount(i16 threads) {
        threads = Max<i16>(0, Min(MaxFullThreadCount, threads));
        with_lock (ChangeThreadsLock) {
            i16 prevCount = GetFullThreadCount();
            AtomicSet(ThreadCount, threads);
            TSemaphore semaphore = TSemaphore::GetSemaphore(AtomicGet(Semaphore));
            i64 oldX = semaphore.ConvertToI64();
            semaphore.CurrentThreadCount = threads;
            if (threads > prevCount) {
                semaphore.CurrentSleepThreadCount += (i64)threads - prevCount;
            } else {
                semaphore.CurrentSleepThreadCount -= (i64)prevCount - threads;
            }
            AtomicAdd(Semaphore, semaphore.ConvertToI64() - oldX);
            LWPROBE(ThreadCount, PoolId, PoolName, threads, MinThreadCount, MaxThreadCount, DefaultThreadCount);
        }
    }

    float TBasicExecutorPool::GetDefaultThreadCount() const {
        return DefaultThreadCount;
    }

    i16 TBasicExecutorPool::GetDefaultFullThreadCount() const {
        return DefaultFullThreadCount;
    }

    float TBasicExecutorPool::GetMinThreadCount() const {
        return MinThreadCount;
    }

    i16 TBasicExecutorPool::GetMinFullThreadCount() const {
        return MinFullThreadCount;
    }

    float TBasicExecutorPool::GetMaxThreadCount() const {
        return MaxThreadCount;
    }

    i16 TBasicExecutorPool::GetMaxFullThreadCount() const {
        return MaxFullThreadCount;
    }
    
    ui32 TBasicExecutorPool::GetThreads() const {
        return MaxFullThreadCount;
    }

    TCpuConsumption TBasicExecutorPool::GetThreadCpuConsumption(i16 threadIdx) {
        if (threadIdx >= MaxFullThreadCount) {
            return {0.0, 0.0};
        }
        TExecutorThreadCtx& threadCtx = Threads[threadIdx];
        TExecutorThreadStats stats;
        threadCtx.Thread->GetCurrentStatsForHarmonizer(stats);
        return {static_cast<double>(stats.CpuUs), Ts2Us(stats.SafeElapsedTicks), stats.NotEnoughCpuExecutions};
    }

    i16 TBasicExecutorPool::GetBlockingThreadCount() const {
        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        return -Min<i16>(semaphore.CurrentSleepThreadCount, 0);
    }

    i16 TBasicExecutorPool::GetPriority() const {
        return Priority;
    }

    void TBasicExecutorPool::SetLocalQueueSize(ui16 size) {
        if constexpr (!NFeatures::TLocalQueuesFeatureFlags::FIXED_LOCAL_QUEUE_SIZE) {
            LocalQueueSize.store(std::max(size, NFeatures::TLocalQueuesFeatureFlags::MAX_LOCAL_QUEUE_SIZE), std::memory_order_relaxed);
        }
    }

    void TBasicExecutorPool::Initialize(TWorkerContext& wctx) {
        if (wctx.WorkerId >= 0) {
            TlsThreadContext->WaitingStats = &WaitingStats[wctx.WorkerId];
        }
    }

    void TBasicExecutorPool::SetSpinThresholdCycles(ui32 cycles) {
        if (ActorSystemProfile == EASProfile::LowLatency) {
            if (DefaultSpinThresholdCycles > cycles) {
                cycles = DefaultSpinThresholdCycles;
            }
        }
        SpinThresholdCycles = cycles;
        double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
        ui32 bucketIdx = cycles / TWaitingStatsConstants::HistogramResolution;
        LWPROBE(ChangeSpinThreshold, PoolId, PoolName, cycles, resolutionUs * bucketIdx, bucketIdx);
    }

    void TBasicExecutorPool::GetWaitingStats(TWaitingStats<ui64> &acc) const {
        acc.Clear();
        double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
        for (ui32 idx = 0; idx < ThreadCount; ++idx) {
            for (ui32 bucketIdx = 0; bucketIdx < TWaitingStatsConstants::BucketCount; ++bucketIdx) {
                LWPROBE(WaitingHistogramPerThread, PoolId, PoolName, idx, resolutionUs * bucketIdx, resolutionUs * (bucketIdx + 1), WaitingStats[idx].WaitingUntilNeedsTimeHist[bucketIdx].load());
            }
            acc.Add(WaitingStats[idx]);
        }
        for (ui32 bucketIdx = 0; bucketIdx < TWaitingStatsConstants::BucketCount; ++bucketIdx) {
            LWPROBE(WaitingHistogram, PoolId, PoolName, resolutionUs * bucketIdx, resolutionUs * (bucketIdx + 1), acc.WaitingUntilNeedsTimeHist[bucketIdx].load());
        }
    }

    void TBasicExecutorPool::ClearWaitingStats() const {
        for (ui32 idx = 0; idx < ThreadCount; ++idx) {
            WaitingStats[idx].Clear();
        }
    }

    void TBasicExecutorPool::CalcSpinPerThread(ui64 wakingUpConsumption) {
        if (ActorSystemProfile == EASProfile::Default) {
            return;
        }
        for (i16 threadIdx = 0; threadIdx < MaxFullThreadCount; ++threadIdx) {
            ui64 newSpinThreshold = 0;
            if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                MovingWaitingStats[threadIdx].Add(WaitingStats[threadIdx], 0.8, 0.2);
                newSpinThreshold = MovingWaitingStats[threadIdx].CalculateGoodSpinThresholdCycles(wakingUpConsumption);
            } else {
                newSpinThreshold = WaitingStats[threadIdx].CalculateGoodSpinThresholdCycles(wakingUpConsumption);
            }

            if (ActorSystemProfile == EASProfile::LowCpuConsumption) {
                SpinThresholdCyclesPerThread[threadIdx].store(newSpinThreshold, std::memory_order_release);
            } else {
                auto oldSpinThreshold = SpinThresholdCyclesPerThread[threadIdx].load(std::memory_order_acquire);
                if (oldSpinThreshold < newSpinThreshold) {
                    SpinThresholdCyclesPerThread[threadIdx].store(newSpinThreshold, std::memory_order_release);
                }
            }

            double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
            ui32 bucketIdx = newSpinThreshold / TWaitingStatsConstants::HistogramResolution;
            LWPROBE(ChangeSpinThresholdPerThread, PoolId, PoolName, threadIdx, newSpinThreshold, resolutionUs * bucketIdx, bucketIdx);
        }
    }

    bool TExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
        EThreadState state = ExchangeState<EThreadState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state == EThreadState::None, "WaitingFlag# %d", int(state));
        if (spinThresholdCycles > 0) {
            // spin configured period
            Spin(spinThresholdCycles, stopFlag);
        }
        return Sleep(stopFlag);
    }

    bool TExecutorThreadCtx::WakeUp() {
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_WAKE_UP, false> activityGuard;
        for (ui32 i = 0; i < 2; ++i) {
            EThreadState state = GetState<EThreadState>();
            switch (state) {
                case EThreadState::None:
                case EThreadState::Work:
                    return false;
                case EThreadState::Spin:
                case EThreadState::Sleep:
                    if (ReplaceState<EThreadState>(state, EThreadState::None)) {
                        if (state == EThreadState::Sleep) {
                            ui64 beforeUnpark = GetCycleCountFast();
                            StartWakingTs = beforeUnpark;
                            WaitingPad.Unpark();
                            if (TlsThreadContext && TlsThreadContext->WaitingStats) {
                                TlsThreadContext->WaitingStats->AddWakingUp(GetCycleCountFast() - beforeUnpark);
                            }
                        }
                        return true;
                    }
                    break;
                default:
                    Y_ABORT();
            }
        }
        return false;
    }

    TBasicExecutorPool::TSemaphore TBasicExecutorPool::GetSemaphore() const {
        return TSemaphore::GetSemaphore(AtomicGet(Semaphore));
    }

    void TBasicExecutorPool::SetSharedPool(TSharedExecutorPool* pool) {
        SharedPool = pool;
    }

    TMailbox* TBasicExecutorPool::GetReadyActivationShared(TWorkerContext& wctx, ui64 revolvingCounter) {
        TWorkerId workerId = wctx.WorkerId;
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        SharedPool->Threads[workerId].UnsetWork();
        if (Harmonizer) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "TBasicExecutorPool::GetReadyActivationShared: try to harmonize");
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
            ACTORLIB_DEBUG(EDebugLevel::Activation, "TBasicExecutorPool::GetReadyActivationShared: harmonize done");
        }
        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Shared_", PoolId, " TBasicExecutorPool::GetReadyActivationShared: revolvingCounter == ", revolvingCounter, " semaphore == ", semaphore.OldSemaphore);
        while (!StopFlag.load(std::memory_order_acquire)) {
            if (!semaphore.OldSemaphore) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " Shared_", PoolId, " TBasicExecutorPool::GetReadyActivationShared: semaphore == 0");
                return nullptr;
            } else {
                TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                if (const ui32 activation = std::visit([&revolvingCounter](auto &x) {return x.Pop(++revolvingCounter);}, Activations)) {
                    SharedPool->Threads[workerId].SetWork();
                    AtomicDecrement(Semaphore);
                    ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " Shared_", PoolId, " TBasicExecutorPool::GetReadyActivationShared: activation == ", activation, " semaphore == ", semaphore.OldSemaphore);
                    return MailboxTable->Get(activation);
                }
            }

            SpinLockPause();
            x = AtomicGet(Semaphore);
            semaphore = TSemaphore::GetSemaphore(x);
        }
        ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " Shared_", PoolId, " TBasicExecutorPool::GetReadyActivation: stop");
        return nullptr;
    }

}
