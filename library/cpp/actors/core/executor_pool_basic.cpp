#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "actor.h"
#include "probes.h"
#include "mailbox.h"
#include <library/cpp/actors/util/affinity.h>
#include <library/cpp/actors/util/datetime.h>

#ifdef _linux_
#include <pthread.h>
#endif

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);


    const double TWaitingStatsConstants::HistogramResolutionUs = MaxSpinThersholdUs / BucketCount;
    const ui64 TWaitingStatsConstants::HistogramResolution = NHPTimer::GetCyclesPerSecond() * 0.000001 * HistogramResolutionUs;

    constexpr TDuration TBasicExecutorPool::DEFAULT_TIME_PER_MAILBOX;

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
        ui32 maxActivityType,
        i16 minThreadCount,
        i16 maxThreadCount,
        i16 defaultThreadCount,
        i16 priority)
        : TExecutorPoolBase(poolId, threads, affinity)
        , DefaultSpinThresholdCycles(spinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , SpinThresholdCycles(DefaultSpinThresholdCycles)
        , SpinThresholdCyclesPerThread(new NThreading::TPadded<std::atomic<ui64>>[threads])
        , Threads(new NThreading::TPadded<TThreadCtx>[threads])
        , WaitingStats(new TWaitingStats<ui64>[threads])
        , PoolName(poolName)
        , TimePerMailbox(timePerMailbox)
        , EventsPerMailbox(eventsPerMailbox)
        , RealtimePriority(realtimePriority)
        , ThreadUtilization(0)
        , MaxUtilizationCounter(0)
        , MaxUtilizationAccumulator(0)
        , WrongWakenedThreadCount(0)
        , ThreadCount(threads)
        , MinThreadCount(minThreadCount)
        , MaxThreadCount(maxThreadCount)
        , DefaultThreadCount(defaultThreadCount)
        , Harmonizer(harmonizer)
        , Priority(priority)
    {
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

        Y_UNUSED(maxActivityType);
        i16 limit = Min(threads, (ui32)Max<i16>());
        if (DefaultThreadCount) {
            DefaultThreadCount = Min(DefaultThreadCount, limit);
        } else {
            DefaultThreadCount = limit;
        }

        MaxThreadCount = Min(Max(MaxThreadCount, DefaultThreadCount), limit);

        if (MinThreadCount) {
            MinThreadCount = Max((i16)1, Min(MinThreadCount, DefaultThreadCount));
        } else {
            MinThreadCount = DefaultThreadCount;
        }
        ThreadCount = MaxThreadCount;
        auto semaphore = TSemaphore();
        semaphore.CurrentThreadCount = ThreadCount;
        Semaphore = semaphore.ConverToI64();
    }

    TBasicExecutorPool::TBasicExecutorPool(const TBasicExecutorPoolConfig& cfg, IHarmonizer *harmonizer)
        : TBasicExecutorPool(
            cfg.PoolId,
            cfg.Threads,
            cfg.SpinThreshold,
            cfg.PoolName,
            harmonizer,
            new TAffinity(cfg.Affinity),
            cfg.TimePerMailbox,
            cfg.EventsPerMailbox,
            cfg.RealtimePriority,
            0,
            cfg.MinThreadCount,
            cfg.MaxThreadCount,
            cfg.DefaultThreadCount,
            cfg.Priority
        )
    {
        SetSharedExecutorsCount(cfg.SharedExecutorsCount);
        SoftProcessingDurationTs = cfg.SoftProcessingDurationTs;
        ActorSystemProfile = cfg.ActorSystemProfile;
    }

    TBasicExecutorPool::~TBasicExecutorPool() {
        Threads.Destroy();
    }

    bool TBasicExecutorPool::GoToSleep(TThreadCtx& threadCtx, TTimers &timers) {
        do {
            timers.HPNow = GetCycleCountFast();
            timers.Elapsed += timers.HPNow - timers.HPStart;
            if (threadCtx.WaitingPad.Park()) // interrupted
                return true;
            timers.HPStart = GetCycleCountFast();
            timers.Parked += timers.HPStart - timers.HPNow;
        } while (AtomicLoad(&threadCtx.WaitingFlag) == TThreadCtx::WS_BLOCKED && !RelaxedLoad(&StopFlag));
        return false;
    }

    ui32 TBasicExecutorPool::GoToSpin(TThreadCtx& threadCtx, i64 start, i64 &end) {
        ui32 spinPauseCount = 0;
        i64 spinThresholdCycles = 0;
        if constexpr (NFeatures::TSpinFeatureFlags::CalcPerThread) {
            spinThresholdCycles = SpinThresholdCyclesPerThread[TlsThreadContext->WorkerId].load();
        } else {
            spinThresholdCycles = SpinThresholdCycles.load();
        }
        do {
            end = GetCycleCountFast();
            if (end >= (start + spinThresholdCycles) || AtomicLoad(&threadCtx.WaitingFlag) != TThreadCtx::WS_ACTIVE) {
                return spinPauseCount;
            }

            SpinLockPause();
            spinPauseCount++;
        } while (!RelaxedLoad(&StopFlag));

        return spinPauseCount;
    }

    bool TBasicExecutorPool::GoToWaiting(TThreadCtx& threadCtx, TTimers &timers, bool needToBlock) {
#if defined ACTORSLIB_COLLECT_EXEC_STATS
        if (AtomicGetAndIncrement(ThreadUtilization) == 0) {
            // Initially counter contains -t0, the pool start timestamp
            // When the first thread goes to sleep we add t1, so the counter
            // becomes t1-t0 >= 0, or the duration of max utilization so far.
            // If the counter was negative and becomes positive, that means
            // counter just turned into a duration and we should store that
            // duration. Otherwise another thread raced with us and
            // subtracted some other timestamp t2.
            const i64 t = GetCycleCountFast();
            const i64 x = AtomicGetAndAdd(MaxUtilizationCounter, t);
            if (x < 0 && x + t > 0)
                AtomicStore(&MaxUtilizationAccumulator, x + t);
        }
#endif

        i64 startWaiting = GetCycleCountFast();
        i64 endSpinning = 0;
        TAtomic state = AtomicLoad(&threadCtx.WaitingFlag);
        bool wasSleeping = false;
        Y_ABORT_UNLESS(state == TThreadCtx::WS_NONE, "WaitingFlag# %d", int(state));

        if (SpinThresholdCycles > 0 && !needToBlock) {
            // spin configured period
            AtomicSet(threadCtx.WaitingFlag, TThreadCtx::WS_ACTIVE);
            ui32 spinPauseCount = GoToSpin(threadCtx, startWaiting, endSpinning);
            SpinningTimeUs += endSpinning - startWaiting;
            // then - sleep
            if (AtomicLoad(&threadCtx.WaitingFlag) == TThreadCtx::WS_ACTIVE) {
                if (AtomicCas(&threadCtx.WaitingFlag, TThreadCtx::WS_BLOCKED, TThreadCtx::WS_ACTIVE)) {
                    if (NFeatures::TCommonFeatureFlags::ProbeSpinCycles) {
                            LWPROBE(SpinCycles, PoolId, PoolName, spinPauseCount, true);
                    }

                    wasSleeping = true;
                    if (GoToSleep(threadCtx, timers)) {  // interrupted
                        return true;
                    }
                    AllThreadsSleep.store(false);
                }
            }
            if (NFeatures::TCommonFeatureFlags::ProbeSpinCycles && !wasSleeping) {
                LWPROBE(SpinCycles, PoolId, PoolName, spinPauseCount, false);
            }
        } else {
            AtomicSet(threadCtx.WaitingFlag, TThreadCtx::WS_BLOCKED);
            wasSleeping = true;
            if (GoToSleep(threadCtx, timers)) {  // interrupted
                return true;
            }
            AllThreadsSleep.store(false);
        }

        i64 needTimeTs = threadCtx.StartWakingTs.exchange(0);
        if (wasSleeping && needTimeTs) {
            ui64 waitingDuration = std::max<i64>(0, needTimeTs - startWaiting);
            ui64 awakingDuration = std::max<i64>(0, GetCycleCountFast() - needTimeTs);
            WaitingStats[TlsThreadContext->WorkerId].AddAwakening(waitingDuration, awakingDuration);
        } else {
            ui64 waitingDuration = std::max<i64>(0, endSpinning - startWaiting);
            if (wasSleeping) {
                WaitingStats[TlsThreadContext->WorkerId].AddFastAwakening(waitingDuration);
            } else {
                WaitingStats[TlsThreadContext->WorkerId].Add(waitingDuration);
            }
        }

        Y_DEBUG_ABORT_UNLESS(AtomicLoad(&StopFlag) || AtomicLoad(&threadCtx.WaitingFlag) == TThreadCtx::WS_NONE);

#if defined ACTORSLIB_COLLECT_EXEC_STATS
        if (AtomicDecrement(ThreadUtilization) == 0) {
            // When we started sleeping counter contained t1-t0, or the
            // last duration of max utilization. Now we subtract t2 >= t1,
            // which turns counter negative again, and the next sleep cycle
            // at timestamp t3 would be adding some new duration t3-t2.
            // If the counter was positive and becomes negative that means
            // there are no current races with other threads and we should
            // store the last positive duration we observed. Multiple
            // threads may be adding and subtracting values in potentially
            // arbitrary order, which would cause counter to oscillate
            // around zero. When it crosses zero is a good indication of a
            // correct value.
            const i64 t = GetCycleCountFast();
            const i64 x = AtomicGetAndAdd(MaxUtilizationCounter, -t);
            if (x > 0 && x - t < 0)
                AtomicStore(&MaxUtilizationAccumulator, x);
        }
#endif
        return false;
    }

    void TBasicExecutorPool::AskToGoToSleep(bool *needToWait, bool *needToBlock) {
        TAtomic x = AtomicGet(Semaphore);
        do {
            i64 oldX = x;
            TSemaphore semaphore = TSemaphore::GetSemaphore(x);;
            if (semaphore.CurrentSleepThreadCount < 0) {
                semaphore.CurrentSleepThreadCount++;
                x = AtomicGetAndCas(&Semaphore, semaphore.ConverToI64(), x);
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
                x = AtomicGetAndCas(&Semaphore, semaphore.ConverToI64(), x);
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

    ui32 TBasicExecutorPool::GetReadyActivationCommon(TWorkerContext& wctx, ui64 revolvingCounter) {
        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        TTimers timers;

        if (Harmonizer) {
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(timers.HPStart);
        }

        if (workerId >= 0) {
            AtomicSet(Threads[workerId].WaitingFlag, TThreadCtx::WS_NONE);
        }

        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        while (!RelaxedLoad(&StopFlag)) {
            if (!semaphore.OldSemaphore || semaphore.CurrentSleepThreadCount < 0) {
                if (workerId < 0 || !wctx.IsNeededToWaitNextActivation) {
                    timers.HPNow = GetCycleCountFast();
                    wctx.AddElapsedCycles(ActorSystemIndex, timers.HPNow - timers.HPStart);
                    return 0;
                }

                bool needToWait = false;
                bool needToBlock = false;
                AskToGoToSleep(&needToWait, &needToBlock);
                if (needToWait) {
                    if (GoToWaiting(Threads[workerId], timers, needToBlock)) { // interrupted
                        return 0;
                    }
                }
            } else {
                if (const ui32 activation = Activations.Pop(++revolvingCounter)) {
                    if (workerId >= 0) {
                        AtomicSet(Threads[workerId].WaitingFlag, TThreadCtx::WS_RUNNING);
                    }
                    AtomicDecrement(Semaphore);
                    timers.HPNow = GetCycleCountFast();
                    timers.Elapsed += timers.HPNow - timers.HPStart;
                    wctx.AddElapsedCycles(ActorSystemIndex, timers.Elapsed);
                    if (timers.Parked > 0) {
                        wctx.AddParkedCycles(timers.Parked);
                    }

                    return activation;
                }
                semaphore.CurrentSleepThreadCount++;
            }

            SpinLockPause();
            x = AtomicGet(Semaphore);
            semaphore = TSemaphore::GetSemaphore(x);
        }

        return 0;
    }

    ui32 TBasicExecutorPool::GetReadyActivationLocalQueue(TWorkerContext& wctx, ui64 revolvingCounter) {
        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < static_cast<i32>(PoolThreads));

        if (workerId >= 0 && LocalQueues[workerId].size()) {
            ui32 activation = LocalQueues[workerId].front();
            LocalQueues[workerId].pop();
            return activation;
        } else {
            TlsThreadContext->WriteTurn = 0;
            TlsThreadContext->LocalQueueSize = LocalQueueSize.load(std::memory_order_relaxed);
        }
        return GetReadyActivationCommon(wctx, revolvingCounter);
    }

    ui32 TBasicExecutorPool::GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
        if constexpr (NFeatures::IsLocalQueues()) {
            if (SharedExecutorsCount) {
                return GetReadyActivationCommon(wctx, revolvingCounter);
            }
            return GetReadyActivationLocalQueue(wctx, revolvingCounter);
        } else {
            return GetReadyActivationCommon(wctx, revolvingCounter);
        }
        return 0;
    }

    inline void TBasicExecutorPool::WakeUpLoop(i16 currentThreadCount) {
        if (AllThreadsSleep) {
            TThreadCtx& hotThreadCtx = Threads[0];
            if (AtomicCas(&hotThreadCtx.WaitingFlag, TThreadCtx::WS_NONE, TThreadCtx::WS_ACTIVE)) {
                return;
            }

            TThreadCtx& coldThreadCtx = Threads[AtomicLoad(&ThreadCount) - 1];
            if (AtomicCas(&coldThreadCtx.WaitingFlag, TThreadCtx::WS_NONE, TThreadCtx::WS_BLOCKED)) {
                if (TlsThreadContext && TlsThreadContext->WaitingStats) {
                    ui64 beforeUnpark = GetCycleCountFast();
                    coldThreadCtx.StartWakingTs = beforeUnpark;
                    coldThreadCtx.WaitingPad.Unpark();
                    TlsThreadContext->WaitingStats->AddWakingUp(GetCycleCountFast() - beforeUnpark);
                } else {
                    coldThreadCtx.WaitingPad.Unpark();
                }
                return;
            }
        }
        for (i16 i = 0;;) {
            TThreadCtx& threadCtx = Threads[i];
            TThreadCtx::EWaitState state = static_cast<TThreadCtx::EWaitState>(AtomicLoad(&threadCtx.WaitingFlag));
            switch (state) {
                case TThreadCtx::WS_NONE:
                case TThreadCtx::WS_RUNNING:
                    if (++i >= MaxThreadCount - SharedExecutorsCount) {
                        i = 0;
                    }
                    break;
                case TThreadCtx::WS_ACTIVE:
                case TThreadCtx::WS_BLOCKED:
                    if (AtomicCas(&threadCtx.WaitingFlag, TThreadCtx::WS_NONE, state)) {
                        if (state  == TThreadCtx::WS_BLOCKED) {
                            ui64 beforeUnpark = GetCycleCountFast();
                            threadCtx.StartWakingTs = beforeUnpark;
                            if (TlsThreadContext && TlsThreadContext->WaitingStats) {
                                threadCtx.WaitingPad.Unpark();
                                TlsThreadContext->WaitingStats->AddWakingUp(GetCycleCountFast() - beforeUnpark);
                            } else {
                                threadCtx.WaitingPad.Unpark();
                            }
                        }
                        if (i >= currentThreadCount) {
                            AtomicIncrement(WrongWakenedThreadCount);
                        }
                        return;
                    }
                    break;
                default:
                    Y_ABORT();
            }
        }
    }

    void TBasicExecutorPool::ScheduleActivationExCommon(ui32 activation, ui64 revolvingCounter, TAtomic x) {
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);

        Activations.Push(activation, revolvingCounter);
        bool needToWakeUp = false;

        do {
            needToWakeUp = semaphore.CurrentSleepThreadCount > SharedExecutorsCount;
            i64 oldX = semaphore.ConverToI64();
            semaphore.OldSemaphore++;
            if (needToWakeUp) {
                semaphore.CurrentSleepThreadCount--;
            }
            x = AtomicGetAndCas(&Semaphore, semaphore.ConverToI64(), oldX);
            if (x == oldX) {
                break;
            }
            semaphore = TSemaphore::GetSemaphore(x);
        } while (true);

        if (needToWakeUp) { // we must find someone to wake-up
            WakeUpLoop(semaphore.CurrentThreadCount);
        }
    }

    void TBasicExecutorPool::ScheduleActivationExLocalQueue(ui32 activation, ui64 revolvingWriteCounter) {
        if (TlsThreadContext && TlsThreadContext->Pool == this && TlsThreadContext->WorkerId >= 0) {
            if (++TlsThreadContext->WriteTurn < TlsThreadContext->LocalQueueSize) {
                LocalQueues[TlsThreadContext->WorkerId].push(activation);
                return;
            }
            if (ActorSystemProfile != EASProfile::Default) {
                TAtomic x = AtomicGet(Semaphore);
                TSemaphore semaphore = TSemaphore::GetSemaphore(x);
                if constexpr (NFeatures::TLocalQueuesFeatureFlags::UseIfAllOtherThreadsAreSleeping) {
                    if (semaphore.CurrentSleepThreadCount == semaphore.CurrentThreadCount - 1 && semaphore.OldSemaphore == 0) {
                        if (LocalQueues[TlsThreadContext->WorkerId].empty()) {
                            LocalQueues[TlsThreadContext->WorkerId].push(activation);
                            return;
                        }
                    }
                }

                if constexpr (NFeatures::TLocalQueuesFeatureFlags::UseOnMicroburst) {
                    if (semaphore.OldSemaphore >= semaphore.CurrentThreadCount) {
                        if (LocalQueues[TlsThreadContext->WorkerId].empty() && TlsThreadContext->WriteTurn < 1) {
                            TlsThreadContext->WriteTurn++;
                            LocalQueues[TlsThreadContext->WorkerId].push(activation);
                            return;
                        }
                    }
                }
                ScheduleActivationExCommon(activation, revolvingWriteCounter, x);
                return;
            }
        }
        ScheduleActivationExCommon(activation, revolvingWriteCounter, AtomicGet(Semaphore));
    }

    void TBasicExecutorPool::ScheduleActivationEx(ui32 activation, ui64 revolvingCounter) {
        if constexpr (NFeatures::IsLocalQueues()) {
            ScheduleActivationExLocalQueue(activation, revolvingCounter);
        } else {
            ScheduleActivationExCommon(activation, revolvingCounter, AtomicGet(Semaphore));
        }
    }

    void TBasicExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        poolStats.MaxUtilizationTime = RelaxedLoad(&MaxUtilizationAccumulator) / (i64)(NHPTimer::GetCyclesPerSecond() / 1000);
        poolStats.WrongWakenedThreadCount = RelaxedLoad(&WrongWakenedThreadCount);
        poolStats.CurrentThreadCount = RelaxedLoad(&ThreadCount);
        poolStats.DefaultThreadCount = DefaultThreadCount;
        poolStats.MaxThreadCount = MaxThreadCount;
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
            poolStats.MaxConsumedCpuUs = stats.MaxConsumedCpu;
            poolStats.MinConsumedCpuUs = stats.MinConsumedCpu;
            poolStats.MaxBookedCpuUs = stats.MaxBookedCpu;
            poolStats.MinBookedCpuUs = stats.MinBookedCpu;
        }

        statsCopy.resize(PoolThreads + 1);
        // Save counters from the pool object
        statsCopy[0] = TExecutorThreadStats();
        statsCopy[0].Aggregate(Stats);
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        RecalculateStuckActors(statsCopy[0]);
#endif
        // Per-thread stats
        for (i16 i = 0; i < PoolThreads; ++i) {
            Threads[i].Thread->GetCurrentStats(statsCopy[i + 1]);
        }
    }

    void TBasicExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());

        ActorSystem = actorSystem;

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[PoolThreads]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[PoolThreads]);

        for (i16 i = 0; i != PoolThreads; ++i) {
            if (i < MaxThreadCount - SharedExecutorsCount) {
                Threads[i].Thread.Reset(
                    new TExecutorThread(
                        i,
                        0, // CpuId is not used in BASIC pool
                        actorSystem,
                        this,
                        MailboxTable.Get(),
                        PoolName,
                        TimePerMailbox,
                        EventsPerMailbox));
            } else {
                Threads[i].Thread.Reset(
                    new TExecutorThread(
                        i,
                        actorSystem,
                        actorSystem->GetBasicExecutorPools(),
                        PoolName,
                        SoftProcessingDurationTs,
                        TimePerMailbox,
                        EventsPerMailbox));
            }
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = PoolThreads;
    }

    void TBasicExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());

        ThreadUtilization = 0;
        AtomicAdd(MaxUtilizationCounter, -(i64)GetCycleCountFast());

        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->Start();
        }
    }

    void TBasicExecutorPool::PrepareStop() {
        AtomicStore(&StopFlag, true);
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->StopFlag = true;
            Threads[i].WaitingPad.Interrupt();
        }
    }

    void TBasicExecutorPool::Shutdown() {
        for (i16 i = 0; i != PoolThreads; ++i)
            Threads[i].Thread->Join();
    }

    void TBasicExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TBasicExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto current = ActorSystem->Monotonic();
        if (deadline < current)
            deadline = current;

        ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TBasicExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto deadline = ActorSystem->Monotonic() + delta;
        ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
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

    i16 TBasicExecutorPool::GetThreadCount() const {
        return AtomicGet(ThreadCount);
    }

    void TBasicExecutorPool::SetThreadCount(i16 threads) {
        threads = Max(i16(1), Min(PoolThreads, threads));
        with_lock (ChangeThreadsLock) {
            i16 prevCount = GetThreadCount();
            AtomicSet(ThreadCount, threads);
            TSemaphore semaphore = TSemaphore::GetSemaphore(AtomicGet(Semaphore));
            i64 oldX = semaphore.ConverToI64();
            semaphore.CurrentThreadCount = threads;
            if (threads > prevCount) {
                semaphore.CurrentSleepThreadCount += (i64)threads - prevCount;
            } else {
                semaphore.CurrentSleepThreadCount -= (i64)prevCount - threads;
            }
            AtomicAdd(Semaphore, semaphore.ConverToI64() - oldX);
            LWPROBE(ThreadCount, PoolId, PoolName, threads, MinThreadCount, MaxThreadCount, DefaultThreadCount);
        }
    }

    i16 TBasicExecutorPool::GetDefaultThreadCount() const {
        return DefaultThreadCount;
    }

    i16 TBasicExecutorPool::GetMinThreadCount() const {
        return MinThreadCount;
    }

    i16 TBasicExecutorPool::GetMaxThreadCount() const {
        return MaxThreadCount;
    }

    TCpuConsumption TBasicExecutorPool::GetThreadCpuConsumption(i16 threadIdx) {
        if (threadIdx >= PoolThreads) {
            return {0.0, 0.0};
        }
        TThreadCtx& threadCtx = Threads[threadIdx];
        TExecutorThreadStats stats;
        threadCtx.Thread->GetCurrentStats(stats);
        return {Ts2Us(stats.SafeElapsedTicks), static_cast<double>(stats.CpuUs), stats.NotEnoughCpuExecutions};
    }

    i16 TBasicExecutorPool::GetBlockingThreadCount() const {
        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        return -Min<i16>(semaphore.CurrentSleepThreadCount, 0);
    }

    i16 TBasicExecutorPool::GetPriority() const {
        return Priority;
    }

    void TBasicExecutorPool::SetSharedExecutorsCount(i16 count) {
        SharedExecutorsCount = count;
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
        for (i16 threadIdx = 0; threadIdx < PoolThreads; ++threadIdx) {
            ui64 newSpinThreshold = 0;
            if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                MovingWaitingStats[threadIdx].Add(WaitingStats[threadIdx], 0.8, 0.2);
                newSpinThreshold = MovingWaitingStats[threadIdx].CalculateGoodSpinThresholdCycles(wakingUpConsumption);
            } else {
                newSpinThreshold = WaitingStats[threadIdx].CalculateGoodSpinThresholdCycles(wakingUpConsumption);
            }
            SpinThresholdCyclesPerThread[threadIdx].store(newSpinThreshold);

            double resolutionUs = TWaitingStatsConstants::HistogramResolutionUs;
            ui32 bucketIdx = newSpinThreshold / TWaitingStatsConstants::HistogramResolution;
            LWPROBE(ChangeSpinThresholdPerThread, PoolId, PoolName, threadIdx, newSpinThreshold, resolutionUs * bucketIdx, bucketIdx);
        }
    }
}
