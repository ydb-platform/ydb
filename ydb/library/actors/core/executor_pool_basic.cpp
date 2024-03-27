#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "actor.h"
#include "config.h"
#include "executor_thread_ctx.h"
#include "probes.h"
#include "mailbox.h"
#include "thread_context.h"
#include <atomic>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/util/datetime.h>

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
        , Threads(new NThreading::TPadded<TExecutorThreadCtx>[threads])
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

        MaxThreadCount = Min(Max<i16>(MaxThreadCount, DefaultThreadCount), limit);

        if (MinThreadCount) {
            MinThreadCount = Max<i16>(0, Min(MinThreadCount, DefaultThreadCount));
        } else {
            MinThreadCount = DefaultThreadCount;
        }
        ThreadCount = MaxThreadCount;
        auto semaphore = TSemaphore();
        semaphore.CurrentThreadCount = ThreadCount;
        Semaphore = semaphore.ConvertToI64();
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
        SoftProcessingDurationTs = cfg.SoftProcessingDurationTs;
        ActorSystemProfile = cfg.ActorSystemProfile;
    }

    TBasicExecutorPool::~TBasicExecutorPool() {
        Threads.Destroy();
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

    ui32 TBasicExecutorPool::GetReadyActivationCommon(TWorkerContext& wctx, ui64 revolvingCounter) {
        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        TlsThreadContext->Timers.Reset();

        if (Harmonizer) {
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(TlsThreadContext->Timers.HPStart);
        }

        if (workerId >= 0) {
            Threads[workerId].UnsetWork();
        } else {
            Y_ABORT_UNLESS(wctx.SharedThread);
            wctx.SharedThread->UnsetWork();
        }

        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        while (!StopFlag.load(std::memory_order_acquire)) {
            if (!semaphore.OldSemaphore || workerId >= 0 && semaphore.CurrentSleepThreadCount < 0) {
                if (workerId < 0 || !wctx.IsNeededToWaitNextActivation) {
                    TlsThreadContext->Timers.HPNow = GetCycleCountFast();
                    wctx.AddElapsedCycles(ActorSystemIndex, TlsThreadContext->Timers.HPNow - TlsThreadContext->Timers.HPStart);
                    return 0;
                }

                bool needToWait = false;
                bool needToBlock = false;
                AskToGoToSleep(&needToWait, &needToBlock);
                if (needToWait) {
                    if (Threads[workerId].Wait(SpinThresholdCycles, &StopFlag)) {
                        return 0;
                    }
                }
            } else {
                if (const ui32 activation = Activations.Pop(++revolvingCounter)) {
                    if (workerId >= 0) {
                        Threads[workerId].SetWork();
                    } else {
                        Y_ABORT_UNLESS(wctx.SharedThread);
                        wctx.SharedThread->SetWork();
                    }
                    AtomicDecrement(Semaphore);
                    TlsThreadContext->Timers.HPNow = GetCycleCountFast();
                    TlsThreadContext->Timers.Elapsed += TlsThreadContext->Timers.HPNow - TlsThreadContext->Timers.HPStart;
                    wctx.AddElapsedCycles(ActorSystemIndex, TlsThreadContext->Timers.Elapsed);
                    if (TlsThreadContext->Timers.Parked > 0) {
                        wctx.AddParkedCycles(TlsThreadContext->Timers.Parked);
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
            return GetReadyActivationLocalQueue(wctx, revolvingCounter);
        } else {
            return GetReadyActivationCommon(wctx, revolvingCounter);
        }
        return 0;
    }

    inline void TBasicExecutorPool::WakeUpLoop(i16 currentThreadCount) {
        for (i16 i = 0;;) {
            if (Threads[i].WakeUp()) {
                if (i >= currentThreadCount) {
                    AtomicIncrement(WrongWakenedThreadCount);
                }
                return;
            }
            if (++i >= PoolThreads) {
                i = 0;
            }
        }
    }

    bool TBasicExecutorPool::WakeUpLoopShared() {
        for (ui32 idx = 0; idx < MaxSharedThreadsForPool; ++idx) {
            TSharedExecutorThreadCtx *thread = SharedThreads[idx].load(std::memory_order_acquire);
            if (!thread) {
                return false;
            }
            if (thread->WakeUp()) {
                return true;
            }
        }
        return false;
    }

    void TBasicExecutorPool::ScheduleActivationExCommon(ui32 activation, ui64 revolvingCounter, TAtomic x) {
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);

        Activations.Push(activation, revolvingCounter);
        bool needToWakeUp = false;

        if (WakeUpLoopShared()) {
            x = AtomicIncrement(Semaphore);
            return;
        }

        do {
            needToWakeUp = semaphore.CurrentSleepThreadCount > 0;
            i64 oldX = semaphore.ConvertToI64();
            semaphore.OldSemaphore++;
            if (needToWakeUp) {
                semaphore.CurrentSleepThreadCount--;
            }
            x = AtomicGetAndCas(&Semaphore, semaphore.ConvertToI64(), oldX);
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
        poolStats.MaxThreadCount = PoolThreads;
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

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[PoolThreads + 2]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[PoolThreads + 2]);

        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread.reset(
                new TExecutorThread(
                    i,
                    0, // CpuId is not used in BASIC pool
                    actorSystem,
                    this,
                    MailboxTable.Get(),
                    PoolName,
                    TimePerMailbox,
                    EventsPerMailbox));
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }

        ScheduleWriters[PoolThreads].Init(ScheduleReaders[PoolThreads]);
        ScheduleWriters[PoolThreads + 1].Init(ScheduleReaders[PoolThreads + 1]);

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = PoolThreads + 2;
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
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Interrupt();
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

        if (workerId >= 0) {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        } else {
            ScheduleWriters[PoolThreads + 2 + workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TBasicExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto deadline = ActorSystem->Monotonic() + delta;
        if (workerId >= 0) {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        } else {
            ScheduleWriters[PoolThreads + 2 + workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
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

    i16 TBasicExecutorPool::GetThreadCount() const {
        return AtomicGet(ThreadCount);
    }

    void TBasicExecutorPool::SetThreadCount(i16 threads) {
        threads = Max<i16>(0, Min(PoolThreads, threads));
        with_lock (ChangeThreadsLock) {
            i16 prevCount = GetThreadCount();
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
        TExecutorThreadCtx& threadCtx = Threads[threadIdx];
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

    bool TExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
        EThreadState state = ExchangeState<EThreadState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state == EThreadState::None, "WaitingFlag# %d", int(state));
        if (spinThresholdCycles > 0) {
            // spin configured period
            Spin(spinThresholdCycles, stopFlag);
        }
        return Sleep(stopFlag);
    }

    bool TSharedExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
        i64 requestsForWakeUp = RequestsForWakeUp.fetch_sub(1, std::memory_order_acq_rel);
        if (requestsForWakeUp) {
            return false;
        }
        TWaitState state = ExchangeState<TWaitState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state.Flag == EThreadState::None, "WaitingFlag# %d", int(state.Flag));
        if (spinThresholdCycles > 0) {
            // spin configured period
            Spin(spinThresholdCycles, stopFlag);
        }
        return Sleep(stopFlag);
    }

    bool TExecutorThreadCtx::WakeUp() {
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

    bool TSharedExecutorThreadCtx::WakeUp() {
        i64 requestsForWakeUp = RequestsForWakeUp.load(std::memory_order_acquire);
        if (requestsForWakeUp > 0) {
            return false;
        }
        for (;;) {
            if (RequestsForWakeUp.compare_exchange_strong(requestsForWakeUp, requestsForWakeUp + 1, std::memory_order_acquire)) {
                if (requestsForWakeUp == -1) {
                    break;
                }
                return false;
            }
            if (requestsForWakeUp > 0) {
                return false;
            }
        }

        for (;;) {
            TWaitState state = GetState<TWaitState>();
            switch (state.Flag) {
                case EThreadState::None:
                case EThreadState::Work:
                    // TODO(kruall): check race
                    continue;
                case EThreadState::Spin:
                case EThreadState::Sleep:
                    if (ReplaceState<TWaitState>(state, {EThreadState::None})) {
                        if (state.Flag == EThreadState::Sleep) {
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

    TSharedExecutorThreadCtx* TBasicExecutorPool::ReleaseSharedThread() {
        ui64 count = SharedThreadsCount.fetch_sub(1, std::memory_order_acq_rel);
        Y_ABORT_UNLESS(count);
        auto *thread = SharedThreads[count - 1].exchange(nullptr);
        Y_ABORT_UNLESS(thread);
        return thread;
    }

    void TBasicExecutorPool::AddSharedThread(TSharedExecutorThreadCtx* thread) {
        ui64 count = SharedThreadsCount.fetch_add(1, std::memory_order_acq_rel);
        Y_ABORT_UNLESS(count < MaxSharedThreadsForPool);
        thread = SharedThreads[count].exchange(thread);
        Y_ABORT_UNLESS(!thread);
    }

}
