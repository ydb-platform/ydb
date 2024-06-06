#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_basic_sanitizer.h"
#include "executor_pool_jail.h"
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

#ifdef NDEBUG
    constexpr bool DebugMode = true;
#else
    constexpr bool DebugMode = false;
#endif



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
        if (affinity != nullptr) {
            delete affinity;
        }
    }

    TBasicExecutorPool::TBasicExecutorPool(const TBasicExecutorPoolConfig& cfg, IHarmonizer *harmonizer, TExecutorPoolJail *jail)
        : TExecutorPoolBase(cfg.PoolId, cfg.Threads, new TAffinity(cfg.Affinity))
        , DefaultSpinThresholdCycles(cfg.SpinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , SpinThresholdCycles(DefaultSpinThresholdCycles)
        , SpinThresholdCyclesPerThread(new NThreading::TPadded<std::atomic<ui64>>[cfg.Threads])
        , Threads(new NThreading::TPadded<TExecutorThreadCtx>[cfg.Threads])
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
        Y_UNUSED(Jail, SoftProcessingDurationTs);
        for (ui32 idx = 0; idx < MaxSharedThreadsForPool; ++idx) {
            SharedThreads[idx].store(nullptr, std::memory_order_release);
        }

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

        if constexpr (DebugMode) {
            Sanitizer.reset(new TBasicExecutorPoolSanitizer(this));
        }
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
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        if (workerId >= 0) {
            Threads[workerId].UnsetWork();
        } else {
            Y_ABORT_UNLESS(wctx.SharedThread);
            wctx.SharedThread->UnsetWork();
        }
        if (Harmonizer) {
            LWPROBE(TryToHarmonize, PoolId, PoolName);
            Harmonizer->Harmonize(hpnow);
        }

        TAtomic x = AtomicGet(Semaphore);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        while (!StopFlag.load(std::memory_order_acquire)) {
            if (!semaphore.OldSemaphore || workerId >= 0 && semaphore.CurrentSleepThreadCount < 0) {
                if (workerId < 0 || !wctx.IsNeededToWaitNextActivation) {
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
                TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
                if (const ui32 activation = Activations.Pop(++revolvingCounter)) {
                    if (workerId >= 0) {
                        Threads[workerId].SetWork();
                    } else {
                        Y_ABORT_UNLESS(wctx.SharedThread);
                        wctx.SharedThread->SetWork();
                    }
                    AtomicDecrement(Semaphore);
                    return activation;
                }
            }

            SpinLockPause();
            x = AtomicGet(Semaphore);
            semaphore = TSemaphore::GetSemaphore(x);
        }

        return 0;
    }

    ui32 TBasicExecutorPool::GetReadyActivationLocalQueue(TWorkerContext& wctx, ui64 revolvingCounter) {
        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < static_cast<i32>(MaxFullThreadCount));

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
            if (++i >= MaxFullThreadCount) {
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
        bool needToChangeOldSemaphore = true;

        i16 sharedThreads = SharedThreadsCount.load(std::memory_order_acquire); // this value changing once in second

        if (sharedThreads) {
            needToChangeOldSemaphore = false;
            x = AtomicIncrement(Semaphore);
            if (WakeUpLoopShared()) {
                return;
            }
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
            poolStats.MaxConsumedCpuUs = stats.MaxConsumedCpu;
            poolStats.MinConsumedCpuUs = stats.MinConsumedCpu;
            poolStats.MaxBookedCpuUs = stats.MaxBookedCpu;
            poolStats.MinBookedCpuUs = stats.MinBookedCpu;
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

    void TBasicExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());

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
                    MailboxTable.Get(),
                    PoolName,
                    TimePerMailbox,
                    EventsPerMailbox));
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }

        ScheduleWriters[MaxFullThreadCount].Init(ScheduleReaders[MaxFullThreadCount]);
        ScheduleWriters[MaxFullThreadCount + 1].Init(ScheduleReaders[MaxFullThreadCount + 1]);

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = MaxFullThreadCount + 2;
    }

    void TBasicExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());

        ThreadUtilization = 0;
        AtomicAdd(MaxUtilizationCounter, -(i64)GetCycleCountFast());

        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread->Start();
        }

        if constexpr (DebugMode) {
            Sanitizer->Start();
        }
    }

    void TBasicExecutorPool::PrepareStop() {
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != MaxFullThreadCount; ++i) {
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Interrupt();
        }
        if constexpr (DebugMode) {
            Sanitizer->Stop();
        }
    }

    void TBasicExecutorPool::Shutdown() {
        for (i16 i = 0; i != MaxFullThreadCount; ++i)
            Threads[i].Thread->Join();
        if constexpr (DebugMode) {
            Sanitizer->Join();
        }
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

        if (workerId >= 0) {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        } else {
            ScheduleWriters[MaxFullThreadCount + 2 + workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        }
    }

    void TBasicExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < MaxFullThreadCount);

        const auto deadline = ActorSystem->Monotonic() + delta;
        if (workerId >= 0) {
            ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
        } else {
            ScheduleWriters[MaxFullThreadCount + 2 + workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
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
        float extraThreads = 0;
        for (ui32 sharedIdx = 0; sharedIdx < 2; ++sharedIdx) {
            auto *sharedThread = SharedThreads[sharedIdx].load(std::memory_order_acquire);
            if (!sharedThread) {
                break;
            }
            i16 poolsInThread = 0;
            for (ui32 poolIdx = 0; poolIdx < MaxPoolsForSharedThreads; ++poolIdx) {
                if (sharedThread->ExecutorPools[poolIdx].load(std::memory_order_acquire)) {
                    poolsInThread++;
                }
            }
            extraThreads += 1.0 / poolsInThread;
        }
        return GetFullThreadCount() + extraThreads;
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

    bool TSharedExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag) {
        i64 requestsForWakeUp = RequestsForWakeUp.fetch_sub(1, std::memory_order_acq_rel);
        if (requestsForWakeUp > 1) {
            RequestsForWakeUp.store(1, std::memory_order_release);
        }
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

    bool TSharedExecutorThreadCtx::WakeUp() {
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION_FROM_QUEUE, false> activityGuard;
        i64 requestsForWakeUp = RequestsForWakeUp.fetch_add(1, std::memory_order_acq_rel);
        if (requestsForWakeUp >= 0) {
            return false;
        }

        for (;;) {
            TWaitState state = GetState<TWaitState>();
            switch (state.Flag) {
                case EThreadState::None:
                case EThreadState::Work:
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
