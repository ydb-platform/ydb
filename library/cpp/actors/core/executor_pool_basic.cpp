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
        , SpinThreshold(spinThreshold)
        , SpinThresholdCycles(spinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , Threads(new TThreadCtx[threads])
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
        SetSharedExecutorsCount(cfg.SharedExecutorsCount);
        SoftProcessingDurationTs = cfg.SoftProcessingDurationTs;
    }

    TBasicExecutorPool::~TBasicExecutorPool() {
        Threads.Destroy();
    }

    bool TBasicExecutorPool::GoToSleep(TThreadCtx& threadCtx, TTimers &timers) {
        do {
            timers.HPNow = GetCycleCountFast();
            timers.Elapsed += timers.HPNow - timers.HPStart;
            if (threadCtx.Pad.Park()) // interrupted
                return true;
            timers.HPStart = GetCycleCountFast();
            timers.Parked += timers.HPStart - timers.HPNow;
        } while (threadCtx.WaitingFlag.load(std::memory_order_acquire) == TThreadCtx::WS_BLOCKED && !StopFlag.load(std::memory_order_relaxed));
        return false;
    }

    void TBasicExecutorPool::GoToSpin(TThreadCtx& threadCtx) {
        ui64 start = GetCycleCountFast();
        bool doSpin = true;

        #pragma clang loop unroll(disable)
        while (true) {
            #pragma clang loop unroll(disable)
            for (ui32 j = 0; doSpin && j < 12; ++j) {
                if (GetCycleCountFast() >= (start + SpinThresholdCycles)) {
                    doSpin = false;
                    break;
                }

                #pragma clang loop unroll(disable)
                for (ui32 i = 0; i < 12; ++i) {
                    if (threadCtx.WaitingFlag.load(std::memory_order_acquire) == TThreadCtx::WS_ACTIVE) {
                        SpinLockPause();
                    } else {
                        doSpin = false;
                        break;
                    }
                }
            }
            if (!doSpin) {
                break;
            }
            // if (StopFlag.load(std::memory_order_relaxed)) {
            //     break;
            // }
        }
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

        TThreadCtx::EWaitState state = threadCtx.WaitingFlag.load(std::memory_order_acquire);
        Y_ABORT_UNLESS(state == TThreadCtx::WS_NONE, "WaitingFlag# %d", int(state));

        if (SpinThreshold > 0 && !needToBlock) {
            // spin configured period
            threadCtx.WaitingFlag.store(TThreadCtx::WS_ACTIVE, std::memory_order_release);
            GoToSpin(threadCtx);
            // then - sleep
            state = threadCtx.WaitingFlag.load(std::memory_order_acquire);
            if (state == TThreadCtx::WS_ACTIVE) {
                if (threadCtx.WaitingFlag.compare_exchange_strong(state, TThreadCtx::WS_BLOCKED)) {
                    if (GoToSleep(threadCtx, timers)) {  // interrupted
                        return true;
                    }
                }
            }
        } else {
            threadCtx.WaitingFlag.store(TThreadCtx::WS_BLOCKED, std::memory_order_release);
            if (GoToSleep(threadCtx, timers)) {  // interrupted
                return true;
            }
        }

        Y_DEBUG_ABORT_UNLESS(StopFlag.load(std::memory_order_acquire) || threadCtx.WaitingFlag.load(std::memory_order_acquire) == TThreadCtx::WS_NONE);

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
        i64 x = Semaphore.load(std::memory_order_relaxed);

        do {
            TSemaphore semaphore = TSemaphore::GetSemaphore(x);
            if (semaphore.CurrentSleepThreadCount < 0) {
                semaphore.CurrentSleepThreadCount++;
                if (Semaphore.compare_exchange_strong(x, semaphore.ConvertToI64())) {
                    *needToWait = true;
                    *needToBlock = true;
                    return;
                }
                continue;
            }

            if (semaphore.OldSemaphore == 0) {
                semaphore.CurrentSleepThreadCount++;
                if (Semaphore.compare_exchange_strong(x, semaphore.ConvertToI64())) {
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
            Threads[workerId].WaitingFlag.store(TThreadCtx::WS_NONE, std::memory_order_release);
        }

        i64 x = Semaphore.load(std::memory_order_acquire);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        while (!StopFlag.load(std::memory_order_relaxed)) {
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
                        Threads[workerId].WaitingFlag.store(TThreadCtx::WS_RUNNING, std::memory_order_release);
                    }

                    Semaphore.fetch_sub(1, std::memory_order_release);
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

            // SpinLockPause();
            x = Semaphore.load(std::memory_order_acquire);
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
        for (i16 i = 0;;) {
            TThreadCtx& threadCtx = Threads[i];
            TThreadCtx::EWaitState state = threadCtx.WaitingFlag.load(std::memory_order_acquire);

            switch (state) {
                case TThreadCtx::WS_NONE:
                case TThreadCtx::WS_RUNNING:
                    if (++i >= MaxThreadCount - SharedExecutorsCount) {
                        i = 0;
                    }
                    break;
                case TThreadCtx::WS_ACTIVE:
                case TThreadCtx::WS_BLOCKED:
                    if (threadCtx.WaitingFlag.compare_exchange_strong(state, TThreadCtx::WS_NONE)) {
                        threadCtx.Pad.Unpark();

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

    void TBasicExecutorPool::ScheduleActivationExCommon(ui32 activation, ui64 revolvingCounter) {
        Activations.Push(activation, revolvingCounter);
        bool needToWakeUp = false;

        i64 x = Semaphore.load(std::memory_order_acquire);
        TSemaphore semaphore = TSemaphore::GetSemaphore(x);
        do {
            needToWakeUp = semaphore.CurrentSleepThreadCount > SharedExecutorsCount;
            semaphore.OldSemaphore++;
            if (needToWakeUp) {
                semaphore.CurrentSleepThreadCount--;
            }

            if (Semaphore.compare_exchange_strong(x, semaphore.ConvertToI64())) {
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
        }
        ScheduleActivationExCommon(activation, revolvingWriteCounter);
    }

    void TBasicExecutorPool::ScheduleActivationEx(ui32 activation, ui64 revolvingCounter) {
        if constexpr (NFeatures::IsLocalQueues()) {
            ScheduleActivationExLocalQueue(activation, revolvingCounter);
        } else {
            ScheduleActivationExCommon(activation, revolvingCounter);
        }
    }

    void TBasicExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        poolStats.MaxUtilizationTime = RelaxedLoad(&MaxUtilizationAccumulator) / (i64)(NHPTimer::GetCyclesPerSecond() / 1000);
        poolStats.WrongWakenedThreadCount = RelaxedLoad(&WrongWakenedThreadCount);
        poolStats.CurrentThreadCount = RelaxedLoad(&ThreadCount);
        poolStats.DefaultThreadCount = DefaultThreadCount;
        poolStats.MaxThreadCount = MaxThreadCount;
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
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Pad.Interrupt();
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
            TSemaphore semaphore = TSemaphore::GetSemaphore(Semaphore.load(std::memory_order_acquire));
            i64 oldX = semaphore.ConvertToI64();
            semaphore.CurrentThreadCount = threads;
            if (threads > prevCount) {
                semaphore.CurrentSleepThreadCount += (i64)threads - prevCount;
            } else {
                semaphore.CurrentSleepThreadCount -= (i64)prevCount - threads;
            }

            Semaphore.fetch_add(semaphore.ConvertToI64() - oldX, std::memory_order_release);
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
        i64 x = Semaphore.load(std::memory_order_acquire);
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
}
