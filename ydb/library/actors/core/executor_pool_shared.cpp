#include "executor_pool_jail.h"
#include "executor_pool_shared.h"
#include "executor_pool_basic.h"
#include "executor_pool_shared_sanitizer.h"
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

    TPoolManager::TPoolManager(const TVector<TPoolShortInfo> &poolInfos)
        : PoolInfos(poolInfos)
    {
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolManager::TPoolManager: poolInfos.size() == ", poolInfos.size());
        PoolThreadRanges.resize(poolInfos.size());
        ui64 totalThreads = 0;
        for (const auto& poolInfo : poolInfos) {
            PoolThreadRanges[poolInfo.PoolId].Begin = totalThreads;
            PoolThreadRanges[poolInfo.PoolId].End = totalThreads + poolInfo.SharedThreadCount;
            ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolManager::TPoolManager: PoolThreadRanges[", poolInfo.PoolId, "]:  Threads: [" , PoolThreadRanges[poolInfo.PoolId].Begin, ";", PoolThreadRanges[poolInfo.PoolId].End, ")");
            totalThreads += poolInfo.SharedThreadCount;
            if (poolInfo.InPriorityOrder) {
                PriorityOrder.push_back(poolInfo.PoolId);
            }
        }
        ACTORLIB_DEBUG(EDebugLevel::ActorSystem, "TPoolManager::TPoolManager: totalThreads == ", totalThreads);
        Sort(PoolInfos.begin(), PoolInfos.end(), [](const TPoolShortInfo& a, const TPoolShortInfo& b) {
            return a.PoolId < b.PoolId;
        });
    }

    LWTRACE_USING(ACTORLIB_PROVIDER);

    TSharedExecutorPool::TSharedExecutorPool(
        const TSharedExecutorPoolConfig& cfg,
        const TVector<TPoolShortInfo> &poolInfos
    )   : TExecutorPoolBaseMailboxed(cfg.PoolId)
        , PoolThreads(SumThreads(poolInfos))
        , PoolManager(poolInfos)
        , DefaultSpinThresholdCycles(cfg.SpinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , PoolName("Shared")
        , TimePerMailbox(cfg.TimePerMailbox)
        , EventsPerMailbox(cfg.EventsPerMailbox)
        , SoftProcessingDurationTs(cfg.SoftProcessingDurationTs)
        , Threads(new NThreading::TPadded<TSharedExecutorThreadCtx>[PoolThreads])
        , ForeignThreadsAllowedByPool(new NThreading::TPadded<std::atomic<ui64>>[poolInfos.size()])
        , ForeignThreadSlots(new NThreading::TPadded<std::atomic<ui64>>[poolInfos.size()])
        , LocalThreads(new NThreading::TPadded<std::atomic<ui64>>[poolInfos.size()])
        , LocalNotifications(new NThreading::TPadded<std::atomic<ui64>>[poolInfos.size()])
        , SpinThresholdCycles(DefaultSpinThresholdCycles)
        , ThreadsState(PoolThreads)
    {
        Y_UNUSED(Barrier);
        ui64 passedThreads = 0;
        for (i16 i = 0; i < static_cast<i16>(PoolManager.PoolThreadRanges.size()); ++i) {
            for (i16 j = PoolManager.PoolThreadRanges[i].Begin; j < PoolManager.PoolThreadRanges[i].End; ++j) {
                Threads[j].OwnerPoolId = i;
                Threads[j].CurrentPoolId = i;
                Threads[j].SoftDeadlineForPool = 0;
                Threads[j].SoftProcessingDurationTs = Us2Ts(100'000);
                passedThreads++;
            }
        }
        for (ui64 i = 0; i < PoolManager.PoolInfos.size(); ++i) {
            ForeignThreadsAllowedByPool[i].store(0, std::memory_order_release);
            ForeignThreadSlots[i].store(0, std::memory_order_release);
            LocalThreads[i].store(PoolManager.PoolInfos[i].SharedThreadCount, std::memory_order_release);
            LocalNotifications[i].store(0, std::memory_order_release);
        }
        Y_ABORT_UNLESS(passedThreads == static_cast<ui64>(PoolThreads), "Passed threads %" PRIu64 " != PoolThreads %" PRIu64, passedThreads, static_cast<ui64>(PoolThreads));

        Pools.resize(PoolManager.PoolThreadRanges.size());
    }

    TSharedExecutorPool::~TSharedExecutorPool() {
        Threads.Destroy();
    }

    i16 TSharedExecutorPool::SumThreads(const TVector<TPoolShortInfo> &poolInfos) {
        i16 threadCount = 0;
        for (const auto &poolInfo : poolInfos) {
            threadCount += poolInfo.SharedThreadCount;
        }
        return threadCount;
    }

    i16 TSharedExecutorPool::FindPoolForWorker(TSharedExecutorThreadCtx& thread, ui64 revolvingCounter) {
        TWorkerId workerId = Max<TWorkerId>();
        if (TlsThreadContext) {
            workerId = TlsThreadContext->WorkerId;
        }
        for (i16 i : PoolManager.PriorityOrder) {
            if (Pools[i] == nullptr) {
                ACTORLIB_DEBUG(EDebugLevel::Trace, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: pool[", i, "] is nullptr; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }
            if (ForeignThreadsAllowedByPool[i].load(std::memory_order_acquire) == 0) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: don't have leases; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }
            ui64 semaphore = Pools[i]->GetSemaphore().OldSemaphore;
            if (semaphore == 0) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: pool[", i, "]::Semaphore == 0; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }

            if (thread.OwnerPoolId == i) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: ownerPoolId == poolId; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i);
                return i;
            }

            ui64 slots = ForeignThreadSlots[i].load(std::memory_order_acquire);

            while (true) {
                if (slots <= 0) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: pool[", i, "]::Slots == 0; OwnerPoolId == ", thread.OwnerPoolId);
                    break;
                }
                if (ForeignThreadSlots[i].compare_exchange_strong(slots, slots - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    ACTORLIB_DEBUG(EDebugLevel::Lease, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: get lease; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i, " semaphore == ", semaphore, " slots == ", slots, " -> ", slots - 1);
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::FindPoolForWorker: found pool; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i, " semaphore == ", semaphore);
                    return i;
                }
            }
        }
        return -1;
    }

    void TSharedExecutorPool::SwitchToPool(TWorkerContext& wctx, i16 poolId, NHPTimer::STime hpNow) {
        ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", wctx.WorkerId, " TSharedExecutorPool::SwitchToPool: start; ownerPoolId == ", Threads[wctx.WorkerId].OwnerPoolId, " currentPoolId == ", Threads[wctx.WorkerId].CurrentPoolId, " poolId = ", poolId);
        wctx.UpdateThreadTime();
        if (Threads[wctx.WorkerId].CurrentPoolId != poolId && Threads[wctx.WorkerId].CurrentPoolId != Threads[wctx.WorkerId].OwnerPoolId) {
            ui64 slots = ForeignThreadSlots[Threads[wctx.WorkerId].CurrentPoolId].fetch_add(1, std::memory_order_acq_rel);
            ACTORLIB_DEBUG(EDebugLevel::Lease, "Worker_", wctx.WorkerId, " TSharedExecutorPool::SwitchToPool: return lease; ownerPoolId == ", Threads[wctx.WorkerId].OwnerPoolId, " currentPoolId == ", Threads[wctx.WorkerId].CurrentPoolId, " poolId = ", poolId, " slots == ", slots, " -> ", slots + 1);
        }
        Threads[wctx.WorkerId].CurrentPoolId = poolId;
        TlsThreadContext->Pool = Pools[poolId];
        Threads[wctx.WorkerId].SoftDeadlineForPool = Max<NHPTimer::STime>();
        Threads[wctx.WorkerId].Thread->SwitchPool(Pools[poolId]);
        ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", wctx.WorkerId, " TSharedExecutorPool::SwitchToPool: end; ownerPoolId == ", Threads[wctx.WorkerId].OwnerPoolId, " currentPoolId == ", Threads[wctx.WorkerId].CurrentPoolId, " poolId = ", poolId);
    }

    TMailbox* TSharedExecutorPool::GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
        Y_UNUSED(SoftProcessingDurationTs);
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        TWorkerId workerId = wctx.WorkerId;
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);
        auto &thread = Threads[workerId];
        thread.UnsetWork();
        TMailbox *mailbox = nullptr;
        while (!StopFlag.load(std::memory_order_acquire)) {
            if (hpnow < thread.SoftDeadlineForPool || thread.CurrentPoolId == thread.OwnerPoolId) {
                ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: continue same pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                if (thread.SoftDeadlineForPool == Max<NHPTimer::STime>()) {
                    thread.SoftDeadlineForPool = GetCycleCountFast() + thread.SoftProcessingDurationTs;
                }
                mailbox = Pools[thread.CurrentPoolId]->GetReadyActivationShared(wctx, revolvingCounter);
                if (mailbox) {
                    TlsThreadContext->ProcessedActivationsByCurrentPool++;
                    ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: found mailbox; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    return mailbox;
                } else if (!wctx.IsNeededToWaitNextActivation) {
                    TlsThreadContext->ProcessedActivationsByCurrentPool++;
                    ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: no mailbox and no need to wait; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    return nullptr;
                } else {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: no mailbox and need to find new pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " processedActivationsByCurrentPool == ", TlsThreadContext->ProcessedActivationsByCurrentPool);
                    TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                    if (thread.CurrentPoolId != thread.OwnerPoolId) {
                        SwitchToPool(wctx, thread.OwnerPoolId, hpnow);
                        continue;
                    }
                }
            } else if (!wctx.IsNeededToWaitNextActivation) {
                TlsThreadContext->ProcessedActivationsByCurrentPool++;
                ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: no mailbox and no need to wait; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                return nullptr;
            } else {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: comeback to owner pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " processedActivationsByCurrentPool == ", TlsThreadContext->ProcessedActivationsByCurrentPool, " hpnow == ", hpnow, " softDeadlineForPool == ", thread.SoftDeadlineForPool);
                TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                SwitchToPool(wctx, thread.OwnerPoolId, hpnow);
                // after soft deadline we check owner pool again
                continue;
            }
            bool goToSleep = true;
            for (i16 attempt = 0; attempt < 1; ++attempt) {
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: attempt == ", attempt, " ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                if (i16 poolId = FindPoolForWorker(thread, revolvingCounter); poolId != -1) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: found pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    SwitchToPool(wctx, poolId, hpnow);
                    goToSleep = false;
                    break;
                }
            }
            if (goToSleep) {
                bool allowedToSleep = false;
                ui64 threadsStateRaw = ThreadsState.load(std::memory_order_acquire);
                TThreadsState threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
                while (true) {
                    if (threadsState.Notifications == 0) {
                        threadsState.WorkingThreadCount--;
                        if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                            allowedToSleep = true;
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: checked global state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " notifications == ", threadsState.Notifications, " workingThreadCount: ", threadsState.WorkingThreadCount + 1, " -> ", threadsState.WorkingThreadCount);
                            break;
                        }
                    } else {
                        threadsState.Notifications--;
                        if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: checked global state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " workingThreadCount == ", threadsState.WorkingThreadCount, " notifications: ", threadsState.Notifications + 1, " -> ", threadsState.Notifications);
                            break;
                        }
                    }
                    threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
                }
                if (allowedToSleep) {
                    ui64 localNotifications = LocalNotifications[thread.OwnerPoolId].load(std::memory_order_acquire);
                    while (true) {
                        if (localNotifications == 0) {
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: checked local state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " localNotifications == 0");
                            break;
                        }
                        if (LocalNotifications[thread.OwnerPoolId].compare_exchange_strong(localNotifications, localNotifications - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                            allowedToSleep = false;
                            ThreadsState.fetch_add(1, std::memory_order_acq_rel);
                            ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: checked local state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " localNotifications: ", localNotifications, " -> ", localNotifications - 1);
                            break;
                        }
                    }
                }
                if (allowedToSleep) {
                    SwitchToPool(wctx, thread.OwnerPoolId, hpnow);
                    LocalThreads[thread.OwnerPoolId].fetch_sub(1, std::memory_order_acq_rel);
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: going to sleep; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    if (thread.Wait(SpinThresholdCycles.load(std::memory_order_relaxed), &StopFlag, &LocalNotifications[thread.OwnerPoolId], &ThreadsState)) {
                        ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::GetReadyActivation: interrupted; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                        return nullptr; // interrupted
                    }
                    LocalThreads[thread.OwnerPoolId].fetch_add(1, std::memory_order_acq_rel);
                    ThreadsState.fetch_add(1, std::memory_order_acq_rel);
                }
            }
            hpnow = GetCycleCountFast();
        }

        return nullptr;
    }

    void TSharedExecutorPool::ScheduleActivationEx(TMailbox* mailbox, ui64 revolvingCounter) {
        // unreachable call
        Y_ABORT("ScheduleActivationEx is not allowed for united pool");
    }

    void TSharedExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        poolStats.CurrentThreadCount = GetThreadCount();
        poolStats.DefaultThreadCount = GetDefaultThreadCount();
        poolStats.MaxThreadCount = GetMaxThreadCount();
        poolStats.SpinningTimeUs = Ts2Us(SpinningTimeUs);
        poolStats.SpinThresholdUs = Ts2Us(SpinThresholdCycles);
        statsCopy.resize(PoolThreads + 1);
        // Save counters from the pool object
        statsCopy[0] = TExecutorThreadStats();
        //statsCopy[0].Aggregate(Stats);
#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
        //RecalculateStuckActors(statsCopy[0]);
#endif
        // Per-thread stats
        for (i16 i = 0; i < PoolThreads; ++i) {
            Threads[i].Thread->GetCurrentStats(statsCopy[i + 1]);
        }
    }

    void TSharedExecutorPool::GetExecutorPoolState(TExecutorPoolState &poolState) const {
        poolState.CurrentLimit = GetThreadCount();
        poolState.MaxLimit = GetMaxThreadCount();
        poolState.MinLimit = GetDefaultThreadCount();
    }

    void TSharedExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        TAffinityGuard affinityGuard(Affinity());

        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Prepare: start");

        ActorSystem = actorSystem;

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[PoolThreads]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[PoolThreads]);


        for (i16 i = 0; i != PoolThreads; ++i) {
            Y_ABORT_UNLESS(Pools[Threads[i].OwnerPoolId] != nullptr, "Pool is nullptr i %" PRIu16, i);
            Y_ABORT_UNLESS(Threads[i].OwnerPoolId < static_cast<i16>(Pools.size()), "OwnerPoolId is out of range i %" PRIu16 " OwnerPoolId == %" PRIu16, i, Threads[i].OwnerPoolId);
            Y_ABORT_UNLESS(Threads[i].OwnerPoolId >= 0, "OwnerPoolId is out of range i %" PRIu16 " OwnerPoolId == %" PRIu16, i, Threads[i].OwnerPoolId);
            ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Prepare: create thread ", i, " OwnerPoolId == ", Threads[i].OwnerPoolId);
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

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = PoolThreads;

        //Sanitizer = std::make_unique<TSharedExecutorPoolSanitizer>(this);
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Prepare: end");
    }

    void TSharedExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());

        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Start");

        for (i16 i = 0; i != PoolThreads; ++i) {
            Y_ABORT_UNLESS(Threads[i].Thread != nullptr, "Thread is nullptr i %" PRIu16, i);
            Threads[i].Thread->Start();
        }
        if (Sanitizer) {
            Sanitizer->Start();
        }
    }

    void TSharedExecutorPool::PrepareStop() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::PrepareStop");
        StopFlag.store(true, std::memory_order_release);
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->StopFlag.store(true, std::memory_order_release);
            Threads[i].Interrupt();
        }
        if (Sanitizer) {
            Sanitizer->Stop();
        }
    }

    void TSharedExecutorPool::Shutdown() {
        ACTORLIB_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Shutdown");
        for (i16 i = 0; i != PoolThreads; ++i) {
            Threads[i].Thread->Join();
        }
        if (Sanitizer) {
            Sanitizer->Join();
        }
    }

    void TSharedExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TSharedExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto current = ActorSystem->Monotonic();
        if (deadline < current)
            deadline = current;

        ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TSharedExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);

        const auto deadline = ActorSystem->Monotonic() + delta;
        ScheduleWriters[workerId].Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    float TSharedExecutorPool::GetThreadCount() const {
        return PoolThreads;
    }

    float TSharedExecutorPool::GetDefaultThreadCount() const {
        return PoolThreads;
    }

    float TSharedExecutorPool::GetMinThreadCount() const {
        return PoolThreads;
    }

    float TSharedExecutorPool::GetMaxThreadCount() const {
        return PoolThreads;
    }

    ui32 TSharedExecutorPool::GetThreads() const {
        return PoolThreads;
    }

    void TSharedExecutorPool::Initialize(TWorkerContext& wctx) {
        ACTORLIB_DEBUG(EDebugLevel::Executor, "TSharedExecutorPool::Initialize: start");
        wctx.SharedExecutor = this;
        wctx.Executor = Pools[Threads[wctx.WorkerId].OwnerPoolId];
        Threads[wctx.WorkerId].Thread->SwitchPool(Pools[Threads[wctx.WorkerId].OwnerPoolId]);
        ACTORLIB_DEBUG(EDebugLevel::Executor, "TSharedExecutorPool::Initialize: end");
    }

    bool TSharedExecutorPool::WakeUpLocalThreads(i16 ownerPoolId) {
        ui64 notifications = LocalNotifications[ownerPoolId].load(std::memory_order_acquire);
        if (notifications == 0) {
            LocalNotifications[ownerPoolId].fetch_add(1, std::memory_order_relaxed);
        }
        ui64 localThreads = LocalThreads[ownerPoolId].load(std::memory_order_acquire);
        if (localThreads == static_cast<ui64>(PoolManager.PoolInfos[ownerPoolId].SharedThreadCount)) {
            return false;
        }

        for (i16 threadId = PoolManager.PoolThreadRanges[ownerPoolId].Begin; threadId < PoolManager.PoolThreadRanges[ownerPoolId].End; ++threadId) {
            auto &thread = Threads[threadId];
            if (thread.WakeUp()) {
                TWorkerId workerId = Max<TWorkerId>();
                if (TlsThreadContext) {
                    workerId = TlsThreadContext->WorkerId;
                }
                ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::TryToWakeUp: wakeup from own pool; ownerPoolId == ", ownerPoolId, " threadId == ", threadId);
                break;
            }
        }
        return true;
    }

    bool TSharedExecutorPool::WakeUpGlobalThreads(i16 ownerPoolId) {
        if (ForeignThreadsAllowedByPool[ownerPoolId].load(std::memory_order_acquire) == 0) {
            return false;
        }
        ui64 slots = ForeignThreadSlots[ownerPoolId].load(std::memory_order_acquire);
        if (slots <= 0) {
            return false;
        }

        TWorkerId workerId = Max<TWorkerId>();
        if (TlsThreadContext) {
            workerId = TlsThreadContext->WorkerId;
        }

        ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::TryToWakeUp: ownerPoolId == ", ownerPoolId);
        ui64 threadsStateRaw = ThreadsState.load(std::memory_order_acquire);
        TThreadsState threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
        bool allowedToWakeUp = false;
        bool increaseNotifications = false;
        while (true) {
            if (threadsState.Notifications >= static_cast<ui64>(PoolThreads) && threadsState.WorkingThreadCount == static_cast<ui64>(PoolThreads)) {
                break;
            }
            threadsState.Notifications++;
            increaseNotifications = true;
            allowedToWakeUp = threadsState.WorkingThreadCount < static_cast<ui64>(PoolThreads);
            if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
            threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
            increaseNotifications = false;
        }
        if (increaseNotifications) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::TryToWakeUp: increase notifications; ", threadsState.Notifications - 1, " -> ", threadsState.Notifications);
        }
        if (!allowedToWakeUp) {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", workerId, " TSharedExecutorPool::TryToWakeUp: no free threads");
            return false;
        }

        for (i16 i = static_cast<i16>(PoolManager.PriorityOrder.size()) - 1; i >= 0; --i) {
            i16 poolId = PoolManager.PriorityOrder[i];
            for (i16 threadId = PoolManager.PoolThreadRanges[poolId].Begin; threadId < PoolManager.PoolThreadRanges[poolId].End; ++threadId) {
                auto &thread = Threads[threadId];
                if (thread.WakeUp()) {
                    ACTORLIB_DEBUG(EDebugLevel::Executor, "Worker_", workerId, " TSharedExecutorPool::TryToWakeUp: wakeup from other pool; ownerPoolId == ", ownerPoolId, " poolId == ", poolId, " threadId == ", threadId);
                    return true;
                }
            }
        }
        return false;
    }

    bool TSharedExecutorThreadCtx::Spin(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState) {
        NHPTimer::STime start = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_SPIN> activityGuard(start);
        while (true) {
            for (ui32 j = 0;j < 12; ++j) {
                NHPTimer::STime hpnow = GetCycleCountFast();
                if (hpnow >= i64(start + spinThresholdCycles)) {
                    return false;
                }
                for (ui32 i = 0; i < 12; ++i) {
                    EThreadState state = GetState<EThreadState>();
                    if (state == EThreadState::Spin) {
                        TSharedExecutorPool::TThreadsState threadsStateValue = TSharedExecutorPool::TThreadsState::GetThreadsState(threadsState->load(std::memory_order_acquire));
                        ui64 localNotificationsValue = localNotifications->load(std::memory_order_acquire);
                        if (threadsStateValue.Notifications == 0 && localNotificationsValue == 0) {
                            SpinLockPause();
                        } else {
                            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", TlsThreadContext->WorkerId, " TSharedExecutorPool::Spin: wake up from notifications; ownerPoolId == ", OwnerPoolId, " notifications == ", threadsStateValue.Notifications, " localNotifications == ", localNotificationsValue);
                            ExchangeState(EThreadState::None);
                            return true;
                        }
                    } else {
                        return true;
                    }
                }
            }
            if (stopFlag->load(std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }


    bool TSharedExecutorThreadCtx::Wait(ui64 spinThresholdCycles, std::atomic<bool> *stopFlag, std::atomic<ui64> *localNotifications, std::atomic<ui64> *threadsState) {
        EThreadState state = ExchangeState<EThreadState>(EThreadState::Spin);
        Y_ABORT_UNLESS(state == EThreadState::None, "WaitingFlag# %d", int(state));
        if (spinThresholdCycles > 0) {
            // spin configured period
            if (Spin(spinThresholdCycles, stopFlag, localNotifications, threadsState)) {
                return false;
            }
        }
        TSharedExecutorPool::TThreadsState threadsStateValue = TSharedExecutorPool::TThreadsState::GetThreadsState(threadsState->load(std::memory_order_acquire));
        ui64 localNotificationsValue = localNotifications->load(std::memory_order_acquire);
        if (threadsStateValue.Notifications != 0 || localNotificationsValue != 0)
        {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", TlsThreadContext->WorkerId, " TSharedExecutorPool::Wait: wake up from notifications; ownerPoolId == ", OwnerPoolId, " notifications == ", threadsStateValue.Notifications, " localNotifications == ", localNotificationsValue);
            ExchangeState(EThreadState::None);
            return false;
        } else {
            ACTORLIB_DEBUG(EDebugLevel::Activation, "Worker_", TlsThreadContext->WorkerId, " TSharedExecutorPool::Wait: going to sleep after checking notifications; ownerPoolId == ", OwnerPoolId);
        }
        return Sleep(stopFlag);
    }
    
    void TSharedExecutorPool::FillForeignThreadsAllowed(std::vector<i16>& foreignThreadsAllowed) const {
        foreignThreadsAllowed.resize(PoolManager.PoolInfos.size());
        for (ui64 i = 0; i < foreignThreadsAllowed.size(); ++i) {
            foreignThreadsAllowed[i] = ForeignThreadsAllowedByPool[i].load(std::memory_order_acquire);
        }
    }

    void TSharedExecutorPool::FillOwnedThreads(std::vector<i16>& ownedThreads) const {
        ownedThreads.resize(PoolManager.PoolInfos.size());
        for (ui64 i = 0; i < ownedThreads.size(); ++i) {
            ownedThreads[i] = PoolManager.PoolInfos[i].SharedThreadCount;
        }
    }

    void TSharedExecutorPool::GetSharedStatsForHarmonizer(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const {
        statsCopy.resize(PoolThreads);
        for (i16 i = 0; i < PoolThreads; ++i) {
            Threads[i].Thread->GetSharedStatsForHarmonizer(poolId, statsCopy[i]);
        }
    }

    void TSharedExecutorPool::GetSharedStats(i16 poolId, TVector<TExecutorThreadStats>& statsCopy) const {
        statsCopy.resize(PoolThreads);
        for (i16 i = 0; i < PoolThreads; ++i) {
            Threads[i].Thread->GetSharedStats(poolId, statsCopy[i]);
        }
    }

    void TSharedExecutorPool::SetForeignThreadSlots(i16 poolId, i16 slots) {
        i16 current = ForeignThreadsAllowedByPool[poolId].load(std::memory_order_acquire);
        if (current == slots) {
            return;
        }
        ForeignThreadsAllowedByPool[poolId].store(slots, std::memory_order_release);
        ForeignThreadSlots[poolId].fetch_add(slots - current, std::memory_order_relaxed);
    }

    void TSharedExecutorPool::SetBasicPool(TBasicExecutorPool* pool) {
        Pools[pool->PoolId] = pool;
        pool->MailboxTable = MailboxTable;
    }

    void TSharedExecutorPool::FillThreadOwners(std::vector<i16>& threadOwners) const {
        threadOwners.resize(PoolThreads);
        for (i16 i = 0; i < PoolThreads; ++i) {
            threadOwners[i] = Threads[i].OwnerPoolId;
        }
    }
}
