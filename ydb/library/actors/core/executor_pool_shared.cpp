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

#define POOL_ID() \
    (!TlsThreadContext ? "OUTSIDE" : \
    (TlsThreadContext->IsShared() ? "Shared[" + ToString(TlsThreadContext->OwnerPoolId()) + "]_" + ToString(TlsThreadContext->PoolId()) : \
    ("Pool_" + ToString(TlsThreadContext->PoolId()))))

#define WORKER_ID() ("Worker_" + ToString(TlsThreadContext ? TlsThreadContext->WorkerId() : Max<TWorkerId>()))

#define EXECUTOR_POOL_SHARED_DEBUG(level, ...) \
    ACTORLIB_DEBUG(level, POOL_ID(), " ", WORKER_ID(), " TExecutorPoolShared::", __func__, ": ", __VA_ARGS__)


namespace NActors {

    TPoolManager::TPoolManager(const std::vector<TPoolShortInfo> &poolInfos)
        : PoolInfos(poolInfos)
    {
        PoolThreadRanges.resize(poolInfos.size());
        AdjacentOwnerByPool.resize(poolInfos.size(), -1);
        ui64 totalThreads = 0;
        for (const auto& poolInfo : poolInfos) {
            PoolThreadRanges[poolInfo.PoolId].Begin = totalThreads;
            PoolThreadRanges[poolInfo.PoolId].End = totalThreads + poolInfo.SharedThreadCount;
            totalThreads += poolInfo.SharedThreadCount;
            if (poolInfo.InPriorityOrder) {
                PriorityOrder.push_back(poolInfo.PoolId);
            }
            Y_ABORT_UNLESS(poolInfo.AdjacentPools.empty() || poolInfo.SharedThreadCount > 0,
                "pool %d has adjacent pools but owns no shared threads", poolInfo.PoolId);
            for (i16 adjacentPoolId : poolInfo.AdjacentPools) {
                Y_ABORT_UNLESS(adjacentPoolId >= 0 && static_cast<size_t>(adjacentPoolId) < poolInfos.size(),
                    "invalid adjacent pool %d for owner pool %d", adjacentPoolId, poolInfo.PoolId);
                Y_ABORT_UNLESS(poolInfo.PoolId != adjacentPoolId,
                    "pool %d cannot be adjacent to itself", poolInfo.PoolId);
                i16& adjacentOwner = AdjacentOwnerByPool[adjacentPoolId];
                Y_ABORT_UNLESS(adjacentOwner == -1,
                    "adjacent pool %d has multiple owner pools: %d and %d",
                    adjacentPoolId, adjacentOwner, poolInfo.PoolId);
                adjacentOwner = poolInfo.PoolId;
            }
        }
        Sort(PoolInfos.begin(), PoolInfos.end(), [](const TPoolShortInfo& a, const TPoolShortInfo& b) {
            return a.PoolId < b.PoolId;
        });
    }

    namespace {
        bool CheckPoolAdjacency(const TPoolManager& poolManager, i16 poolId, i16 adjacentPoolId) {
            if (poolId == adjacentPoolId) {
                return true;
            }
            Y_ABORT_UNLESS((ui32)poolId < poolManager.PoolInfos.size());
            const auto& poolInfo = poolManager.PoolInfos[poolId];
            return std::find(poolInfo.AdjacentPools.begin(), poolInfo.AdjacentPools.end(), adjacentPoolId) != poolInfo.AdjacentPools.end();
        }

        bool HasAdjacentPools(const TPoolManager& poolManager, i16 poolId) {
            Y_ABORT_UNLESS((ui32)poolId < poolManager.PoolInfos.size());
            const auto& poolInfo = poolManager.PoolInfos[poolId];
            return !poolInfo.AdjacentPools.empty();
        }

        i16 NextAdjacentPool(const TPoolManager& poolManager, i16 poolId, i16 currentPoolId) {
            if (poolId == currentPoolId) {
                if (poolManager.PoolInfos[poolId].AdjacentPools.empty()) {
                    return poolId;
                }
                return poolManager.PoolInfos[poolId].AdjacentPools[0];
            }
            Y_ABORT_UNLESS((ui32)poolId < poolManager.PoolInfos.size());
            const auto& poolInfo = poolManager.PoolInfos[poolId];
            auto it = std::find(poolInfo.AdjacentPools.begin(), poolInfo.AdjacentPools.end(), currentPoolId);
            if (it == poolInfo.AdjacentPools.end() || it + 1 == poolInfo.AdjacentPools.end()) {
                return poolId;
            }
            return *(it + 1);
        }

        std::optional<i16> GetForcedForeignSlots(const TPoolManager& poolManager, i16 poolId) {
            const auto& poolInfo = poolManager.PoolInfos[poolId];
            if (poolInfo.ForcedForeignSlots) {
                return poolInfo.ForcedForeignSlots;
            }
            return std::nullopt;
        }

    }

    LWTRACE_USING(ACTORLIB_PROVIDER);

    TSharedExecutorPool::TSharedExecutorPool(
        const TSharedExecutorPoolConfig& cfg,
        const std::vector<TPoolShortInfo> &poolInfos
    )   : TExecutorPoolBaseMailboxed(0)
        , PoolThreads(SumThreads(poolInfos))
        , PoolManager(poolInfos)
        , DefaultSpinThresholdCycles(cfg.SpinThreshold * NHPTimer::GetCyclesPerSecond() * 0.000001) // convert microseconds to cycles
        , PoolName("Shared")
        , SoftProcessingDurationTs(cfg.SoftProcessingDurationTs)
        , United(cfg.United)
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
            auto &poolInfo = PoolManager.PoolInfos[i];
            ForeignThreadsAllowedByPool[i].store(poolInfo.ForeignSlots, std::memory_order_release);
            ForeignThreadSlots[i].store(poolInfo.ForeignSlots, std::memory_order_release);
            LocalThreads[i].store(poolInfo.SharedThreadCount, std::memory_order_release);
            LocalNotifications[i].store(0, std::memory_order_release);
        }
        Y_ABORT_UNLESS(passedThreads == static_cast<ui64>(PoolThreads), "Passed threads %" PRIu64 " != PoolThreads %" PRIu64, passedThreads, static_cast<ui64>(PoolThreads));

        Pools.resize(PoolManager.PoolThreadRanges.size());
        SleepingWorkers.resize(PoolThreads, false);
    }

    TSharedExecutorPool::~TSharedExecutorPool() {
        Threads.Destroy();
    }

    i16 TSharedExecutorPool::SumThreads(const std::vector<TPoolShortInfo> &poolInfos) {
        i16 threadCount = 0;
        for (const auto &poolInfo : poolInfos) {
            threadCount += poolInfo.SharedThreadCount;
        }
        return threadCount;
    }

    i16 TSharedExecutorPool::FindPoolForWorker(TSharedExecutorThreadCtx& thread, ui64) {
        for (i16 i : PoolManager.PriorityOrder) {
            if (Pools[i] == nullptr) {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Trace, "pool[", i, "] is nullptr; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }

            bool adj = false;
            if (CheckPoolAdjacency(PoolManager, thread.OwnerPoolId, i)) {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "ownerPoolId == poolId; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i);
                adj = true;
            }

            if (ForeignThreadsAllowedByPool[i].load(std::memory_order_acquire) == 0 && !adj) {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "don't have leases; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }
            ui64 semaphore = Pools[i]->GetSemaphore().OldSemaphore;
            if (semaphore == 0) {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "pool[", i, "]::Semaphore == 0; OwnerPoolId == ", thread.OwnerPoolId);
                continue;
            }

            if (adj) {
                return i;
            }

            ui64 slots = ForeignThreadSlots[i].load(std::memory_order_acquire);

            while (true) {
                if (slots <= 0) {
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "pool[", i, "]::Slots == 0; OwnerPoolId == ", thread.OwnerPoolId);
                    break;
                }
                if (ForeignThreadSlots[i].compare_exchange_strong(slots, slots - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Lease, "found pool; get lease; ownerPoolId == ", thread.OwnerPoolId, " poolId == ", i, " semaphore == ", semaphore, " slots == ", slots, " -> ", slots - 1);
                    return i;
                }
            }
        }
        return -1;
    }

    void TSharedExecutorPool::SwitchToPool(i16 poolId, NHPTimer::STime) {
        TWorkerId workerId = TlsThreadContext->WorkerId();
        TlsThreadContext->ExecutionStats->UpdateThreadTime();
        if (Threads[workerId].CurrentPoolId != poolId && !CheckPoolAdjacency(PoolManager, Threads[workerId].OwnerPoolId, Threads[workerId].CurrentPoolId)) {
            ui64 slots = ForeignThreadSlots[Threads[workerId].CurrentPoolId].fetch_add(1, std::memory_order_acq_rel);
            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Lease, "return lease; ownerPoolId == ", Threads[workerId].OwnerPoolId, " currentPoolId == ", Threads[workerId].CurrentPoolId, " poolId = ", poolId, " slots == ", slots, " -> ", slots + 1);
        }
        Threads[workerId].CurrentPoolId = poolId;
        Threads[workerId].SoftDeadlineForPool = Max<NHPTimer::STime>();
        Threads[workerId].Thread->SwitchPool(Pools[poolId]);
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "end; ownerPoolId == ", Threads[workerId].OwnerPoolId, " currentPoolId == ", Threads[workerId].CurrentPoolId, " poolId = ", poolId);
    }

    TMailbox* TSharedExecutorPool::GetReadyActivation(ui64 revolvingCounter) {
        if (HasWakerPools) {
            return GetReadyActivationWaker(revolvingCounter);
        }
        Y_UNUSED(SoftProcessingDurationTs);
        NHPTimer::STime hpnow = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard(hpnow);

        TWorkerId workerId = TlsThreadContext->WorkerId();
        Y_DEBUG_ABORT_UNLESS(workerId < PoolThreads);
        auto &thread = Threads[workerId];
        thread.UnsetWork();
        TMailbox *mailbox = nullptr;
        bool hasAdjacentPools = HasAdjacentPools(PoolManager, thread.OwnerPoolId);
        while (!StopFlag.load(std::memory_order_acquire)) {
            bool adjacentPool = CheckPoolAdjacency(PoolManager, thread.OwnerPoolId, thread.CurrentPoolId);
            if (hpnow < thread.SoftDeadlineForPool || !hasAdjacentPools && adjacentPool) {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "continue same pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                if (thread.SoftDeadlineForPool == Max<NHPTimer::STime>()) {
                    thread.SoftDeadlineForPool = GetCycleCountFast() + thread.SoftProcessingDurationTs;
                }
                mailbox = Pools[thread.CurrentPoolId]->GetReadyActivationShared(revolvingCounter);
                if (mailbox) {
                    TlsThreadContext->ProcessedActivationsByCurrentPool++;
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "found mailbox; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    return mailbox;
                } else if (!TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
                    TlsThreadContext->ProcessedActivationsByCurrentPool++;
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "no mailbox and no need to wait; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    return nullptr;
                } else {
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "no mailbox and need to find new pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " processedActivationsByCurrentPool == ", TlsThreadContext->ProcessedActivationsByCurrentPool);
                    TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                    if (!adjacentPool) {
                        thread.AdjacentPoolId = NextAdjacentPool(PoolManager, thread.OwnerPoolId, thread.AdjacentPoolId);
                        SwitchToPool(thread.AdjacentPoolId, hpnow);
                        continue;
                    }
                }
            } else if (!TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
                TlsThreadContext->ProcessedActivationsByCurrentPool++;
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "no mailbox and no need to wait; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                return nullptr;
            } else {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "change adjacent pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " processedActivationsByCurrentPool == ", TlsThreadContext->ProcessedActivationsByCurrentPool, " hpnow == ", hpnow, " softDeadlineForPool == ", thread.SoftDeadlineForPool);
                TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                thread.AdjacentPoolId = NextAdjacentPool(PoolManager, thread.OwnerPoolId, thread.AdjacentPoolId);
                SwitchToPool(thread.AdjacentPoolId, hpnow);
                // after soft deadline we check adjacent pool
                continue;
            }
            bool goToSleep = true;
            for (i16 attempt = 0; attempt < 2; ++attempt) {
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "attempt == ", attempt, " ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                if (i16 poolId = FindPoolForWorker(thread, revolvingCounter); poolId != -1) {
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "found pool; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    SwitchToPool(poolId, hpnow);
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
                            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "checked global state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " notifications == ", threadsState.Notifications, " workingThreadCount: ", threadsState.WorkingThreadCount + 1, " -> ", threadsState.WorkingThreadCount);
                            break;
                        }
                    } else {
                        threadsState.Notifications--;
                        if (ThreadsState.compare_exchange_strong(threadsStateRaw, threadsState.ConvertToUI64(), std::memory_order_acq_rel, std::memory_order_acquire)) {
                            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "checked global state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " workingThreadCount == ", threadsState.WorkingThreadCount, " notifications: ", threadsState.Notifications + 1, " -> ", threadsState.Notifications);
                            break;
                        }
                    }
                    threadsState = TThreadsState::GetThreadsState(threadsStateRaw);
                }
                if (allowedToSleep) {
                    ui64 localNotifications = LocalNotifications[thread.OwnerPoolId].load(std::memory_order_acquire);
                    while (true) {
                        if (localNotifications == 0) {
                            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "checked local state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " localNotifications == 0");
                            break;
                        }
                        if (LocalNotifications[thread.OwnerPoolId].compare_exchange_strong(localNotifications, localNotifications - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                            allowedToSleep = false;
                            ThreadsState.fetch_add(1, std::memory_order_acq_rel);
                            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "checked local state; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId, " localNotifications: ", localNotifications, " -> ", localNotifications - 1);
                            break;
                        }
                    }
                }
                if (allowedToSleep) {
                    SwitchToPool(thread.OwnerPoolId, hpnow); // already switched, needless
                    LocalThreads[thread.OwnerPoolId].fetch_sub(1, std::memory_order_acq_rel);
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "going to sleep; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
                    Y_DEBUG_ABORT_UNLESS(thread.OwnerPoolId < static_cast<i16>(Pools.size()), "thread.OwnerPoolId == ", thread.OwnerPoolId, " Pools.size() == ", Pools.size());
                    Y_DEBUG_ABORT_UNLESS(Pools[thread.OwnerPoolId] != nullptr, "Pools[thread.OwnerPoolId] is nullptr");
                    if (thread.Wait(Pools[thread.OwnerPoolId]->SpinThresholdCycles.load(std::memory_order_relaxed), &StopFlag, &LocalNotifications[thread.OwnerPoolId], &ThreadsState)) {
                        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "interrupted; ownerPoolId == ", thread.OwnerPoolId, " currentPoolId == ", thread.CurrentPoolId);
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

    TMailbox* TSharedExecutorPool::GetReadyActivationWaker(ui64 revolvingCounter) {
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_GET_ACTIVATION, false> activityGuard;
        const TWorkerId workerId = TlsThreadContext->WorkerId();
        auto& thread = Threads[workerId];
        thread.UnsetWorkForWaker();
        if (!TlsThreadContext->ExecutionContext.IsNeededToWaitNextActivation) {
            // The executor already owns a captured mailbox. Finish it before
            // honoring Blocking, without switching away from its pool.
            thread.SetWorkForWaker();
            return nullptr;
        }
        ui32 searchIterations = 0;
        bool announcedSpin = false;

        while (!StopFlag.load(std::memory_order_acquire)) {
            EThreadState state = thread.GetState<EThreadState>();
            if (IsNeedToBeWaker(state)) {
                RunWaker(workerId);
                continue;
            }
            if (state == EThreadState::Blocking) {
                // Return a foreign lease before parking. Reconsider demand now
                // that another worker can acquire that slot.
                const bool foreign = !CheckPoolAdjacency(PoolManager, thread.OwnerPoolId, thread.CurrentPoolId);
                SwitchToPool(thread.OwnerPoolId, GetCycleCountFast());
                if (foreign) {
                    RequestWaker(workerId);
                }
                if (thread.ParkForWaker(StopFlag)) {
                    return nullptr;
                }
                searchIterations = 0;
                announcedSpin = false;
                continue;
            }
            Y_DEBUG_ABORT_UNLESS(state == EThreadState::None || state == EThreadState::Spin);
            if (announcedSpin && state == EThreadState::None) {
                searchIterations = 0;
                announcedSpin = false;
            }

            const auto tryPool = [&](i16 poolId) -> TMailbox* {
                EThreadState currentState = thread.GetState<EThreadState>();
                if (currentState != EThreadState::None && currentState != EThreadState::Spin) {
                    return nullptr;
                }
                if (poolId != thread.CurrentPoolId) {
                    const bool adjacent = CheckPoolAdjacency(PoolManager, thread.OwnerPoolId, poolId);
                    if (!adjacent) {
                        if (ForeignThreadsAllowedByPool[poolId].load(std::memory_order_acquire) == 0) {
                            return nullptr;
                        }
                        ui64 slots = ForeignThreadSlots[poolId].load(std::memory_order_acquire);
                        while (slots != 0 && !ForeignThreadSlots[poolId].compare_exchange_weak(
                                slots, slots - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                        }
                        if (slots == 0) {
                            return nullptr;
                        }
                    }
                    TlsThreadContext->ProcessedActivationsByCurrentPool = 0;
                    SwitchToPool(poolId, GetCycleCountFast());
                }
                if (thread.SoftDeadlineForPool == Max<NHPTimer::STime>()) {
                    thread.SoftDeadlineForPool = GetCycleCountFast() + thread.SoftProcessingDurationTs;
                }
                return Pools[poolId]->GetReadyActivationShared(revolvingCounter++);
            };

            // Preserve the current pool's time slice, then inspect the other
            // pools. A failed dequeue cannot monopolize a search iteration.
            const NHPTimer::STime hpnow = GetCycleCountFast();
            i16 preferredPoolId = thread.CurrentPoolId;
            if (hpnow >= thread.SoftDeadlineForPool &&
                    (HasAdjacentPools(PoolManager, thread.OwnerPoolId) ||
                     !CheckPoolAdjacency(PoolManager, thread.OwnerPoolId, thread.CurrentPoolId))) {
                thread.AdjacentPoolId = NextAdjacentPool(PoolManager, thread.OwnerPoolId, thread.AdjacentPoolId);
                preferredPoolId = thread.AdjacentPoolId;
            }
            if (auto* mailbox = tryPool(preferredPoolId)) {
                ++TlsThreadContext->ProcessedActivationsByCurrentPool;
                return mailbox;
            }
            for (i16 poolId : PoolManager.PriorityOrder) {
                if (!Pools[poolId] || Pools[poolId]->GetSemaphore().OldSemaphore == 0) {
                    continue;
                }
                if (auto* mailbox = tryPool(poolId)) {
                    ++TlsThreadContext->ProcessedActivationsByCurrentPool;
                    return mailbox;
                }
            }
            if (!announcedSpin && ++searchIterations >= 2) {
                state = EThreadState::None;
                if (thread.ReplaceState(state, EThreadState::Spin)) {
                    announcedSpin = true;
                    RequestWaker(workerId);
                }
            }
            SpinLockPause();
        }
        return nullptr;
    }

    void TSharedExecutorPool::ScheduleActivationEx(TMailbox*, ui64) {
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

        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::ExecutorPool, "TSharedExecutorPool::Prepare: start");

        ActorSystem = actorSystem;

        ScheduleReaders.Reset(new NSchedulerQueue::TReader[PoolThreads]);
        ScheduleWriters.Reset(new NSchedulerQueue::TWriter[PoolThreads]);


        for (i16 i = 0; i != PoolThreads; ++i) {
            Y_ABORT_UNLESS(Pools[Threads[i].OwnerPoolId] != nullptr, "Pool is nullptr i %" PRIu16, i);
            Y_ABORT_UNLESS(Threads[i].OwnerPoolId < static_cast<i16>(Pools.size()), "OwnerPoolId is out of range i %" PRIu16 " OwnerPoolId == %" PRIu16, i, Threads[i].OwnerPoolId);
            Y_ABORT_UNLESS(Threads[i].OwnerPoolId >= 0, "OwnerPoolId is out of range i %" PRIu16 " OwnerPoolId == %" PRIu16, i, Threads[i].OwnerPoolId);
            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::ExecutorPool, "create thread ", i, " OwnerPoolId == ", Threads[i].OwnerPoolId);
            Threads[i].Thread.reset(
                new TExecutorThread(
                    i,
                    actorSystem,
                    this,
                    Pools[Threads[i].OwnerPoolId],
                    static_cast<i16>(Pools.size()),
                    PoolName,
                    SoftProcessingDurationTs
                    ));
            ScheduleWriters[i].Init(ScheduleReaders[i]);
        }

        *scheduleReaders = ScheduleReaders.Get();
        *scheduleSz = PoolThreads;

        //Sanitizer = std::make_unique<TSharedExecutorPoolSanitizer>(this);
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::ExecutorPool, "end");
    }

    void TSharedExecutorPool::Start() {
        TAffinityGuard affinityGuard(Affinity());

        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::ExecutorPool, "start");

        for (i16 i = 0; i != PoolThreads; ++i) {
            Y_ABORT_UNLESS(Threads[i].Thread != nullptr, "Thread is nullptr i %" PRIu16, i);
            Threads[i].Thread->Start();
        }
        if (Sanitizer) {
            Sanitizer->Start();
        }
    }

    void TSharedExecutorPool::PrepareStop() {
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::ExecutorPool, "prepare stop");
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
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::ExecutorPool, "shutdown");
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

    i16 TSharedExecutorPool::GetMaxFullThreadCount() const {
        return PoolThreads;
    }

    i16 TSharedExecutorPool::GetMinFullThreadCount() const {
        return PoolThreads;
    }

    ui32 TSharedExecutorPool::GetThreads() const {
        return PoolThreads;
    }

    void TSharedExecutorPool::Initialize() {
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "start");
        TWorkerId workerId = TlsThreadContext->WorkerId();
        Threads[workerId].Thread->SwitchPool(Pools[Threads[workerId].OwnerPoolId]);
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "end");
    }

    bool TSharedExecutorPool::WakeUpLocalThreads(i16 ownerPoolId) {
        if (HasWakerPools) {
            RequestWaker();
            return LocalThreads[ownerPoolId].load(std::memory_order_acquire) <
                static_cast<ui64>(PoolManager.PoolInfos[ownerPoolId].SharedThreadCount);
        }
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
                EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "wakeup from own pool; ownerPoolId == ", ownerPoolId, " threadId == ", threadId);
                break;
            }
        }
        return true;
    }

    bool TSharedExecutorPool::WakeUpAdjacentOwner(i16 poolId) {
        Y_ABORT_UNLESS(poolId >= 0 && static_cast<size_t>(poolId) < PoolManager.AdjacentOwnerByPool.size());
        const i16 adjacentOwnerPoolId = PoolManager.AdjacentOwnerByPool[poolId];
        if (adjacentOwnerPoolId == -1) {
            return false;
        }
        if (WakeUpLocalThreads(adjacentOwnerPoolId)) {
            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "wakeup from adjacent pool owner; poolId == ", poolId, " adjacentOwnerPoolId == ", adjacentOwnerPoolId);
            return true;
        }
        return false;
    }

    bool TSharedExecutorPool::WakeUpGlobalThreads(i16 ownerPoolId) {
        if (HasWakerPools) {
            RequestWaker();
            return ForeignThreadsAllowedByPool[ownerPoolId].load(std::memory_order_acquire) != 0 &&
                ForeignThreadSlots[ownerPoolId].load(std::memory_order_acquire) != 0 &&
                SharedSleepingCount.load(std::memory_order_acquire) > 0;
        }
        if (ForeignThreadsAllowedByPool[ownerPoolId].load(std::memory_order_acquire) == 0) {
            return false;
        }
        ui64 slots = ForeignThreadSlots[ownerPoolId].load(std::memory_order_acquire);
        if (slots <= 0) {
            return false;
        }

        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "ownerPoolId == ", ownerPoolId);
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
            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "increase notifications; ", threadsState.Notifications - 1, " -> ", threadsState.Notifications);
        }
        if (!allowedToWakeUp) {
            EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "no free threads");
            return false;
        }

        for (i16 i = static_cast<i16>(PoolManager.PriorityOrder.size()) - 1; i >= 0; --i) {
            i16 poolId = PoolManager.PriorityOrder[i];
            for (i16 threadId = PoolManager.PoolThreadRanges[poolId].Begin; threadId < PoolManager.PoolThreadRanges[poolId].End; ++threadId) {
                auto &thread = Threads[threadId];
                if (thread.WakeUp()) {
                    EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Executor, "wakeup from other pool; ownerPoolId == ", ownerPoolId, " poolId == ", poolId, " threadId == ", threadId);
                    return true;
                }
            }
        }
        EXECUTOR_POOL_SHARED_DEBUG(EDebugLevel::Activation, "no threads");
        return false;
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
        if (auto forcedSlots = GetForcedForeignSlots(PoolManager, poolId)) {
            return;
        }
        i16 current = ForeignThreadsAllowedByPool[poolId].load(std::memory_order_acquire);
        if (current == slots) {
            return;
        }
        ForeignThreadsAllowedByPool[poolId].store(slots, std::memory_order_release);
        ForeignThreadSlots[poolId].fetch_add(slots - current, std::memory_order_relaxed);
    }

    void TSharedExecutorPool::SetBasicPool(TBasicExecutorPool* pool) {
        Pools[pool->PoolId] = pool;
        HasWakerPools |= pool->EnableWaker;
    }

    void TSharedExecutorPool::RequestWaker(i16 workerId) {
        if (PoolThreads == 0) {
            return;
        }
        const bool wasPending = WakerPending.exchange(true, std::memory_order_acq_rel);
        if (wasPending && workerId == InvalidWakerWorkerId) {
            return;
        }
        // Pair with owner release: either that CAS acquires this request or
        // we observe the released role and nominate a worker ourselves.
        if (WakerWorkerId.fetch_add(0, std::memory_order_acq_rel) != InvalidWakerWorkerId) {
            return;
        }
        if (workerId != InvalidWakerWorkerId && Threads[workerId].TrySetNeedToBeWaker()) {
            return;
        }
        for (i16 i = 0; i < PoolThreads; ++i) {
            EThreadState state = Threads[i].GetState<EThreadState>();
            while (true) {
                if (IsNeedToBeWaker(state) || state == EThreadState::Waker) {
                    return;
                }
                if (state != EThreadState::Spin && state != EThreadState::Blocking) {
                    break;
                }
                const EThreadState previousState = state;
                if (Threads[i].TrySetNeedToBeWaker(&state)) {
                    if (previousState == EThreadState::Blocking) {
                        Threads[i].WaitingPad.Unpark();
                    }
                    return;
                }
            }
        }
        // If every worker is busy, the next worker announcing Spin will claim
        // the pending request before it can be put to sleep.
    }

    void TSharedExecutorPool::RunWaker(TWorkerId workerId) {
        EThreadState resumeState = EThreadState::None;
        bool hasResumeState = false;

        while (!StopFlag.load(std::memory_order_acquire)) {
            const EThreadState state = Threads[workerId].GetState<EThreadState>();
            if (IsNeedToBeWaker(state)) {
                const i16 owner = WakerWorkerId.load(std::memory_order_acquire);
                if (owner == InvalidWakerWorkerId) {
                    i16 expected = InvalidWakerWorkerId;
                    if (!WakerWorkerId.compare_exchange_weak(expected, workerId,
                            std::memory_order_acq_rel, std::memory_order_acquire)) {
                        continue;
                    }
                    Y_ABORT_UNLESS(Threads[workerId].TryBecomeWaker(&resumeState));
                    hasResumeState = true;
                    continue;
                }
                if (owner == workerId) {
                    Y_ABORT_UNLESS(Threads[workerId].TryBecomeWaker(&resumeState));
                    hasResumeState = true;
                    continue;
                }
                Threads[workerId].CancelWakerRequest();
                return;
            }

            if (state != EThreadState::Waker) {
                return;
            }

            Y_ABORT_UNLESS(hasResumeState);
            Y_ABORT_UNLESS(WakerWorkerId.load(std::memory_order_acquire) == workerId);
            WakerLoop(workerId, &resumeState);

            if (WakerPending.load(std::memory_order_acquire)) {
                continue;
            }

            EThreadState finalState = resumeState;
            EThreadState expectedState = EThreadState::Waker;
            Y_ABORT_UNLESS(Threads[workerId].ReplaceState(expectedState, finalState));

            if (WakerPending.load(std::memory_order_acquire)) {
                // A delayed requester may already have nominated this worker
                // after Waker -> finalState. Both states resume the same owner.
                Y_ABORT_UNLESS(Threads[workerId].TryBecomeWaker(&resumeState));
                continue;
            }

            i16 expectedOwner = workerId;
            Y_ABORT_UNLESS(WakerWorkerId.compare_exchange_strong(expectedOwner, InvalidWakerWorkerId,
                std::memory_order_acq_rel, std::memory_order_acquire));

            if (WakerPending.load(std::memory_order_acquire)) {
                EThreadState currentState = Threads[workerId].GetState<EThreadState>();
                if (IsNeedToBeWaker(currentState) || currentState == EThreadState::Waker) {
                    continue;
                }
                if (currentState != EThreadState::Work &&
                        Threads[workerId].TrySetNeedToBeWaker(&currentState)) {
                    continue;
                }
            }
            return;
        }
    }

    void TSharedExecutorPool::SetSleeping(TWorkerId workerId, bool sleeping) {
        if (SleepingWorkers[workerId] == sleeping) {
            return;
        }
        SleepingWorkers[workerId] = sleeping;
        const i16 ownerPoolId = Threads[workerId].OwnerPoolId;
        if (sleeping) {
            LocalThreads[ownerPoolId].fetch_sub(1, std::memory_order_acq_rel);
            ThreadsState.fetch_sub(1, std::memory_order_acq_rel);
            SharedSleepingCount.fetch_add(1, std::memory_order_acq_rel);
        } else {
            LocalThreads[ownerPoolId].fetch_add(1, std::memory_order_acq_rel);
            ThreadsState.fetch_add(1, std::memory_order_acq_rel);
            SharedSleepingCount.fetch_sub(1, std::memory_order_acq_rel);
        }
    }

    void TSharedExecutorPool::WakerLoop(TWorkerId wakerWorkerId, EThreadState* resumeState) {
        const auto logicalState = [](EThreadState state) {
            switch (state) {
                case EThreadState::NeedToBeWaker:
                    return EThreadState::None;
                case EThreadState::NeedToBeWakerFromSpin:
                    return EThreadState::Spin;
                case EThreadState::NeedToBeWakerFromBlocking:
                    return EThreadState::Blocking;
                default:
                    return state;
            }
        };
        WakerPending.exchange(false, std::memory_order_acq_rel);
        TStackVec<i64, 8> creditsByPool(Pools.size(), 0);
        TStackVec<bool, 64> selected(PoolThreads, false);
        for (i16 poolId : PoolManager.PriorityOrder) {
            auto* pool = Pools[poolId];
            if (!pool) {
                continue;
            }
            const i64 credits = pool->GetSemaphore().OldSemaphore;
            creditsByPool[poolId] = credits;
            i64 budget = pool->EnableWaker && pool->GetFullThreadCount() != 0
                ? Min<i64>(credits, pool->DesiredSharedThreads.load(std::memory_order_acquire))
                : Min<i64>(credits, PoolThreads);
            ui64 foreignSlots = ForeignThreadsAllowedByPool[poolId].load(std::memory_order_acquire) != 0
                ? ForeignThreadSlots[poolId].load(std::memory_order_acquire) : 0;
            // Prefer workers already searching, then workers assigned to sleep.
            // Assign each worker to at most one pool in this pass.
            for (ui32 sleeping = 0; sleeping < 2 && budget > 0; ++sleeping) {
                for (i16 workerId = 0; workerId < PoolThreads && budget > 0; ++workerId) {
                    if (selected[workerId] || SleepingWorkers[workerId] != bool(sleeping)) {
                        continue;
                    }
                    const bool adjacent = CheckPoolAdjacency(PoolManager, Threads[workerId].OwnerPoolId, poolId);
                    if (!adjacent && foreignSlots == 0) {
                        continue;
                    }
                    EThreadState state = workerId == wakerWorkerId
                        ? *resumeState : Threads[workerId].GetState<EThreadState>();
                    const EThreadState currentState = logicalState(state);
                    if (currentState != EThreadState::None && currentState != EThreadState::Spin && currentState != EThreadState::Blocking) {
                        continue;
                    }
                    if (workerId == wakerWorkerId) {
                        *resumeState = EThreadState::None;
                    } else {
                        bool changed = false;
                        while (logicalState(state) == EThreadState::None ||
                                logicalState(state) == EThreadState::Spin ||
                                logicalState(state) == EThreadState::Blocking) {
                            if (Threads[workerId].ReplaceState(state, EThreadState::None)) {
                                changed = true;
                                break;
                            }
                        }
                        if (!changed) {
                            continue;
                        }
                    }
                    const bool wasSleeping = SleepingWorkers[workerId];
                    SetSleeping(workerId, false);
                    if (wasSleeping && workerId != wakerWorkerId) {
                        Threads[workerId].WaitingPad.Unpark();
                    }
                    selected[workerId] = true;
                    --budget;
                    if (!adjacent) {
                        --foreignSlots;
                    }
                }
            }
        }
        for (i16 workerId = 0; workerId < PoolThreads; ++workerId) {
            if (selected[workerId]) {
                continue;
            }
            if (workerId == wakerWorkerId) {
                if (*resumeState == EThreadState::Spin) {
                    *resumeState = EThreadState::Blocking;
                    SetSleeping(workerId, true);
                }
            } else {
                EThreadState state = Threads[workerId].GetState<EThreadState>();
                while (logicalState(state) == EThreadState::Spin) {
                    if (Threads[workerId].ReplaceState(state, EThreadState::Blocking)) {
                        SetSleeping(workerId, true);
                        break;
                    }
                }
            }
        }
        // Publish sleep decisions before rechecking credits. This RMW pairs
        // with producers: either we see their new demand or they see sleepers.
        for (i16 poolId : PoolManager.PriorityOrder) {
            auto* pool = Pools[poolId];
            if (pool && pool->EnableWaker &&
                    pool->ActivationCredits.fetch_add(0, std::memory_order_acq_rel) > creditsByPool[poolId]) {
                WakerPending.store(true, std::memory_order_release);
            }
        }
    }

    void TSharedExecutorPool::FillThreadOwners(std::vector<i16>& threadOwners) const {
        threadOwners.resize(PoolThreads);
        for (i16 i = 0; i < PoolThreads; ++i) {
            threadOwners[i] = Threads[i].OwnerPoolId;
        }
    }

    i16 TSharedExecutorPool::GetSharedThreadCount() const {
        return PoolThreads;
    }

    ui64 TSharedExecutorPool::TimePerMailboxTs() const {
        Y_ABORT("TSharedExecutorPool::TimePerMailboxTs is not allowed");
        return 0;
    }

    ui32 TSharedExecutorPool::EventsPerMailbox() const {
        Y_ABORT("TSharedExecutorPool::EventsPerMailbox is not allowed");
        return 0;
    }
}
