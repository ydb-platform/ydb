#include "harmonizer.h"
#include "history.h"
#include "pool.h"
#include "waiting_stats.h"
#include "cpu_consumption.h"
#include "shared_info.h"
#include "debug.h"
#include <ydb/library/actors/core/executor_pool.h>

#include <ydb/library/actors/core/executor_thread_ctx.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/probes.h>

#include <ydb/library/actors/core/activity_guard.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_basic_feature_flags.h>
#include <ydb/library/actors/core/executor_pool_shared.h>

#include <atomic>
#include <ydb/library/actors/util/cpu_load_log.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/intrinsics.h>

#include <util/system/spinlock.h>

#include <algorithm>

namespace NActors {

constexpr float kMinFreeCpuThreshold = 0.1;

LWTRACE_USING(ACTORLIB_PROVIDER);


constexpr size_t SAVED_ITERATION_COUNT = 60; // 1 minute


class THarmonizer: public IHarmonizer {
private:
    std::atomic<ui64> Iteration = 0;
    std::atomic<bool> IsDisabled = false;
    TSpinLock Lock;
    std::atomic<ui64> NextHarmonizeTs = 0;
    std::vector<std::unique_ptr<TPoolInfo>> Pools;
    std::vector<ui16> PriorityOrder;

    TValueHistory<16> UsedCpu;
    TValueHistory<16> ElapsedCpu;

    std::atomic<float> MaxUsedCpu = 0;
    std::atomic<float> MinUsedCpu = 0;
    std::atomic<float> MaxElapsedCpu = 0;
    std::atomic<float> MinElapsedCpu = 0;

    ISharedPool* Shared = nullptr;
    TSharedInfo SharedInfo;

    TWaitingInfo WaitingInfo;
    THarmonizerCpuConsumption CpuConsumption;
    THarmonizerStats Stats;
    float ProcessingBudget = 0.0;

    std::vector<THarmonizerIterationThreadState> IterationThreadState;
    std::vector<THarmonizerPersistentPoolState> PersistentPoolState;
    std::vector<THarmonizerIterationPoolState> IterationPoolState;
    std::unique_ptr<THarmonizerPersistentSharedPoolState> PersistentSharedPoolState;
    std::vector<THarmonizerIterationSharedThreadState> IterationSharedThreadState;
    std::vector<THarmonizerIterationState> IterationState;

    void PullStats(ui64 ts);
    void PullSharedInfo();
    void ProcessWaitingStats();
    void HarmonizeImpl(ui64 ts);
    void CalculatePriorityOrder();
    void ProcessStarvedState();
    void ProcessNeedyState();
    void ProcessExchange();
    void ProcessHoggishState();

    void InitIterationState();
public:
    THarmonizer(ui64 ts);
    virtual ~THarmonizer();
    double Rescale(double value) const;
    void Harmonize(ui64 ts) override;
    void DeclareEmergency(ui64 ts) override;
    void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo, bool ignoreFullThreadQuota = false) override;
    void Enable(bool enable) override;
    TPoolHarmonizerStats GetPoolStats(i16 poolId) const override;
    THarmonizerStats GetStats() const override;
    void SetSharedPool(ISharedPool* pool) override;
    bool InvokeReadHistory(TIterationViewerCallback callback) override;
    THarmonizerIterationState& GetIterationState(ui64 iteration);
    THarmonizerIterationPoolState& GetIterationPoolState(ui64 iteration, ui16 poolIdx);
    THarmonizerIterationThreadState& GetIterationThreadState(ui64 iteration, ui16 poolIdx, ui16 threadIdx);
};

THarmonizer::THarmonizer(ui64 ts) {
    NextHarmonizeTs = ts;
}

THarmonizer::~THarmonizer() {
}

THarmonizerIterationState& THarmonizer::GetIterationState(ui64 iteration) {
    return IterationState[iteration % SAVED_ITERATION_COUNT];
}

THarmonizerIterationPoolState& THarmonizer::GetIterationPoolState(ui64 iteration, ui16 poolIdx) {
    return IterationState[iteration % SAVED_ITERATION_COUNT].Pools[poolIdx];
}

THarmonizerIterationThreadState& THarmonizer::GetIterationThreadState(ui64 iteration, ui16 poolIdx, ui16 threadIdx) {
    return IterationState[iteration % SAVED_ITERATION_COUNT].Pools[poolIdx].Threads[threadIdx];
}

void THarmonizer::PullStats(ui64 ts) {
    HARMONIZER_DEBUG_PRINT("PullStats");
    TCpuConsumption acc;
    for (auto &pool : Pools) {
        TCpuConsumption consumption = pool->PullStats(ts);
        acc.Add(consumption);
    }
    UsedCpu.Register(ts, acc.CpuUs / 1'000'000.0);
    MaxUsedCpu.store(UsedCpu.GetMax(), std::memory_order_relaxed);
    MinUsedCpu.store(UsedCpu.GetMin(), std::memory_order_relaxed);
    ElapsedCpu.Register(ts, acc.ElapsedUs / 1'000'000.0);
    MaxElapsedCpu.store(ElapsedCpu.GetMax(), std::memory_order_relaxed);
    MinElapsedCpu.store(ElapsedCpu.GetMin(), std::memory_order_relaxed);

    WaitingInfo.Pull(Pools);
    if (Shared) {
        SharedInfo.Pull(Pools, *Shared);
    }
    CpuConsumption.Pull(Pools, SharedInfo);
    ProcessingBudget = CpuConsumption.BudgetWithoutSharedCpu;
}

void THarmonizer::ProcessWaitingStats() {
    HARMONIZER_DEBUG_PRINT("ProcessWaitingStats");
    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = *Pools[poolIdx];
        if (!pool.BasicPool) {
            continue;
        }
        if (pool.BasicPool->ActorSystemProfile != EASProfile::Default) {
            if constexpr (NFeatures::TSpinFeatureFlags::CalcPerThread) {
                pool.BasicPool->CalcSpinPerThread(WaitingInfo.AvgWakingUpTimeUs);
            } else if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                ui64 newSpinThreshold = pool.MovingWaitingStats->CalculateGoodSpinThresholdCycles(WaitingInfo.AvgWakingUpTimeUs);
                pool.BasicPool->SetSpinThresholdCycles(newSpinThreshold);
            } else {
                ui64 newSpinThreshold = pool.WaitingStats->CalculateGoodSpinThresholdCycles(WaitingInfo.AvgWakingUpTimeUs);
                pool.BasicPool->SetSpinThresholdCycles(newSpinThreshold);
            }
            pool.BasicPool->ClearWaitingStats();
        }
    }
}

void THarmonizer::ProcessStarvedState() {
    HARMONIZER_DEBUG_PRINT("ProcessStarvedState", "PriorityOrderLength", PriorityOrder.size());
    for (ui16 poolIdx : PriorityOrder) {
        TPoolInfo &pool = *Pools[poolIdx];
        i64 threadCount = pool.GetFullThreadCount();
        i64 oldThreadCount = threadCount;
        HARMONIZER_DEBUG_PRINT("poolIdx", poolIdx, "threadCount", threadCount, "pool.DefaultFullThreadCount", pool.DefaultFullThreadCount);
        while (threadCount > pool.DefaultFullThreadCount) {
            pool.SetFullThreadCount(--threadCount);
            pool.DecreasingThreadsByStarvedState.fetch_add(1, std::memory_order_relaxed);
            CpuConsumption.AdditionalThreads--;
            CpuConsumption.StoppingThreads++;

            LWPROBE_WITH_DEBUG(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by starving", threadCount - 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
            if (CpuConsumption.Overbooked <= CpuConsumption.StoppingThreads) {
                break;
            }
        }
        if (oldThreadCount != threadCount) {
            auto &iterationPoolState = GetIterationPoolState(Iteration.load(std::memory_order_relaxed) - 1, poolIdx);
            iterationPoolState.Operation.Type = THarmonizerIterationOperation::DecreaseThreadByStarvedState{static_cast<i16>(oldThreadCount - threadCount)};
        }
        HARMONIZER_DEBUG_PRINT("check overbooked and stopping threads", "poolIdx", poolIdx, "CpuConsumption.Overbooked", CpuConsumption.Overbooked, "CpuConsumption.StoppingThreads", CpuConsumption.StoppingThreads);
        if (CpuConsumption.Overbooked <= CpuConsumption.StoppingThreads) {
            break;
        }
    }
}

void THarmonizer::ProcessNeedyState() {
    HARMONIZER_DEBUG_PRINT("ProcessNeedyState");
    if (CpuConsumption.NeedyPools.empty()) {
        HARMONIZER_DEBUG_PRINT("No needy pools");
        return;
    }
    for (size_t needyPoolIdx : CpuConsumption.NeedyPools) {
        TPoolInfo &pool = *Pools[needyPoolIdx];
        if (!CpuConsumption.IsNeedyByPool[needyPoolIdx]) {
            continue;
        }
        float threadCount = pool.GetFullThreadCount() + SharedInfo.CpuConsumption[needyPoolIdx].CpuQuota;

        if (ProcessingBudget >= 1.0 && threadCount + 1 <= pool.MaxFullThreadCount && SharedInfo.FreeCpu < kMinFreeCpuThreshold) {
            pool.IncreasingThreadsByNeedyState.fetch_add(1, std::memory_order_relaxed);
            CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
            CpuConsumption.AdditionalThreads++;
            pool.SetFullThreadCount(threadCount + 1);
            if (Shared) {
                bool hasOwnSharedThread = SharedInfo.OwnedThreads[needyPoolIdx] != -1;
                i16 slots = pool.MaxThreadCount - threadCount - 1 - hasOwnSharedThread;
                i16 maxSlots = Shared->GetSharedThreadCount();
                Shared->SetForeignThreadSlots(needyPoolIdx, Min<i16>(slots, maxSlots));
            }
            ProcessingBudget -= 1.0;
            LWPROBE_WITH_DEBUG(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by needs", threadCount + 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
            auto &iterationPoolState = GetIterationPoolState(Iteration.load(std::memory_order_relaxed) - 1, needyPoolIdx);
            iterationPoolState.Operation.Type = THarmonizerIterationOperation::IncreaseThreadByNeedyState{};
        }
        if (pool.MaxLocalQueueSize) {
            bool needToExpandLocalQueue = ProcessingBudget < 1.0 || threadCount >= pool.MaxFullThreadCount;
            needToExpandLocalQueue &= (bool)pool.BasicPool;
            needToExpandLocalQueue &= (pool.MaxFullThreadCount > 1);
            needToExpandLocalQueue &= (pool.LocalQueueSize < pool.MaxLocalQueueSize);
            if (needToExpandLocalQueue) {
                pool.BasicPool->SetLocalQueueSize(++pool.LocalQueueSize);
            }
        }
    }
}

void THarmonizer::ProcessExchange() {
    HARMONIZER_DEBUG_PRINT("ProcessExchange");
    if (CpuConsumption.NeedyPools.empty()) {
        HARMONIZER_DEBUG_PRINT("No needy pools");
        return;
    }
    size_t takingAwayThreads = 0;
    size_t sumOfAdditionalThreads = CpuConsumption.AdditionalThreads;
    for (size_t needyPoolIdx : CpuConsumption.NeedyPools) {
        TPoolInfo &pool = *Pools[needyPoolIdx];
        i64 fullThreadCount = pool.GetFullThreadCount();
        float threadCount = fullThreadCount + SharedInfo.CpuConsumption[needyPoolIdx].CpuQuota;

        sumOfAdditionalThreads -= fullThreadCount - pool.DefaultFullThreadCount;
        if (sumOfAdditionalThreads < takingAwayThreads + 1) {
            break;
        }

        if (threadCount + 1 > pool.MaxFullThreadCount) {
            continue;
        }

        if (!CpuConsumption.IsNeedyByPool[needyPoolIdx]) {
            continue;
        }

        pool.IncreasingThreadsByExchange.fetch_add(1, std::memory_order_relaxed);
        CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
        takingAwayThreads++;
        pool.SetFullThreadCount(fullThreadCount + 1);
        auto &iterationPoolState = GetIterationPoolState(Iteration.load(std::memory_order_relaxed) - 1, needyPoolIdx);
        iterationPoolState.Operation.Type = THarmonizerIterationOperation::IncreaseThreadByExchange{};
        if (Shared) {
            bool hasOwnSharedThread = SharedInfo.OwnedThreads[needyPoolIdx] != -1;
            i16 slots = pool.MaxThreadCount - fullThreadCount - 1 - hasOwnSharedThread;
            i16 maxSlots = Shared->GetSharedThreadCount();
            Shared->SetForeignThreadSlots(needyPoolIdx, Min<i16>(slots, maxSlots));
        }

        LWPROBE_WITH_DEBUG(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by exchanging", threadCount + 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
    }

    for (ui16 poolIdx : PriorityOrder) {
        if (takingAwayThreads <= 0) {
            break;
        }

        TPoolInfo &pool = *Pools[poolIdx];
        size_t fullThreadCount = pool.GetFullThreadCount();
        size_t additionalThreadsCount = Max<size_t>(0L, fullThreadCount - pool.DefaultFullThreadCount);
        size_t currentTakingAwayThreads = Min(additionalThreadsCount, takingAwayThreads);

        if (!currentTakingAwayThreads) {
            continue;
        }
        takingAwayThreads -= currentTakingAwayThreads;
        pool.SetFullThreadCount(fullThreadCount - currentTakingAwayThreads);
        auto &iterationPoolState = GetIterationPoolState(Iteration.load(std::memory_order_relaxed) - 1, poolIdx);
        iterationPoolState.Operation.Type = THarmonizerIterationOperation::DecreaseThreadByExchange{};
        if (Shared) {
            bool hasOwnSharedThread = SharedInfo.OwnedThreads[poolIdx] != -1;
            i16 slots = pool.MaxThreadCount - fullThreadCount + currentTakingAwayThreads - hasOwnSharedThread;
            i16 maxSlots = Shared->GetSharedThreadCount();
            Shared->SetForeignThreadSlots(poolIdx, Min<i16>(slots, maxSlots));
        }

        pool.DecreasingThreadsByExchange.fetch_add(currentTakingAwayThreads, std::memory_order_relaxed);
        LWPROBE_WITH_DEBUG(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by exchanging", fullThreadCount - currentTakingAwayThreads, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
    }
}

void THarmonizer::ProcessHoggishState() {
    HARMONIZER_DEBUG_PRINT("ProcessHoggishState");
    for (auto &[hoggishPoolIdx, freeCpu] : CpuConsumption.HoggishPools) {
        TPoolInfo &pool = *Pools[hoggishPoolIdx];
        i64 fullThreadCount = pool.GetFullThreadCount();
        if (fullThreadCount > pool.MinFullThreadCount && freeCpu >= 1) {
            pool.DecreasingThreadsByHoggishState.fetch_add(1, std::memory_order_relaxed);
            pool.SetFullThreadCount(fullThreadCount - 1);
            auto &iterationPoolState = GetIterationPoolState(Iteration.load(std::memory_order_relaxed) - 1, hoggishPoolIdx);
            iterationPoolState.Operation.Type = THarmonizerIterationOperation::DecreaseThreadByHoggishState{};
            if (Shared) {
                bool hasOwnSharedThread = SharedInfo.OwnedThreads[hoggishPoolIdx] != -1;
                i16 slots = pool.MaxThreadCount - fullThreadCount + 1 - hasOwnSharedThread;
                i16 maxSlots = Shared->GetSharedThreadCount();
                Shared->SetForeignThreadSlots(hoggishPoolIdx, Min<i16>(slots, maxSlots));
            }
            LWPROBE_WITH_DEBUG(HarmonizeOperation, hoggishPoolIdx, pool.Pool->GetName(), "decrease by hoggish", fullThreadCount - 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
        }
        if (pool.BasicPool && pool.LocalQueueSize > pool.MinLocalQueueSize) {
            pool.LocalQueueSize = std::min<ui16>(pool.MinLocalQueueSize, pool.LocalQueueSize / 2);
            pool.BasicPool->SetLocalQueueSize(pool.LocalQueueSize);
        }
        HARMONIZER_DEBUG_PRINT("poolIdx", hoggishPoolIdx, "threadCount", fullThreadCount, "pool.MinFullThreadCount", pool.MinFullThreadCount, "freeCpu", freeCpu);
    }
}

void THarmonizer::HarmonizeImpl(ui64 ts) {
    ui64 iteration = Iteration.fetch_add(1, std::memory_order_relaxed);
    HARMONIZER_DEBUG_PRINT("HarmonizeImpl", "Iteration", iteration);
    THarmonizerIterationState& iterationState = GetIterationState(iteration);
    iterationState.Iteration = iteration;
    iterationState.Ts = ts;
    iterationState.Budget = CpuConsumption.Budget;
    iterationState.LostCpu = CpuConsumption.LostCpu;
    iterationState.FreeSharedCpu = SharedInfo.FreeCpu;

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo &pool = *Pools[poolIdx];
        pool.SharedCpuQuota.store(SharedInfo.CpuConsumption[poolIdx].CpuQuota, std::memory_order_relaxed);
        if (pool.BasicPool) {
            pool.BasicPool->SetSharedCpuQuota(SharedInfo.CpuConsumption[poolIdx].CpuQuota);
        }
        THarmonizerIterationPoolState& iterationPoolState = iterationState.Pools[poolIdx];
        iterationPoolState.MaxAvgPingUs = pool.MaxAvgPingUs;
        iterationPoolState.AvgPingUs = pool.LastAvgPingUs;
        iterationPoolState.AvgPingUsWithSmallWindow = pool.LastAvgPingUsWithSmallWindow;
        iterationPoolState.LocalQueueSize = pool.LocalQueueSize;
        iterationPoolState.IsNeedy = pool.IsNeedy;
        iterationPoolState.IsStarved = pool.IsStarved;
        iterationPoolState.IsHoggish = pool.IsHoggish;
        iterationPoolState.CurrentThreadCount = pool.GetThreadCount();
        iterationPoolState.Operation.Type = THarmonizerIterationOperation::NoOperation{};
        for (size_t threadIdx = 0; threadIdx < pool.ThreadInfo.size(); ++threadIdx) {
            THarmonizerIterationThreadState& iterationThreadState = iterationPoolState.Threads[threadIdx];
            iterationThreadState.UsedCpu.Cpu = pool.ThreadInfo[threadIdx].UsedCpu.GetAvgPart();
            iterationThreadState.UsedCpu.LastSecondCpu = pool.ThreadInfo[threadIdx].UsedCpu.GetAvgPartForLastSeconds<true>(1);
            iterationThreadState.ElapsedCpu.Cpu = pool.ThreadInfo[threadIdx].ElapsedCpu.GetAvgPart();
            iterationThreadState.ElapsedCpu.LastSecondCpu = pool.ThreadInfo[threadIdx].ElapsedCpu.GetAvgPartForLastSeconds<true>(1);
            iterationThreadState.ParkedCpu.Cpu = Max<float>(0.0f, 1.0f - iterationThreadState.ElapsedCpu.Cpu);
            iterationThreadState.ParkedCpu.LastSecondCpu = Max<float>(0.0f, 1.0f - iterationThreadState.ElapsedCpu.LastSecondCpu);
        }
        for (i16 sharedThreadIdx = 0; sharedThreadIdx < static_cast<i16>(pool.SharedInfo.size()); ++sharedThreadIdx) {
            THarmonizerIterationThreadState& iterationSharedThreadState = iterationState.Shared.Threads[sharedThreadIdx].ByPool[poolIdx];
            iterationSharedThreadState.UsedCpu.Cpu = pool.SharedInfo[sharedThreadIdx].UsedCpu.GetAvgPart();
            iterationSharedThreadState.UsedCpu.LastSecondCpu = pool.SharedInfo[sharedThreadIdx].UsedCpu.GetAvgPartForLastSeconds<true>(1);
            iterationSharedThreadState.ElapsedCpu.Cpu = pool.SharedInfo[sharedThreadIdx].ElapsedCpu.GetAvgPart();
            iterationSharedThreadState.ElapsedCpu.LastSecondCpu = pool.SharedInfo[sharedThreadIdx].ElapsedCpu.GetAvgPartForLastSeconds<true>(1);
            iterationSharedThreadState.ParkedCpu.Cpu = 0.0f;
            iterationSharedThreadState.ParkedCpu.LastSecondCpu = 0.0f;
        }

        iterationState.Pools[poolIdx].SharedQuota = pool.SharedCpuQuota.load(std::memory_order_relaxed);
    }

    for (size_t sharedThreadIdx = 0; sharedThreadIdx < static_cast<size_t>(Shared ? SharedInfo.ThreadOwners.size() : 0); ++sharedThreadIdx) {
        HARMONIZER_DEBUG_PRINT("FillParkedCpu", "sharedThreadIdx", sharedThreadIdx);
        auto &sharedThreadState = iterationState.Shared.Threads[sharedThreadIdx];
        float parkedCpu = 1.0f;
        float lastSecondParkedCpu = 1.0f;
        for (size_t poolIdx = 0; poolIdx < sharedThreadState.ByPool.size(); ++poolIdx) {
            auto &poolThreadState = sharedThreadState.ByPool[poolIdx];
            parkedCpu -= poolThreadState.ElapsedCpu.Cpu;
            lastSecondParkedCpu -= poolThreadState.ElapsedCpu.LastSecondCpu;
        }
        i16 poolIdx = SharedInfo.ThreadOwners[sharedThreadIdx];
        if (poolIdx != -1) {
            auto &poolSharedThreadState = sharedThreadState.ByPool[poolIdx];
            poolSharedThreadState.ParkedCpu.Cpu = Max<float>(0.0f, parkedCpu);
            poolSharedThreadState.ParkedCpu.LastSecondCpu = Max<float>(0.0f, lastSecondParkedCpu);
        }
    }


    ProcessWaitingStats();

    for (size_t needyPoolIdx : CpuConsumption.NeedyPools) {
        TPoolInfo &pool = *Pools[needyPoolIdx];
        if (pool.AvgPingCounter && pool.LastUpdateTs + Us2Ts(3'000'000) > ts) {
            CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
            HARMONIZER_DEBUG_PRINT("pool won't be updated because time", needyPoolIdx);
        }
    }

    HARMONIZER_DEBUG_PRINT("IsStarvedPresent", CpuConsumption.IsStarvedPresent, "Overbooked", CpuConsumption.Overbooked, "StoppingThreads", CpuConsumption.StoppingThreads);
    if (CpuConsumption.IsStarvedPresent && CpuConsumption.Overbooked >= CpuConsumption.StoppingThreads) {
        ProcessStarvedState();
    } else if (!CpuConsumption.IsStarvedPresent) {
        ProcessNeedyState();
    }

    if (ProcessingBudget < 1.0) {
        ProcessExchange();
    }

    if (!CpuConsumption.HoggishPools.empty()) {
        ProcessHoggishState();
    }

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = *Pools[poolIdx];

        float freeSharedCpu = SharedInfo.FreeCpu;
        float budgetWithoutSharedCpu = std::max<float>(0.0f, CpuConsumption.BudgetWithoutSharedCpu);

        float possibleMaxSharedQuota = 0.0f;
        if (Shared) {
            bool hasOwnSharedThread = SharedInfo.OwnedThreads[poolIdx] != -1;
            i16 sharedThreads = std::min<i16>(SharedInfo.ForeignThreadsAllowed[poolIdx] + hasOwnSharedThread, SharedInfo.ThreadCount);
            float poolSharedElapsedCpu = SharedInfo.CpuConsumption[poolIdx].Elapsed;
            possibleMaxSharedQuota = std::min<float>(poolSharedElapsedCpu + freeSharedCpu, sharedThreads);
        }
        float fullThreadCount = pool.GetFullThreadCount();
        float elapsedCpu = pool.ElapsedCpu.GetAvgPart();
        float parkedCpu = Max<float>(0.0f, fullThreadCount - elapsedCpu);
        float budgetWithoutSharedAndParkedCpu = std::max<float>(0.0f, budgetWithoutSharedCpu - parkedCpu);
        i16 potentialMaxThreadCountWithoutSharedCpu = std::min<float>(pool.MaxThreadCount, fullThreadCount + budgetWithoutSharedAndParkedCpu);
        float potentialMaxThreadCount = std::min<float>(pool.MaxThreadCount, potentialMaxThreadCountWithoutSharedCpu + possibleMaxSharedQuota);

        pool.PotentialMaxThreadCount.store(potentialMaxThreadCount, std::memory_order_relaxed);
        HARMONIZER_DEBUG_PRINT(poolIdx, pool.Pool->GetName(),
            "budget: ", CpuConsumption.Budget,
            "free shared cpu: ", freeSharedCpu,
            "budget without shared cpu: ", budgetWithoutSharedCpu,
            "budget without shared and parked cpu: ", budgetWithoutSharedAndParkedCpu,
            "potential max thread count: ", potentialMaxThreadCount,
            "potential max thread count without shared cpu: ", potentialMaxThreadCountWithoutSharedCpu,
            "possible max shared quota: ", possibleMaxSharedQuota,
            "full thread count: ", fullThreadCount,
            "elapsed cpu: ", elapsedCpu,
            "parked cpu: ", parkedCpu
        );
    }
}

void THarmonizer::CalculatePriorityOrder() {
    PriorityOrder.resize(Pools.size());
    Iota(PriorityOrder.begin(), PriorityOrder.end(), 0);
    Sort(PriorityOrder.begin(), PriorityOrder.end(), [&] (i16 lhs, i16 rhs) {
        if (Pools[lhs]->Priority != Pools[rhs]->Priority)  {
            return Pools[lhs]->Priority < Pools[rhs]->Priority;
        }
        return Pools[lhs]->Pool->PoolId > Pools[rhs]->Pool->PoolId;
    });
}

void THarmonizer::InitIterationState() {
    IterationState.resize(SAVED_ITERATION_COUNT);
    ui64 poolsPerIteration = Pools.size();
    IterationPoolState.resize(poolsPerIteration * SAVED_ITERATION_COUNT);
    ui64 sharedThreadsPerIteration = Shared ? Shared->GetSharedThreadCount() : 0;
    IterationSharedThreadState.resize(sharedThreadsPerIteration * SAVED_ITERATION_COUNT);
    ui64 threadsPerIteration = sharedThreadsPerIteration * poolsPerIteration;
    for (auto &pool : Pools) {
        threadsPerIteration += pool->ThreadInfo.size();
    }
    IterationThreadState.resize(threadsPerIteration * SAVED_ITERATION_COUNT);

    PersistentPoolState.resize(Pools.size());
    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        PersistentPoolState[poolIdx].Name = Pools[poolIdx]->Pool->GetName();
        PersistentPoolState[poolIdx].Priority = Pools[poolIdx]->Priority;
        PersistentPoolState[poolIdx].DefaultFullThreadCount = Pools[poolIdx]->DefaultFullThreadCount;
        PersistentPoolState[poolIdx].MinFullThreadCount = Pools[poolIdx]->MinFullThreadCount;
        PersistentPoolState[poolIdx].MaxFullThreadCount = Pools[poolIdx]->MaxFullThreadCount;
        PersistentPoolState[poolIdx].DefaultThreadCount = Pools[poolIdx]->DefaultThreadCount;
        PersistentPoolState[poolIdx].MinThreadCount = Pools[poolIdx]->MinThreadCount;
        PersistentPoolState[poolIdx].MaxThreadCount = Pools[poolIdx]->MaxThreadCount;
        PersistentPoolState[poolIdx].MinLocalQueueSize = Pools[poolIdx]->MinLocalQueueSize;
        PersistentPoolState[poolIdx].MaxLocalQueueSize = Pools[poolIdx]->MaxLocalQueueSize;
    }

    if (Shared) {
        PersistentSharedPoolState.reset(new THarmonizerPersistentSharedPoolState);
        Shared->FillThreadOwners(PersistentSharedPoolState->ThreadOwners);
    }

    for (size_t i = 0; i < SAVED_ITERATION_COUNT; ++i) {
        THarmonizerIterationPoolState *poolState = IterationPoolState.data() + i * poolsPerIteration;
        IterationState[i].Pools.Set(poolState, poolState + poolsPerIteration);
        THarmonizerIterationSharedThreadState *sharedThreadState = IterationSharedThreadState.data() + i * sharedThreadsPerIteration;
        IterationState[i].Shared.PersistentState = PersistentSharedPoolState.get();
        IterationState[i].Shared.Threads.Set(sharedThreadState, sharedThreadState + sharedThreadsPerIteration);
        THarmonizerIterationThreadState *threadState = IterationThreadState.data() + i * threadsPerIteration;
        for (size_t poolIdx = 0; poolIdx < poolsPerIteration; ++poolIdx) {
            TPoolInfo &pool = *Pools[poolIdx];
            poolState[poolIdx].Threads.Set(threadState, threadState + pool.ThreadInfo.size());
            poolState[poolIdx].PersistentState = &PersistentPoolState[poolIdx];
            threadState += pool.ThreadInfo.size();
        }
        for (size_t sharedThreadIdx = 0; sharedThreadIdx < sharedThreadsPerIteration; ++sharedThreadIdx) {
            sharedThreadState[sharedThreadIdx].ByPool.Set(threadState, threadState + Pools.size());
            threadState += Pools.size();
        }
    }
}

bool THarmonizer::InvokeReadHistory(TIterationViewerCallback callback) {
    if (IsDisabled.load(std::memory_order_relaxed)) {
        return true;
    }
    if (!Lock.TryAcquire()) {
        return false;
    }
    auto iteration = Iteration.load(std::memory_order_relaxed);
    if (iteration < SAVED_ITERATION_COUNT) {
        THarmonizerIterationState *end = IterationState.data() + iteration;
        TIterableDoubleRange<THarmonizerIterationState> range(IterationState.begin(), end, end, end);
        callback(range);
    } else {
        ui64 endIdx = iteration % SAVED_ITERATION_COUNT;
        THarmonizerIterationState *end = IterationState.data() + endIdx;
        TIterableDoubleRange<THarmonizerIterationState> range(end, IterationState.end(), IterationState.begin(), end);
        callback(range);
    }
    Lock.Release();
    return true;
}

void THarmonizer::Harmonize(ui64 ts) {
    if (IsDisabled.load(std::memory_order_relaxed) || NextHarmonizeTs.load(std::memory_order_acquire) > ts || !Lock.TryAcquire()) {
        LWPROBE(TryToHarmonizeFailed, ts, NextHarmonizeTs.load(std::memory_order_relaxed), IsDisabled.load(std::memory_order_relaxed), false);
        return;
    }

    if (NextHarmonizeTs.load(std::memory_order_acquire) > ts) {
        Lock.Release();
        return;
    }

    // Check again under the lock
    if (IsDisabled.load(std::memory_order_relaxed)) {
        LWPROBE(TryToHarmonizeFailed, ts, NextHarmonizeTs.load(std::memory_order_relaxed), IsDisabled.load(std::memory_order_relaxed), true);
        Lock.Release();
        return;
    }
    // Will never reach this line disabled
    ui64 previousNextHarmonizeTs = NextHarmonizeTs.exchange(ts + Us2Ts(1'000'000ull), std::memory_order_acquire);
    LWPROBE_WITH_DEBUG(TryToHarmonizeSuccess, ts, NextHarmonizeTs.load(std::memory_order_relaxed), previousNextHarmonizeTs);

    {
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_HARMONIZER> activityGuard;

        if (PriorityOrder.empty()) {
            CalculatePriorityOrder();
            CpuConsumption.Init(Pools.size());
            SharedInfo.Init(Pools.size(), Shared);
            InitIterationState();
        }

        PullStats(ts);
        HarmonizeImpl(ts);
    }

    Lock.Release();
}

void THarmonizer::DeclareEmergency(ui64 ts) {
    NextHarmonizeTs = ts;
}

void THarmonizer::AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo, bool ignoreFullThreadQuota) {
    TGuard<TSpinLock> guard(Lock);
    Pools.emplace_back(new TPoolInfo);
    TPoolInfo &poolInfo = *Pools.back();
    poolInfo.Pool = pool;
    poolInfo.Shared = Shared;
    poolInfo.BasicPool = dynamic_cast<TBasicExecutorPool*>(pool);

    poolInfo.DefaultThreadCount = pool->GetDefaultThreadCount();
    poolInfo.ThreadQuota = ignoreFullThreadQuota ? 0 : poolInfo.DefaultThreadCount;
    poolInfo.MinThreadCount = pool->GetMinThreadCount();
    poolInfo.MaxThreadCount = pool->GetMaxThreadCount();
    poolInfo.PotentialMaxThreadCount = poolInfo.MaxThreadCount;

    poolInfo.DefaultFullThreadCount = pool->GetDefaultFullThreadCount();
    poolInfo.MinFullThreadCount = pool->GetMinFullThreadCount();
    poolInfo.MaxFullThreadCount = pool->GetMaxFullThreadCount();
    poolInfo.ThreadInfo.resize(poolInfo.MaxFullThreadCount);
    poolInfo.SharedInfo.resize(Shared ? Shared->GetSharedThreadCount() : 0);
    poolInfo.Priority = pool->GetPriority();
    pool->SetFullThreadCount(poolInfo.DefaultFullThreadCount);
    if (Shared) {
        TVector<i16> ownedThreads(Pools.size(), -1);
        Shared->FillOwnedThreads(ownedThreads);
        bool hasOwnSharedThread = ownedThreads[pool->PoolId] != -1;
        Shared->SetForeignThreadSlots(pool->PoolId, Min<i16>(poolInfo.MaxThreadCount - hasOwnSharedThread, Shared->GetSharedThreadCount()));
    }
    if (pingInfo) {
        poolInfo.AvgPingCounter = pingInfo->AvgPingCounter;
        poolInfo.AvgPingCounterWithSmallWindow = pingInfo->AvgPingCounterWithSmallWindow;
        poolInfo.MaxAvgPingUs = pingInfo->MaxAvgPingUs;
    }
    if (poolInfo.BasicPool) {
        poolInfo.WaitingStats.reset(new TWaitingStats<ui64>());
        poolInfo.MovingWaitingStats.reset(new TWaitingStats<double>());
        poolInfo.MinLocalQueueSize = poolInfo.BasicPool->GetMinLocalQueueSize();
        poolInfo.MaxLocalQueueSize = poolInfo.BasicPool->GetMaxLocalQueueSize();
        poolInfo.LocalQueueSize = poolInfo.MinLocalQueueSize;
    }
    PriorityOrder.clear();
}

void THarmonizer::Enable(bool enable) {
    TGuard<TSpinLock> guard(Lock);
    IsDisabled = enable;
}

std::unique_ptr<IHarmonizer> MakeHarmonizer(ui64 ts) {
    return std::make_unique<THarmonizer>(ts);
}

TPoolHarmonizerStats THarmonizer::GetPoolStats(i16 poolId) const {
    const TPoolInfo &pool = *Pools[poolId];
    ui64 flags = pool.LastFlags.load(std::memory_order_relaxed);
    return TPoolHarmonizerStats{
        .IncreasingThreadsByNeedyState = pool.IncreasingThreadsByNeedyState.load(std::memory_order_relaxed),
        .IncreasingThreadsByExchange = pool.IncreasingThreadsByExchange.load(std::memory_order_relaxed),
        .DecreasingThreadsByStarvedState = pool.DecreasingThreadsByStarvedState.load(std::memory_order_relaxed),
        .DecreasingThreadsByHoggishState = pool.DecreasingThreadsByHoggishState.load(std::memory_order_relaxed),
        .DecreasingThreadsByExchange = pool.DecreasingThreadsByExchange.load(std::memory_order_relaxed),
        .MaxUsedCpu = pool.MaxUsedCpu.load(std::memory_order_relaxed),
        .MinUsedCpu = pool.MinUsedCpu.load(std::memory_order_relaxed),
        .AvgUsedCpu = pool.AvgUsedCpu.load(std::memory_order_relaxed),
        .MaxElapsedCpu = pool.MaxElapsedCpu.load(std::memory_order_relaxed),
        .MinElapsedCpu = pool.MinElapsedCpu.load(std::memory_order_relaxed),
        .AvgElapsedCpu = pool.AvgElapsedCpu.load(std::memory_order_relaxed),
        .PotentialMaxThreadCount = pool.PotentialMaxThreadCount.load(std::memory_order_relaxed),
        .SharedCpuQuota = pool.SharedCpuQuota.load(std::memory_order_relaxed),
        .IsNeedy = static_cast<bool>(flags & 1),
        .IsStarved = static_cast<bool>(flags & 2),
        .IsHoggish = static_cast<bool>(flags & 4),
    };
}

THarmonizerStats THarmonizer::GetStats() const {
    return THarmonizerStats{
        .MaxUsedCpu = MaxUsedCpu.load(std::memory_order_relaxed),
        .MinUsedCpu = MinUsedCpu.load(std::memory_order_relaxed),
        .MaxElapsedCpu = MaxElapsedCpu.load(std::memory_order_relaxed),
        .MinElapsedCpu = MinElapsedCpu.load(std::memory_order_relaxed),
        .AvgAwakeningTimeUs = WaitingInfo.AvgAwakeningTimeUs.load(std::memory_order_relaxed),
        .AvgWakingUpTimeUs = WaitingInfo.AvgWakingUpTimeUs.load(std::memory_order_relaxed),
    };
}

void THarmonizer::SetSharedPool(ISharedPool* pool) {
    Shared = pool;
}

TString TPoolHarmonizerStats::ToString() const {
    return TStringBuilder() << '{'
        << "IncreasingThreadsByNeedyState: " << IncreasingThreadsByNeedyState << ", "
        << "IncreasingThreadsByExchange: " << IncreasingThreadsByExchange << ", "
        << "DecreasingThreadsByStarvedState: " << DecreasingThreadsByStarvedState << ", "
        << "DecreasingThreadsByHoggishState: " << DecreasingThreadsByHoggishState << ", "
        << "DecreasingThreadsByExchange: " << DecreasingThreadsByExchange << ", "
        << "ReceivedHalfThreadByNeedyState: " << ReceivedHalfThreadByNeedyState << ", "
        << "GivenHalfThreadByOtherStarvedState: " << GivenHalfThreadByOtherStarvedState << ", "
        << "GivenHalfThreadByHoggishState: " << GivenHalfThreadByHoggishState << ", "
        << "GivenHalfThreadByOtherNeedyState: " << GivenHalfThreadByOtherNeedyState << ", "
        << "ReturnedHalfThreadByStarvedState: " << ReturnedHalfThreadByStarvedState << ", "
        << "ReturnedHalfThreadByOtherHoggishState: " << ReturnedHalfThreadByOtherHoggishState << ", "
        << "MaxUsedCpu: " << MaxUsedCpu << ", "
        << "MinUsedCpu: " << MinUsedCpu << ", "
        << "AvgUsedCpu: " << AvgUsedCpu << ", "
        << "MaxElapsedCpu: " << MaxElapsedCpu << ", "
        << "MinElapsedCpu: " << MinElapsedCpu << ", "
        << "AvgElapsedCpu: " << AvgElapsedCpu << ", "
        << "PotentialMaxThreadCount: " << PotentialMaxThreadCount << ", "
        << "SharedCpuQuota: " << SharedCpuQuota << ", "
        << "IsNeedy: " << IsNeedy << ", "
        << "IsStarved: " << IsStarved << ", "
        << "IsHoggish: " << IsHoggish << '}';
}

TString THarmonizerStats::ToString() const {
    return TStringBuilder() << '{'
        << "MaxUsedCpu: " << MaxUsedCpu << ", "
        << "MinUsedCpu: " << MinUsedCpu << ", "
        << "MaxElapsedCpu: " << MaxElapsedCpu << ", "
        << "MinElapsedCpu: " << MinElapsedCpu << ", "
        << "AvgAwakeningTimeUs: " << AvgAwakeningTimeUs << ", "
        << "AvgWakingUpTimeUs: " << AvgWakingUpTimeUs << '}';
}

}
