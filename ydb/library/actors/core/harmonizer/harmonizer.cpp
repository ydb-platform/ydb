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



LWTRACE_USING(ACTORLIB_PROVIDER);


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

    void PullStats(ui64 ts);
    void PullSharedInfo();
    void ProcessWaitingStats();
    void HarmonizeImpl(ui64 ts);
    void CalculatePriorityOrder();
    void ProcessStarvedState();
    void ProcessNeedyState();
    void ProcessExchange();
    void ProcessHoggishState();
public:
    THarmonizer(ui64 ts);
    virtual ~THarmonizer();
    double Rescale(double value) const;
    void Harmonize(ui64 ts) override;
    void DeclareEmergency(ui64 ts) override;
    void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo) override;
    void Enable(bool enable) override;
    TPoolHarmonizerStats GetPoolStats(i16 poolId) const override;
    THarmonizerStats GetStats() const override;
    void SetSharedPool(ISharedPool* pool) override;
};

THarmonizer::THarmonizer(ui64 ts) {
    NextHarmonizeTs = ts;
}

THarmonizer::~THarmonizer() {
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
        SharedInfo.Pull(*Shared);
    }
    CpuConsumption.Pull(Pools, SharedInfo);
    ProcessingBudget = CpuConsumption.Budget;
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
    HARMONIZER_DEBUG_PRINT("ProcessStarvedState");
    HARMONIZER_DEBUG_PRINT("shared info", SharedInfo.ToString());
    for (ui16 poolIdx : PriorityOrder) {
        TPoolInfo &pool = *Pools[poolIdx];
        i64 threadCount = pool.GetFullThreadCount();
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

        if (Shared && SharedInfo.ForeignThreadsAllowed[needyPoolIdx] == 0) {
            Shared->SetForeignThreadSlots(needyPoolIdx, static_cast<i16>(Pools.size()));
            CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
        } else if (ProcessingBudget >= 1.0 && threadCount + 1 <= pool.MaxFullThreadCount) {
            pool.IncreasingThreadsByNeedyState.fetch_add(1, std::memory_order_relaxed);
            CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
            CpuConsumption.AdditionalThreads++;
            pool.SetFullThreadCount(threadCount + 1);
            ProcessingBudget -= 1.0;
            LWPROBE_WITH_DEBUG(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by needs", threadCount + 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
        }
        if constexpr (NFeatures::IsLocalQueues()) {
            bool needToExpandLocalQueue = ProcessingBudget < 1.0 || threadCount >= pool.MaxFullThreadCount;
            needToExpandLocalQueue &= (bool)pool.BasicPool;
            needToExpandLocalQueue &= (pool.MaxFullThreadCount > 1);
            needToExpandLocalQueue &= (pool.LocalQueueSize < NFeatures::TLocalQueuesFeatureFlags::MAX_LOCAL_QUEUE_SIZE);
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
        i64 threadCount = pool.GetFullThreadCount();

        sumOfAdditionalThreads -= threadCount - pool.DefaultFullThreadCount;
        if (sumOfAdditionalThreads < takingAwayThreads + 1) {
            break;
        }

        if (Shared && SharedInfo.ForeignThreadsAllowed[needyPoolIdx] < static_cast<i16>(Pools.size())) {
            Shared->SetForeignThreadSlots(needyPoolIdx, static_cast<i16>(Pools.size()));
            CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
        }

        if (!CpuConsumption.IsNeedyByPool[needyPoolIdx]) {
            continue;
        }
        pool.IncreasingThreadsByExchange.fetch_add(1, std::memory_order_relaxed);
        CpuConsumption.IsNeedyByPool[needyPoolIdx] = false;
        takingAwayThreads++;
        pool.SetFullThreadCount(threadCount + 1);

        LWPROBE_WITH_DEBUG(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by exchanging", threadCount + 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
    }

    for (ui16 poolIdx : PriorityOrder) {
        if (takingAwayThreads <= 0) {
            break;
        }

        TPoolInfo &pool = *Pools[poolIdx];
        size_t threadCount = pool.GetFullThreadCount();
        size_t additionalThreadsCount = Max<size_t>(0L, threadCount - pool.DefaultFullThreadCount);
        size_t currentTakingAwayThreads = Min(additionalThreadsCount, takingAwayThreads);

        if (!currentTakingAwayThreads) {
            continue;
        }
        takingAwayThreads -= currentTakingAwayThreads;
        pool.SetFullThreadCount(threadCount - currentTakingAwayThreads);

        pool.DecreasingThreadsByExchange.fetch_add(currentTakingAwayThreads, std::memory_order_relaxed);
        LWPROBE_WITH_DEBUG(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by exchanging", threadCount - currentTakingAwayThreads, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
    }
}

void THarmonizer::ProcessHoggishState() {
    HARMONIZER_DEBUG_PRINT("ProcessHoggishState");
    for (auto &[hoggishPoolIdx, freeCpu] : CpuConsumption.HoggishPools) {
        TPoolInfo &pool = *Pools[hoggishPoolIdx];
        i64 threadCount = pool.GetFullThreadCount();
        if (threadCount > pool.MinFullThreadCount && freeCpu >= 1) {
            pool.DecreasingThreadsByHoggishState.fetch_add(1, std::memory_order_relaxed);
            pool.SetFullThreadCount(threadCount - 1);
            LWPROBE_WITH_DEBUG(HarmonizeOperation, hoggishPoolIdx, pool.Pool->GetName(), "decrease by hoggish", threadCount - 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
        }
        if (threadCount == pool.MinFullThreadCount && Shared && SharedInfo.ForeignThreadsAllowed[hoggishPoolIdx] != 0) {
            Shared->SetForeignThreadSlots(hoggishPoolIdx, 0);
        }
        if (pool.BasicPool && pool.LocalQueueSize > NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE) {
            pool.LocalQueueSize = std::min<ui16>(NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE, pool.LocalQueueSize / 2);
            pool.BasicPool->SetLocalQueueSize(pool.LocalQueueSize);
        }
        HARMONIZER_DEBUG_PRINT("poolIdx", hoggishPoolIdx, "threadCount", threadCount, "pool.MinFullThreadCount", pool.MinFullThreadCount, "freeCpu", freeCpu);
    }
}

void THarmonizer::HarmonizeImpl(ui64 ts) {
    HARMONIZER_DEBUG_PRINT("HarmonizeImpl", "Iteration", Iteration.fetch_add(1, std::memory_order_relaxed));
    Y_UNUSED(ts);

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo &pool = *Pools[poolIdx];
        pool.SharedCpuQuota.store(SharedInfo.CpuConsumption[poolIdx].CpuQuota, std::memory_order_relaxed);
        if (pool.BasicPool) {
            pool.BasicPool->SetSharedCpuQuota(SharedInfo.CpuConsumption[poolIdx].CpuQuota);
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
        pool.PotentialMaxThreadCount.store(std::min<i64>(pool.MaxThreadCount, static_cast<i64>(pool.GetThreadCount() + CpuConsumption.Budget)), std::memory_order_relaxed);
        HARMONIZER_DEBUG_PRINT(poolIdx, pool.Pool->GetName(), "potential max thread count", pool.PotentialMaxThreadCount.load(std::memory_order_relaxed), "budget", CpuConsumption.Budget, "thread count", pool.GetThreadCount());
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
        }

        PullStats(ts);
        HarmonizeImpl(ts);
    }

    Lock.Release();
}

void THarmonizer::DeclareEmergency(ui64 ts) {
    NextHarmonizeTs = ts;
}

void THarmonizer::AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo) {
    TGuard<TSpinLock> guard(Lock);
    Pools.emplace_back(new TPoolInfo);
    TPoolInfo &poolInfo = *Pools.back();
    poolInfo.Pool = pool;
    poolInfo.Shared = Shared;
    poolInfo.BasicPool = dynamic_cast<TBasicExecutorPool*>(pool);
    poolInfo.DefaultThreadCount = pool->GetDefaultThreadCount();
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
    if (pingInfo) {
        poolInfo.AvgPingCounter = pingInfo->AvgPingCounter;
        poolInfo.AvgPingCounterWithSmallWindow = pingInfo->AvgPingCounterWithSmallWindow;
        poolInfo.MaxAvgPingUs = pingInfo->MaxAvgPingUs;
    }
    if (poolInfo.BasicPool) {
        poolInfo.WaitingStats.reset(new TWaitingStats<ui64>());
        poolInfo.MovingWaitingStats.reset(new TWaitingStats<double>());
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
        .ReceivedHalfThreadByNeedyState = pool.ReceivedHalfThreadByNeedyState.load(std::memory_order_relaxed),
        .GivenHalfThreadByOtherStarvedState = pool.GivenHalfThreadByOtherStarvedState.load(std::memory_order_relaxed),
        .GivenHalfThreadByHoggishState = pool.GivenHalfThreadByHoggishState.load(std::memory_order_relaxed),
        .GivenHalfThreadByOtherNeedyState = pool.GivenHalfThreadByOtherNeedyState.load(std::memory_order_relaxed),
        .ReturnedHalfThreadByStarvedState = pool.ReturnedHalfThreadByStarvedState.load(std::memory_order_relaxed),
        .ReturnedHalfThreadByOtherHoggishState = pool.ReturnedHalfThreadByOtherHoggishState.load(std::memory_order_relaxed),
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
