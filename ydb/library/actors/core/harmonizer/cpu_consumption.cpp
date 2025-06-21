#include "cpu_consumption.h"
#include "debug.h"

namespace NActors {

LWTRACE_USING(ACTORLIB_PROVIDER);

void TCpuConsumptionInfo::Clear() {
    Elapsed = 0.0;
    Cpu = 0.0;
    LastSecondElapsed = 0.0;
    LastSecondCpu = 0.0;
}

void THarmonizerCpuConsumption::Init(i16 poolCount) {
    PoolConsumption.resize(poolCount);
    PoolFullThreadConsumption.resize(poolCount);
    PoolForeignConsumption.resize(poolCount);
    IsNeedyByPool.reserve(poolCount);
    NeedyPools.reserve(poolCount);
    HoggishPools.reserve(poolCount);
}

namespace {

    float Rescale(float value) {
        return Max(0.0, Min(1.0, value * (1.0/0.9)));
    }

    void UpdatePoolConsumption(const TPoolInfo& pool, TCpuConsumptionInfo *poolConsumption, TCpuConsumptionInfo *poolFullThreadConsumption) {
        poolConsumption->Clear();
        poolFullThreadConsumption->Clear();
        for (i16 threadIdx = 0; threadIdx < pool.MaxThreadCount; ++threadIdx) {
            float threadElapsed = Rescale(pool.GetElapsed(threadIdx));
            float threadLastSecondElapsed = Rescale(pool.GetLastSecondElapsed(threadIdx));
            float threadCpu = Rescale(pool.GetCpu(threadIdx));
            float threadLastSecondCpu = Rescale(pool.GetLastSecondCpu(threadIdx));
            poolConsumption->Elapsed += threadElapsed;
            poolConsumption->LastSecondElapsed += threadLastSecondElapsed;
            poolConsumption->Cpu += threadCpu;
            poolConsumption->LastSecondCpu += threadLastSecondCpu;
            poolFullThreadConsumption->Elapsed += threadElapsed;
            poolFullThreadConsumption->LastSecondElapsed += threadLastSecondElapsed;
            poolFullThreadConsumption->Cpu += threadCpu;
            poolFullThreadConsumption->LastSecondCpu += threadLastSecondCpu;
            LWPROBE_WITH_DEBUG(HarmonizeCheckPoolByThread, pool.Pool->PoolId, pool.Pool->GetName(), threadIdx, threadElapsed, threadCpu, threadLastSecondElapsed, threadLastSecondCpu);
        }
        for (i16 sharedIdx = 0; sharedIdx < static_cast<i16>(pool.SharedInfo.size()); ++sharedIdx) {
            float sharedElapsed = pool.GetSharedElapsed(sharedIdx);
            float sharedLastSecondElapsed = pool.GetLastSecondSharedElapsed(sharedIdx);
            float sharedCpu = pool.GetSharedCpu(sharedIdx);
            float sharedLastSecondCpu = pool.GetLastSecondSharedCpu(sharedIdx);
            poolConsumption->Elapsed += sharedElapsed;
            poolConsumption->LastSecondElapsed += sharedLastSecondElapsed;
            poolConsumption->Cpu += sharedCpu;
            poolConsumption->LastSecondCpu += sharedLastSecondCpu;
            LWPROBE_WITH_DEBUG(HarmonizeCheckPoolByThread, pool.Pool->PoolId, pool.Pool->GetName(), -1 - sharedIdx, sharedElapsed, sharedCpu, sharedLastSecondElapsed, sharedLastSecondCpu);
        }
    }

    void UpdatePoolForeignConsumption(const TPoolInfo& pool, TPoolForeignConsumptionInfo *poolForeignConsumption, const TSharedInfo& sharedInfo) {
        float prevElapsed = std::exchange(poolForeignConsumption->PrevElapsedValue, sharedInfo.CpuConsumption[pool.Pool->PoolId].ForeignElapsed);
        float prevCpu = std::exchange(poolForeignConsumption->PrevCpuValue, sharedInfo.CpuConsumption[pool.Pool->PoolId].ForeignCpu);
        poolForeignConsumption->Elapsed = sharedInfo.CpuConsumption[pool.Pool->PoolId].ForeignElapsed - prevElapsed;
        poolForeignConsumption->Cpu = sharedInfo.CpuConsumption[pool.Pool->PoolId].ForeignCpu - prevCpu;
    }

    bool IsStarved(double elapsed, double cpu) {
        return Max(elapsed, cpu) > 0.1 && (cpu < elapsed * 0.7 || elapsed - cpu > 0.5);
    }

    bool IsHoggish(double elapsed, double currentThreadCount) {
        return elapsed <= currentThreadCount - 1.0;
    }

} // namespace


void THarmonizerCpuConsumption::Pull(const std::vector<std::unique_ptr<TPoolInfo>> &pools, const TSharedInfo& sharedInfo) {
    NeedyPools.clear();
    HoggishPools.clear();
    IsNeedyByPool.clear();


    TotalCores = 0;
    AdditionalThreads = 0;
    StoppingThreads = 0;
    IsStarvedPresent = false;
    Elapsed = 0.0;
    Cpu = 0.0;
    LastSecondElapsed = 0.0;
    LastSecondCpu = 0.0;
    for (size_t poolIdx = 0; poolIdx < pools.size(); ++poolIdx) {
        TPoolInfo& pool = *pools[poolIdx];
        TotalCores += pool.ThreadQuota;

        AdditionalThreads += Max(0, pool.GetFullThreadCount() - pool.DefaultFullThreadCount);
        float currentThreadCount = pool.GetThreadCount();
        float currentFullThreadCount = pool.GetFullThreadCount();
        StoppingThreads += pool.Pool->GetBlockingThreadCount();
        HARMONIZER_DEBUG_PRINT("pool", poolIdx, "pool name", pool.Pool->GetName(), "current thread count", currentThreadCount, "stopping threads", StoppingThreads, "default thread count", pool.DefaultThreadCount);

        UpdatePoolConsumption(pool, &PoolConsumption[poolIdx], &PoolFullThreadConsumption[poolIdx]);
        UpdatePoolForeignConsumption(pool, &PoolForeignConsumption[poolIdx], sharedInfo);

        HARMONIZER_DEBUG_PRINT("CpuConsumption::Pull",
            "pool:", poolIdx,
            "pool name:", pool.Pool->GetName(),
            "elapsed:", PoolConsumption[poolIdx].Elapsed,
            "cpu:", PoolConsumption[poolIdx].Cpu,
            "last second elapsed:", PoolConsumption[poolIdx].LastSecondElapsed,
            "last second cpu:", PoolConsumption[poolIdx].LastSecondCpu,
            "full thread elapsed:", PoolFullThreadConsumption[poolIdx].Elapsed,
            "full thread cpu:", PoolFullThreadConsumption[poolIdx].Cpu,
            "last second full thread elapsed:", PoolFullThreadConsumption[poolIdx].LastSecondElapsed,
            "last second full thread cpu:", PoolFullThreadConsumption[poolIdx].LastSecondCpu,
            "foreign elapsed:", PoolForeignConsumption[poolIdx].Elapsed,
            "foreign cpu:", PoolForeignConsumption[poolIdx].Cpu
        );

        bool isStarved = IsStarved(PoolConsumption[poolIdx].Elapsed, PoolConsumption[poolIdx].Cpu)
                || IsStarved(PoolConsumption[poolIdx].LastSecondElapsed, PoolConsumption[poolIdx].LastSecondCpu);
        if (isStarved) {
            IsStarvedPresent = true;
        }

        float expectedThreadCount = pool.GetFullThreadCount() + (sharedInfo.OwnedThreads[poolIdx] != -1 ? 1 : 0) + 0.5;
        bool isMoreThanExpected = (PoolConsumption[poolIdx].LastSecondCpu >= expectedThreadCount) && (PoolFullThreadConsumption[poolIdx].LastSecondCpu >= currentFullThreadCount - 1);
        bool isNeedy = (pool.IsAvgPingGood() || pool.NewNotEnoughCpuExecutions) && (PoolConsumption[poolIdx].LastSecondCpu >= currentThreadCount || isMoreThanExpected);
        IsNeedyByPool.push_back(isNeedy);
        if (isNeedy) {
            NeedyPools.push_back(poolIdx);
        }

        bool isHoggish = !isNeedy && IsHoggish(PoolFullThreadConsumption[poolIdx].Elapsed, currentFullThreadCount) && IsHoggish(PoolFullThreadConsumption[poolIdx].LastSecondElapsed, currentFullThreadCount);
        if (isHoggish) {
            float freeCpu = std::min(currentFullThreadCount - PoolFullThreadConsumption[poolIdx].Elapsed, currentFullThreadCount - PoolFullThreadConsumption[poolIdx].LastSecondElapsed);
            HoggishPools.push_back({poolIdx, freeCpu});
        }

        Elapsed += PoolConsumption[poolIdx].Elapsed;
        Cpu += PoolConsumption[poolIdx].Cpu;
    LastSecondElapsed += PoolConsumption[poolIdx].LastSecondElapsed;
        LastSecondCpu += PoolConsumption[poolIdx].LastSecondCpu;
        pool.LastFlags.store((i64)isNeedy | ((i64)isStarved << 1) | ((i64)isHoggish << 2), std::memory_order_relaxed);
        LWPROBE_WITH_DEBUG(
            HarmonizeCheckPool,
            poolIdx,
            pool.Pool->GetName(),
            PoolConsumption[poolIdx].Elapsed,
            PoolConsumption[poolIdx].Cpu,
            PoolConsumption[poolIdx].LastSecondElapsed,
            PoolConsumption[poolIdx].LastSecondCpu,
            currentThreadCount,
            pool.MaxFullThreadCount,
            isStarved,
            isNeedy,
            isHoggish
        );
    }

    if (NeedyPools.size()) {
        Sort(NeedyPools.begin(), NeedyPools.end(), [&] (i16 lhs, i16 rhs) {
            if (pools[lhs]->Priority != pools[rhs]->Priority)  {
                return pools[lhs]->Priority > pools[rhs]->Priority;
            }
            return pools[lhs]->Pool->PoolId < pools[rhs]->Pool->PoolId;
        });
    }

    HARMONIZER_DEBUG_PRINT("NeedyPools", NeedyPools.size(), "HoggishPools", HoggishPools.size());

    Budget = TotalCores - Elapsed;
    BudgetLS = TotalCores - LastSecondElapsed;
    BudgetWithoutSharedCpu = Budget - sharedInfo.FreeCpu;
    BudgetLSWithoutSharedCpu = BudgetLS - sharedInfo.FreeCpu;
    Overbooked = -Budget;
    LostCpu = Max<float>(0.0f, Elapsed - Cpu);
    if (BudgetLS < -0.1) {
        IsStarvedPresent = true;
    }
    HARMONIZER_DEBUG_PRINT("IsStarvedPresent", IsStarvedPresent, "Budget", Budget, "Overbooked", Overbooked, "TotalCores", TotalCores, "Elapsed", Elapsed, "Cpu", Cpu, "LastSecondElapsed", LastSecondElapsed, "LastSecondCpu", LastSecondCpu);
}

} // namespace NActors
