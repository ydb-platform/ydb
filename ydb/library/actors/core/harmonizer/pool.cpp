#include "pool.h"

#include <ydb/library/actors/core/executor_pool.h>
#include <ydb/library/actors/core/executor_pool_shared.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_basic_feature_flags.h>

#include "debug.h"

namespace NActors {

LWTRACE_USING(ACTORLIB_PROVIDER);

TPoolInfo::TPoolInfo()
    : LocalQueueSize(NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE)
{}

double TPoolInfo::GetCpu(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].UsedCpu.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetSharedCpu(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].UsedCpu.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondCpu(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].UsedCpu.GetAvgPartForLastSeconds<true>(1);
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondSharedCpu(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].UsedCpu.GetAvgPartForLastSeconds<true>(1);
    }
    return 0.0;
}

double TPoolInfo::GetElapsed(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].ElapsedCpu.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetSharedElapsed(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].ElapsedCpu.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondElapsed(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].ElapsedCpu.GetAvgPartForLastSeconds<true>(1);
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondSharedElapsed(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].ElapsedCpu.GetAvgPartForLastSeconds<true>(1);
    }
    return 0.0;
}

#define UNROLL_HISTORY(history) (history)[0], (history)[1], (history)[2], (history)[3], (history)[4], (history)[5], (history)[6], (history)[7]
TCpuConsumption TPoolInfo::PullStats(ui64 ts) {
    TCpuConsumption acc;
    for (i16 threadIdx = 0; threadIdx < MaxFullThreadCount; ++threadIdx) {
        TThreadInfo &threadInfo = ThreadInfo[threadIdx];
        TCpuConsumption cpuConsumption = Pool->GetThreadCpuConsumption(threadIdx);
        acc.Add(cpuConsumption);
        threadInfo.ElapsedCpu.Register(ts, cpuConsumption.ElapsedUs / 1'000'000.0);
        LWPROBE_WITH_TRACE(SavedValues, Pool->PoolId, Pool->GetName(), "elapsed", UNROLL_HISTORY(threadInfo.ElapsedCpu.History));
        threadInfo.UsedCpu.Register(ts, cpuConsumption.CpuUs / 1'000'000.0);
        LWPROBE_WITH_TRACE(SavedValues, Pool->PoolId, Pool->GetName(), "cpu", UNROLL_HISTORY(threadInfo.UsedCpu.History));
    }
    TVector<TExecutorThreadStats> sharedStats;
    if (Shared) {
        Shared->GetSharedStatsForHarmonizer(Pool->PoolId, sharedStats);
    }

    for (ui32 sharedIdx = 0; sharedIdx < SharedInfo.size(); ++sharedIdx) {
        auto stat = sharedStats[sharedIdx];
        TCpuConsumption sharedConsumption{
            static_cast<float>(stat.CpuUs),
            Ts2Us(stat.SafeElapsedTicks),
            stat.NotEnoughCpuExecutions
        };
        acc.Add(sharedConsumption);
        SharedInfo[sharedIdx].ElapsedCpu.Register(ts, sharedConsumption.ElapsedUs / 1'000'000.0);
        LWPROBE_WITH_TRACE(SavedValues, Pool->PoolId, Pool->GetName(), "elapsed", UNROLL_HISTORY(SharedInfo[sharedIdx].ElapsedCpu.History));
        SharedInfo[sharedIdx].UsedCpu.Register(ts, sharedConsumption.CpuUs / 1'000'000.0);
        LWPROBE_WITH_TRACE(SavedValues, Pool->PoolId, Pool->GetName(), "cpu", UNROLL_HISTORY(SharedInfo[sharedIdx].UsedCpu.History));
    }

    UsedCpu.Register(ts, acc.CpuUs / 1'000'000.0);
    MaxUsedCpu.store(UsedCpu.GetMax(), std::memory_order_relaxed);
    MinUsedCpu.store(UsedCpu.GetMin(), std::memory_order_relaxed);
    AvgUsedCpu.store(UsedCpu.GetAvgPart(), std::memory_order_relaxed);
    ElapsedCpu.Register(ts, acc.ElapsedUs / 1'000'000.0);
    MaxElapsedCpu.store(ElapsedCpu.GetMax(), std::memory_order_relaxed);
    MinElapsedCpu.store(ElapsedCpu.GetMin(), std::memory_order_relaxed);
    AvgElapsedCpu.store(ElapsedCpu.GetAvgPart(), std::memory_order_relaxed);
    NewNotEnoughCpuExecutions = acc.NotEnoughCpuExecutions - NotEnoughCpuExecutions;
    NotEnoughCpuExecutions = acc.NotEnoughCpuExecutions;
    if (WaitingStats && BasicPool) {
        WaitingStats->Clear();
        BasicPool->GetWaitingStats(*WaitingStats);
        if constexpr (!NFeatures::TSpinFeatureFlags::CalcPerThread) {
            MovingWaitingStats->Add(*WaitingStats, 0.8, 0.2);
        }
    }
    return acc;
}
#undef UNROLL_HISTORY

float TPoolInfo::GetThreadCount() {
    return Pool->GetThreadCount();
}

i16 TPoolInfo::GetFullThreadCount() {
    return Pool->GetFullThreadCount();
}

void TPoolInfo::SetFullThreadCount(i16 threadCount) {
    HARMONIZER_DEBUG_PRINT(Pool->PoolId, Pool->GetName(), "set full thread count", threadCount);
    Pool->SetFullThreadCount(threadCount);
}

bool TPoolInfo::IsAvgPingGood() {
    bool res = true;
    if (AvgPingCounter) {
        res &= *AvgPingCounter > MaxAvgPingUs;
    }
    if (AvgPingCounterWithSmallWindow) {
        res &= *AvgPingCounterWithSmallWindow > MaxAvgPingUs;
    }
    return res;
}

} // namespace NActors
