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
        return ThreadInfo[threadIdx].CpuUs.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetSharedCpu(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].CpuUs.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondCpu(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].CpuUs.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondSharedCpu(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].CpuUs.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetElapsed(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].ElapsedUs.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetSharedElapsed(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].ElapsedUs.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondElapsed(i16 threadIdx) const {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].ElapsedUs.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondSharedElapsed(i16 sharedThreadIdx) const {
    if ((size_t)sharedThreadIdx < SharedInfo.size()) {
        return SharedInfo[sharedThreadIdx].ElapsedUs.GetAvgPartForLastSeconds(1);
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
        threadInfo.ElapsedUs.Register(ts, cpuConsumption.ElapsedUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "elapsed", UNROLL_HISTORY(threadInfo.ElapsedUs.History));
        threadInfo.CpuUs.Register(ts, cpuConsumption.CpuUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "cpu", UNROLL_HISTORY(threadInfo.CpuUs.History));
    }
    TVector<TExecutorThreadStats> sharedStats;
    if (Shared) {
        Shared->GetSharedStatsForHarmonizer(Pool->PoolId, sharedStats);
    }

    for (ui32 sharedIdx = 0; sharedIdx < SharedInfo.size(); ++sharedIdx) {
        auto stat = sharedStats[sharedIdx];
        TCpuConsumption sharedConsumption{
            Ts2Us(stat.SafeElapsedTicks),
            static_cast<double>(stat.CpuUs),
            stat.NotEnoughCpuExecutions
        };
        acc.Add(sharedConsumption);
        SharedInfo[sharedIdx].ElapsedUs.Register(ts, sharedConsumption.ElapsedUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "elapsed", UNROLL_HISTORY(SharedInfo[sharedIdx].ElapsedUs.History));
        SharedInfo[sharedIdx].CpuUs.Register(ts, sharedConsumption.CpuUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "cpu", UNROLL_HISTORY(SharedInfo[sharedIdx].CpuUs.History));
    }

    CpuUs.Register(ts, acc.CpuUs);
    MaxCpuUs.store(CpuUs.GetMax() / 1'000'000, std::memory_order_relaxed);
    MinCpuUs.store(CpuUs.GetMin() / 1'000'000, std::memory_order_relaxed);
    AvgCpuUs.store(CpuUs.GetAvgPart() / 1'000'000, std::memory_order_relaxed);
    ElapsedUs.Register(ts, acc.ElapsedUs);
    MaxElapsedUs.store(ElapsedUs.GetMax() / 1'000'000, std::memory_order_relaxed);
    MinElapsedUs.store(ElapsedUs.GetMin() / 1'000'000, std::memory_order_relaxed);
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
