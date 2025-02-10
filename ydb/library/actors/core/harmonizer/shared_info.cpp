#include "shared_info.h"
#include <util/string/builder.h>

#include <ydb/library/actors/core/executor_pool_shared.h>

namespace NActors {

void TSharedInfo::Pull(const ISharedPool& shared) {
    shared.FillForeignThreadsAllowed(ForeignThreadsAllowed);
    shared.FillOwnedThreads(OwnedThreads);

    for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
        shared.GetSharedStatsForHarmonizer(poolId, ThreadStats);
        for (i16 threadId = 0; threadId < static_cast<i16>(ThreadStats.size()); ++threadId) {
            ui64 elapsed = std::exchange(CpuConsumptionByPool[poolId][threadId].Elapsed, ThreadStats[threadId].SafeElapsedTicks);
            CpuConsumptionByPool[poolId][threadId].DiffElapsed = CpuConsumptionByPool[poolId][threadId].Elapsed - elapsed;
            ui64 cpu = std::exchange(CpuConsumptionByPool[poolId][threadId].Cpu, ThreadStats[threadId].CpuUs);
            CpuConsumptionByPool[poolId][threadId].DiffCpu = CpuConsumptionByPool[poolId][threadId].Cpu - cpu;
            ui64 parked = std::exchange(CpuConsumptionByPool[poolId][threadId].Parked, ThreadStats[threadId].SafeParkedTicks);
            CpuConsumptionByPool[poolId][threadId].DiffParked = CpuConsumptionByPool[poolId][threadId].Parked - parked;
        }
    }

    bool isFirst = true;

    for (i16 threadId = 0; threadId < static_cast<i16>(ThreadStats.size()); ++threadId) {
        TStackVec<ui64, 8> elapsedByPool;
        TStackVec<ui64, 8> cpuByPool;
        ui64 parked = 0;

        for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
            elapsedByPool.push_back(CpuConsumptionByPool[poolId][threadId].DiffElapsed);
            cpuByPool.push_back(CpuConsumptionByPool[poolId][threadId].DiffCpu);
            parked += CpuConsumptionByPool[poolId][threadId].DiffParked;
        }

        ui64 threadTime = std::accumulate(elapsedByPool.begin(), elapsedByPool.end(), 0ull) + parked;
        if (threadTime == 0) {
            continue;
        } else if (isFirst) {
            isFirst = false;
            for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
                CpuConsumption[poolId].Elapsed = 0;
                CpuConsumption[poolId].Cpu = 0;
                CpuConsumption[poolId].CpuQuota = 0;
            }
        }

        
        for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
            float elapsedCpu = static_cast<float>(CpuConsumptionByPool[poolId][threadId].DiffElapsed) / threadTime;
            float cpu = static_cast<float>(CpuConsumptionByPool[poolId][threadId].DiffCpu) / threadTime;
            CpuConsumption[poolId].Elapsed += elapsedCpu;
            CpuConsumption[poolId].Cpu += cpu;
            CpuConsumption[poolId].CpuQuota += elapsedCpu;
        }
        float parkedCpu = static_cast<float>(parked) / threadTime;
        CpuConsumption[ThreadOwners[threadId]].CpuQuota += parkedCpu;
    }
}

void TSharedInfo::Init(i16 poolCount, const ISharedPool *shared) {
    PoolCount = poolCount;
    ForeignThreadsAllowed.resize(poolCount);
    OwnedThreads.resize(poolCount);
    CpuConsumption.resize(poolCount);
    if (shared) {
        shared->FillThreadOwners(ThreadOwners);
        CpuConsumptionByPool.resize(poolCount);
        for (i16 i = 0; i < poolCount; ++i) {
            CpuConsumptionByPool[i].resize(ThreadOwners.size());
        }
        ThreadStats.resize(ThreadOwners.size());
        for (ui32 i = 0; i < ThreadOwners.size(); ++i) {
            i16 owner = ThreadOwners[i];
            if (owner >= 0 && owner < poolCount) {
                CpuConsumption[owner].CpuQuota += 1.0f;
            }
        }
    }
}

TString TSharedInfo::ToString() const {
    TStringBuilder builder;
    builder << "{";
    builder << " ForeignThreadsAllowed: {";
    for (ui32 i = 0; i < ForeignThreadsAllowed.size(); ++i) {
        builder << "Pool[" << i << "]: " << ForeignThreadsAllowed[i] << "; ";
    }
    builder << "} OwnedThreads: {";
    for (ui32 i = 0; i < OwnedThreads.size(); ++i) {
        builder << "Pool[" << i << "]: " << OwnedThreads[i] << "; ";
    }
    builder << "} CpuConsumption: {";
    for (ui32 i = 0; i < CpuConsumption.size(); ++i) {
        builder << "Pool[" << i << "]: " << CpuConsumption[i].ToString() << "; ";
    }
    builder << "} CpuConsumptionByPool: {";
    for (ui32 i = 0; i < CpuConsumptionByPool.size(); ++i) {
        builder << "Pool[" << i << "]: {";
        for (ui32 j = 0; j < CpuConsumptionByPool[i].size(); ++j) {
            builder << "Thread[" << j << "]: " << CpuConsumptionByPool[i][j].ToString() << "; ";
        }
        builder << "} ";
    }
    builder << "} }";
    return builder;
}

TString TPoolSharedThreadCpuConsumption::ToString() const {
    TStringBuilder builder;
    builder << "{";
    builder << " Elapsed: " << Elapsed << "; ";
    builder << " Cpu: " << Cpu << "; ";
    builder << " CpuQuota: " << CpuQuota << "; ";
    builder << "}";
    return builder;
}

TString TSharedThreadCpuConsumptionByPool::ToString() const {
    TStringBuilder builder;
    builder << "{";
    builder << " Elapsed: " << Elapsed << "; ";
    builder << " Cpu: " << Cpu << "; ";
    builder << " Parked: " << Parked << "; ";
    builder << " DiffElapsed: " << DiffElapsed << "; ";
    builder << " DiffCpu: " << DiffCpu << "; ";
    builder << " DiffParked: " << DiffParked << "; ";
    builder << "}";
    return builder;
}

} // namespace NActors
