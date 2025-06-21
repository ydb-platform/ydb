#include "shared_info.h"
#include "pool.h"
#include <util/string/builder.h>

#include <ydb/library/actors/core/executor_pool_shared.h>

namespace NActors {

void TSharedInfo::Pull(const std::vector<std::unique_ptr<TPoolInfo>> &pools, const ISharedPool& shared) {
    shared.FillForeignThreadsAllowed(ForeignThreadsAllowed);

    for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
        for (i16 threadId = 0; threadId < ThreadCount; ++threadId) {
            CpuConsumptionPerThread[poolId][threadId].Elapsed = pools[poolId]->SharedInfo[threadId].ElapsedCpu.GetAvgPartForLastSeconds<true>(1);
            CpuConsumptionPerThread[poolId][threadId].Cpu = pools[poolId]->SharedInfo[threadId].UsedCpu.GetAvgPartForLastSeconds<true>(1);
        }
    }

    FreeCpu = 0.0;
    for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
        CpuConsumption[poolId].Elapsed = 0;
        CpuConsumption[poolId].Cpu = 0;
        CpuConsumption[poolId].CpuQuota = 0;
    }

    for (i16 threadId = 0; threadId < ThreadCount; ++threadId) {
        float parked = 1;
        for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
            parked -= CpuConsumptionPerThread[poolId][threadId].Elapsed;
            CpuConsumption[poolId].Elapsed += CpuConsumptionPerThread[poolId][threadId].Elapsed;
            CpuConsumption[poolId].Cpu += CpuConsumptionPerThread[poolId][threadId].Cpu;
            if (poolId != ThreadOwners[threadId]) {
                CpuConsumption[poolId].ForeignElapsed += CpuConsumptionPerThread[poolId][threadId].Elapsed;
                CpuConsumption[poolId].ForeignCpu += CpuConsumptionPerThread[poolId][threadId].Cpu;
            }
        }
        CpuConsumption[ThreadOwners[threadId]].CpuQuota += parked;
        FreeCpu += parked;
    }

    for (i16 poolId = 0; poolId < PoolCount; ++poolId) {
        CpuConsumption[poolId].CpuQuota += CpuConsumption[poolId].Elapsed;
    }
}

void TSharedInfo::Init(i16 poolCount, const ISharedPool *shared) {
    PoolCount = poolCount;
    ForeignThreadsAllowed.resize(poolCount);
    OwnedThreads.resize(poolCount);
    CpuConsumption.resize(poolCount);
    if (shared) {
        shared->FillThreadOwners(ThreadOwners);
        shared->FillOwnedThreads(OwnedThreads);
        ThreadCount = ThreadOwners.size();
        CpuConsumptionPerThread.resize(poolCount);
        for (i16 i = 0; i < poolCount; ++i) {
            CpuConsumptionPerThread[i].resize(ThreadCount);
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
    builder << "} CpuConsumptionPerThread: {";
    for (ui32 i = 0; i < CpuConsumptionPerThread.size(); ++i) {
        builder << "Pool[" << i << "]: {";
        for (ui32 j = 0; j < CpuConsumptionPerThread[i].size(); ++j) {
            builder << "Thread[" << j << "]: " << CpuConsumptionPerThread[i][j].ToString() << "; ";
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
    builder << " ForeignElapsed: " << ForeignElapsed << "; ";
    builder << " ForeignCpu: " << ForeignCpu << "; ";
    builder << "}";
    return builder;
}

TString TSharedThreadCpuConsumptionByPool::ToString() const {
    TStringBuilder builder;
    builder << "{";
    builder << " Elapsed: " << Elapsed << "; ";
    builder << " Cpu: " << Cpu << "; ";
    builder << "}";
    return builder;
}

} // namespace NActors
