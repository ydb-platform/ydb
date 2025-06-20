#pragma once

#include "defs.h"

namespace NActors {

class ISharedPool;
struct TExecutorThreadStats;
struct TPoolInfo;


struct TPoolSharedThreadCpuConsumption {
    float Elapsed = 0;
    float Cpu = 0;
    float CpuQuota = 0;
    float ForeignElapsed = 0;
    float ForeignCpu = 0;

    TString ToString() const;
};

struct TSharedThreadCpuConsumptionByPool {
    float Elapsed = 0;
    float Cpu = 0;

    TString ToString() const;
};

struct TSharedInfo {
    i16 PoolCount = 0;
    i16 ThreadCount = 0;
    std::vector<i16> ForeignThreadsAllowed;
    std::vector<i16> OwnedThreads;
    std::vector<i16> ThreadOwners;
    std::vector<TPoolSharedThreadCpuConsumption> CpuConsumption;
    std::vector<std::vector<TSharedThreadCpuConsumptionByPool>> CpuConsumptionPerThread;
    TVector<TExecutorThreadStats> ThreadStats; // for pulling only
    float FreeCpu = 0.0;
    float FreeCpuLS = 0.0;

    void Init(i16 poolCount, const ISharedPool *shared);
    void Pull(const std::vector<std::unique_ptr<TPoolInfo>>& pools, const ISharedPool& shared);

    TString ToString() const;
}; // struct TSharedInfo

} // namespace NActors
