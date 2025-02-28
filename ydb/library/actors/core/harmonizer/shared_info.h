#pragma once

#include "defs.h"

namespace NActors {

class ISharedPool;
struct TExecutorThreadStats;


struct TPoolSharedThreadCpuConsumption {
    float Elapsed = 0;
    float Cpu = 0;
    float CpuQuota = 0;

    TString ToString() const;
};

struct TSharedThreadCpuConsumptionByPool {
    ui64 Elapsed = 0;
    ui64 Cpu = 0;
    ui64 Parked = 0;
    ui64 DiffElapsed = 0;
    ui64 DiffCpu = 0;
    ui64 DiffParked = 0;

    TString ToString() const;
};

struct TSharedInfo {
    i16 PoolCount = 0;
    std::vector<i16> ForeignThreadsAllowed;
    std::vector<i16> OwnedThreads;
    std::vector<i16> ThreadOwners;
    std::vector<TPoolSharedThreadCpuConsumption> CpuConsumption;
    std::vector<std::vector<TSharedThreadCpuConsumptionByPool>> CpuConsumptionByPool;
    TVector<TExecutorThreadStats> ThreadStats; // for pulling only

    void Init(i16 poolCount, const ISharedPool *shared);
    void Pull(const ISharedPool& shared);

    TString ToString() const;
}; // struct TSharedInfo

} // namespace NActors
