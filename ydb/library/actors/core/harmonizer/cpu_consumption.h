#pragma once

#include "defs.h"
#include "pool.h"
#include "shared_info.h"
namespace NActors {

struct TPoolInfo;

struct TCpuConsumptionInfo {
    float Elapsed;
    float Cpu;
    float LastSecondElapsed;
    float LastSecondCpu;

    void Clear();
}; // struct TCpuConsumptionInfo

struct TPoolForeignConsumptionInfo {
    float Elapsed;
    float Cpu;
    float PrevElapsedValue;
    float PrevCpuValue;
}; // struct TPoolForeignConsumptionInfo

struct THarmonizerCpuConsumption {
    std::vector<TCpuConsumptionInfo> PoolConsumption;
    std::vector<TCpuConsumptionInfo> PoolFullThreadConsumption;
    std::vector<TPoolForeignConsumptionInfo> PoolForeignConsumption;

    float TotalCores = 0;
    i16 AdditionalThreads = 0;
    i16 StoppingThreads = 0;
    bool IsStarvedPresent = false;

    float Budget = 0.0;
    float BudgetLS = 0.0;
    float BudgetWithoutSharedCpu = 0.0;
    float BudgetLSWithoutSharedCpu = 0.0;

    float Overbooked = 0.0;
    float LostCpu = 0.0;

    float Elapsed = 0.0;
    float Cpu = 0.0;
    float LastSecondElapsed = 0.0;
    float LastSecondCpu = 0.0;
    TStackVec<i16, 8> NeedyPools;
    TStackVec<std::pair<i16, float>, 8> HoggishPools;
    TStackVec<bool, 8> IsNeedyByPool;
    TStackVec<i16, 8> FreeHalfSharedThreads;

    void Init(i16 poolCount);

    void Pull(const std::vector<std::unique_ptr<TPoolInfo>> &pools, const TSharedInfo& sharedInfo);

}; // struct THarmonizerCpuConsumption

} // namespace NActors
