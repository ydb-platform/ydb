#pragma once

#include "defs.h"

#include "history.h"
#include <ydb/library/actors/core/executor_pool.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {

class ISharedPool;
class TBasicExecutorPool;
class IExecutorPool;
template <typename T>
struct TWaitingStats;

struct TThreadInfo {
    TValueHistory<8> UsedCpu;
    TValueHistory<8> ElapsedCpu;
}; // struct TThreadInfo

struct TPoolInfo {
    std::vector<TThreadInfo> ThreadInfo;
    std::vector<TThreadInfo> SharedInfo;
    ISharedPool* Shared = nullptr;
    IExecutorPool* Pool = nullptr;
    TBasicExecutorPool* BasicPool = nullptr;

    i16 DefaultFullThreadCount = 0;
    i16 MinFullThreadCount = 0;
    i16 MaxFullThreadCount = 0;

    float ThreadQuota = 0;
    float DefaultThreadCount = 0;
    float MinThreadCount = 0;
    float MaxThreadCount = 0;

    ui16 LocalQueueSize;
    ui16 MaxLocalQueueSize = 0;
    ui16 MinLocalQueueSize = 0;

    i16 Priority = 0;
    NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounter;
    NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounterWithSmallWindow;
    ui32 MaxAvgPingUs = 0;
    ui64 LastUpdateTs = 0;
    ui64 NotEnoughCpuExecutions = 0;
    ui64 NewNotEnoughCpuExecutions = 0;

    std::atomic<float> SharedCpuQuota = 0;
    std::atomic<i64> LastFlags = 0; // 0 - isNeedy; 1 - isStarved; 2 - isHoggish
    std::atomic<ui64> IncreasingThreadsByNeedyState = 0;
    std::atomic<ui64> IncreasingThreadsByExchange = 0;
    std::atomic<ui64> DecreasingThreadsByStarvedState = 0;
    std::atomic<ui64> DecreasingThreadsByHoggishState = 0;
    std::atomic<ui64> DecreasingThreadsByExchange = 0;
    std::atomic<float> PotentialMaxThreadCount = 0;

    TValueHistory<16> UsedCpu;
    TValueHistory<16> ElapsedCpu;

    std::atomic<float> MaxUsedCpu = 0;
    std::atomic<float> MinUsedCpu = 0;
    std::atomic<float> AvgUsedCpu = 0;
    std::atomic<float> MaxElapsedCpu = 0;
    std::atomic<float> MinElapsedCpu = 0;
    std::atomic<float> AvgElapsedCpu = 0;

    std::unique_ptr<TWaitingStats<ui64>> WaitingStats;
    std::unique_ptr<TWaitingStats<double>> MovingWaitingStats;

    TPoolInfo();

    double GetCpu(i16 threadIdx) const;
    double GetElapsed(i16 threadIdx) const;
    double GetLastSecondCpu(i16 threadIdx) const;
    double GetLastSecondElapsed(i16 threadIdx) const;

    double GetSharedCpu(i16 sharedThreadIdx) const;
    double GetSharedElapsed(i16 sharedThreadIdx) const;
    double GetLastSecondSharedCpu(i16 sharedThreadIdx) const;
    double GetLastSecondSharedElapsed(i16 sharedThreadIdx) const;

    TCpuConsumption PullStats(ui64 ts);
    i16 GetFullThreadCount();
    float GetThreadCount();
    void SetFullThreadCount(i16 threadCount);
    bool IsAvgPingGood();
}; // struct TPoolInfo

} // namespace NActors
