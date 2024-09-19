#include "harmonizer.h"

#include "executor_thread_ctx.h"
#include "executor_thread.h"
#include "probes.h"

#include "activity_guard.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_shared.h"

#include <atomic>
#include <ydb/library/actors/util/cpu_load_log.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/intrinsics.h>

#include <util/system/spinlock.h>

#include <algorithm>

namespace NActors {

namespace {
    constexpr bool IsDebug = false;
    constexpr bool IsDebugByThread = false;

#define DEBUG_PRINT(x) do { if constexpr (IsDebug) { (Cerr << x); } } while (0)
#define DEBUG_PRINT_BY_THREAD(x) do { if constexpr (IsDebug && IsDebugByThread) { (Cerr << x); } } while (0)

}

LWTRACE_USING(ACTORLIB_PROVIDER);

constexpr bool CheckBinaryPower(ui64 value) {
    return !(value & (value - 1));
}

template <ui8 HistoryBufferSize = 8>
struct TValueHistory {
    static_assert(CheckBinaryPower(HistoryBufferSize));

    double History[HistoryBufferSize] = {0.0};
    ui64 HistoryIdx = 0;
    ui64 LastTs = Max<ui64>();
    double LastUs = 0.0;
    double AccumulatedUs = 0.0;
    ui64 AccumulatedTs = 0;

    template <bool WithTail=false>
    double Accumulate(auto op, auto comb, ui8 seconds) {
        double acc = AccumulatedUs;
        size_t idx = HistoryIdx;
        ui8 leftSeconds = seconds;
        if constexpr (!WithTail) {
            idx--;
            leftSeconds--;
            if (idx >= HistoryBufferSize) {
                idx = HistoryBufferSize - 1;
            }
            acc = History[idx];
        }
        do {
            idx--;
            leftSeconds--;
            if (idx >= HistoryBufferSize) {
                idx = HistoryBufferSize - 1;
            }
            if constexpr (WithTail) {
                acc = op(acc, History[idx]);
            } else if (leftSeconds) {
                acc = op(acc, History[idx]);
            } else {
                ui64 tsInSecond = Us2Ts(1'000'000.0);
                acc = op(acc, History[idx] * (tsInSecond - AccumulatedTs) / tsInSecond);
            }
        } while (leftSeconds);
        double duration = 1'000'000.0 * seconds;
        if constexpr (WithTail) {
            duration += Ts2Us(AccumulatedTs);
        }
        return comb(acc, duration);
    }

    template <bool WithTail=false>
    double GetAvgPartForLastSeconds(ui8 seconds) {
        auto sum = [](double acc, double value) {
            return acc + value;
        };
        auto avg = [](double sum, double duration) {
            return sum / duration;
        };
        return Accumulate<WithTail>(sum, avg, seconds);
    }

    double GetAvgPart() {
        return GetAvgPartForLastSeconds<true>(HistoryBufferSize);
    }

    double GetMaxForLastSeconds(ui8 seconds) {
        auto max = [](const double& acc, const double& value) {
            return Max(acc, value);
        };
        auto fst = [](const double& value, const double&) { return value; };
        return Accumulate<false>(max, fst, seconds);
    }

    double GetMax() {
        return GetMaxForLastSeconds(HistoryBufferSize);
    }

    i64 GetMaxInt() {
        return static_cast<i64>(GetMax());
    }

    double GetMinForLastSeconds(ui8 seconds) {
        auto min = [](const double& acc, const double& value) {
            return Min(acc, value);
        };
        auto fst = [](const double& value, const double&) { return value; };
        return Accumulate<false>(min, fst, seconds);
    }

    double GetMin() {
        return GetMinForLastSeconds(HistoryBufferSize);
    }

    i64 GetMinInt() {
        return static_cast<i64>(GetMin());
    }

    void Register(ui64 ts, double valueUs) {
        if (ts < LastTs) {
            LastTs = ts;
            LastUs = valueUs;
            AccumulatedUs = 0.0;
            AccumulatedTs = 0;
            return;
        }
        ui64 lastTs = std::exchange(LastTs, ts);
        ui64 dTs = ts - lastTs;
        double lastUs = std::exchange(LastUs, valueUs);
        double dUs = valueUs - lastUs;
        LWPROBE(RegisterValue, ts, lastTs, dTs, Us2Ts(8'000'000.0), valueUs, lastUs, dUs);

        if (dTs > Us2Ts(8'000'000.0)) {
            dUs = dUs * 1'000'000.0 / Ts2Us(dTs);
            for (size_t idx = 0; idx < HistoryBufferSize; ++idx) {
                History[idx] = dUs;
            }
            AccumulatedUs = 0.0;
            AccumulatedTs = 0;
            return;
        }

        while (dTs > 0) {
            if (AccumulatedTs + dTs < Us2Ts(1'000'000.0)) {
                AccumulatedTs += dTs;
                AccumulatedUs += dUs;
                break;
            } else {
                ui64 addTs = Us2Ts(1'000'000.0) - AccumulatedTs;
                double addUs = dUs * addTs / dTs;
                dTs -= addTs;
                dUs -= addUs;
                History[HistoryIdx] = AccumulatedUs + addUs;
                HistoryIdx = (HistoryIdx + 1) % HistoryBufferSize;
                AccumulatedUs = 0.0;
                AccumulatedTs = 0;
            }
        }
    }
};

struct TThreadInfo {
    TValueHistory<8> Elapsed;
    TValueHistory<8> Cpu;
};

struct TPoolInfo {
    std::vector<TThreadInfo> ThreadInfo;
    std::vector<TThreadInfo> SharedInfo;
    TSharedExecutorPool* Shared = nullptr;
    IExecutorPool* Pool = nullptr;
    TBasicExecutorPool* BasicPool = nullptr;

    i16 DefaultFullThreadCount = 0;
    i16 MinFullThreadCount = 0;
    i16 MaxFullThreadCount = 0;

    float DefaultThreadCount = 0;
    float MinThreadCount = 0;
    float MaxThreadCount = 0;

    i16 Priority = 0;
    NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounter;
    NMonitoring::TDynamicCounters::TCounterPtr AvgPingCounterWithSmallWindow;
    ui32 MaxAvgPingUs = 0;
    ui64 LastUpdateTs = 0;
    ui64 NotEnoughCpuExecutions = 0;
    ui64 NewNotEnoughCpuExecutions = 0;
    ui16 LocalQueueSize = NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE;

    TAtomic LastFlags = 0; // 0 - isNeedy; 1 - isStarved; 2 - isHoggish
    TAtomic IncreasingThreadsByNeedyState = 0;
    TAtomic IncreasingThreadsByExchange = 0;
    TAtomic DecreasingThreadsByStarvedState = 0;
    TAtomic DecreasingThreadsByHoggishState = 0;
    TAtomic DecreasingThreadsByExchange = 0;
    TAtomic PotentialMaxThreadCount = 0;

    TValueHistory<16> Elapsed;
    TValueHistory<16> Cpu;

    std::atomic<float> MaxElapsed = 0;
    std::atomic<float> MinElapsed = 0;
    std::atomic<float> AvgElapsed = 0;
    std::atomic<float> MaxCpu = 0;
    std::atomic<float> MinCpu = 0;

    std::unique_ptr<TWaitingStats<ui64>> WaitingStats;
    std::unique_ptr<TWaitingStats<double>> MovingWaitingStats;

    double GetElapsed(i16 threadIdx);
    double GetSharedElapsed(i16 threadIdx);
    double GetLastSecondElapsed(i16 threadIdx);
    double GetLastSecondSharedElapsed(i16 threadIdx);
    double GetCpu(i16 threadIdx);
    double GetSharedCpu(i16 threadIdx);
    double GetLastSecondCpu(i16 threadIdx);
    double GetLastSecondSharedCpu(i16 threadIdx);
    TCpuConsumption PullStats(ui64 ts);
    i16 GetFullThreadCount();
    float GetThreadCount();
    void SetFullThreadCount(i16 threadCount);
    bool IsAvgPingGood();
};

double TPoolInfo::GetCpu(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Cpu.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetSharedCpu(i16 threadIdx) {
    if ((size_t)threadIdx < SharedInfo.size()) {
        return SharedInfo[threadIdx].Cpu.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondCpu(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Cpu.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondSharedCpu(i16 threadIdx) {
    if ((size_t)threadIdx < SharedInfo.size()) {
        return SharedInfo[threadIdx].Cpu.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetElapsed(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Elapsed.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetSharedElapsed(i16 threadIdx) {
    if ((size_t)threadIdx < SharedInfo.size()) {
        return SharedInfo[threadIdx].Elapsed.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondElapsed(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Elapsed.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondSharedElapsed(i16 threadIdx) {
    if ((size_t)threadIdx < SharedInfo.size()) {
        return SharedInfo[threadIdx].Elapsed.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

#define UNROLL_HISTORY(history) (history)[0], (history)[1], (history)[2], (history)[3], (history)[4], (history)[5], (history)[6], (history)[7]
TCpuConsumption TPoolInfo::PullStats(ui64 ts) {
    DEBUG_PRINT("TPoolInfo::PullStats " << ts << " PoolId: " << Pool->PoolId << " Name: " << Pool->GetName() << "\n");
    TCpuConsumption acc;
    for (i16 threadIdx = 0; threadIdx < MaxFullThreadCount; ++threadIdx) {
        DEBUG_PRINT_BY_THREAD("  threadIdx: " << threadIdx << "\n");
        TThreadInfo &threadInfo = ThreadInfo[threadIdx];
        TCpuConsumption cpuConsumption = Pool->GetThreadCpuConsumption(threadIdx);
        DEBUG_PRINT_BY_THREAD("    CpuUs: " << cpuConsumption.CpuUs << " ElapsedUs: " << cpuConsumption.ElapsedUs << " NotEnoughCpuExecutions: " << cpuConsumption.NotEnoughCpuExecutions << "\n");
        acc.Add(cpuConsumption);
        threadInfo.Elapsed.Register(ts, cpuConsumption.ElapsedUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "consumed", UNROLL_HISTORY(threadInfo.Elapsed.History));
        threadInfo.Cpu.Register(ts, cpuConsumption.CpuUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "booked", UNROLL_HISTORY(threadInfo.Cpu.History));
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
        SharedInfo[sharedIdx].Elapsed.Register(ts, sharedConsumption.ElapsedUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "shared_consumed", UNROLL_HISTORY(SharedInfo[sharedIdx].Elapsed.History));
        SharedInfo[sharedIdx].Cpu.Register(ts, sharedConsumption.CpuUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "shared_booked", UNROLL_HISTORY(SharedInfo[sharedIdx].Cpu.History));
    }

    DEBUG_PRINT("  ElapsedUs: " << acc.ElapsedUs << " CpuUs: " << acc.CpuUs << "\n");

    Elapsed.Register(ts, acc.ElapsedUs);
    MaxElapsed.store(Elapsed.GetMax() / 1'000'000, std::memory_order_relaxed);
    MinElapsed.store(Elapsed.GetMin() / 1'000'000, std::memory_order_relaxed);
    AvgElapsed.store(Elapsed.GetAvgPart() / 1'000'000, std::memory_order_relaxed);
    Cpu.Register(ts, acc.CpuUs);
    MaxCpu.store(Cpu.GetMax() / 1'000'000, std::memory_order_relaxed);
    MinCpu.store(Cpu.GetMin() / 1'000'000, std::memory_order_relaxed);
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

class THarmonizer: public IHarmonizer {
private:
    std::atomic<bool> IsDisabled = false;
    TSpinLock Lock;
    std::atomic<ui64> NextHarmonizeTs = 0;
    std::vector<std::unique_ptr<TPoolInfo>> Pools;
    std::vector<ui16> PriorityOrder;

    TValueHistory<16> Elapsed;
    TValueHistory<16> Cpu;

    TAtomic MaxElapsedCpu = 0;
    TAtomic MinElapsedCpu = 0;
    TAtomic MaxCpu = 0;
    TAtomic MinCpu = 0;

    TSharedExecutorPool* Shared = nullptr;

    std::atomic<double> AvgAwakeningTimeUs = 0;
    std::atomic<double> AvgWakingUpTimeUs = 0;

    void PullStats(ui64 ts);
    void HarmonizeImpl(ui64 ts);
    void CalculatePriorityOrder();
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
    void SetSharedPool(TSharedExecutorPool* pool) override;
};

THarmonizer::THarmonizer(ui64 ts) {
    NextHarmonizeTs = ts;
}

THarmonizer::~THarmonizer() {
}

double THarmonizer::Rescale(double value) const {
  return Max(0.0, Min(1.0, value * (1.0/0.9)));
}

void THarmonizer::PullStats(ui64 ts) {
    DEBUG_PRINT("THarmonizer::PullStats " << ts << "\n");
    TCpuConsumption acc;
    for (auto &pool : Pools) {
        TCpuConsumption consumption = pool->PullStats(ts);
        acc.Add(consumption);
    }
    Elapsed.Register(ts, acc.ElapsedUs);
    RelaxedStore(&MaxElapsedCpu, Elapsed.GetMaxInt());
    RelaxedStore(&MinElapsedCpu, Elapsed.GetMinInt());
    Cpu.Register(ts, acc.CpuUs);
    RelaxedStore(&MaxCpu, Cpu.GetMaxInt());
    RelaxedStore(&MinCpu, Cpu.GetMinInt());
}

Y_FORCE_INLINE bool IsStarved(double elapsed, double cpu) {
    return std::max(elapsed, cpu) > 0.5 && elapsed > cpu + std::min(0.5, elapsed * 0.3);
}

Y_FORCE_INLINE bool IsHoggish(double cpu, double currentThreadCount) {
    return cpu < currentThreadCount - 0.5;
}

void THarmonizer::HarmonizeImpl(ui64 ts) {
    DEBUG_PRINT("THarmonizer::HarmonizeImpl " << ts << "\n");
    bool isStarvedPresent = false;
    double cpu = 0.0;
    double elapsed = 0.0;
    double lastSecondCpu = 0.0;
    double lastSecondElapsed = 0.0;
    i64 beingStopped = 0;
    double total = 0;
    TStackVec<size_t, 8> needyPools;
    TStackVec<std::pair<size_t, double>, 8> hoggishPools;
    TStackVec<bool, 8> isNeedyByPool;

    size_t sumOfAdditionalThreads = 0;

    ui64 TotalWakingUpTime = 0;
    ui64 TotalWakingUps = 0;
    ui64 TotalAwakeningTime = 0;
    ui64 TotalAwakenings = 0;
    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = *Pools[poolIdx];
        if (pool.WaitingStats) {
            TotalWakingUpTime += pool.WaitingStats->WakingUpTotalTime;
            TotalWakingUps += pool.WaitingStats->WakingUpCount;
            TotalAwakeningTime += pool.WaitingStats->AwakingTotalTime;
            TotalAwakenings += pool.WaitingStats->AwakingCount;
        }
    }

    constexpr ui64 knownAvgWakingUpTime = TWaitingStatsConstants::KnownAvgWakingUpTime;
    constexpr ui64 knownAvgAwakeningUpTime = TWaitingStatsConstants::KnownAvgAwakeningTime;

    ui64 realAvgWakingUpTime = (TotalWakingUps ? TotalWakingUpTime / TotalWakingUps : knownAvgWakingUpTime);
    ui64 avgWakingUpTime = realAvgWakingUpTime;
    if (avgWakingUpTime > 2 * knownAvgWakingUpTime || !realAvgWakingUpTime) {
        avgWakingUpTime = knownAvgWakingUpTime;
    }
    AvgWakingUpTimeUs = Ts2Us(avgWakingUpTime);

    ui64 realAvgAwakeningTime = (TotalAwakenings ? TotalAwakeningTime / TotalAwakenings : knownAvgAwakeningUpTime);
    ui64 avgAwakeningTime = realAvgAwakeningTime;
    if (avgAwakeningTime > 2 * knownAvgAwakeningUpTime || !realAvgAwakeningTime) {
        avgAwakeningTime = knownAvgAwakeningUpTime;
    }
    AvgAwakeningTimeUs = Ts2Us(avgAwakeningTime);

    ui64 avgWakingUpConsumption = avgWakingUpTime + avgAwakeningTime;
    LWPROBE(WakingUpConsumption, Ts2Us(avgWakingUpTime), Ts2Us(avgWakingUpTime), Ts2Us(avgAwakeningTime), Ts2Us(realAvgAwakeningTime), Ts2Us(avgWakingUpConsumption));

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = *Pools[poolIdx];
        if (!pool.BasicPool) {
            continue;
        }
        if (pool.BasicPool->ActorSystemProfile != EASProfile::Default) {
            if constexpr (NFeatures::TSpinFeatureFlags::CalcPerThread) {
                pool.BasicPool->CalcSpinPerThread(avgWakingUpConsumption);
            } else if constexpr (NFeatures::TSpinFeatureFlags::UsePseudoMovingWindow) {
                ui64 newSpinThreshold = pool.MovingWaitingStats->CalculateGoodSpinThresholdCycles(avgWakingUpConsumption);
                pool.BasicPool->SetSpinThresholdCycles(newSpinThreshold);
            } else {
                ui64 newSpinThreshold = pool.WaitingStats->CalculateGoodSpinThresholdCycles(avgWakingUpConsumption);
                pool.BasicPool->SetSpinThresholdCycles(newSpinThreshold);
            }
            pool.BasicPool->ClearWaitingStats();
        }
    }

    std::vector<bool> hasSharedThread(Pools.size());
    std::vector<bool> hasSharedThreadWhichWasNotBorrowed(Pools.size());
    std::vector<bool> hasBorrowedSharedThread(Pools.size());
    std::vector<i16> freeHalfThread;
    if (Shared) {
        auto sharedState = Shared->GetState();
        for (ui32 poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
            i16 threadIdx = sharedState.ThreadByPool[poolIdx];
            if (threadIdx != -1) {
                hasSharedThread[poolIdx] = true;
                if (sharedState.PoolByBorrowedThread[threadIdx] == -1) {
                    hasSharedThreadWhichWasNotBorrowed[poolIdx] = true;
                }

            }
            if (sharedState.BorrowedThreadByPool[poolIdx] != -1) {
                hasBorrowedSharedThread[poolIdx] = true;
            }
        }
    }

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        DEBUG_PRINT("  poolIdx: " << poolIdx << " PoolId: " << Pools[poolIdx]->Pool->PoolId << " Name: " << Pools[poolIdx]->Pool->GetName() << "\n");
        TPoolInfo& pool = *Pools[poolIdx];
        total += pool.DefaultThreadCount;

        i16 currentFullThreadCount = pool.GetFullThreadCount();
        DEBUG_PRINT("    currentFullThreadCount: " << currentFullThreadCount << "\n");
        sumOfAdditionalThreads += Max(0, currentFullThreadCount - pool.DefaultFullThreadCount);
        DEBUG_PRINT("    sumOfAdditionalThreads: " << sumOfAdditionalThreads << "\n");
        float currentThreadCount = pool.GetThreadCount();
        DEBUG_PRINT("    currentThreadCount: " << currentThreadCount << "\n");
        double poolCpu = 0.0;
        double poolElapsed = 0.0;
        double lastSecondPoolCpu = 0.0;
        double lastSecondPoolElapsed = 0.0;
        beingStopped += pool.Pool->GetBlockingThreadCount();
        DEBUG_PRINT("    beingStopped: " << beingStopped << "\n");
        for (i16 threadIdx = 0; threadIdx < pool.MaxThreadCount; ++threadIdx) {
            DEBUG_PRINT_BY_THREAD("    threadIdx: " << threadIdx << "\n");
            double threadElapsed = Rescale(pool.GetElapsed(threadIdx));
            double threadLastSecondElapsed = Rescale(pool.GetLastSecondElapsed(threadIdx));
            double threadCpu = Rescale(pool.GetCpu(threadIdx));
            double threadLastSecondCpu = Rescale(pool.GetLastSecondCpu(threadIdx));
            DEBUG_PRINT_BY_THREAD("        threadCpu: " << threadCpu << "\n");
            DEBUG_PRINT_BY_THREAD("        threadLastSecondCpu: " << threadLastSecondCpu << "\n");
            DEBUG_PRINT_BY_THREAD("        threadElapsed: " << threadElapsed << "\n");
            DEBUG_PRINT_BY_THREAD("        threadLastSecondElapsed: " << threadLastSecondElapsed << "\n");
            poolElapsed += threadElapsed;
            lastSecondPoolElapsed += threadLastSecondElapsed;
            poolCpu += threadCpu;
            lastSecondPoolCpu += threadLastSecondCpu;
            LWPROBE(HarmonizeCheckPoolByThread, poolIdx, pool.Pool->GetName(), threadIdx, threadCpu, threadElapsed, threadLastSecondCpu, threadLastSecondElapsed);
        }
        
        for (ui32 sharedIdx = 0; sharedIdx < pool.SharedInfo.size(); ++sharedIdx) {
            double sharedElapsed = Rescale(pool.GetSharedElapsed(sharedIdx));
            double sharedLastSecondElapsed = Rescale(pool.GetLastSecondSharedElapsed(sharedIdx));
            double sharedCpu = Rescale(pool.GetSharedCpu(sharedIdx));
            double sharedLastSecondCpu = Rescale(pool.GetLastSecondSharedCpu(sharedIdx));
            poolElapsed += sharedElapsed;
            lastSecondPoolElapsed += sharedLastSecondElapsed;
            poolCpu += sharedCpu;
            lastSecondPoolCpu += sharedLastSecondCpu;
            LWPROBE(HarmonizeCheckPoolByThread, poolIdx, pool.Pool->GetName(), -1 - sharedIdx, sharedCpu, sharedElapsed, sharedLastSecondCpu, sharedLastSecondElapsed);
        }

        DEBUG_PRINT("    poolElapsed: " << poolElapsed << "\n");
        DEBUG_PRINT("    lastSecondPoolElapsed: " << lastSecondPoolElapsed << "\n");
        DEBUG_PRINT("    poolCpu: " << poolCpu << "\n");
        DEBUG_PRINT("    lastSecondPoolCpu: " << lastSecondPoolCpu << "\n");

        bool isStarved = IsStarved(poolElapsed, poolCpu) || IsStarved(lastSecondPoolElapsed, lastSecondPoolCpu);
        if (isStarved) {
            isStarvedPresent = true;
        }
        DEBUG_PRINT("    isStarved: " << isStarved << "\n");

        bool isNeedy = (pool.IsAvgPingGood() || pool.NewNotEnoughCpuExecutions) && (poolCpu >= currentThreadCount);
        if (pool.AvgPingCounter) {
            if (pool.LastUpdateTs + Us2Ts(3'000'000ull) > ts) {
                isNeedy = false;
            } else {
                pool.LastUpdateTs = ts;
            }
        }
        DEBUG_PRINT("    isNeedy: " << isNeedy << "\n");

        if (currentThreadCount - poolCpu > 0.5) {
            if (hasBorrowedSharedThread[poolIdx] || hasSharedThreadWhichWasNotBorrowed[poolIdx]) {
                freeHalfThread.push_back(poolIdx);
            }
        }
        isNeedyByPool.push_back(isNeedy);
        if (isNeedy) {
            needyPools.push_back(poolIdx);
        }

        bool isHoggish = IsHoggish(poolElapsed, currentThreadCount)
                || IsHoggish(lastSecondPoolElapsed, currentThreadCount);
        if (isHoggish) {
            hoggishPools.push_back({poolIdx, std::max(currentThreadCount - poolElapsed, currentThreadCount - lastSecondPoolElapsed)});
        }
        DEBUG_PRINT("    isHoggish: " << isHoggish << "\n");

        cpu += poolCpu;
        elapsed += poolElapsed;
        lastSecondCpu += lastSecondPoolCpu;
        lastSecondElapsed += lastSecondPoolElapsed;
        AtomicSet(pool.LastFlags, (i64)isNeedy | ((i64)isStarved << 1) | ((i64)isHoggish << 2));
        LWPROBE(HarmonizeCheckPool, poolIdx, pool.Pool->GetName(), poolCpu, poolElapsed, lastSecondPoolCpu, lastSecondPoolElapsed, currentThreadCount, pool.MaxFullThreadCount, isStarved, isNeedy, isHoggish);
    }
    Y_UNUSED(lastSecondElapsed);
    DEBUG_PRINT("  total: " << total << " cpu: " << cpu << " lastSecondCpu: " << lastSecondCpu << "\n");

    double budget = total - Max(cpu, lastSecondCpu);
    DEBUG_PRINT("  budget: " << budget << "\n");
    i16 budgetInt = static_cast<i16>(Max(budget, 0.0));
    DEBUG_PRINT("  budgetInt: " << budgetInt << "\n");
    if (budget < -0.1) {
        isStarvedPresent = true;
    }
    double overbooked = elapsed - cpu;
    if (overbooked < 0) {
        isStarvedPresent = false;
    }

    if (needyPools.size()) {
        Sort(needyPools.begin(), needyPools.end(), [&] (i16 lhs, i16 rhs) {
            if (Pools[lhs]->Priority != Pools[rhs]->Priority)  {
                return Pools[lhs]->Priority > Pools[rhs]->Priority;
            }
            return Pools[lhs]->Pool->PoolId < Pools[rhs]->Pool->PoolId;
        });
    }

    if (freeHalfThread.size()) {
        Sort(freeHalfThread.begin(), freeHalfThread.end(), [&] (i16 lhs, i16 rhs) {
            if (Pools[lhs]->Priority != Pools[rhs]->Priority)  {
                return Pools[lhs]->Priority > Pools[rhs]->Priority;
            }
            return Pools[lhs]->Pool->PoolId < Pools[rhs]->Pool->PoolId;
        });
    }

    if (isStarvedPresent) {
        // last_starved_at_consumed_value = сумма по всем пулам consumed;
        // TODO(cthulhu): использовать как лимит планвно устремлять этот лимит к total,
        // использовать вместо total
        DEBUG_PRINT("  isStarvedPresent\n");
        if (beingStopped && beingStopped >= overbooked) {
            // do nothing
        } else {
            for (ui16 poolIdx : PriorityOrder) {
                TPoolInfo &pool = *Pools[poolIdx];
                i64 threadCount = pool.GetFullThreadCount();
                if (hasSharedThread[poolIdx] && !hasSharedThreadWhichWasNotBorrowed[poolIdx]) {
                    Shared->ReturnOwnHalfThread(poolIdx);
                    overbooked -= 0.5;
                }
                while (threadCount > pool.DefaultFullThreadCount) {
                    pool.SetFullThreadCount(--threadCount);
                    AtomicIncrement(pool.DecreasingThreadsByStarvedState);
                    overbooked--;
                    sumOfAdditionalThreads--;

                    LWPROBE(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by starving", threadCount - 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
                    if (overbooked < 1) {
                        break;
                    }
                }
                if (overbooked < 1) {
                    break;
                }
            }
        }
    } else {
        for (size_t needyPoolIdx : needyPools) {
            TPoolInfo &pool = *Pools[needyPoolIdx];
            i64 threadCount = pool.GetFullThreadCount();
            DEBUG_PRINT("  needyPoolIdx: " << needyPoolIdx << " poolId: " << pool.Pool->PoolId << " poolName: " << pool.Pool->GetName() << " threadCount: " << threadCount << " budget: " << budget << "\n");
            if (budget >= 1.0) {
                if (threadCount + 1 <= pool.MaxFullThreadCount) {
                    AtomicIncrement(pool.IncreasingThreadsByNeedyState);
                    isNeedyByPool[needyPoolIdx] = false;
                    sumOfAdditionalThreads++;
                    pool.SetFullThreadCount(threadCount + 1);
                    budget -= 1.0;
                    DEBUG_PRINT("    added 1 thread budget: " << budget << "\n");
                    LWPROBE(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by needs", threadCount + 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
                }
            } else if (Shared && budget >= 0.5 && !hasBorrowedSharedThread[needyPoolIdx] && freeHalfThread.size()) {
                Shared->GiveHalfThread(freeHalfThread.back(), needyPoolIdx);
                freeHalfThread.pop_back();
                isNeedyByPool[needyPoolIdx] = false;
                budget -= 0.5;
                DEBUG_PRINT("    added 0.5 thread budget: " << budget << "\n");
            }
            if constexpr (NFeatures::IsLocalQueues()) {
                bool needToExpandLocalQueue = budget < 1.0 || threadCount >= pool.MaxFullThreadCount;
                needToExpandLocalQueue &= (bool)pool.BasicPool;
                needToExpandLocalQueue &= (pool.MaxFullThreadCount > 1);
                needToExpandLocalQueue &= (pool.LocalQueueSize < NFeatures::TLocalQueuesFeatureFlags::MAX_LOCAL_QUEUE_SIZE);
                if (needToExpandLocalQueue) {
                    pool.BasicPool->SetLocalQueueSize(++pool.LocalQueueSize);
                }
            }
        }
    }

    if (budget < 1.0) {
        size_t takingAwayThreads = 0;
        DEBUG_PRINT("  takingAwayThreads: " << takingAwayThreads << " sumOfAdditionalThreads: " << sumOfAdditionalThreads << " budget: " << budget << "\n");
        for (size_t needyPoolIdx : needyPools) {
            TPoolInfo &pool = *Pools[needyPoolIdx];
            i64 threadCount = pool.GetFullThreadCount();
            sumOfAdditionalThreads -= threadCount - pool.DefaultFullThreadCount;
            DEBUG_PRINT("  needyPoolIdx: " << needyPoolIdx << " poolId: " << pool.Pool->PoolId << " poolName: " << pool.Pool->GetName() << " threadCount: " << threadCount << " budget: " << budget << "\n");
            DEBUG_PRINT("    sumOfAdditionalThreads: " << sumOfAdditionalThreads << " takingAwayThreads: " << takingAwayThreads << "\n");
            if (sumOfAdditionalThreads < takingAwayThreads + 1) {
                DEBUG_PRINT("    sumOfAdditionalThreads < takingAwayThreads + 1\n");
                break;
            }
            if (!isNeedyByPool[needyPoolIdx]) {
                DEBUG_PRINT("    !isNeedyByPool[needyPoolIdx]\n");
                continue;
            }
            AtomicIncrement(pool.IncreasingThreadsByExchange);
            isNeedyByPool[needyPoolIdx] = false;
            takingAwayThreads++;
            pool.SetFullThreadCount(threadCount + 1);
            DEBUG_PRINT("    added 1 thread by exchanging budget: " << budget << " takingAwayThreads: " << takingAwayThreads << " sumOfAdditionalThreads: " << sumOfAdditionalThreads << "\n");
            LWPROBE(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by exchanging", threadCount + 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
        }

        for (ui16 poolIdx : PriorityOrder) {
            DEBUG_PRINT("  poolIdx: " << poolIdx << " takingAwayThreads: " << takingAwayThreads << " budget: " << budget << "\n");
            if (takingAwayThreads <= 0) {
                DEBUG_PRINT("    takingAwayThreads <= 0\n");
                break;
            }

            TPoolInfo &pool = *Pools[poolIdx];
            size_t threadCount = pool.GetFullThreadCount();
            size_t additionalThreadsCount = Max<size_t>(0L, threadCount - pool.DefaultFullThreadCount);
            size_t currentTakingAwayThreads = Min(additionalThreadsCount, takingAwayThreads);
            DEBUG_PRINT("  poolIdx: " << poolIdx << " poolId: " << pool.Pool->PoolId << " poolName: " << pool.Pool->GetName() << " threadCount: " << threadCount << " additionalThreadsCount: " << additionalThreadsCount << " currentTakingAwayThreads: " << currentTakingAwayThreads << "\n");

            if (!currentTakingAwayThreads) {
                DEBUG_PRINT("    !currentTakingAwayThreads\n");
                continue;
            }
            takingAwayThreads -= currentTakingAwayThreads;
            pool.SetFullThreadCount(threadCount - currentTakingAwayThreads);
            DEBUG_PRINT("    takingAwayThreads by exchanging: " << currentTakingAwayThreads << " takingAwayThreads: " << takingAwayThreads << "\n");
            AtomicAdd(pool.DecreasingThreadsByExchange, currentTakingAwayThreads);
            LWPROBE(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by exchanging", threadCount - currentTakingAwayThreads, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
        }
    }

    for (auto &[hoggishPoolIdx, freeCpu]  : hoggishPools) {
        TPoolInfo &pool = *Pools[hoggishPoolIdx];
        i64 threadCount = pool.GetFullThreadCount();
        DEBUG_PRINT("  hoggishPoolIdx: " << hoggishPoolIdx << " poolId: " << pool.Pool->PoolId << " poolName: " << pool.Pool->GetName() << " threadCount: " << threadCount << " freeCpu: " << freeCpu << "\n");
        if (hasBorrowedSharedThread[hoggishPoolIdx]) {
            Shared->ReturnBorrowedHalfThread(hoggishPoolIdx);
            freeCpu -= 0.5;
            DEBUG_PRINT("    return borrowed half thread freeCpu: " << freeCpu << "\n");
            continue;
        }
        if (pool.BasicPool && pool.LocalQueueSize > NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE) {
            pool.LocalQueueSize = std::min<ui16>(NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE, pool.LocalQueueSize / 2);
            pool.BasicPool->SetLocalQueueSize(pool.LocalQueueSize);
        }
        if (threadCount > pool.MinFullThreadCount && freeCpu >= 1) {
            AtomicIncrement(pool.DecreasingThreadsByHoggishState);
            LWPROBE(HarmonizeOperation, hoggishPoolIdx, pool.Pool->GetName(), "decrease by hoggish", threadCount - 1, pool.DefaultFullThreadCount, pool.MaxFullThreadCount);
            pool.SetFullThreadCount(threadCount - 1);
            DEBUG_PRINT("    decrease by hoggish threadCount: " << threadCount - 1 << "\n");
        }
    }

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = *Pools[poolIdx];
        AtomicSet(pool.PotentialMaxThreadCount, std::min<i64>(pool.MaxThreadCount, pool.GetThreadCount() + budgetInt));
        DEBUG_PRINT("  poolIdx: " << poolIdx << " PotentialMaxThreadCount: " << pool.PotentialMaxThreadCount << " MaxThreadCount: " << pool.MaxThreadCount << " GetThreadCount: " << pool.GetThreadCount() << " budgetInt: " << budgetInt << "\n");
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
    bool isDisabled = IsDisabled.load(std::memory_order_acquire);
    ui64 nextHarmonizeTs = NextHarmonizeTs.load(std::memory_order_acquire); 
    if (isDisabled || nextHarmonizeTs > ts || !Lock.TryAcquire()) {
        DEBUG_PRINT("TryToHarmonizeFailed: " << ts << " " << nextHarmonizeTs << " " << isDisabled << " " << false << "\n");
        LWPROBE(TryToHarmonizeFailed, ts, NextHarmonizeTs, IsDisabled, false);
        return;
    }
    // Check again under the lock
    isDisabled = IsDisabled.load(std::memory_order_acquire);
    if (isDisabled) {
        DEBUG_PRINT("TryToHarmonizeFailed: " << ts << " " << nextHarmonizeTs << " " << isDisabled << " " << true << "\n");
        LWPROBE(TryToHarmonizeFailed, ts, nextHarmonizeTs, isDisabled, true);
        Lock.Release();
        return;
    }
    // Will never reach this line disabled
    ui64 previousNextHarmonizeTs = NextHarmonizeTs.exchange(ts + Us2Ts(1'000'000ull));
    LWPROBE(TryToHarmonizeSuccess, ts, NextHarmonizeTs, previousNextHarmonizeTs);

    {
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_HARMONIZER> activityGuard;

        if (PriorityOrder.empty()) {
            DEBUG_PRINT("CalculatePriorityOrder\n");
            CalculatePriorityOrder();
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

    poolInfo.DefaultFullThreadCount = pool->GetDefaultFullThreadCount();
    poolInfo.MinFullThreadCount = pool->GetMinFullThreadCount();
    poolInfo.MaxFullThreadCount = pool->GetMaxFullThreadCount();
    poolInfo.PotentialMaxThreadCount = poolInfo.MaxFullThreadCount;
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

IHarmonizer* MakeHarmonizer(ui64 ts) {
    return new THarmonizer(ts);
}

TPoolHarmonizerStats THarmonizer::GetPoolStats(i16 poolId) const {
    const TPoolInfo &pool = *Pools[poolId];
    ui64 flags = RelaxedLoad(&pool.LastFlags);
    return TPoolHarmonizerStats{
        .IncreasingThreadsByNeedyState = static_cast<ui64>(RelaxedLoad(&pool.IncreasingThreadsByNeedyState)),
        .IncreasingThreadsByExchange = static_cast<ui64>(RelaxedLoad(&pool.IncreasingThreadsByExchange)),
        .DecreasingThreadsByStarvedState = static_cast<ui64>(RelaxedLoad(&pool.DecreasingThreadsByStarvedState)),
        .DecreasingThreadsByHoggishState = static_cast<ui64>(RelaxedLoad(&pool.DecreasingThreadsByHoggishState)),
        .DecreasingThreadsByExchange = static_cast<ui64>(RelaxedLoad(&pool.DecreasingThreadsByExchange)),
        .MaxElapsedCpu = pool.MaxElapsed.load(std::memory_order_relaxed),
        .MinElapsedCpu = pool.MinElapsed.load(std::memory_order_relaxed),
        .AvgElapsedCpu = pool.AvgElapsed.load(std::memory_order_relaxed),
        .MaxCpu = pool.MaxCpu.load(std::memory_order_relaxed),
        .MinCpu = pool.MinCpu.load(std::memory_order_relaxed),
        .PotentialMaxThreadCount = static_cast<i16>(RelaxedLoad(&pool.PotentialMaxThreadCount)),
        .IsNeedy = static_cast<bool>(flags & 1),
        .IsStarved = static_cast<bool>(flags & 2),
        .IsHoggish = static_cast<bool>(flags & 4),
    };
}

THarmonizerStats THarmonizer::GetStats() const {
    return THarmonizerStats{
        .MaxElapsedCpu = static_cast<i64>(RelaxedLoad(&MaxElapsedCpu)),
        .MinElapsedCpu = static_cast<i64>(RelaxedLoad(&MinElapsedCpu)),
        .MaxCpu = static_cast<i64>(RelaxedLoad(&MaxCpu)),
        .MinCpu = static_cast<i64>(RelaxedLoad(&MinCpu)),
        .AvgAwakeningTimeUs = AvgAwakeningTimeUs,
        .AvgWakingUpTimeUs = AvgWakingUpTimeUs,
    };
}

void THarmonizer::SetSharedPool(TSharedExecutorPool* pool) {
    Shared = pool;
}

TString TPoolHarmonizerStats::ToString() const {
    return TStringBuilder()
        << "IncreasingThreadsByNeedyState: " << IncreasingThreadsByNeedyState << "\n"
        << "IncreasingThreadsByExchange: " << IncreasingThreadsByExchange << "\n"
        << "DecreasingThreadsByStarvedState: " << DecreasingThreadsByStarvedState << "\n"
        << "DecreasingThreadsByHoggishState: " << DecreasingThreadsByHoggishState << "\n"
        << "DecreasingThreadsByExchange: " << DecreasingThreadsByExchange << "\n"
        << "MaxElapsedCpu: " << MaxElapsedCpu << "\n"
        << "MinElapsedCpu: " << MinElapsedCpu << "\n"
        << "AvgElapsedCpu: " << AvgElapsedCpu << "\n"
        << "MaxCpu: " << MaxCpu << "\n"
        << "MinCpu: " << MinCpu << "\n"
        << "PotentialMaxThreadCount: " << PotentialMaxThreadCount << "\n"
        << "IsNeedy: " << IsNeedy << "\n"
        << "IsStarved: " << IsStarved << "\n"
        << "IsHoggish: " << IsHoggish << "\n";
}

TString THarmonizerStats::ToString() const {
    return TStringBuilder()
        << "MaxElapsedCpu: " << MaxElapsedCpu << "\n"
        << "MinElapsedCpu: " << MinElapsedCpu << "\n"
        << "MaxCpu: " << MaxCpu << "\n"
        << "MinCpu: " << MinCpu << "\n"
        << "AvgAwakeningTimeUs: " << AvgAwakeningTimeUs << "\n"
        << "AvgWakingUpTimeUs: " << AvgWakingUpTimeUs << "\n";
}

} // namespace NActors