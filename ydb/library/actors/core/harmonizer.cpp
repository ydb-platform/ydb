#include "harmonizer.h"

#include "probes.h"
#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_pool_shared.h"

#include <ydb/library/actors/util/cpu_load_log.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/util/intrinsics.h>

#include <util/system/spinlock.h>

#include <algorithm>

namespace NActors {

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
    TValueHistory<8> Consumed;
    TValueHistory<8> Booked;
};

struct TPoolInfo {
    std::vector<TThreadInfo> ThreadInfo;
    std::vector<TThreadInfo> SharedThreadInfo;
    IExecutorPool* Pool = nullptr;
    TBasicExecutorPool* BasicPool = nullptr;
    i16 DefaultThreadCount = 0;
    i16 MinThreadCount = 0;
    i16 MaxThreadCount = 0;
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

    TValueHistory<16> Consumed;
    TValueHistory<16> Booked;

    TAtomic MaxConsumedCpu = 0;
    TAtomic MinConsumedCpu = 0;
    TAtomic MaxBookedCpu = 0;
    TAtomic MinBookedCpu = 0;

    std::unique_ptr<TWaitingStats<ui64>> WaitingStats;
    std::unique_ptr<TWaitingStats<double>> MovingWaitingStats;

    bool HasSharedThread = false;
    bool UseSecondHalfOfOwnSharedThread = false;
    std::optional<i16> OtherHalfSharedThread = std::nullopt;

    TSharedExecutorPool *Shared = nullptr;

    double GetBooked(i16 threadIdx);
    double GetLastSecondPoolBooked(i16 threadIdx);
    double GetConsumed(i16 threadIdx);
    double GetLastSecondPoolConsumed(i16 threadIdx);

    double GetSharedBooked();
    double GetSharedConsumed();

    TCpuConsumption PullStats(ui64 ts);
    i16 GetThreadCount();
    void SetThreadCount(i16 threadCount);
    bool IsAvgPingGood();
};

double TPoolInfo::GetBooked(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Booked.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondPoolBooked(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Booked.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetSharedBooked() {
    double acc = 0;
    for (auto &info : SharedThreadInfo) {
        acc += info.Booked.GetAvgPart();
    }
    return acc;
}

double TPoolInfo::GetConsumed(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Consumed.GetAvgPart();
    }
    return 0.0;
}

double TPoolInfo::GetLastSecondPoolConsumed(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Consumed.GetAvgPartForLastSeconds(1);
    }
    return 0.0;
}

double TPoolInfo::GetSharedConsumed() {
    double acc = 0;
    for (auto &info : SharedThreadInfo) {
        acc += info.Consumed.GetAvgPart();
    }
    return acc;
}

#define UNROLL_HISTORY(history) (history)[0], (history)[1], (history)[2], (history)[3], (history)[4], (history)[5], (history)[6], (history)[7]
TCpuConsumption TPoolInfo::PullStats(ui64 ts) {
    TCpuConsumption acc;
    
    auto pullThreadInfo = [&](TThreadInfo *threadInfo, const TCpuConsumption &cpuConsumption) {
        acc.Add(cpuConsumption);
        threadInfo->Consumed.Register(ts, cpuConsumption.ConsumedUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "consumed", UNROLL_HISTORY(threadInfo->Consumed.History));
        threadInfo->Booked.Register(ts, cpuConsumption.BookedUs);
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "booked", UNROLL_HISTORY(threadInfo->Booked.History));
    };

    for (i16 threadIdx = 0; threadIdx < MaxThreadCount; ++threadIdx) {
        TCpuConsumption cpuConsumption = Pool->GetThreadCpuConsumption(threadIdx);
        pullThreadInfo(&ThreadInfo[threadIdx], cpuConsumption);
    }

     if (Shared) {
        std::vector<TCpuConsumption> sharedConsumptions = Shared->GetThreadsCpuConsumption(Pool->PoolId);
        for (ui32 threadIdx = 0; threadIdx < SharedThreadInfo.size(); ++threadIdx) {
            pullThreadInfo(&SharedThreadInfo[threadIdx], sharedConsumptions[threadIdx]);
        }
    }

    Consumed.Register(ts, acc.ConsumedUs);
    RelaxedStore(&MaxConsumedCpu, Consumed.GetMaxInt());
    RelaxedStore(&MinConsumedCpu, Consumed.GetMinInt());
    Booked.Register(ts, acc.BookedUs);
    RelaxedStore(&MaxBookedCpu, Booked.GetMaxInt());
    RelaxedStore(&MinBookedCpu, Booked.GetMinInt());
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

i16 TPoolInfo::GetThreadCount() {
    return Pool->GetThreadCount();
}

void TPoolInfo::SetThreadCount(i16 threadCount) {
    Pool->SetThreadCount(threadCount);
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

struct TWakingUpStats {
    ui64 TotalWakingUpTime = 0;
    ui64 TotalWakingUps = 0;
    ui64 TotalAwakeningTime = 0;
    ui64 TotalAwakenings = 0;

    static constexpr ui64 KnownAvgWakingUpTime = TWaitingStatsConstants::KnownAvgWakingUpTime;
    static constexpr ui64 KnownAvgAwakeningUpTime = TWaitingStatsConstants::KnownAvgAwakeningTime;

    ui64 RealAvgWakingUpTime = KnownAvgWakingUpTime;
    ui64 RealAvgAwakeningTime = KnownAvgAwakeningUpTime;

    ui64 AvgWakingUpTime = KnownAvgWakingUpTime;
    ui64 AvgAwakeningTime = KnownAvgAwakeningUpTime;

    static TWakingUpStats CalculateStats(const std::vector<TPoolInfo> &pools) {
        TWakingUpStats result;
        for (size_t poolIdx = 0; poolIdx < pools.size(); ++poolIdx) {
            const TPoolInfo& pool = pools[poolIdx];
            if (pool.WaitingStats) {
                result.TotalWakingUpTime += pool.WaitingStats->WakingUpTotalTime;
                result.TotalWakingUps += pool.WaitingStats->WakingUpCount;
                result.TotalAwakeningTime += pool.WaitingStats->AwakingTotalTime;
                result.TotalAwakenings += pool.WaitingStats->AwakingCount;
            }
        }
        if (result.TotalWakingUps) {
            result.RealAvgWakingUpTime = result.TotalWakingUpTime / result.TotalWakingUps;
        }
        if (result.TotalAwakenings) {
            result.RealAvgWakingUpTime = result.TotalAwakeningTime / result.TotalAwakenings;
        }
        if (result.RealAvgWakingUpTime <= 2 * KnownAvgWakingUpTime) {
            result.AvgWakingUpTime = result.RealAvgWakingUpTime;
        }
        if (result.RealAvgAwakeningTime <= 2 * KnownAvgAwakeningUpTime) {
            result.AvgAwakeningTime = result.RealAvgAwakeningTime;
        }
        return result;
    }

};

struct TAccumulatedStatistics {
    bool IsStarvedPresent = false;
    double Booked = 0.0;
    double Consumed = 0.0;
    double LastSecondBooked = 0.0;
    i64 BeingStopped = 0;
    i64 Total = 0;
    TStackVec<size_t, 8> NeedyPools;
    TStackVec<size_t, 8> HoggishPools;
    TStackVec<bool, 8> IsNeedyByPool;
    TStackVec<bool, 8> HasFreeHalfThread;
    size_t SumOfAdditionalThreads = 0;
};

class THarmonizer: public IHarmonizer {
private:
    std::atomic<bool> IsDisabled = false;
    TSpinLock Lock;
    std::atomic<ui64> NextHarmonizeTs = 0;
    std::vector<TPoolInfo> Pools;
    std::vector<ui16> PriorityOrder;

    TSharedExecutorPool *Shared;

    TValueHistory<16> Consumed;
    TValueHistory<16> Booked;

    TAtomic MaxConsumedCpu = 0;
    TAtomic MinConsumedCpu = 0;
    TAtomic MaxBookedCpu = 0;
    TAtomic MinBookedCpu = 0;

    std::atomic<double> AvgAwakeningTimeUs = 0;
    std::atomic<double> AvgWakingUpTimeUs = 0;

    void PullStats(ui64 ts);
    void HarmonizeImpl(ui64 ts);
    void CalculatePriorityOrder();
public:
    THarmonizer(ui64 ts, TSharedExecutorPool *shared);
    virtual ~THarmonizer();
    double Rescale(double value) const;
    void Harmonize(ui64 ts) override;
    void DeclareEmergency(ui64 ts) override;
    void AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo, bool hasSharedThread, TSharedExecutorPool *shared) override;
    void Enable(bool enable) override;
    TPoolHarmonizerStats GetPoolStats(i16 poolId) const override;
    THarmonizerStats GetStats() const override;

    TAccumulatedStatistics AccumulateStatistics(ui64 ts);
};

THarmonizer::THarmonizer(ui64 ts, TSharedExecutorPool *shared)
    : NextHarmonizeTs(ts)
    , Shared(shared)
{}

THarmonizer::~THarmonizer() {
}

double THarmonizer::Rescale(double value) const {
  return Max(0.0, Min(1.0, value * (1.0/0.9)));
}

void THarmonizer::PullStats(ui64 ts) {
    TCpuConsumption acc;
    for (TPoolInfo &pool : Pools) {
        TCpuConsumption consumption = pool.PullStats(ts);
        acc.Add(consumption);
    }
    Consumed.Register(ts, acc.ConsumedUs);
    RelaxedStore(&MaxConsumedCpu, Consumed.GetMaxInt());
    RelaxedStore(&MinConsumedCpu, Consumed.GetMinInt());
    Booked.Register(ts, acc.BookedUs);
    RelaxedStore(&MaxBookedCpu, Booked.GetMaxInt());
    RelaxedStore(&MinBookedCpu, Booked.GetMinInt());
}

Y_FORCE_INLINE bool IsStarved(double consumed, double booked) {
    return Max(consumed, booked) > 0.1 && consumed < booked * 0.7;
}

Y_FORCE_INLINE bool IsHoggish(double booked, ui16 currentThreadCount) {
    return booked < currentThreadCount - 1;
}

TAccumulatedStatistics THarmonizer::AccumulateStatistics(ui64 ts) {
    TAccumulatedStatistics result;

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = Pools[poolIdx];
        result.Total += pool.DefaultThreadCount;

        ui32 currentThreadCount = pool.GetThreadCount();
        result.SumOfAdditionalThreads += currentThreadCount - pool.DefaultThreadCount;

        double poolBooked = 0.0;
        double poolConsumed = 0.0;
        double lastSecondPoolBooked = 0.0;
        double lastSecondPoolConsumed = 0.0;
        result.BeingStopped += pool.Pool->GetBlockingThreadCount();
        for (i16 threadIdx = 0; threadIdx < pool.MaxThreadCount; ++threadIdx) {
            poolBooked += Rescale(pool.GetBooked(threadIdx));
            lastSecondPoolBooked += Rescale(pool.GetLastSecondPoolBooked(threadIdx));
            poolConsumed += Rescale(pool.GetConsumed(threadIdx));
            lastSecondPoolConsumed += Rescale(pool.GetLastSecondPoolConsumed(threadIdx));
        }

        poolBooked += Rescale(pool.GetSharedBooked());
        poolConsumed += Rescale(pool.GetSharedConsumed());

        // Cerr << (TStringBuilder() << "PoolId# " << poolIdx << " Booked# "
                << poolBooked << " Consumed# " << poolConsumed << " HasShared# " << pool.HasSharedThread
                << " HasBorrowedSharedThread# " << (pool.OtherHalfSharedThread ? std::to_string(*pool.OtherHalfSharedThread) : "none") << Endl);

        bool isStarved = IsStarved(poolConsumed, poolBooked) || IsStarved(lastSecondPoolConsumed, lastSecondPoolBooked);
        if (isStarved) {
            result.IsStarvedPresent = true;
        }

        bool isNeedy = (pool.IsAvgPingGood() || pool.NewNotEnoughCpuExecutions) && poolBooked >= currentThreadCount;
        if (pool.AvgPingCounter) {
            if (pool.LastUpdateTs + Us2Ts(3'000'000ull) > ts) {
                isNeedy = false;
            } else if (isNeedy) {
                pool.LastUpdateTs = ts;
            }
        }
        if (currentThreadCount - poolBooked > 0.5) {
            if (pool.OtherHalfSharedThread || pool.UseSecondHalfOfOwnSharedThread) {
                result.HasFreeHalfThread.push_back(poolIdx);
            }
        }
        result.IsNeedyByPool.push_back(isNeedy);
        if (isNeedy) {
            result.NeedyPools.push_back(poolIdx);
        }
        bool isHoggish = IsHoggish(poolBooked, currentThreadCount)
                || IsHoggish(lastSecondPoolBooked, currentThreadCount);
        if (isHoggish) {
            result.HoggishPools.push_back(poolIdx);
        }
        result.Booked += poolBooked;
        result.Consumed += poolConsumed;
        AtomicSet(pool.LastFlags, (i64)isNeedy | ((i64)isStarved << 1) | ((i64)isHoggish << 2));
        LWPROBE(HarmonizeCheckPool, poolIdx, pool.Pool->GetName(), poolBooked, poolConsumed, lastSecondPoolBooked, lastSecondPoolConsumed, pool.GetThreadCount(), pool.MaxThreadCount, isStarved, isNeedy, isHoggish);
    }

    if (result.NeedyPools.size()) {
        Sort(result.NeedyPools.begin(), result.NeedyPools.end(), [&] (i16 lhs, i16 rhs) {
            if (Pools[lhs].Priority != Pools[rhs].Priority)  {
                return Pools[lhs].Priority > Pools[rhs].Priority;
            }
            return Pools[lhs].Pool->PoolId < Pools[rhs].Pool->PoolId;
        });
    }

    if (result.HasFreeHalfThread.size()) {
        Sort(result.HasFreeHalfThread.begin(), result.HasFreeHalfThread.end(), [&] (i16 lhs, i16 rhs) {
            if (Pools[lhs].Priority != Pools[rhs].Priority)  {
                return Pools[lhs].Priority > Pools[rhs].Priority;
            }
            return Pools[lhs].Pool->PoolId < Pools[rhs].Pool->PoolId;
        });
    }

    return result;
}

void THarmonizer::HarmonizeImpl(ui64 ts) {
    TWakingUpStats wakingUpStats = TWakingUpStats::CalculateStats(Pools);
    ui64 avgWakingUpConsumption = wakingUpStats.AvgWakingUpTime + wakingUpStats.AvgAwakeningTime;
    LWPROBE(WakingUpConsumption, Ts2Us(wakingUpStats.AvgWakingUpTime), Ts2Us(wakingUpStats.RealAvgWakingUpTime), Ts2Us(wakingUpStats.AvgAwakeningTime), Ts2Us(wakingUpStats.RealAvgAwakeningTime), Ts2Us(avgWakingUpConsumption));

    AvgWakingUpTimeUs = Ts2Us(wakingUpStats.AvgWakingUpTime);
    AvgAwakeningTimeUs = Ts2Us(wakingUpStats.AvgAwakeningTime);

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = Pools[poolIdx];
        if (!pool.BasicPool) {
            continue;
        }
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
    
    TAccumulatedStatistics statistic = AccumulateStatistics(ts);

    double budget = statistic.Total - Max(statistic.Booked, statistic.LastSecondBooked);
    i16 budgetInt = static_cast<i16>(Max(budget, 0.0));
    if (budget < -0.1) {
        statistic.IsStarvedPresent = true;
    }
    double overbooked = statistic.Consumed - statistic.Booked;
    if (overbooked < 0) {
        statistic.IsStarvedPresent = false;
    }

    // Cerr << (TStringBuilder() << "Budget# " << budget << " needsPool# " << statistic.NeedyPools.size() << " hasFreeHalfThread# " << statistic.HasFreeHalfThread.size() << Endl);

    if (statistic.IsStarvedPresent) {
        // last_starved_at_consumed_value = сумма по всем пулам consumed;
        // TODO(cthulhu): использовать как лимит планвно устремлять этот лимит к total,
        // использовать вместо total
        if (statistic.BeingStopped && statistic.BeingStopped >= overbooked) {
            // do nothing
        } else {
            for (ui16 poolIdx : PriorityOrder) {
                TPoolInfo &pool = Pools[poolIdx];
                i64 threadCount = pool.GetThreadCount();
                while (threadCount > pool.DefaultThreadCount) {
                    pool.SetThreadCount(--threadCount);
                    AtomicIncrement(pool.DecreasingThreadsByStarvedState);
                    overbooked--;
                    statistic.SumOfAdditionalThreads--;

                    LWPROBE(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by starving", threadCount - 1, pool.DefaultThreadCount, pool.MaxThreadCount);
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
        for (size_t needyPoolIdx : statistic.NeedyPools) {
            TPoolInfo &pool = Pools[needyPoolIdx];
            i64 threadCount = pool.GetThreadCount();
            if (budget >= 1.0) {
                if (threadCount + 1 <= pool.MaxThreadCount) {
                    AtomicIncrement(pool.IncreasingThreadsByNeedyState);
                    statistic.IsNeedyByPool[needyPoolIdx] = false;
                    statistic.SumOfAdditionalThreads++;
                    pool.SetThreadCount(threadCount + 1);
                    budget -= 1.0;
                    LWPROBE(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by needs", threadCount + 1, pool.DefaultThreadCount, pool.MaxThreadCount);
                }
            } else if (Shared && budget >= 0.5 && !pool.OtherHalfSharedThread && statistic.HasFreeHalfThread.size()) {
                // Cerr << (TStringBuilder() << "  budget(" << budget << ") >= 0.5" << Endl);
                TPoolInfo &poolWithFreeHalfThread = Pools[statistic.HasFreeHalfThread.back()];
                if (poolWithFreeHalfThread.OtherHalfSharedThread) {
                    size_t owner = *poolWithFreeHalfThread.OtherHalfSharedThread;
                    poolWithFreeHalfThread.OtherHalfSharedThread = std::nullopt;
                    if (owner == needyPoolIdx) {
                        pool.UseSecondHalfOfOwnSharedThread = true;
                    } else {
                        pool.OtherHalfSharedThread = owner;
                    }
                } else {
                    poolWithFreeHalfThread.UseSecondHalfOfOwnSharedThread = false;
                    pool.OtherHalfSharedThread = statistic.HasFreeHalfThread.back();
                }
                Shared->MoveHalfThread(statistic.HasFreeHalfThread.back(), needyPoolIdx);
                statistic.IsNeedyByPool[needyPoolIdx] = false;
                budget -= 0.5;
                statistic.HasFreeHalfThread.pop_back();
            }
            if constexpr (NFeatures::IsLocalQueues()) {
                bool needToExpandLocalQueue = budget < 1.0 || threadCount >= pool.MaxThreadCount;
                needToExpandLocalQueue &= (bool)pool.BasicPool;
                needToExpandLocalQueue &= (pool.MaxThreadCount > 1);
                needToExpandLocalQueue &= (pool.LocalQueueSize < NFeatures::TLocalQueuesFeatureFlags::MAX_LOCAL_QUEUE_SIZE);
                if (needToExpandLocalQueue) {
                    pool.BasicPool->SetLocalQueueSize(++pool.LocalQueueSize);
                }
            }
        }
    }

    if (budget < 1.0) {
        size_t takingAwayThreads = 0;
        for (size_t needyPoolIdx : statistic.NeedyPools) {
            TPoolInfo &pool = Pools[needyPoolIdx];
            i64 threadCount = pool.GetThreadCount();
            statistic.SumOfAdditionalThreads -= threadCount - pool.DefaultThreadCount;
            if (statistic.SumOfAdditionalThreads < takingAwayThreads + 1) {
                break;
            }
            if (!statistic.IsNeedyByPool[needyPoolIdx]) {
                continue;
            }
            AtomicIncrement(pool.IncreasingThreadsByExchange);
            statistic.IsNeedyByPool[needyPoolIdx] = false;
            takingAwayThreads++;
            pool.SetThreadCount(threadCount + 1);

            LWPROBE(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase by exchanging", threadCount + 1, pool.DefaultThreadCount, pool.MaxThreadCount);
        }

        for (ui16 poolIdx : PriorityOrder) {
            if (takingAwayThreads <= 0) {
                break;
            }

            TPoolInfo &pool = Pools[poolIdx];
            size_t threadCount = pool.GetThreadCount();
            size_t additionalThreadsCount = Max<size_t>(0L, threadCount - pool.DefaultThreadCount);
            size_t currentTakingAwayThreads = Min(additionalThreadsCount, takingAwayThreads);

            if (!currentTakingAwayThreads) {
                continue;
            }
            takingAwayThreads -= currentTakingAwayThreads;
            pool.SetThreadCount(threadCount - currentTakingAwayThreads);

            AtomicAdd(pool.DecreasingThreadsByExchange, takingAwayThreads);
            LWPROBE(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease by exchanging", threadCount - currentTakingAwayThreads, pool.DefaultThreadCount, pool.MaxThreadCount);
        }
    }

    for (size_t hoggishPoolIdx : statistic.HoggishPools) {
        TPoolInfo &pool = Pools[hoggishPoolIdx];
        i64 threadCount = pool.GetThreadCount();
        if (pool.BasicPool && pool.LocalQueueSize > NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE) {
            pool.LocalQueueSize = std::min<ui16>(NFeatures::TLocalQueuesFeatureFlags::MIN_LOCAL_QUEUE_SIZE, pool.LocalQueueSize / 2);
            pool.BasicPool->SetLocalQueueSize(pool.LocalQueueSize);
        }
        if (threadCount > pool.MinThreadCount) {
            AtomicIncrement(pool.DecreasingThreadsByHoggishState);
            LWPROBE(HarmonizeOperation, hoggishPoolIdx, pool.Pool->GetName(), "decrease by hoggish", threadCount - 1, pool.DefaultThreadCount, pool.MaxThreadCount);
            pool.SetThreadCount(threadCount - 1);
        }
    }

    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = Pools[poolIdx];
        AtomicSet(pool.PotentialMaxThreadCount, std::min<i64>(pool.MaxThreadCount, pool.GetThreadCount() + budgetInt));
    }
}

void THarmonizer::CalculatePriorityOrder() {
    PriorityOrder.resize(Pools.size());
    Iota(PriorityOrder.begin(), PriorityOrder.end(), 0);
    Sort(PriorityOrder.begin(), PriorityOrder.end(), [&] (i16 lhs, i16 rhs) {
        if (Pools[lhs].Priority != Pools[rhs].Priority)  {
            return Pools[lhs].Priority < Pools[rhs].Priority;
        }
        return Pools[lhs].Pool->PoolId > Pools[rhs].Pool->PoolId;
    });
}

void THarmonizer::Harmonize(ui64 ts) {
    if (IsDisabled || NextHarmonizeTs > ts || !Lock.TryAcquire()) {
        LWPROBE(TryToHarmonizeFailed, ts, NextHarmonizeTs, IsDisabled, false);
        return;
    }
    // Check again under the lock
    if (IsDisabled) {
        LWPROBE(TryToHarmonizeFailed, ts, NextHarmonizeTs, IsDisabled, true);
        Lock.Release();
        return;
    }
    // Will never reach this line disabled
    ui64 previousNextHarmonizeTs = NextHarmonizeTs.exchange(ts + Us2Ts(1'000'000ull));
    LWPROBE(TryToHarmonizeSuccess, ts, NextHarmonizeTs, previousNextHarmonizeTs);

    if (PriorityOrder.empty()) {
        CalculatePriorityOrder();
    }

    PullStats(ts);
    HarmonizeImpl(ts);

    Lock.Release();
}

void THarmonizer::DeclareEmergency(ui64 ts) {
    NextHarmonizeTs = ts;
}

void THarmonizer::AddPool(IExecutorPool* pool, TSelfPingInfo *pingInfo, bool hasSharedThread, TSharedExecutorPool *shared) {
    Y_UNUSED(hasSharedThread, shared);
    TGuard<TSpinLock> guard(Lock);
    TPoolInfo poolInfo;
    poolInfo.Pool = pool;
    poolInfo.BasicPool = dynamic_cast<TBasicExecutorPool*>(pool);
    poolInfo.DefaultThreadCount = pool->GetDefaultThreadCount();
    poolInfo.MinThreadCount = pool->GetMinThreadCount();
    poolInfo.MaxThreadCount = pool->GetMaxThreadCount();
    poolInfo.ThreadInfo.resize(poolInfo.MaxThreadCount);
    poolInfo.Priority = pool->GetPriority();
    poolInfo.HasSharedThread = hasSharedThread;
    poolInfo.UseSecondHalfOfOwnSharedThread = hasSharedThread;
    pool->SetThreadCount(poolInfo.DefaultThreadCount);
    if (shared) {
        poolInfo.Shared = shared;
        poolInfo.SharedThreadInfo.resize(poolInfo.Shared->GetSharedThreadCount());
    }
    if (pingInfo) {
        poolInfo.AvgPingCounter = pingInfo->AvgPingCounter;
        poolInfo.AvgPingCounterWithSmallWindow = pingInfo->AvgPingCounterWithSmallWindow;
        poolInfo.MaxAvgPingUs = pingInfo->MaxAvgPingUs;
    }
    if (poolInfo.BasicPool) {
        poolInfo.WaitingStats.reset(new TWaitingStats<ui64>());
        poolInfo.MovingWaitingStats.reset(new TWaitingStats<double>());
    }
    Pools.push_back(std::move(poolInfo));
    PriorityOrder.clear();
}

void THarmonizer::Enable(bool enable) {
    TGuard<TSpinLock> guard(Lock);
    IsDisabled = enable;
}

IHarmonizer* MakeHarmonizer(ui64 ts, TSharedExecutorPool *shared) {
    return new THarmonizer(ts, shared);
}

TPoolHarmonizerStats THarmonizer::GetPoolStats(i16 poolId) const {
    const TPoolInfo &pool = Pools[poolId];
    ui64 flags = RelaxedLoad(&pool.LastFlags);
    return TPoolHarmonizerStats{
        .IncreasingThreadsByNeedyState = static_cast<ui64>(RelaxedLoad(&pool.IncreasingThreadsByNeedyState)),
        .IncreasingThreadsByExchange = static_cast<ui64>(RelaxedLoad(&pool.IncreasingThreadsByExchange)),
        .DecreasingThreadsByStarvedState = static_cast<ui64>(RelaxedLoad(&pool.DecreasingThreadsByStarvedState)),
        .DecreasingThreadsByHoggishState = static_cast<ui64>(RelaxedLoad(&pool.DecreasingThreadsByHoggishState)),
        .DecreasingThreadsByExchange = static_cast<ui64>(RelaxedLoad(&pool.DecreasingThreadsByExchange)),
        .MaxConsumedCpu = static_cast<i64>(RelaxedLoad(&pool.MaxConsumedCpu)),
        .MinConsumedCpu = static_cast<i64>(RelaxedLoad(&pool.MinConsumedCpu)),
        .MaxBookedCpu = static_cast<i64>(RelaxedLoad(&pool.MaxBookedCpu)),
        .MinBookedCpu = static_cast<i64>(RelaxedLoad(&pool.MinBookedCpu)),
        .PotentialMaxThreadCount = static_cast<i16>(RelaxedLoad(&pool.PotentialMaxThreadCount)),
        .IsNeedy = static_cast<bool>(flags & 1),
        .IsStarved = static_cast<bool>(flags & 2),
        .IsHoggish = static_cast<bool>(flags & 4),
    };
}

THarmonizerStats THarmonizer::GetStats() const {
    return THarmonizerStats{
        .MaxConsumedCpu = static_cast<i64>(RelaxedLoad(&MaxConsumedCpu)),
        .MinConsumedCpu = static_cast<i64>(RelaxedLoad(&MinConsumedCpu)),
        .MaxBookedCpu = static_cast<i64>(RelaxedLoad(&MaxBookedCpu)),
        .MinBookedCpu = static_cast<i64>(RelaxedLoad(&MinBookedCpu)),
        .AvgAwakeningTimeUs = AvgAwakeningTimeUs,
        .AvgWakingUpTimeUs = AvgWakingUpTimeUs,
    };
}

}
