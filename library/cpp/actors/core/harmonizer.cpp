#include "harmonizer.h"

#include "probes.h"
#include "actorsystem.h"

#include <library/cpp/actors/util/cpu_load_log.h>
#include <library/cpp/actors/util/datetime.h>
#include <library/cpp/actors/util/intrinsics.h>

#include <util/system/spinlock.h>

#include <algorithm>

namespace NActors {

LWTRACE_USING(ACTORLIB_PROVIDER);

struct TValueHistory {
    double History[8] = {0.0};
    ui64 HistoryIdx = 0;
    ui64 LastTs = Max<ui64>();
    double LastUs = 0.0;
    double AccumulatedUs = 0.0;
    ui64 AccumulatedTs = 0;

    double GetAvgPart() {
        double sum = AccumulatedUs;
        for (size_t idx = 0; idx < (sizeof(History) / sizeof(History[0])); ++idx) {
          sum += History[idx];
        }
        double duration = 1'000'000.0 * (sizeof(History) / sizeof(History[0])) + Ts2Us(AccumulatedTs);
        double avg = sum / duration;
        return avg;
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
            for (size_t idx = 0; idx < (sizeof(History) / sizeof(History[0])); ++idx) {
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
                HistoryIdx = (HistoryIdx + 1) % (sizeof(History) / sizeof(History[0]));
                AccumulatedUs = 0.0;
                AccumulatedTs = 0;
            }
        }
    }
};

struct TThreadInfo {
    TValueHistory Consumed;
    TValueHistory Booked;
};

struct TPoolInfo {
    std::vector<TThreadInfo> ThreadInfo;
    IExecutorPool* Pool = nullptr;
    i16 DefaultThreadCount = 0;
    i16 MinThreadCount = 0;
    i16 MaxThreadCount = 0;

    bool IsBeingStopped(i16 threadIdx);
    double GetBooked(i16 threadIdx);
    double GetConsumed(i16 threadIdx);
    void PullStats(ui64 ts);
    i16 GetThreadCount();
    void SetThreadCount(i16 threadCount);
};

bool TPoolInfo::IsBeingStopped(i16 threadIdx) {
    return Pool->IsThreadBeingStopped(threadIdx);
}

double TPoolInfo::GetBooked(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Booked.GetAvgPart();
    }
    return 0.0;
    //return Pool->GetThreadBooked(threadIdx);
}

double TPoolInfo::GetConsumed(i16 threadIdx) {
    if ((size_t)threadIdx < ThreadInfo.size()) {
        return ThreadInfo[threadIdx].Consumed.GetAvgPart();
    }
    return 0.0;
    //return Pool->GetThreadConsumed(threadIdx);
}

#define UNROLL_HISTORY(history) (history)[0], (history)[1], (history)[2], (history)[3], (history)[4], (history)[5], (history)[6], (history)[7]
void TPoolInfo::PullStats(ui64 ts) {
    for (i16 threadIdx = 0; threadIdx < MaxThreadCount; ++threadIdx) {
        TThreadInfo &threadInfo = ThreadInfo[threadIdx];
        threadInfo.Consumed.Register(ts, Pool->GetThreadConsumedUs(threadIdx));
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "consumed", UNROLL_HISTORY(threadInfo.Consumed.History));
        threadInfo.Booked.Register(ts, Pool->GetThreadBookedUs(threadIdx));
        LWPROBE(SavedValues, Pool->PoolId, Pool->GetName(), "booked", UNROLL_HISTORY(threadInfo.Booked.History));
    }
}
#undef UNROLL_HISTORY

i16 TPoolInfo::GetThreadCount() {
    return Pool->GetThreadCount();
}

void TPoolInfo::SetThreadCount(i16 threadCount) {
    Pool->SetThreadCount(threadCount);
}

class THarmonizer: public IHarmonizer {
private:
    std::atomic<bool> IsDisabled = false;
    TSpinLock Lock;
    std::atomic<ui64> NextHarmonizeTs = 0;
    std::vector<TPoolInfo> Pools;

    void PullStats(ui64 ts);
    void HarmonizeImpl();
public:
    THarmonizer(ui64 ts);
    virtual ~THarmonizer();
    double Rescale(double value) const;
    void Harmonize(ui64 ts) override;
    void DeclareEmergency(ui64 ts) override;
    void AddPool(IExecutorPool* pool) override;
    void Enable(bool enable) override;
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
    for (TPoolInfo &pool : Pools) {
        pool.PullStats(ts);
    }
}

void THarmonizer::HarmonizeImpl() {
    bool isStarvedPresent = false;
    double booked = 0.0;
    double consumed = 0.0;
    i64 beingStopped = 0;
    i64 total = 0;
    TStackVec<size_t, 8> needyPools;
    TStackVec<size_t, 8> hoggishPools;
    for (size_t poolIdx = 0; poolIdx < Pools.size(); ++poolIdx) {
        TPoolInfo& pool = Pools[poolIdx];
        total += pool.DefaultThreadCount;
        double poolBooked = 0.0;
        double poolConsumed = 0.0;
        beingStopped += pool.Pool->GetBlockingThreadCount();
        for (i16 threadIdx = 0; threadIdx < pool.MaxThreadCount; ++threadIdx) {
            poolBooked += Rescale(pool.GetBooked(threadIdx));
            poolConsumed += Rescale(pool.GetConsumed(threadIdx));
        }
        bool isStarved = false;
        if (Max(consumed, booked) > 0.1 && consumed < booked * 0.7) {
            isStarvedPresent = true;
            isStarved = true;
        }
        ui32 currentThreadCount = pool.GetThreadCount();
        bool isNeedy = false;
        if (poolBooked >= currentThreadCount) {
            needyPools.push_back(poolIdx);
            isNeedy = true;
        }
        bool isHoggish = false;
        if (poolBooked < currentThreadCount - 1) {
            hoggishPools.push_back(poolIdx);
            isHoggish = true;
        }
        booked += poolBooked;
        consumed += poolConsumed;
        LWPROBE(HarmonizeCheckPool, poolIdx, pool.Pool->GetName(), poolBooked, poolConsumed, pool.GetThreadCount(), pool.MaxThreadCount, isStarved, isNeedy, isHoggish);
    }
    double budget = total - booked;
    double overbooked = consumed - booked;
    if (isStarvedPresent) {
      // last_starved_at_consumed_value = сумма по всем пулам consumed;
      // TODO(cthulhu): использовать как лимит планвно устремлять этот лимит к total,
      // использовать вместо total
      if (beingStopped && beingStopped >= overbooked) {
          // do nothing
      } else {
          TStackVec<size_t> reorder;
          for (size_t i = 0; i < Pools.size(); ++i) {
              reorder.push_back(i);
          }
          while (!reorder.empty()) {
              size_t rndIdx = rand() % reorder.size();
              size_t poolIdx = reorder[rndIdx];
              reorder[rndIdx] = reorder.back();
              reorder.pop_back();

              TPoolInfo &pool = Pools[poolIdx];
              i64 threadCount = pool.GetThreadCount();
              if (threadCount > pool.DefaultThreadCount) {
                  pool.SetThreadCount(threadCount - 1);
                  overbooked--;
                  LWPROBE(HarmonizeOperation, poolIdx, pool.Pool->GetName(), "decrease", threadCount - 1, pool.DefaultThreadCount, pool.MaxThreadCount);
                  if (overbooked < 1) {
                      break;
                  }
              }
          }
      }
    } else {
        for (size_t needyPoolIdx : needyPools) {
            TPoolInfo &pool = Pools[needyPoolIdx];
            if (budget >= 1.0) {
                i64 threadCount = pool.GetThreadCount();
                if (threadCount + 1 <= pool.MaxThreadCount) {
                    pool.SetThreadCount(threadCount + 1);
                    budget -= 1.0;
                    LWPROBE(HarmonizeOperation, needyPoolIdx, pool.Pool->GetName(), "increase", threadCount + 1, pool.DefaultThreadCount, pool.MaxThreadCount);
                }
            }
        }
    }
    for (size_t hoggishPoolIdx : hoggishPools) {
        TPoolInfo &pool = Pools[hoggishPoolIdx];
        i64 threadCount = pool.GetThreadCount();
        if (threadCount > pool.MinThreadCount) {
            LWPROBE(HarmonizeOperation, hoggishPoolIdx, pool.Pool->GetName(), "decrease", threadCount - 1, pool.DefaultThreadCount, pool.MaxThreadCount);
            pool.SetThreadCount(threadCount - 1);
        }
    }
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

    PullStats(ts);
    HarmonizeImpl();

    Lock.Release();
}

void THarmonizer::DeclareEmergency(ui64 ts) {
    NextHarmonizeTs = ts;
}

void THarmonizer::AddPool(IExecutorPool* pool) {
    TGuard<TSpinLock> guard(Lock);
    TPoolInfo poolInfo;
    poolInfo.Pool = pool;
    poolInfo.DefaultThreadCount = pool->GetDefaultThreadCount();
    poolInfo.MinThreadCount = pool->GetMinThreadCount();
    poolInfo.MaxThreadCount = pool->GetMaxThreadCount();
    poolInfo.ThreadInfo.resize(poolInfo.MaxThreadCount);
    pool->SetThreadCount(poolInfo.DefaultThreadCount);
    Pools.push_back(poolInfo);
};

void THarmonizer::Enable(bool enable) {
    TGuard<TSpinLock> guard(Lock);
    IsDisabled = enable;
}

IHarmonizer* MakeHarmonizer(ui64 ts) {
    return new THarmonizer(ts);
}

}
