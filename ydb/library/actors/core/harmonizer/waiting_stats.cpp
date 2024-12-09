#include "waiting_stats.h"

#include "pool.h"
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/probes.h>

namespace NActors {

LWTRACE_USING(ACTORLIB_PROVIDER);

void TWaitingInfo::Pull(const std::vector<std::unique_ptr<TPoolInfo>> &pools) {
    WakingUpTotalTime = 0;
    WakingUpCount = 0;
    AwakingTotalTime = 0;
    AwakingCount = 0;

    for (size_t poolIdx = 0; poolIdx < pools.size(); ++poolIdx) {
        TPoolInfo& pool = *pools[poolIdx];
        if (pool.WaitingStats) {
            WakingUpTotalTime += pool.WaitingStats->WakingUpTotalTime;
            WakingUpCount += pool.WaitingStats->WakingUpCount;
            AwakingTotalTime += pool.WaitingStats->AwakingTotalTime;
            AwakingCount += pool.WaitingStats->AwakingCount;
        }
    }

    constexpr ui64 knownAvgWakingUpTime = TWaitingStatsConstants::KnownAvgWakingUpTime;
    constexpr ui64 knownAvgAwakeningUpTime = TWaitingStatsConstants::KnownAvgAwakeningTime;

    ui64 realAvgWakingUpTime = (WakingUpCount ? WakingUpTotalTime / WakingUpCount : knownAvgWakingUpTime);
    ui64 avgWakingUpTime = realAvgWakingUpTime;
    if (avgWakingUpTime > 2 * knownAvgWakingUpTime || !realAvgWakingUpTime) {
        avgWakingUpTime = knownAvgWakingUpTime;
    }
    AvgWakingUpTimeUs.store(Ts2Us(avgWakingUpTime), std::memory_order_relaxed);

    ui64 realAvgAwakeningTime = (AwakingCount ? AwakingTotalTime / AwakingCount : knownAvgAwakeningUpTime);
    ui64 avgAwakeningTime = realAvgAwakeningTime;
    if (avgAwakeningTime > 2 * knownAvgAwakeningUpTime || !realAvgAwakeningTime) {
        avgAwakeningTime = knownAvgAwakeningUpTime;
    }
    AvgAwakeningTimeUs.store(Ts2Us(avgAwakeningTime), std::memory_order_relaxed);

    ui64 avgWakingUpConsumption = avgWakingUpTime + avgAwakeningTime;
    LWPROBE(WakingUpConsumption, Ts2Us(avgWakingUpTime), Ts2Us(avgWakingUpTime), Ts2Us(avgAwakeningTime), Ts2Us(realAvgAwakeningTime), Ts2Us(avgWakingUpConsumption));
}

} // namespace NActors
