#pragma once

#include "defs.h"

namespace NActors {

struct TPoolInfo;

struct TWaitingInfo {
    ui64 WakingUpTotalTime = 0;
    ui64 WakingUpCount = 0;
    ui64 AwakingTotalTime = 0;
    ui64 AwakingCount = 0;
    std::atomic<float> AvgWakingUpTimeUs = 0;
    std::atomic<float> AvgAwakeningTimeUs = 0;

    void Pull(const std::vector<std::unique_ptr<TPoolInfo>> &pools);

}; // struct TWaitingInfo

} // namespace NActors
