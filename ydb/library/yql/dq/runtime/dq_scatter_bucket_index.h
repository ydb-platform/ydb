#pragma once

#include "dq_output.h"

#include <util/generic/vector.h>

#include <array>

namespace NYql::NDq {

// O(1) bucket index for adaptive scatter routing.
//
// Channels are partitioned into three buckets by their current fill level.
// PickBest() returns a channel at the lowest (best) fill level in O(1).
// Update() moves a channel between buckets in O(1) via swap-with-last.
// Within each bucket channels are served round-robin to ensure fair distribution.
struct TScatterBucketIndex {
    static_assert(static_cast<ui32>(NoLimit) == 0 && static_cast<ui32>(SoftLimit) == 1 && static_cast<ui32>(HardLimit) == 2,
        "EDqFillLevel values changed; review PickBest iteration order in TScatterBucketIndex");

    static constexpr ui32 kLevelCount = static_cast<ui32>(HardLimit) + 1;

    std::array<TVector<ui32>, kLevelCount> Buckets;
    TVector<ui32> ChannelPos;           // position of channel i within its current bucket
    TVector<EDqFillLevel> ChannelLevel; // current level of channel i
    // Round-robin cursor per bucket: index into Buckets[L], not a channel ID.
    // After swap-with-last removal the occupant at that position may change,
    // but no element is skipped or repeated.
    std::array<ui32, kLevelCount> RoundRobinPos = {};

    explicit TScatterBucketIndex(ui32 channelCount)
        : ChannelPos(channelCount)
        , ChannelLevel(channelCount, NoLimit)
    {
        Y_ENSURE(channelCount > 0, "TScatterBucketIndex requires at least one channel");
        for (auto& bucket : Buckets) {
            bucket.reserve(channelCount);
        }
        for (ui32 i = 0; i < channelCount; ++i) {
            ChannelPos[i] = i;
            Buckets[NoLimit].push_back(i);
        }
    }

    void Update(ui32 idx, EDqFillLevel newLevel) {
        EDqFillLevel oldLevel = ChannelLevel[idx];
        if (oldLevel == newLevel) {
            return;
        }
        const ui32 oldLevelIdx = static_cast<ui32>(oldLevel);
        auto& oldBucket = Buckets[oldLevelIdx];
        const ui32 pos = ChannelPos[idx];
        const ui32 last = oldBucket.back();
        oldBucket[pos] = last;
        ChannelPos[last] = pos;
        oldBucket.pop_back();

        if (RoundRobinPos[oldLevelIdx] >= oldBucket.size() && !oldBucket.empty()) {
            RoundRobinPos[oldLevelIdx] = 0;
        }

        const ui32 newLevelIdx = static_cast<ui32>(newLevel);
        ChannelPos[idx] = static_cast<ui32>(Buckets[newLevelIdx].size());
        Buckets[newLevelIdx].push_back(idx);
        ChannelLevel[idx] = newLevel;
    }

    ui32 PickBest() {
        for (auto level : {NoLimit, SoftLimit, HardLimit}) {
            const ui32 levelIdx = static_cast<ui32>(level);
            auto& bucket = Buckets[levelIdx];
            if (bucket.empty()) {
                continue;
            }
            ui32& pos = RoundRobinPos[levelIdx];
            pos %= bucket.size();
            const ui32 idx = bucket[pos];
            ++pos;
            return idx;
        }
        Y_UNREACHABLE();
    }
};

} // namespace NYql::NDq
