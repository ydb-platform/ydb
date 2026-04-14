#pragma once

#include "dq_output.h"

#include <util/generic/vector.h>

#include <array>
#include <deque>
#include <mutex>
#include <utility>

namespace NYql::NDq {

#if 0 // replaced by per-channel atomic<EDqFillLevel> vector in TDqOutputScatterConsumer
// O(1) adaptive scatter routing index.
// Channels are bucketed by fill level (NoLimit/SoftLimit/HardLimit); PickBest() picks
// the least-loaded channel round-robin within the best available level.
// Starts with a single active channel (primaryIdx); additional channels are activated
// lazily in Update() when the NoLimit bucket empties.
//
// Thread-safety model:
//   Single producer thread calls PickBest(); one or more consumer threads call Update()
//   via LevelChangeCallback from Pop(). A plain mutex serializes all access — no concurrent
//   readers exist in practice, so a RW-lock would add complexity without benefit.
class TScatterBucketIndex {
    static_assert(
        static_cast<ui32>(NoLimit) == 0 &&
        static_cast<ui32>(SoftLimit) == 1 &&
        static_cast<ui32>(HardLimit) == 2,
        "EDqFillLevel values changed; review TScatterBucketIndex");

public:
    static constexpr ui32 kLevelCount = static_cast<ui32>(HardLimit) + 1;

private:
    mutable std::mutex Lock_;

    std::array<TVector<ui32>, kLevelCount> Buckets_;
    TVector<ui32> ChannelPos_;
    TVector<EDqFillLevel> ChannelLevel_;
    std::deque<ui32> Inactive_;
    TVector<bool> IsActive_;

    ui32 RoundRobinPos_[kLevelCount] = {};

    // Must be called under lock.
    void ActivateNextLocked() {
        if (Inactive_.empty()) return;
        const ui32 idx = Inactive_.front();
        Inactive_.pop_front();
        IsActive_[idx] = true;
        ChannelLevel_[idx] = NoLimit;
        ChannelPos_[idx] = static_cast<ui32>(Buckets_[NoLimit].size());
        Buckets_[NoLimit].push_back(idx);
    }

public:
    explicit TScatterBucketIndex(ui32 channelCount, ui32 primaryIdx = 0)
        : ChannelPos_(channelCount, 0)
        , ChannelLevel_(channelCount, NoLimit)
        , IsActive_(channelCount, false)
    {
        Y_ENSURE(channelCount > 0, "TScatterBucketIndex requires at least one channel");
        Y_ENSURE(primaryIdx < channelCount, "primaryIdx out of range");
        for (auto& bucket : Buckets_) {
            bucket.reserve(channelCount);
        }
        IsActive_[primaryIdx] = true;
        ChannelPos_[primaryIdx] = 0;
        Buckets_[NoLimit].push_back(primaryIdx);
        for (ui32 i = 1; i < channelCount; ++i) {
            Inactive_.push_back((primaryIdx + i) % channelCount);
        }
    }

    // Called from Finish() only; no concurrent activity at that point.
    ui32 InactiveCount() const noexcept {
        return static_cast<ui32>(Inactive_.size());
    }

    // Called from downstream Pop() thread via LevelChangeCallback.
    void Update(ui32 idx, EDqFillLevel newLevel) {
        std::lock_guard guard(Lock_);

        if (!IsActive_[idx]) return;

        const EDqFillLevel oldLevel = ChannelLevel_[idx];
        if (oldLevel == newLevel) return;

        const ui32 oldLevelIdx = static_cast<ui32>(oldLevel);
        auto& oldBucket = Buckets_[oldLevelIdx];
        const ui32 pos = ChannelPos_[idx];
        const ui32 last = oldBucket.back();
        oldBucket[pos] = last;
        ChannelPos_[last] = pos;
        oldBucket.pop_back();

        const ui32 newLevelIdx = static_cast<ui32>(newLevel);
        ChannelPos_[idx] = static_cast<ui32>(Buckets_[newLevelIdx].size());
        Buckets_[newLevelIdx].push_back(idx);
        ChannelLevel_[idx] = newLevel;

        // If NoLimit just emptied, activate the next inactive channel.
        if (oldLevelIdx == static_cast<ui32>(NoLimit) && Buckets_[NoLimit].empty()) {
            ActivateNextLocked();
        }
    }

    // Called from Consume() thread for every row pushed through scatter.
    // Returns {channelIdx, level} under lock so both are consistent.
    std::pair<ui32, EDqFillLevel> PickBest() {
        std::lock_guard guard(Lock_);

        for (auto level : {NoLimit, SoftLimit, HardLimit}) {
            const ui32 levelIdx = static_cast<ui32>(level);
            const auto& bucket = Buckets_[levelIdx];
            if (bucket.empty()) continue;
            const ui32 pos = RoundRobinPos_[levelIdx]++ % bucket.size();
            return {bucket[pos], level};
        }
        Y_UNREACHABLE();
    }
};

#endif // replaced by per-channel atomic<EDqFillLevel> vector

} // namespace NYql::NDq
