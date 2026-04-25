#pragma once

// KLL-based partitioning keys for split-by-load; used only by autopartitioning_manager in this module.

#include "last_counter.h"

#include <util/datetime/base.h>
#include <ydb/library/kll_median/dynamic_sketch.h>

#include <deque>

namespace NKikimr::NPQ {

using TUint128 = unsigned __int128;

struct TPartitioningKeysManager {
    TPartitioningKeysManager(size_t numSketches, TDuration windowSize, size_t initialWeight = DEFAULT_MIN_WEIGHT);
    void Add(TUint128 key, ui64 weight);
    void Add(TUint128 key, ui64 weight, TInstant now);
    TUint128 GetMedianKey();
    bool MoreThanOneKey(TInstant since);
    void Merge(const TPartitioningKeysManager& other);

private:
    void RemoveOldSketches(TInstant now);
    void EnsureSketch(TInstant now);

    static constexpr auto DEFAULT_SKETCH_LEVEL_SIZE = 100;
    static constexpr auto DEFAULT_MIN_WEIGHT = 16; // 16 bytes

    struct KllSketchWrapper {
        NKll::TDynamicKllSketch<TUint128> BytesSketch;
        TInstant StartTime;
    };

    std::deque<KllSketchWrapper> Sketches;
    const TDuration WindowSize;
    TDuration SketchWindowSize;
    TLastCounter<TUint128> KeysCounter;

    std::mt19937_64 Rng;
    size_t InitialWeight;
};

} // namespace NKikimr::NPQ
