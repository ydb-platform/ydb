#pragma once

#include <util/datetime/base.h>
#include <ydb/library/kll_median/dynamic_sketch.h>

#include <deque>

namespace NKikimr::NPQ {

struct TPartitioningKeysManager {
    TPartitioningKeysManager(size_t numSketches, TDuration windowSize);
    void Add(const TString& key, ui64 msgSize);
    TString GetMedianKey();

private:
    void RemoveOldSketches();

    static constexpr auto DEFAULT_SKETCH_K = 1000;
    static constexpr auto DEFAULT_MIN_WEIGHT = 512; // 512 bytes

    struct KllSketchWrapper {
        NKll::TDynamicKllSketch<TString> Sketch;
        TInstant StartTime;
    };

    std::deque<KllSketchWrapper> Sketches;
    const TDuration WindowSize;
    const TDuration SketchWindowSize;
};

} // namespace NKikimr::NPQ
