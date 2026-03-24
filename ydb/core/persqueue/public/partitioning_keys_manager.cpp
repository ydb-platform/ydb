#include "partitioning_keys_manager.h"

#include <util/generic/vector.h>

#include <algorithm>
#include <random>

namespace NKikimr::NPQ {

TPartitioningKeysManager::TPartitioningKeysManager(size_t numSketches, TDuration windowSize)
    : WindowSize(windowSize)
    , SketchWindowSize(windowSize / numSketches)
{
}

void TPartitioningKeysManager::Add(const TString& key, ui64 msgSize) {
    RemoveOldSketches();
    if (Sketches.empty() || Sketches.back().StartTime + SketchWindowSize <= Now()) {
        Sketches.emplace_back(
            NKll::TDynamicKllSketch<TString>(DEFAULT_SKETCH_K, std::random_device{}(), DEFAULT_MIN_WEIGHT),
            Now());
    }
    Sketches.back().Sketch.Add(key, msgSize);
}

TString TPartitioningKeysManager::GetMedianKey() {
    RemoveOldSketches();

    TVector<TString> medians;
    for (const auto& sketch : Sketches) {
        TString median = sketch.Sketch.Median();
        if (median == "") {
            continue;
        }
        medians.push_back(std::move(median));
    }

    if (medians.empty()) {
        return {};
    }

    std::sort(medians.begin(), medians.end());
    return medians[medians.size() / 2];
}

void TPartitioningKeysManager::RemoveOldSketches() {
    while (!Sketches.empty() && Sketches.front().StartTime + WindowSize < Now()) {
        Sketches.pop_front();
    }
}

} // namespace NKikimr::NPQ
