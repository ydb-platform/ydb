#include "partitioning_keys_manager.h"

#include <util/generic/vector.h>

#include <random>

namespace NKikimr::NPQ {

TPartitioningKeysManager::TPartitioningKeysManager(size_t numSketches, TDuration windowSize)
    : WindowSize(windowSize)
    , SketchWindowSize(windowSize / numSketches)
{
}

void TPartitioningKeysManager::Add(const TString& key, ui64 msgSize) {
    auto now = Now();
    KeysCounter.Use(key, now);
    RemoveOldSketches();
    if (Sketches.empty() || Sketches.back().StartTime + SketchWindowSize <= now) {
        Sketches.emplace_back(
            NKll::TDynamicKllSketch<TString>(DEFAULT_SKETCH_K, std::random_device{}(), DEFAULT_MIN_WEIGHT),
            now);
    }
    Sketches.back().Sketch.Add(key, msgSize);
}

TString TPartitioningKeysManager::GetMedianKey() {
    RemoveOldSketches();

    TVector<std::pair<TString, ui64>> keysWithWeights;
    for (const auto& sketch : Sketches) {
        const auto& levels = sketch.Sketch.GetLevels();
        for (const auto& level : levels) {
            for (const auto& item : level.Items) {
                keysWithWeights.emplace_back(item, level.Weight);
            }
        }
    }

    if (keysWithWeights.empty()) {
        return {};
    }

    return NKll::GetQuantile(keysWithWeights, 0.5);
}

void TPartitioningKeysManager::RemoveOldSketches() {
    while (!Sketches.empty() && Sketches.front().StartTime + WindowSize < Now()) {
        Sketches.pop_front();
    }
}

bool TPartitioningKeysManager::MoreThanOneKey(TInstant since) {
    return KeysCounter.Count(since) > 1;
}

} // namespace NKikimr::NPQ
