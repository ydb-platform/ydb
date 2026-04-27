#include "partitioning_keys_manager.h"

#include <util/generic/vector.h>

#include <random>

namespace NKikimr::NPQ {

TPartitioningKeysManager::TPartitioningKeysManager(size_t numSketches, TDuration windowSize, size_t initialWeight)
    : WindowSize(windowSize),
      Rng(std::random_device{}()),
      InitialWeight(initialWeight)
{
    Y_ENSURE(numSketches > 0, "numSketches must be greater than 0");
    SketchWindowSize = Max(Min(TDuration::Seconds(1), windowSize), windowSize / numSketches);
}

void TPartitioningKeysManager::Add(TUint128 key, ui64 weight) {
    const TInstant now = Now();
    KeysCounter.Use(key, now);
    RemoveOldSketches(now);
    EnsureSketch(now);
    Sketches.back().Sketch.Add(key, weight);
}

void TPartitioningKeysManager::Add(TUint128 key, ui64 weight, TInstant now) {
    KeysCounter.Use(key, now);
    RemoveOldSketches(now);
    EnsureSketch(now);
    Sketches.back().Sketch.Add(key, weight);
}

void TPartitioningKeysManager::Merge(const TPartitioningKeysManager& other) {
    KeysCounter.Merge(other.KeysCounter);
    auto currentSketch = Sketches.begin();
    for (const auto& sketch : other.Sketches) {
        while (currentSketch != Sketches.end() && currentSketch->StartTime + WindowSize < sketch.StartTime) {
            ++currentSketch;
        }
        
        if (currentSketch == Sketches.end()) {
            EnsureSketch(sketch.StartTime);
            currentSketch = std::prev(Sketches.end());
        }

        currentSketch->Sketch.Merge(sketch.Sketch);
    }
}

TUint128 TPartitioningKeysManager::GetMedianKey() {
    RemoveOldSketches(Now());

    TVector<std::pair<TUint128, ui64>> keysWithWeights;
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

void TPartitioningKeysManager::RemoveOldSketches(TInstant now) {
    while (!Sketches.empty() && Sketches.front().StartTime + WindowSize < now) {
        Sketches.pop_front();
    }
}

void TPartitioningKeysManager::EnsureSketch(TInstant now) {
    if (Sketches.empty() || Sketches.back().StartTime + SketchWindowSize <= now) {
        Sketches.push_back(
            KllSketchWrapper{
                NKll::TDynamicKllSketch<TUint128>(DEFAULT_SKETCH_LEVEL_SIZE, Rng(), InitialWeight),
                now
            }
        );
    }
}

bool TPartitioningKeysManager::MoreThanOneKey(TInstant since) {
    return KeysCounter.Count(since) > 1;
}

} // namespace NKikimr::NPQ
