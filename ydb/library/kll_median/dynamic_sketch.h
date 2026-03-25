#pragma once

#include "sketch.h"

#include <util/generic/string.h>

#include <random>

namespace NKikimr::NKll {

namespace {

/** Bernoulli(weight / w0); requires 0 < weight < w0. */
inline bool TryAcceptSmallWeight(ui64 weight, ui64 w0, std::mt19937_64& rng) {
    Y_ASSERT(w0 > 0);
    Y_ASSERT(weight > 0 && weight < w0);

    if ((w0 & (w0 - 1)) == 0) { // w0 is power of two
        return (static_cast<ui64>(rng()) & (w0 - 1)) < weight;
    }
    std::uniform_int_distribution<ui64> dist(0, w0 - 1);
    return dist(rng) < weight;
}

} // namespace

template <class T>
class TDynamicKllSketch {
public:
    explicit TDynamicKllSketch(size_t k, ui64 seed = std::random_device{}(), ui64 initialWeight = 1) : Sketch_(k, seed, initialWeight) {
        Y_ENSURE(initialWeight > 0 && (initialWeight & (initialWeight - 1)) == 0, "initialWeight must be a power of two");
    }

    T Median() const {
        return Sketch_.Median();
    }

    void Add(const T& x, ui64 weight) {
        if (weight == 0) {
            return;
        }

        ui64 placementWeight = weight;
        ui64 minWeight = Sketch_.CurrentWeight_;
        if (!Sketch_.Levels_.empty()) {
            minWeight = Sketch_.Levels_[0].Weight;
        }
        
        if (minWeight > weight) {
            if (!TryAcceptSmallWeight(weight, minWeight, Sketch_.Rng_)) {
                return;
            }
            placementWeight = minWeight;
        }

        if ((weight & (weight - 1)) != 0) { // weight is not a power of two
            for (ui32 power = 0; (1ULL << power) <= weight; ++power) {
                if ((weight & (1ULL << power)) != 0) {
                    Add(x, 1ULL << power);
                }
            }
            return;
        }

        Sketch_.N_ += weight;
        EnsureMaxWeightAtLeast(placementWeight);

        size_t level = FindSuitableLevel(placementWeight);
        Sketch_.AddToLevel(level, x);

        while (Sketch_.Levels_.size() >= MAX_LEVELS) {
            Sketch_.CompactLevel(0, true);
            Sketch_.Levels_.pop_front();
        }
    }

    const TDeque<typename TKllSketch<T>::TLevel>& GetLevels() const {
        return Sketch_.Levels_;
    }

private:
    static constexpr size_t MAX_LEVELS = 30;

    size_t FindSuitableLevel(ui64 w) {
        Y_ENSURE((w & (w - 1)) == 0, "w must be a power of two");

        const auto& levels = Sketch_.Levels_;
        if (levels.empty()) {
            return 0;
        }
        auto lb = std::lower_bound(
            levels.begin(),
            levels.end(),
            w,
            [](const auto& level, ui64 weight) { return level.Weight < weight; });

        Y_ENSURE(lb != levels.end(), "w must be greater than the last level weight");
        Y_ENSURE(lb->Weight == w, "w must be equal to the level weight");

        return static_cast<size_t>(std::distance(levels.begin(), lb));
    }

    void EnsureMaxWeightAtLeast(ui64 w) {
        if (Sketch_.Levels_.empty()) {
            Sketch_.EnsureLevel(0);
        }
        while (Sketch_.Levels_.back().Weight < w) {
            Sketch_.EnsureLevel(Sketch_.Levels_.size()); // add next level
        }
    }

private:
    TKllSketch<T> Sketch_;
};

} // namespace NKikimr::NKll
