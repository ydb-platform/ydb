#pragma once

#include "sketch.h"

#include <util/generic/string.h>

#include <random>

namespace NKikimr::NKll {

namespace {

/** Bernoulli(weight / w0); requires w0 > 2 * weight (same as w0 > weight && w0 - weight > weight). */
inline bool TryAcceptSmallWeight(ui64 weight, ui64 w0, std::mt19937_64& rng) {
    Y_ASSERT(weight > 0 && w0 > 0);
    Y_ASSERT(w0 > weight && w0 - weight > weight);
    // Single engine call when w0 is a power of two (typical: base weight 1, then 2,4,8,...).
    if ((w0 & (w0 - 1)) == 0) {
        return (static_cast<ui64>(rng()) & (w0 - 1)) < weight;
    }
    std::uniform_int_distribution<ui64> dist(0, w0 - 1);
    return dist(rng) < weight;
}

} // namespace

template <class T>
class TDynamicKllSketch {
public:
    explicit TDynamicKllSketch(size_t k, ui64 seed = std::random_device{}(), ui64 initialWeight = 1) : Sketch_(k, seed, initialWeight) {}

    TString Median() const {
        return Sketch_.Median();
    }

    void Add(const T& x, ui64 weight) {
        if (weight == 0) {
            return;
        }

        ui64 placementWeight = weight;
        if (!Sketch_.Levels_.empty()) {
            const ui64 w0 = Sketch_.Levels_[0].Weight;
            if (w0 > weight && w0 - weight > weight) {
                if (!TryAcceptSmallWeight(weight, w0, Sketch_.Rng_)) {
                    return;
                }
                placementWeight = w0;
            }
        }

        Sketch_.N_ += weight;
        size_t level = FindSuitableLevel(placementWeight);
        Sketch_.AddToLevel(level, x);

        if (Sketch_.IsLevelFull(level)) {
            Sketch_.CompactLevel(level);

            while (Sketch_.Levels_.size() >= MAX_LEVELS) {
                Sketch_.CompactLevel(0, true);
                Sketch_.Levels_.pop_front();
            }
        }
    }

private:
    static constexpr size_t MAX_LEVELS = 30;

    size_t FindSuitableLevel(ui64 w) const {
        auto lb = std::lower_bound(
            Sketch_.Levels_.begin(),
            Sketch_.Levels_.end(),
            w,
            [](const auto& level, ui64 weight) { return level.Weight < weight; });
        return static_cast<size_t>(std::distance(Sketch_.Levels_.begin(), lb));
    }

    TKllSketch<TString> Sketch_;
};

} // namespace NKikimr::NKll
