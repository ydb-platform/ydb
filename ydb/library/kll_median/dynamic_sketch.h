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
    explicit TDynamicKllSketch(size_t k, ui64 seed = std::random_device{}(), ui64 initialWeight = 1) : Sketch_(k, seed, initialWeight) {}

    T Median() const {
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
        EnsureMaxWeightAtLeast(placementWeight);
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

    /**
     * Levels have weights W_lo = w0*2^k and W_hi = w0*2^(k+1). When W_lo < w < W_hi,
     * pick the lower or upper level with probabilities (W_hi-w)/(W_hi-W_lo) and (w-W_lo)/(W_hi-W_lo)
     * so the expected represented mass matches w.
     */
    size_t FindSuitableLevel(ui64 w) {
        const auto& levels = Sketch_.Levels_;
        if (levels.empty()) {
            return 0;
        }
        auto lb = std::lower_bound(
            levels.begin(),
            levels.end(),
            w,
            [](const auto& level, ui64 weight) { return level.Weight < weight; });

        if (lb == levels.end()) {
            return levels.size();
        }
        if (lb->Weight == w) {
            return static_cast<size_t>(std::distance(levels.begin(), lb));
        }
        if (lb == levels.begin()) {
            return 0;
        }

        const ui64 wLo = (lb - 1)->Weight;
        const ui64 wHi = lb->Weight;
        Y_ASSERT(wLo < w && w < wHi);

        const size_t iLo = static_cast<size_t>(std::distance(levels.begin(), lb - 1));
        const size_t iHi = static_cast<size_t>(std::distance(levels.begin(), lb));
        const ui64 d = wHi - wLo;
        const ui64 gapAbove = wHi - w;

        if ((d & (d - 1)) == 0) {
            const ui64 u = static_cast<ui64>(Sketch_.Rng_()) & (d - 1);
            return (u < gapAbove) ? iLo : iHi;
        }
        std::uniform_int_distribution<ui64> dist(0, d - 1);
        const ui64 u = dist(Sketch_.Rng_);
        return (u < gapAbove) ? iLo : iHi;
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
