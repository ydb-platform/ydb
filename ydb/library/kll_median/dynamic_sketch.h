#pragma once

#include "sketch.h"

#include <util/generic/string.h>

#include <random>

namespace NKikimr::NKll {

    /** Bernoulli(weight / w0); requires 0 < weight < w0. */
    bool TryAcceptSmallWeight(ui64 weight, ui64 w0, std::mt19937_64& rng);

    template <class T>
    class TDynamicKllSketch {
    public:
        explicit TDynamicKllSketch(size_t k, ui64 seed = std::random_device{}(), ui64 initialWeight = 1)
            : Sketch_(k, seed, initialWeight)
        {
            Y_ENSURE(initialWeight > 0 && (initialWeight & (initialWeight - 1)) == 0, "initialWeight must be a power of two");
        }

        T Median() const {
            return Sketch_.Median();
        }

        void Add(const T& x, ui64 weight) {
            if (!weight) {
                return;
            }

            for (ui32 p = 0; p < 64; ++p) {
                ui64 bit = 1ULL << p;
                if (bit > weight) {
                    break;
                }
                if (weight & bit) {
                    AddPow2(x, bit); // bit is power-of-two
                }
            }
        }

        const TDeque<typename TKllSketch<T>::TLevel>& GetLevels() const {
            return Sketch_.Levels_;
        }

    private:
        static constexpr size_t MAX_LEVELS = 30;

        void AddPow2(const T& x, ui64 w) {
            ui64 minWeight = Sketch_.Levels_.empty() ? Sketch_.CurrentWeight_
                                                     : Sketch_.Levels_[0].Weight;

            ui64 placementWeight = w;
            if (w < minWeight) {
                if (!TryAcceptSmallWeight(w, minWeight, Sketch_.Rng_)) {
                    return;
                }
                placementWeight = minWeight;
            }

            Sketch_.N_ += w;
            EnsureMaxWeightAtLeast(placementWeight);
            size_t level = FindSuitableLevel(placementWeight);
            Sketch_.AddToLevel(level, x);

            while (Sketch_.Levels_.size() > MAX_LEVELS) {
                Sketch_.CompactLevel(0, true);
                Sketch_.Levels_.pop_front();
            }
        }

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
