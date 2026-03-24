#pragma once

#include <util/generic/vector.h>
#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <utility>

namespace NKikimr::NKll {

/**
 * KLL sketch for approximate streaming quantiles.
 * k: accuracy parameter (typically hundreds/thousands). cap = 2*k.
 */
template <class T>
class TKllSketch {
public:
    explicit TKllSketch(size_t k, ui64 seed = std::random_device{}(), size_t initialWeight = 1)
        : Cap_(2 * k)
        , N_(0)
        , CurrentWeight_(initialWeight)
        , Rng_(seed)
        , Bit_(0, 1)
    {
        Y_ENSURE(k >= 2, "k must be >= 2");
    }

    void Add(const T& x) {
        ++N_;
        EnsureLevel(0);
        Levels_[0].Items.push_back(x);
        if (Levels_[0].Items.size() > Cap_) {
            CompactLevel(0);
        }
    }

    void AddToLevel(size_t lvl, const T& x) {
        EnsureLevel(lvl);
        Levels_[lvl].Items.push_back(x);
        if (Levels_[lvl].Items.size() > Cap_) {
            CompactLevel(lvl);
        }
    }

    T Median() const {
        return Quantile(0.5L);
    }

    T Quantile(long double phi) const {
        Y_ENSURE(TotalStored() > 0, "Quantile() on empty sketch");
        if (phi < 0) {
            phi = 0;
        }
        if (phi > 1) {
            phi = 1;
        }

        TVector<std::pair<T, ui64>> items;
        items.reserve(TotalStored());

        for (size_t lvl = 0; lvl < Levels_.size(); ++lvl) {
            ui64 w = Levels_[lvl].Weight;
            for (const auto& v : Levels_[lvl].Items) {
                items.emplace_back(v, w);
            }
        }

        std::sort(items.begin(), items.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

        ui64 totalWeight = 0;
        for (const auto& [v, w] : items) {
            totalWeight += w;
        }

        long double target = phi * static_cast<long double>(totalWeight);
        if (target <= 0) {
            return items.front().first;
        }

        ui64 accWeight = 0;
        for (const auto& [v, w] : items) {
            accWeight += w;
            if (static_cast<long double>(accWeight) >= target) {
                return v;
            }
        }

        return items.back().first;
    }

    ui64 Count() const {
        return N_;
    }

    size_t NumLevels() const {
        return Levels_.size();
    }

    template <class U>
    friend class TDynamicKllSketch;

private:
    size_t TotalStored() const {
        size_t s = 0;
        for (const auto& lvl : Levels_) {
            s += lvl.Items.size();
        }
        return s;
    }

    void EnsureLevel(size_t lvl) {
        while (Levels_.size() <= lvl) {
            Levels_.emplace_back(CurrentWeight_);
            CurrentWeight_ <<= 1;
        }
    }

    void CompactLevel(size_t lvl, bool force = false) {
        EnsureLevel(lvl);
        auto& buf = Levels_[lvl].Items;
        if (buf.size() <= Cap_ && !force) {
            return;
        }

        std::sort(buf.begin(), buf.end());

        bool keepOne = (buf.size() % 2 == 1);
        T kept{};
        if (keepOne) {
            std::uniform_int_distribution<size_t> pick(0, buf.size() - 1);
            size_t idx = pick(Rng_);
            kept = buf[idx];
            buf.erase(buf.begin() + static_cast<std::ptrdiff_t>(idx));
        }

        int r = Bit_(Rng_);
        TVector<T> promoted;
        promoted.reserve(buf.size() / 2);

        for (size_t i = static_cast<size_t>(r); i < buf.size(); i += 2) {
            promoted.push_back(buf[i]);
        }

        buf.clear();
        if (keepOne) {
            buf.push_back(kept);
        }

        EnsureLevel(lvl + 1);
        auto& up = Levels_[lvl + 1].Items;
        up.insert(up.end(), promoted.begin(), promoted.end());

        if (up.size() > Cap_) {
            CompactLevel(lvl + 1);
        }
    }

    void Clear() {
        N_ = 0;
        Levels_.clear();
    }

    void Merge(const TKllSketch& other) {
        if (this == &other) {
            return;
        }
        if (other.TotalStored() == 0) {
            return;
        }

        Y_ENSURE(Cap_ == other.Cap_, "Cannot merge KLL sketches with different cap");

        N_ += other.N_;

        for (size_t lvl = 0; lvl < other.Levels_.size(); ++lvl) {
            EnsureLevel(lvl);
            Y_ENSURE(Levels_[lvl].Weight == other.Levels_[lvl].Weight, "Cannot merge: level weight mismatch");
            auto& dst = Levels_[lvl].Items;
            const auto& src = other.Levels_[lvl].Items;
            dst.insert(dst.end(), src.begin(), src.end());
        }

        for (size_t lvl = 0; lvl < Levels_.size(); ++lvl) {
            if (Levels_[lvl].Items.size() > Cap_) {
                CompactLevel(lvl);
            }
        }
    }

    bool IsLevelFull(size_t lvl) const {
        return Levels_[lvl].Items.size() >= Cap_;
    }

private:
    struct TLevel {
        TLevel(ui64 weight) : Weight(weight) {}

        TVector<T> Items;
        ui64 Weight;
    };

    const size_t Cap_;
    ui64 N_;
    ui64 CurrentWeight_;

    TDeque<TLevel> Levels_;

    mutable std::mt19937_64 Rng_;
    mutable std::uniform_int_distribution<int> Bit_;
};

} // namespace NKikimr::NKll
