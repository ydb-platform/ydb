#pragma once

#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <utility>

namespace NKikimr::NKll {

template <class U>
class TWindowedKll;

/**
 * KLL sketch for approximate streaming quantiles.
 * k: accuracy parameter (typically hundreds/thousands). cap = 2*k.
 */
template <class T>
class TKllSketch {
public:
    explicit TKllSketch(size_t k, ui64 seed = std::random_device{}())
        : K_(k)
        , Cap_(2 * k)
        , N_(0)
        , Rng_(seed)
        , Bit_(0, 1)
    {
        Y_ENSURE(K_ >= 2, "k must be >= 2");
    }

    void Add(const T& x) {
        ++N_;
        if (Levels_.empty()) {
            Levels_.push_back({});
        }
        Levels_[0].push_back(x);
        if (Levels_[0].size() > Cap_) {
            CompactLevel(0);
        }
    }

    void AddToLevel(size_t lvl, const T& x) {
        EnsureLevel_(lvl);
        Levels_[lvl].push_back(x);
        if (Levels_[lvl].size() > Cap_) {
            CompactLevel(lvl);
        }
    }

    T Median() const {
        return Quantile(0.5L);
    }

    T Quantile(long double phi) const {
        Y_ENSURE(N_ > 0, "Quantile() on empty sketch");
        if (phi < 0) {
            phi = 0;
        }
        if (phi > 1) {
            phi = 1;
        }

        TVector<std::pair<T, long double>> items;
        items.reserve(TotalStored_());

        for (size_t lvl = 0; lvl < Levels_.size(); ++lvl) {
            long double w = std::ldexp(1.0L, static_cast<int>(lvl));
            for (const auto& v : Levels_[lvl]) {
                items.emplace_back(v, w);
            }
        }

        std::sort(items.begin(), items.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

        long double target = phi * static_cast<long double>(N_);
        if (target <= 0) {
            return items.front().first;
        }

        long double acc = 0;
        for (const auto& [v, w] : items) {
            acc += w;
            if (acc >= target) {
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
    friend class TWindowedKll;

private:
    size_t TotalStored_() const {
        size_t s = 0;
        for (const auto& lvl : Levels_) {
            s += lvl.size();
        }
        return s;
    }

    void EnsureLevel_(size_t lvl) {
        if (Levels_.size() <= lvl) {
            Levels_.resize(lvl + 1);
        }
    }

    void CompactLevel(size_t lvl) {
        EnsureLevel_(lvl);
        auto& buf = Levels_[lvl];
        if (buf.size() <= Cap_) {
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

        EnsureLevel_(lvl + 1);
        auto& up = Levels_[lvl + 1];
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
        if (other.N_ == 0) {
            return;
        }

        Y_ENSURE(K_ == other.K_, "Cannot merge KLL sketches with different k");
        Y_ENSURE(Cap_ == other.Cap_, "Cannot merge KLL sketches with different cap");

        N_ += other.N_;

        if (Levels_.size() < other.Levels_.size()) {
            Levels_.resize(other.Levels_.size());
        }

        for (size_t lvl = 0; lvl < other.Levels_.size(); ++lvl) {
            const auto& src = other.Levels_[lvl];
            auto& dst = Levels_[lvl];
            dst.insert(dst.end(), src.begin(), src.end());
        }

        for (size_t lvl = 0; lvl < Levels_.size(); ++lvl) {
            if (Levels_[lvl].size() > Cap_) {
                CompactLevel(lvl);
            }
        }
    }

private:
    const size_t K_;
    const size_t Cap_;
    ui64 N_;

    TVector<TVector<T>> Levels_;

    mutable std::mt19937_64 Rng_;
    mutable std::uniform_int_distribution<int> Bit_;
};

} // namespace NKikimr::NKll
