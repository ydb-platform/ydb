#pragma once
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/yexception.h>
#include <util/generic/vector.h>

#include <algorithm>

namespace NReservoirSampling {
template <typename T>
class TReservoir {
public:
    TReservoir(TVector<T>&& items, ui64 count, ui64 limit)
        : Items_(std::move(items))
        , Count_(count)
        , Limit_(limit)
    {
        Y_ENSURE(Count_ > Limit_ || Items_.size() == Count_);
        Y_ENSURE(Count_ <= Limit_ || Items_.size() == Limit_);
    }

    TReservoir(T item, ui64 limit)
        : Count_(std::min<ui64>(limit, 1))
        , Limit_(limit)
    {
        Items_.reserve(limit);
        if (limit) {
            Items_.emplace_back(std::move(item));
        }
    }

    inline void Add(IRandomProvider& rnd, T&& item) {
        auto idx = rnd.GenRand64() % ++Count_;
        if (Items_.size() < Limit_) {
            Items_.emplace_back(std::move(item));
            std::swap(Items_[idx], Items_.back());
        } else if (idx < Limit_) {
            Items_[idx] = std::move(item);
        }
    }

    inline void Add(IRandomProvider& rnd, const T& item) {
        Add(rnd, std::move(T(item)));
    }

    // Returns side of the reuslt.
    // false -- result stored in the left, true -- in the right
    [[nodiscard]] bool Merge(IRandomProvider& rnd, TReservoir<T>& rhs) {
        Y_ENSURE(Limit_ == rhs.Limit_); // both sides must have same limit
        // 1. More effective cases
        if (rhs.Count_ <= Limit_) {
            for (auto& item : rhs.Items_) {
                Add(rnd, std::move(item));
            }
            return false; // merged into current
        }

        if (Count_ <= Limit_) {
            return !rhs.Merge(rnd, *this); // merged into opposite side
        }

        // 2. Fairy merge, no optimizations
        ui64 newCount = Count_ + rhs.Count_;
        TVector<T> result(Reserve(Limit_));
        std::array<TReservoir*, 2> sides{this, &rhs};
        for (size_t i = 0; i < Limit_; ++i) {
            ui64 idx = rnd.GenRand64() % (Count_ + rhs.Count_);
            bool isRight = idx >= Count_;
            // choose side
            auto& side = sides[isRight];
            // choose item
            idx -= isRight * Count_; // normalize inside side
            auto& selected = side->Items_[idx % side->Items_.size()];
            result.emplace_back(std::move(selected));
            // move back to it's position
            selected = std::move(side->Items_.back());
            // remove last (now it's empty)
            side->Items_.pop_back();
            --side->Count_;
        }
        Items_ = std::move(result);
        Count_ = newCount;
        return false; // merged into current
    }

    [[nodiscard]] const TVector<T>& GetItems() const {
        return Items_;
    }

    [[nodiscard]] TVector<T>& GetItemsMut() {
        return Items_;
    }

    [[nodiscard]] ui64 GetCount() const {
        return Count_;
    }

    [[nodiscard]] ui64 GetLimit() const {
        return Limit_;
    }

private:
    TVector<T> Items_;
    ui64 Count_;
    ui64 Limit_;
};
} // namespace NReservoirSampling
