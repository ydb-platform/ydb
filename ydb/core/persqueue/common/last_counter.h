#pragma once

// Sliding window of distinct values with last-use timestamps (used by autopartitioning / KLL key stats).

#include <algorithm>
#include <deque>

#include <util/datetime/base.h>

namespace NKikimr::NPQ {

template <typename T>
class TLastCounter {
    static constexpr size_t MaxValueCount = 2;

public:
    void Use(const T& value, const TInstant& now) {
        const auto full = MaxValueCount == Values.size();
        if (!Values.empty() && Values[0].Value == value) {
            auto& v0 = Values[0];
            if (v0.LastUseTime < now) {
                v0.LastUseTime = now;
                if (full && Values[1].LastUseTime != now) {
                    Values.push_back(std::move(v0));
                    Values.pop_front();
                }
            }
        } else if (full && Values[1].Value == value) {
            Values[1].LastUseTime = now;
        } else if (!full || Values[0].LastUseTime < now) {
            if (full) {
                Values.pop_front();
            }
            Values.push_back(Data{now, value});
        }
    }

    size_t Count(const TInstant& expirationTime) {
        return std::count_if(Values.begin(), Values.end(), [&](const auto& i) {
            return i.LastUseTime >= expirationTime;
        });
    }

    const T& LastValue() const {
        return Values.back().Value;
    }

private:
    struct Data {
        TInstant LastUseTime;
        T Value;
    };
    std::deque<Data> Values;
};

} // namespace NKikimr::NPQ
