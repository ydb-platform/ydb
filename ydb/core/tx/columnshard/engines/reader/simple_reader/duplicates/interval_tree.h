#pragma once

#include <ydb/library/accessor/accessor.h>

#include <map>
#include <tuple>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

template <typename TKey>
class TInterval {
private:
    YDB_READONLY_DEF(TKey, Start);
    YDB_READONLY_DEF(TKey, Finish);

public:
    TInterval(TKey start, TKey finish)
        : Start(std::move(start))
        , Finish(std::move(finish)) {
    }

    std::partial_ordering operator<=>(const TInterval& other) const {
        return std::tie(Start, Finish) <=> std::tie(other.Start, other.Finish);
    }
    bool operator==(const TInterval& other) const {
        return Start == other.Start && Finish == other.Finish;
    }
};

template <class TKey, class TValue>
class TFakeIntervalTree {
private:
    std::multimap<TInterval<TKey>, TValue> Intervals;

public:
    void Insert(const TInterval<TKey>& interval, TValue&& value) {
        Intervals.emplace(interval, std::move(value));
    }

    template <class TCallback>
    void FindIntersections(const TKey& start, const TKey& finish, TCallback&& callback) const {
        for (const auto& [interval, value] : Intervals) {
            if (interval.GetStart() <= finish && interval.GetFinish() >= start) {
                callback(interval, value);
            }
        }
    }
};

template <class TKey, class TValue>
using TIntervalTree = TFakeIntervalTree<TKey, TValue>;

}   // namespace NKikimr::NOlap::NReader::NSimple
