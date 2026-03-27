#pragma once

#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
    
class TIntervalsIterator {
    friend class TIntervalsIteratorBuilder;

private:
    class TPortionSpan {
    private:
        YDB_READONLY_DEF(ui64, PortionId);
        YDB_READONLY_DEF(ui64, FirstIntervalIdx);
        YDB_ACCESSOR_DEF(ui64, LastIntervalIdx);

    public:
        TPortionSpan(const ui64 portionId, const ui64 first, const ui64 last);
    
        auto operator==(const TPortionSpan& other) const;
        class TComparatorByLeftBorder {
        public:
            bool operator()(const TPortionSpan& lhs, const TPortionSpan& rhs) const;
        };
        class TComparatorByRightBorder {
        public:
            bool operator()(const TPortionSpan& lhs, const TPortionSpan& rhs) const;
        };
    };

private:
    YDB_READONLY_DEF(std::vector<TIntervalInfo>, Intervals);
    std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder> Portions;
    std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByRightBorder> CurrentPortions;
    ui64 NextInterval = 0;

private:
    TIntervalsIterator(
        std::vector<TIntervalInfo>&& intervals, std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder>&& portions);

public:
    bool Next();
    THashSet<ui64> GetCurrentPortionIds() const;
    TIntervalInfo GetCurrentInterval() const;
    THashSet<ui64> GetNeededPortions() const;
    bool IsDone() const;
};

class TIntervalsIteratorBuilder {
private:
    std::vector<TIntervalInfo> Intervals;
    std::set<TIntervalsIterator::TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder> Portions;
    THashMap<ui64, ui64> FirstIntervalByTrailingPortionId;

public:
    void AppendInterval(const TIntervalBorder& begin, const TIntervalBorder& end, const THashSet<ui64>& portions);
    TIntervalsIterator Build();
    ui64 NumIntervals() const;
};

class TBuildFilterTaskExecutor: public std::enable_shared_from_this<TBuildFilterTaskExecutor>, TNonCopyable {
private:
    inline static const ui64 BATCH_PORTIONS_COUNT_SOFT_LIMIT = 10;

    TIntervalsIterator Portions;

public:
    TBuildFilterTaskExecutor(TIntervalsIterator&& portions)
        : Portions(std::move(portions))
    {
    }

    bool ScheduleNext(TBuildFilterContext&& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
