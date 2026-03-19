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
        TPortionSpan(const ui64 portionId, const ui64 first, const ui64 last)
            : PortionId(portionId)
            , FirstIntervalIdx(first)
            , LastIntervalIdx(last)
        {
            AFL_VERIFY(FirstIntervalIdx <= LastIntervalIdx);
        }

        auto operator==(const TPortionSpan& other) const {
            return std::tie(PortionId, FirstIntervalIdx, LastIntervalIdx) ==
                   std::tie(other.PortionId, other.FirstIntervalIdx, other.LastIntervalIdx);
        }
        class TComparatorByLeftBorder {
        public:
            bool operator()(const TPortionSpan& lhs, const TPortionSpan& rhs) const {
                return std::tie(lhs.FirstIntervalIdx, lhs.LastIntervalIdx, lhs.PortionId) <
                       std::tie(rhs.FirstIntervalIdx, rhs.LastIntervalIdx, rhs.PortionId);
            }
        };
        class TComparatorByRightBorder {
        public:
            bool operator()(const TPortionSpan& lhs, const TPortionSpan& rhs) const {
                return std::tie(lhs.LastIntervalIdx, lhs.FirstIntervalIdx, lhs.PortionId) <
                       std::tie(rhs.LastIntervalIdx, rhs.FirstIntervalIdx, rhs.PortionId);
            }
        };
    };

private:
    YDB_READONLY_DEF(std::vector<TIntervalInfo>, Intervals);
    std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder> Portions;
    std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByRightBorder> CurrentPortions;
    ui64 NextInterval = 0;

private:
    TIntervalsIterator(
        std::vector<TIntervalInfo>&& intervals, std::set<TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder>&& portions)
        : Intervals(std::move(intervals))
        , Portions(std::move(portions))
    {
    }

public:
    bool Next() {
        if (NextInterval == Intervals.size()) {
            return false;
        }
        AFL_VERIFY(NextInterval < Intervals.size());

        for (auto it = CurrentPortions.begin(); it != CurrentPortions.end() && it->GetLastIntervalIdx() < NextInterval;) {
            it = CurrentPortions.erase(it);
        }
        for (auto it = Portions.begin(); it != Portions.end() && it->GetFirstIntervalIdx() == NextInterval;) {
            AFL_VERIFY(CurrentPortions.emplace(*it).second);
            it = Portions.erase(it);
        }

        ++NextInterval;
        return true;
    }

    THashSet<ui64> GetCurrentPortionIds() const {
        THashSet<ui64> portions;
        for (const auto& portion : CurrentPortions) {
            portions.emplace(portion.GetPortionId());
        }
        return portions;
    }

    TIntervalInfo GetCurrentInterval() const {
        AFL_VERIFY(NextInterval <= Intervals.size());
        AFL_VERIFY(NextInterval > 0);
        return Intervals[NextInterval - 1];
    }

    THashSet<ui64> GetNeededPortions() const {
        THashSet<ui64> portions;
        for (const auto& portion : Portions) {
            AFL_VERIFY(portions.emplace(portion.GetPortionId()).second);
        }
        for (const auto& portion : CurrentPortions) {
            AFL_VERIFY(portions.emplace(portion.GetPortionId()).second);
        }
        return portions;
    }

    bool IsDone() const {
        AFL_VERIFY(Portions.empty() == (NextInterval == Intervals.size()))("portions", Portions.size())("current", CurrentPortions.size())(
                                         "next", NextInterval)("intervals", Intervals.size());
        return NextInterval == Intervals.size();
    }
};

class TIntervalsIteratorBuilder {
private:
    std::vector<TIntervalInfo> Intervals;
    std::set<TIntervalsIterator::TPortionSpan, TIntervalsIterator::TPortionSpan::TComparatorByLeftBorder> Portions;
    THashMap<ui64, ui64> FirstIntervalByTrailingPortionId;

public:
    void AppendInterval(const TIntervalBorder& begin, const TIntervalBorder& end, const THashSet<ui64>& portions) {
        AFL_VERIFY(Intervals.empty() || Intervals.back().GetEnd().IsEquivalent(begin) || Intervals.back().GetEnd() < begin)(
                                                                                          "last", Intervals.back().GetEnd().DebugString())(
                                                                                          "new", begin.DebugString());
        const ui64 currentIntervalIdx = Intervals.size();
        Intervals.emplace_back(begin, end, portions);

        std::vector<ui64> completedPortions;
        for (const auto& [portion, firstInterval] : FirstIntervalByTrailingPortionId) {
            if (!portions.contains(portion)) {
                completedPortions.emplace_back(portion);
                AFL_VERIFY(Portions.emplace(portion, firstInterval, currentIntervalIdx - 1).second);
            }
        }
        for (const auto& portion : completedPortions) {
            FirstIntervalByTrailingPortionId.erase(portion);
        }

        for (const ui64 portion : portions) {
            Y_UNUSED(FirstIntervalByTrailingPortionId.emplace(portion, currentIntervalIdx));
        }
    }

    TIntervalsIterator Build() {
        for (const auto& [portion, firstInterval] : FirstIntervalByTrailingPortionId) {
            AFL_VERIFY(Portions.emplace(portion, firstInterval, Intervals.size() - 1).second);
        }
        FirstIntervalByTrailingPortionId.clear();

        return TIntervalsIterator(std::move(Intervals), std::move(Portions));
    }

    ui64 NumIntervals() const {
        return Intervals.size();
    }
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
