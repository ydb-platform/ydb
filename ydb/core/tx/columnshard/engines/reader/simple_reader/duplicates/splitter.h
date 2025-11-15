#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/formats/arrow/special_keys.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering  {

class TColumnDataSplitter {
private:
    std::vector<TIntervalBorder> Borders;

public:
    TColumnDataSplitter(const THashMap<ui64, TSortableBorders>& sources) {
        AFL_VERIFY(sources.size());
        for (const auto& [id, borders] : sources) {
            Borders.emplace_back(TIntervalBorder::First(borders.GetBegin(), id));
            Borders.emplace_back(TIntervalBorder::Last(borders.GetEnd(), id));
        }
        std::sort(Borders.begin(), Borders.end());
    }

    template <class Callback>
    void ForEachIntersectingInterval(Callback&& callback, const ui64 intersectingPortionId) const {
        AFL_VERIFY(Borders.size());
        THashSet<ui64> currentPortions;
        const TIntervalBorder* prevBorder = &Borders.front();

        bool intersectionsStarted = false;
        for (ui64 i = 1; i < Borders.size(); ++i) {
            const auto& currentBorder = Borders[i];

            if (prevBorder->GetIsLast()) {
                AFL_VERIFY(currentPortions.erase(prevBorder->GetPortionId()));
            } else {
                AFL_VERIFY(currentPortions.insert(prevBorder->GetPortionId()).second);
            }

            if (prevBorder->GetPortionId() == intersectingPortionId) {
                if (prevBorder->GetIsLast()) {
                    AFL_VERIFY(intersectionsStarted);
                    break;
                } else {
                    AFL_VERIFY(!intersectionsStarted);
                    intersectionsStarted = true;
                }
            }

            if (intersectionsStarted && !currentBorder.IsEquivalent(*prevBorder)) {
                if (!callback(*prevBorder, currentBorder, currentPortions)) {
                    break;
                }
            }

            prevBorder = &currentBorder;
        }
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "[";
        for (const auto& border : Borders) {
            sb << border.DebugString();
            sb << ";";
        }
        sb << "]";
        return sb;
    }
};

class TIntervalsInterator {
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
    std::set<TPortionSpan, TIntervalsInterator::TPortionSpan::TComparatorByLeftBorder> Portions;
    std::set<TPortionSpan, TIntervalsInterator::TPortionSpan::TComparatorByRightBorder> CurrentPortions;
    ui64 NextInterval = 0;

private:
    TIntervalsInterator(
        std::vector<TIntervalInfo>&& intervals, std::set<TPortionSpan, TIntervalsInterator::TPortionSpan::TComparatorByLeftBorder>&& portions)
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
    std::set<TIntervalsInterator::TPortionSpan, TIntervalsInterator::TPortionSpan::TComparatorByLeftBorder> Portions;
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

    TIntervalsInterator Build() {
        for (const auto& [portion, firstInterval] : FirstIntervalByTrailingPortionId) {
            AFL_VERIFY(Portions.emplace(portion, firstInterval, Intervals.size() - 1).second);
        }
        FirstIntervalByTrailingPortionId.clear();

        return TIntervalsInterator(std::move(Intervals), std::move(Portions));
    }

    static TIntervalsInterator BuildFromSplitter(
        const TColumnDataSplitter& splitter, const std::vector<ui32>& intervalIdxs, const ui64 basePortionId) {
        TIntervalsIteratorBuilder builder;
        auto intervalIt = intervalIdxs.begin();
        ui64 currentInterval = 0;
        const auto callback = [&intervalIdxs, &builder, &intervalIt, &currentInterval](
                                  const TIntervalBorder& begin, const TIntervalBorder& end, const THashSet<ui64>& portions) {
            if (intervalIt == intervalIdxs.end()) {
                return false;
            }

            if (currentInterval != *intervalIt) {
                AFL_VERIFY(currentInterval < *intervalIt);
                ++currentInterval;
                return true;
            }

            builder.AppendInterval(begin, end, portions);

            ++currentInterval;
            ++intervalIt;
            return true;
        };

        splitter.ForEachIntersectingInterval(callback, basePortionId);
        AFL_VERIFY(builder.NumIntervals() == intervalIdxs.size())("builder", builder.NumIntervals())(
                                               "intervals", TStringBuilder() << '[' << JoinSeq(',', intervalIdxs) << ']');
        return builder.Build();
    }

    ui64 NumIntervals() const {
        return Intervals.size();
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
