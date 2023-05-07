#include "filter.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap {

NKikimr::NArrow::TColumnFilter TPKRangesFilter::BuildFilter(std::shared_ptr<arrow::RecordBatch> data) const {
    if (SortedRanges.empty()) {
        return NArrow::TColumnFilter();
    }
    NArrow::TColumnFilter result = SortedRanges.front().BuildFilter(data);
    for (ui32 i = 1; i < SortedRanges.size(); ++i) {
        result.Or(SortedRanges[i].BuildFilter(data));
    }
    return result;
}

bool TPKRangesFilter::Add(std::shared_ptr<NOlap::TPredicate> f, std::shared_ptr<NOlap::TPredicate> t, const TIndexInfo* indexInfo) {
    if ((!f || f->Empty()) && (!t || t->Empty())) {
        return true;
    }
    auto fromContainer = TPredicateContainer::BuildPredicateFrom(f, indexInfo);
    auto toContainer = TPredicateContainer::BuildPredicateTo(t, indexInfo);
    if (!fromContainer || !toContainer) {
        return false;
    }
    if (SortedRanges.size() && NotFakeRanges) {
        if (ReverseFlag) {
            if (fromContainer->CrossRanges(SortedRanges.front().GetPredicateTo())) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not sorted sequence");
                return false;
            }
        } else {
            if (fromContainer->CrossRanges(SortedRanges.back().GetPredicateTo())) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "add_range_filter")("problem", "not sorted sequence");
                return false;
            }
        }
    }
    auto pkRangeFilter = TPKRangeFilter::Build(std::move(*fromContainer), std::move(*toContainer));
    if (!pkRangeFilter) {
        return false;
    }
    if (!NotFakeRanges) {
        NotFakeRanges = true;
        SortedRanges.clear();
    }
    if (ReverseFlag) {
        SortedRanges.emplace_front(std::move(*pkRangeFilter));
    } else {
        SortedRanges.emplace_back(std::move(*pkRangeFilter));
    }
    return true;
}

TString TPKRangesFilter::DebugString() const {
    if (SortedRanges.empty()) {
        return "no_ranges";
    } else {
        TStringBuilder sb;
        for (auto&& i : SortedRanges) {
            sb << " range{" << i.DebugString() << "}";
        }
        return sb;
    }
}

std::set<ui32> TPKRangesFilter::GetColumnIds(const TIndexInfo& indexInfo) const {
    std::set<ui32> result;
    for (auto&& i : SortedRanges) {
        for (auto&& c : i.GetColumnIds(indexInfo)) {
            result.emplace(c);
        }
    }
    return result;
}

bool TPKRangesFilter::IsPortionInUsage(const TPortionInfo& info, const TIndexInfo& indexInfo) const {
    for (auto&& i : SortedRanges) {
        if (i.IsPortionInUsage(info, indexInfo)) {
            return true;
        }
    }
    return SortedRanges.empty();
}

TPKRangesFilter::TPKRangesFilter(const bool reverse)
    : ReverseFlag(reverse)
{
    auto range = TPKRangeFilter::Build(TPredicateContainer::BuildNullPredicateFrom(), TPredicateContainer::BuildNullPredicateTo());
    Y_VERIFY(range);
    SortedRanges.emplace_back(*range);
}

}
