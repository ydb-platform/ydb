#include "range.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

std::set<ui32> TPKRangeFilter::GetColumnIds(const TIndexInfo& indexInfo) const {
    std::set<ui32> result;
    for (auto&& i : PredicateFrom.GetColumnNames()) {
        result.emplace(indexInfo.GetColumnId(i));
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("predicate_column", i);
    }
    for (auto&& i : PredicateTo.GetColumnNames()) {
        result.emplace(indexInfo.GetColumnId(i));
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("predicate_column", i);
    }
    return result;
}

TString TPKRangeFilter::DebugString() const {
    TStringBuilder sb;
    sb << " from {" << PredicateFrom.DebugString() << "}";
    sb << " to {" << PredicateTo.DebugString() << "}";
    return sb;
}

std::set<std::string> TPKRangeFilter::GetColumnNames() const {
    std::set<std::string> result;
    for (auto&& i : PredicateFrom.GetColumnNames()) {
        result.emplace(i);
    }
    for (auto&& i : PredicateTo.GetColumnNames()) {
        result.emplace(i);
    }
    return result;
}

NKikimr::NArrow::TColumnFilter TPKRangeFilter::BuildFilter(const arrow::Datum& data) const {
    NArrow::TColumnFilter result = PredicateTo.BuildFilter(data);
    return result.And(PredicateFrom.BuildFilter(data));
}

bool TPKRangeFilter::IsPortionInUsage(const TPortionInfo& info) const {
    if (auto from = PredicateFrom.GetReplaceKey()) {
        const auto& portionEnd = info.IndexKeyEnd();
        const int commonSize = std::min(from->Size(), portionEnd.Size());
        if (std::is_gt(from->ComparePartNotNull(portionEnd, commonSize))) {
            return false;
        }
    }

    if (auto to = PredicateTo.GetReplaceKey()) {
        const auto& portionStart = info.IndexKeyStart();
        const int commonSize = std::min(to->Size(), portionStart.Size());
        if (std::is_lt(to->ComparePartNotNull(portionStart, commonSize))) {
            return false;
        }
    }

    return true;
}

bool TPKRangeFilter::IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end) const {
    bool fromInternal = false;
    if (auto from = PredicateFrom.GetReplaceKey()) {
        bool cmpStart = std::is_lteq(start.ComparePartNotNull(*from, from->Size()));
        bool cmpEnd = std::is_gteq(end.ComparePartNotNull(*from, from->Size()));
        fromInternal = cmpStart && cmpEnd;
    }

    bool toInternal = false;
    if (auto to = PredicateTo.GetReplaceKey()) {
        bool cmpStart = std::is_lteq(start.ComparePartNotNull(*to, to->Size()));
        bool cmpEnd = std::is_gteq(end.ComparePartNotNull(*to, to->Size()));
        toInternal = cmpStart && cmpEnd;
    }

//    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("start", start.DebugString())("end", end.DebugString())("from", PredicateFrom.DebugString())("to", PredicateTo.DebugString())
//        ("start_usage", startUsage)("end_usage", endUsage);

    return toInternal != fromInternal;
}

std::optional<NKikimr::NOlap::TPKRangeFilter> TPKRangeFilter::Build(TPredicateContainer&& from, TPredicateContainer&& to) {
    if (!from.CrossRanges(to)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot_build_predicate_range")("error", "predicates from/to not intersected");
        return {};
    }
    return TPKRangeFilter(std::move(from), std::move(to));
}

}
