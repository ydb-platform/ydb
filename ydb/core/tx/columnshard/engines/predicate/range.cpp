#include "range.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

std::set<ui32> TPKRangeFilter::GetColumnIds(const TIndexInfo& indexInfo) const {
    std::set<ui32> result;
    for (auto&& i : PredicateFrom.GetColumnNames()) {
        result.emplace(indexInfo.GetColumnId(i));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("predicate_column", i);
    }
    for (auto&& i : PredicateTo.GetColumnNames()) {
        result.emplace(indexInfo.GetColumnId(i));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("predicate_column", i);
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

bool TPKRangeFilter::IsPortionInUsage(const TPortionInfo& info, const TIndexInfo& indexInfo) const {
    if (auto from = PredicateFrom.ExtractKey(indexInfo.GetPrimaryKey())) {
        const auto& portionEnd = info.IndexKeyEnd();
        const int commonSize = std::min(from->Size(), portionEnd.Size());
        if (std::is_gt(from->ComparePartNotNull(portionEnd, commonSize))) {
            return false;
        }
    }

    if (auto to = PredicateTo.ExtractKey(indexInfo.GetPrimaryKey())) {
        const auto& portionStart = info.IndexKeyStart();
        const int commonSize = std::min(to->Size(), portionStart.Size());
        if (std::is_lt(to->ComparePartNotNull(portionStart, commonSize))) {
            return false;
        }
    }

    return true;
}

bool TPKRangeFilter::IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end, const TIndexInfo& indexInfo) const {
    bool startUsage = false;
    bool endUsage = false;
    if (auto from = PredicateFrom.ExtractKey(indexInfo.GetPrimaryKey())) {
        AFL_VERIFY(from->Size() <= start.Size());
        if (PredicateFrom.IsInclude()) {
            startUsage = std::is_lt(start.ComparePartNotNull(*from, from->Size()));
        } else {
            startUsage = std::is_lteq(start.ComparePartNotNull(*from, from->Size()));
        }
    } else {
        startUsage = true;
    }

    if (auto to = PredicateTo.ExtractKey(indexInfo.GetPrimaryKey())) {
        AFL_VERIFY(to->Size() <= end.Size());
        if (PredicateTo.IsInclude()) {
            endUsage = std::is_gt(end.ComparePartNotNull(*to, to->Size()));
        } else {
            endUsage = std::is_gteq(end.ComparePartNotNull(*to, to->Size()));
        }
    } else {
        endUsage = true;
    }

//    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("start", start.DebugString())("end", end.DebugString())("from", PredicateFrom.DebugString())("to", PredicateTo.DebugString())
//        ("start_usage", startUsage)("end_usage", endUsage);

    return endUsage || startUsage;
}

std::optional<NKikimr::NOlap::TPKRangeFilter> TPKRangeFilter::Build(TPredicateContainer&& from, TPredicateContainer&& to) {
    if (!from.CrossRanges(to)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot_build_predicate_range")("error", "predicates from/to not intersected");
        return {};
    }
    return TPKRangeFilter(std::move(from), std::move(to));
}

}
