#include "range.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

std::set<ui32> TPKRangeFilter::GetColumnIds(const TIndexInfo& indexInfo) const {
    std::set<ui32> result;
    for (auto&& i : PredicateFrom.GetColumnNames()) {
        result.emplace(indexInfo.GetColumnIdVerified(i));
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("predicate_column", i);
    }
    for (auto&& i : PredicateTo.GetColumnNames()) {
        result.emplace(indexInfo.GetColumnIdVerified(i));
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
    if (const auto& from = PredicateFrom.GetReplaceKey()) {
        const auto& portionEnd = info.IndexKeyEnd();
        const int commonSize = std::min(from->Size(), portionEnd.Size());
        if (std::is_gt(from->ComparePartNotNull(portionEnd, commonSize))) {
            return false;
        }
    }

    if (const auto& to = PredicateTo.GetReplaceKey()) {
        const auto& portionStart = info.IndexKeyStart();
        const int commonSize = std::min(to->Size(), portionStart.Size());
        if (std::is_lt(to->ComparePartNotNull(portionStart, commonSize))) {
            return false;
        }
    }
//    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("start", info.IndexKeyStart().DebugString())("end", info.IndexKeyEnd().DebugString())(
//        "from", PredicateFrom.DebugString())("to", PredicateTo.DebugString());

    return true;
}

TPKRangeFilter::EUsageClass TPKRangeFilter::IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end) const {
    {
        std::partial_ordering equalityStartWithFrom = std::partial_ordering::greater;
        if (const auto& from = PredicateFrom.GetReplaceKey()) {
            equalityStartWithFrom = start.ComparePartNotNull(*from, from->Size());
        }
        std::partial_ordering equalityEndWithTo = std::partial_ordering::less;
        if (const auto& to = PredicateTo.GetReplaceKey()) {
            equalityEndWithTo = end.ComparePartNotNull(*to, to->Size());
        }
        const bool startInternal = (equalityStartWithFrom == std::partial_ordering::equivalent && PredicateFrom.IsInclude()) ||
                                   (equalityStartWithFrom == std::partial_ordering::greater);
        const bool endInternal = (equalityEndWithTo == std::partial_ordering::equivalent && PredicateTo.IsInclude()) ||
                                 (equalityEndWithTo == std::partial_ordering::less);
        if (startInternal && endInternal) {
            return EUsageClass::FullUsage;
        }
    }
    

    if (const auto& from = PredicateFrom.GetReplaceKey()) {
        const std::partial_ordering equalityEndWithFrom = end.ComparePartNotNull(*from, from->Size());
        if (equalityEndWithFrom == std::partial_ordering::less) {
            return EUsageClass::DontUsage;
        } else if (equalityEndWithFrom == std::partial_ordering::equivalent) {
            if (PredicateFrom.IsInclude()) {
                return EUsageClass::PartialUsage;
            } else {
                return EUsageClass::DontUsage;
            }
        }
    }

    if (const auto& to = PredicateTo.GetReplaceKey()) {
        const std::partial_ordering equalityStartWithTo = start.ComparePartNotNull(*to, to->Size());
        if (equalityStartWithTo == std::partial_ordering::greater) {
            return EUsageClass::DontUsage;
        } else if (equalityStartWithTo == std::partial_ordering::equivalent) {
            if (PredicateTo.IsInclude()) {
                return EUsageClass::PartialUsage;
            } else {
                return EUsageClass::DontUsage;
            }
        }
    }

//    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("start", start.DebugString())("end", end.DebugString())("from", PredicateFrom.DebugString())(
//        "to", PredicateTo.DebugString());

    return EUsageClass::PartialUsage;
}

TConclusion<TPKRangeFilter> TPKRangeFilter::Build(TPredicateContainer&& from, TPredicateContainer&& to) {
    if (!from.CrossRanges(to)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot_build_predicate_range")("error", "predicates from/to not intersected");
        return TConclusionStatus::Fail("predicates from/to not intersected");
    }
    return TPKRangeFilter(std::move(from), std::move(to));
}

bool TPKRangeFilter::CheckPoint(const NArrow::TReplaceKey& point) const {
    std::partial_ordering equalityWithFrom = std::partial_ordering::greater;
    if (const auto& from = PredicateFrom.GetReplaceKey()) {
        equalityWithFrom = point.ComparePartNotNull(*from, from->Size());
    }
    std::partial_ordering equalityWithTo = std::partial_ordering::less;
    if (const auto& to = PredicateTo.GetReplaceKey()) {
        equalityWithTo = point.ComparePartNotNull(*to, to->Size());
    }
    const bool startInternal = (equalityWithFrom == std::partial_ordering::equivalent && PredicateFrom.IsInclude()) ||
                               (equalityWithFrom == std::partial_ordering::greater);
    const bool endInternal = (equalityWithTo == std::partial_ordering::equivalent && PredicateTo.IsInclude()) ||
                             (equalityWithTo == std::partial_ordering::less);
    return startInternal && endInternal;
}

}
