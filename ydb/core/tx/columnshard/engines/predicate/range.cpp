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

bool TPKRangeFilter::IsUsed(const TPortionInfo& info) const {
    return GetUsageClass(info.IndexKeyStart().BuildSortablePosition(), info.IndexKeyEnd().BuildSortablePosition()) !=
           TPKRangeFilter::EUsageClass::NoUsage;
}

TPKRangeFilter::EUsageClass TPKRangeFilter::GetUsageClass(
    const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& end) const {
    {
        std::partial_ordering equalityFromWithStart = std::partial_ordering::less;
        if (!PredicateFrom.IsAll()) {
            equalityFromWithStart = PredicateFrom.ComparePartial(start);
        }
        std::partial_ordering equalityToWithEnd = std::partial_ordering::greater;
        if (!PredicateTo.IsAll()) {
            equalityToWithEnd = PredicateTo.ComparePartial(end);
        }
        const bool startInternal = (equalityFromWithStart == std::partial_ordering::less) ||
                                   (equalityFromWithStart == std::partial_ordering::equivalent && PredicateFrom.IsInclude());
        const bool endInternal = (equalityToWithEnd == std::partial_ordering::greater) ||
                                 (equalityToWithEnd == std::partial_ordering::equivalent && PredicateTo.IsInclude());
        if (startInternal && endInternal) {
            return EUsageClass::FullUsage;
        }
    }

    if (!PredicateFrom.IsAll()) {
        const std::partial_ordering equalityFromWithEnd = PredicateFrom.ComparePartial(end);
        if (equalityFromWithEnd == std::partial_ordering::greater) {
            return EUsageClass::NoUsage;
        } else if (equalityFromWithEnd == std::partial_ordering::equivalent) {
            if (PredicateFrom.IsInclude()) {
                return EUsageClass::PartialUsage;
            } else {
                return EUsageClass::NoUsage;
            }
        }
    }

    if (!PredicateTo.IsAll()) {
        const std::partial_ordering equalityToWithStart = PredicateTo.ComparePartial(start);
        if (equalityToWithStart == std::partial_ordering::less) {
            return EUsageClass::NoUsage;
        } else if (equalityToWithStart == std::partial_ordering::equivalent) {
            if (PredicateTo.IsInclude()) {
                return EUsageClass::PartialUsage;
            } else {
                return EUsageClass::NoUsage;
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

bool TPKRangeFilter::CheckPoint(const NArrow::NMerger::TSortableBatchPosition& point) const {
    std::partial_ordering equalityFromWithPoint = std::partial_ordering::less;
    if (!PredicateFrom.IsAll()) {
        equalityFromWithPoint = PredicateFrom.ComparePartial(point);
    }
    std::partial_ordering equalityToWithPoint = std::partial_ordering::greater;
    if (!PredicateTo.IsAll()) {
        equalityToWithPoint = PredicateTo.ComparePartial(point);
    }
    const bool startInternal = (equalityFromWithPoint == std::partial_ordering::less) ||
                               (equalityFromWithPoint == std::partial_ordering::equivalent && PredicateFrom.IsInclude());
    const bool endInternal = (equalityToWithPoint == std::partial_ordering::greater) ||
                             (equalityToWithPoint == std::partial_ordering::equivalent && PredicateTo.IsInclude());
    return startInternal && endInternal;
}
}
