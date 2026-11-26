#include "loaded_key_ranges.h"

#include <ydb/core/tx/columnshard/engines/predicate/filter.h>

#include <algorithm>

namespace NKikimr::NOlap {

namespace {

// rangeStart is always a pk prefix: KQP sends NKikimrTx::TKeyRange, whose From is bare cell values without column names, the i-th is the i-th pk column
bool StartsAtOrAbove(const TPredicateContainer& rangeStart, const NArrow::NMerger::TSortableBatchPosition& loadedFrom) {
    if (rangeStart.IsAll()) {
        return false;
    }
    const std::partial_ordering cmp = rangeStart.ComparePartial(loadedFrom);
    if (cmp == std::partial_ordering::greater) {
        return true;
    }
    if (cmp != std::partial_ordering::equivalent) {
        return false;
    }
    const bool isFullKey = rangeStart.NumColumns() == loadedFrom.GetSortFields().size();
    if (isFullKey) {
        return true;
    }
    // loadedFrom = (100, "m")
    if (rangeStart.IsInclude()) {
        // rangeStart ">= 100" selects (100, "a"), but (100, "a") < loadedFrom
        return false;
    } else {
        // rangeStart "> 100" selects (101, *), loadedFrom < (101, *)
        return true;
    }
}

}   // namespace

bool TLoadedKeyRanges::Covers(const TPKRangesFilter& ranges) const {
    switch (Coverage) {
        case ECoverage::None:
            return false;
        case ECoverage::All:
            return true;
        case ECoverage::FromKey:
            break;
    }
    if (ranges.IsEmpty()) {
        return false;
    }
    return std::all_of(ranges.begin(), ranges.end(), [&](const TPKRangeFilter& range) {
        return StartsAtOrAbove(range.GetPredicateFrom(), *FromKeyPosition);
    });
}

bool TLoadedKeyRanges::CoversFrom(const NArrow::TSimpleRow& key) const {
    switch (Coverage) {
        case ECoverage::None:
            return false;
        case ECoverage::All:
            return true;
        case ECoverage::FromKey:
            break;
    }
    return key >= *FromKey;
}

}   // namespace NKikimr::NOlap
