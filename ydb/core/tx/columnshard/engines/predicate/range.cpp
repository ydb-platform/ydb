#include "range.h"
#include <library/cpp/actors/core/log.h>

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

NKikimr::NArrow::TColumnFilter TPKRangeFilter::BuildFilter(std::shared_ptr<arrow::RecordBatch> data) const {
    NArrow::TColumnFilter result = PredicateTo.BuildFilter(data);
    result.And(PredicateFrom.BuildFilter(data));
    return result;
}

bool TPKRangeFilter::IsPortionInUsage(const TPortionInfo& info, const TIndexInfo& indexInfo) const {
    ui32 idx = 0;
    bool matchFrom = false;
    bool matchTo = false;
    for (auto&& c : indexInfo.GetReplaceKey()->field_names()) {
        std::shared_ptr<arrow::Scalar> minValue;
        std::shared_ptr<arrow::Scalar> maxValue;
        info.MinMaxValue(indexInfo.GetColumnId(c), minValue, maxValue);
        if (!matchFrom) {
            const int result = PredicateFrom.MatchScalar(idx, maxValue);
            if (result < 0) {
                return false;
            } else if (result > 0) {
                matchFrom = true;
                if (matchTo) {
                    return true;
                }
            } else if (result == 0) {
                ++idx;
                if (matchTo) {
                    continue;
                }
            }
        }
        if (!matchTo) {
            const int result = PredicateTo.MatchScalar(idx, minValue);
            if (result < 0) {
                return false;
            } else if (result > 0) {
                matchTo = true;
                if (matchFrom) {
                    return true;
                }
            } else if (result == 0) {
                ++idx;
                if (matchFrom) {
                    continue;
                }
            }
        }
        ++idx;
    }
    return true;
}

std::optional<NKikimr::NOlap::TPKRangeFilter> TPKRangeFilter::Build(TPredicateContainer&& from, TPredicateContainer&& to) {
    if (!from.CrossRanges(to)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "cannot_build_predicate_range")("error", "predicates from/to not intersected");
        return {};
    }
    return TPKRangeFilter(std::move(from), std::move(to));
}

}
