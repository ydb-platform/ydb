#pragma once
#include "container.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

class TPKRangeFilter {
private:
    TPredicateContainer PredicateFrom;
    TPredicateContainer PredicateTo;
    TPKRangeFilter(TPredicateContainer&& f, TPredicateContainer&& t)
        : PredicateFrom(std::move(f))
        , PredicateTo(std::move(t)) {
    }

public:
    bool IsEmpty() const {
        return PredicateFrom.IsEmpty() && PredicateTo.IsEmpty();
    }

    bool IsPointRange(const std::shared_ptr<arrow::Schema>& pkSchema) const {
        return PredicateFrom.GetCompareType() == NArrow::ECompareType::GREATER_OR_EQUAL &&
               PredicateTo.GetCompareType() == NArrow::ECompareType::LESS_OR_EQUAL && PredicateFrom.IsEqualPointTo(PredicateTo) &&
               PredicateFrom.IsSchemaEqualTo(pkSchema);
    }

    const TPredicateContainer& GetPredicateFrom() const {
        return PredicateFrom;
    }

    const TPredicateContainer& GetPredicateTo() const {
        return PredicateTo;
    }

    static TConclusion<TPKRangeFilter> Build(TPredicateContainer&& from, TPredicateContainer&& to);

    NArrow::TColumnFilter BuildFilter(const arrow::Datum& data) const;

    bool IsPortionInUsage(const TPortionInfo& info) const;
    bool CheckPoint(const NArrow::TReplaceKey& point) const;

    enum class EUsageClass {
        DontUsage,
        PartialUsage,
        FullUsage
    };

    EUsageClass IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end) const;

    std::set<ui32> GetColumnIds(const TIndexInfo& indexInfo) const;
    TString DebugString() const;
    std::set<std::string> GetColumnNames() const;
};

}   // namespace NKikimr::NOlap
