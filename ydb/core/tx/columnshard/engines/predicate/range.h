#pragma once
#include "container.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

class TPKRangeFilter: public TMoveOnly {
private:
    TPredicateContainer PredicateFrom;
    TPredicateContainer PredicateTo;
    TPKRangeFilter(TPredicateContainer&& f, TPredicateContainer&& t);

public:
    TPKRangeFilter& operator=(TPKRangeFilter&& rhs);

    TPKRangeFilter(TPKRangeFilter&& rhs);

    bool IsEmpty() const;

    bool IsPointRange(const std::shared_ptr<arrow::Schema>& pkSchema) const;

    const TPredicateContainer& GetPredicateFrom() const {
        return PredicateFrom;
    }

    const TPredicateContainer& GetPredicateTo() const {
        return PredicateTo;
    }

    static TConclusion<TPKRangeFilter> Build(TPredicateContainer&& from, TPredicateContainer&& to);

    bool IsUsed(const TPortionInfo& info) const;
    bool CheckPoint(const NArrow::NMerger::TSortableBatchPosition& point) const;

    enum class EUsageClass {
        NoUsage,
        PartialUsage,
        FullUsage
    };

    EUsageClass GetUsageClass(const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& end) const;

    std::set<ui32> GetColumnIds(const TIndexInfo& indexInfo) const;
    TString DebugString() const;
    std::set<std::string> GetColumnNames() const;
};

}   // namespace NKikimr::NOlap
