#pragma once
#include "container.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

class TPKRangeFilter: public TMoveOnly {
private:
    TPredicateContainer PredicateFrom;
    TPredicateContainer PredicateTo;
    TPKRangeFilter(TPredicateContainer&& f, TPredicateContainer&& t)
        : PredicateFrom(std::move(f))
        , PredicateTo(std::move(t)) {
    }

public:
    TPKRangeFilter& operator=(TPKRangeFilter&& rhs) {
        PredicateFrom = std::move(rhs.PredicateFrom);
        PredicateTo = std::move(rhs.PredicateTo);
        return *this;
    }

    TPKRangeFilter(TPKRangeFilter&& rhs)
        : PredicateFrom([&]() {
            return std::move(rhs.PredicateFrom);
        }())
        , PredicateTo([&]() {
            return std::move(rhs.PredicateTo);
        }()) {
    }

    bool IsEmpty() const {
        return PredicateFrom.IsAll() && PredicateTo.IsAll();
    }

    bool IsPointRange(const std::shared_ptr<arrow::Schema>& pkSchema) const {
        if (PredicateFrom.IsAll() || PredicateTo.IsAll()) {
            return false;
        }
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
