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
        TotalFiltersMemorySize.Add(PredicateFrom.GetMemorySize() + PredicateTo.GetMemorySize());
    }

    static inline TPositiveControlInteger TotalFiltersMemorySize;

public:
    TPKRangeFilter& operator=(TPKRangeFilter&& rhs) {
        TotalFiltersMemorySize.Sub(PredicateFrom.GetMemorySize() + PredicateTo.GetMemorySize() + rhs.PredicateFrom.GetMemorySize() + rhs.PredicateTo.GetMemorySize());
        PredicateFrom = std::move(rhs.PredicateFrom);
        PredicateTo = std::move(rhs.PredicateTo);
        TotalFiltersMemorySize.Add(PredicateFrom.GetMemorySize() + PredicateTo.GetMemorySize() + rhs.PredicateFrom.GetMemorySize() + rhs.PredicateTo.GetMemorySize());
        return *this;
    }

    TPKRangeFilter(TPKRangeFilter&& rhs)
        : PredicateFrom([&]() {
            TotalFiltersMemorySize.Sub(rhs.PredicateFrom.GetMemorySize());
            return std::move(rhs.PredicateFrom);
        }())
        , PredicateTo([&]() {
            TotalFiltersMemorySize.Sub(rhs.PredicateTo.GetMemorySize());
            return std::move(rhs.PredicateTo);
        }()) {
        TotalFiltersMemorySize.Add(PredicateFrom.GetMemorySize() + PredicateTo.GetMemorySize() + rhs.PredicateFrom.GetMemorySize() + rhs.PredicateTo.GetMemorySize());
    }

    ~TPKRangeFilter() {
        TotalFiltersMemorySize.Sub(PredicateFrom.GetMemorySize() + PredicateTo.GetMemorySize());
    }

    bool IsEmpty() const {
        return PredicateFrom.IsEmpty() && PredicateTo.IsEmpty();
    }

    bool IsPointRange(const std::shared_ptr<arrow::Schema>& pkSchema) const {
        if (PredicateFrom.IsEmpty() || PredicateTo.IsEmpty()) {
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

    NArrow::TColumnFilter BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const;

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

    static size_t GetFiltersTotalMemorySize() {
        return TotalFiltersMemorySize.Val();
    }
};

}   // namespace NKikimr::NOlap
