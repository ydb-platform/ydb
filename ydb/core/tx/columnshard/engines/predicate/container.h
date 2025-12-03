#pragma once
#include "predicate.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/rows/view.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/replace_key.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <optional>

namespace NKikimr::NOlap {

struct TIndexInfo;

class TPredicateContainer {
private:
    std::optional<NOlap::TPredicate> Object;

    TPredicateContainer(std::optional<NOlap::TPredicate> object)
        : Object(object)
    {
    }

    TPredicateContainer() {
    }

    static std::partial_ordering ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r);

public:
    bool IsSchemaEqualTo(const std::shared_ptr<arrow::Schema>& schema) const {
        if (!Object) {
            return false;
        }
        return Object->IsEqualSchema(schema);
    }

    bool IsEqualPointTo(const TPredicateContainer& item) const {
        if (IsAll() != item.IsAll()) {
            return false;
        }
        if (!Object) {
            return true;
        }
        return Object->IsEqualPointTo(*item.Object);
    }

    NArrow::ECompareType GetCompareType() const {
        AFL_VERIFY(Object);
        return Object->GetCompareType();
    }

    std::partial_ordering ComparePartial(const NArrow::NMerger::TSortableBatchPosition& pk) const {
        AFL_VERIFY(Object);
        return Object->Batch.ComparePartial(pk);
    }

    void AppendPointTo(std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) const {
        AFL_VERIFY(Object);
        Object->Batch.BuildSortingCursor().AppendPositionTo(builders, nullptr);
    }

    bool IsAll() const {
        return !Object;
    }

    TString DebugString() const;

    int MatchScalar(const ui32 columnIdx, const std::shared_ptr<arrow::Scalar>& s) const;

    std::vector<std::string> GetColumnNames() const;
    ui32 NumColumns() const {
        return Object ? Object->NumColumns() : 0;
    }

    bool IsForwardInterval() const;
    bool IsBackwardInterval() const;

    bool IsInclude() const;

    bool CrossRanges(const TPredicateContainer& ext) const;

    static TPredicateContainer BuildNullPredicateFrom() {
        return TPredicateContainer();
    }

    static TConclusion<TPredicateContainer> BuildPredicateFrom(std::optional<TPredicate> object);
    static TConclusion<TPredicateContainer> BuildPredicateTo(std::optional<TPredicate> object);

    static TPredicateContainer BuildNullPredicateTo() {
        return TPredicateContainer();
    }

    std::optional<NArrow::NMerger::TSortableBatchPosition::TFoundPosition> FindFirstIncluded(
        NArrow::NMerger::TRWSortableBatchPosition& begin) const;
    std::optional<NArrow::NMerger::TSortableBatchPosition::TFoundPosition> FindFirstExcluded(
        NArrow::NMerger::TRWSortableBatchPosition& begin) const;
};

}   // namespace NKikimr::NOlap
