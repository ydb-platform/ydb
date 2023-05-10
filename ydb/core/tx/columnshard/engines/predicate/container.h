#pragma once
#include "predicate.h"
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <optional>

namespace NKikimr::NOlap {

class TPredicateContainer {
private:
    std::shared_ptr<NOlap::TPredicate> Object;
    NArrow::ECompareType CompareType;
    mutable std::optional<std::vector<TString>> ColumnNames;

    TPredicateContainer(std::shared_ptr<NOlap::TPredicate> object)
        : Object(object)
        , CompareType(Object->GetCompareType()) {
    }

    TPredicateContainer(const NArrow::ECompareType compareType)
        : CompareType(compareType) {
    }

    static std::partial_ordering ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r);

public:

    template <class TArrayColumn>
    std::optional<typename TArrayColumn::value_type> Get(const ui32 colIndex, const ui32 rowIndex,
        const std::optional<typename TArrayColumn::value_type> defaultValue = {}) const {
        if (!Object) {
            return defaultValue;
        } else {
            return Object->Get<TArrayColumn>(colIndex, rowIndex, defaultValue);
        }
    }

    TString DebugString() const;

    int MatchScalar(const ui32 columnIdx, const std::shared_ptr<arrow::Scalar>& s) const;

    const std::vector<TString>& GetColumnNames() const;

    bool IsForwardInterval() const;

    bool IsInclude() const;

    bool CrossRanges(const TPredicateContainer& ext);

    static TPredicateContainer BuildNullPredicateFrom() {
        return TPredicateContainer(NArrow::ECompareType::GREATER_OR_EQUAL);
    }

    static std::optional<TPredicateContainer> BuildPredicateFrom(std::shared_ptr<NOlap::TPredicate> object, const TIndexInfo* indexInfo);

    static TPredicateContainer BuildNullPredicateTo() {
        return TPredicateContainer(NArrow::ECompareType::LESS_OR_EQUAL);
    }

    static std::optional<TPredicateContainer> BuildPredicateTo(std::shared_ptr<NOlap::TPredicate> object, const TIndexInfo* indexInfo);

    NKikimr::NArrow::TColumnFilter BuildFilter(std::shared_ptr<arrow::RecordBatch> data) const {
        if (!Object) {
            return NArrow::TColumnFilter();
        }
        return NArrow::TColumnFilter::MakePredicateFilter(data, Object->Batch, CompareType);
    }

    std::optional<NArrow::TReplaceKey> ExtractKey(const std::shared_ptr<arrow::Schema>& key) const {
        if (Object) {
            return NArrow::TReplaceKey::FromBatch(Object->Batch, key, 0);
        }
        return {};
    }
};

}
