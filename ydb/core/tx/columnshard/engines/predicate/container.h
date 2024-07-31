#pragma once
#include "predicate.h"
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <optional>

namespace NKikimr::NOlap {

struct TIndexInfo;

class TPredicateContainer {
private:
    std::shared_ptr<NOlap::TPredicate> Object;
    NArrow::ECompareType CompareType;
    mutable std::optional<std::vector<TString>> ColumnNames;
    std::shared_ptr<NArrow::TReplaceKey> ReplaceKey;

    TPredicateContainer(std::shared_ptr<NOlap::TPredicate> object, const std::shared_ptr<NArrow::TReplaceKey>& replaceKey)
        : Object(object)
        , CompareType(Object->GetCompareType())
        , ReplaceKey(replaceKey) {
        AFL_VERIFY(!!ReplaceKey);
    }

    TPredicateContainer(const NArrow::ECompareType compareType)
        : CompareType(compareType) {
    }

    static std::partial_ordering ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r);

    static std::shared_ptr<NArrow::TReplaceKey> ExtractKey(const NOlap::TPredicate& predicate, const std::shared_ptr<arrow::Schema>& key) {
        AFL_VERIFY(predicate.Batch);
        const auto& batchFields = predicate.Batch->schema()->fields();
        const auto& keyFields = key->fields();
        size_t minSize = std::min(batchFields.size(), keyFields.size());
        for (size_t i = 0; i < minSize; ++i) {
            Y_DEBUG_ABORT_UNLESS(batchFields[i]->type()->Equals(*keyFields[i]->type()));
        }
        if (batchFields.size() <= keyFields.size()) {
            return std::make_shared<NArrow::TReplaceKey>(NArrow::TReplaceKey::FromBatch(predicate.Batch, predicate.Batch->schema(), 0));
        } else {
            return std::make_shared<NArrow::TReplaceKey>(NArrow::TReplaceKey::FromBatch(predicate.Batch, key, 0));
        }
    }

public:

    const NArrow::TReplaceKey& GetReplaceKey() const {
        return *ReplaceKey;
    }

    bool IsEmpty() const {
        return !Object;
    }

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

    NKikimr::NArrow::TColumnFilter BuildFilter(const arrow::Datum& data) const {
        if (!Object) {
            return NArrow::TColumnFilter::BuildAllowFilter();
        }
        return NArrow::TColumnFilter::MakePredicateFilter(data, Object->Batch, CompareType);
    }
};

}
