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
    std::shared_ptr<NOlap::TPredicate> Object;
    NArrow::ECompareType CompareType;
    mutable std::optional<std::vector<TString>> ColumnNames;
    std::shared_ptr<NArrow::TSimpleRow> ReplaceKey;

    TPredicateContainer(std::shared_ptr<NOlap::TPredicate> object, const std::shared_ptr<NArrow::TSimpleRow>& replaceKey)
        : Object(object)
        , CompareType(Object->GetCompareType())
        , ReplaceKey(replaceKey) {
    }

    TPredicateContainer(const NArrow::ECompareType compareType)
        : CompareType(compareType) {
    }

    static std::partial_ordering ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r);

    static std::shared_ptr<NArrow::TSimpleRow> ExtractKey(const NOlap::TPredicate& predicate, const std::shared_ptr<arrow::Schema>& key) {
        AFL_VERIFY(predicate.Batch);
        const auto& batchFields = predicate.Batch->schema()->fields();
        const auto& keyFields = key->fields();
        AFL_VERIFY(batchFields.size() <= keyFields.size());
        for (size_t i = 0; i < batchFields.size(); ++i) {
            Y_DEBUG_ABORT_UNLESS(batchFields[i]->type()->Equals(*keyFields[i]->type()));
        }
        return std::make_shared<NArrow::TSimpleRow>(predicate.Batch, 0);
    }

public:
    bool IsSchemaEqualTo(const std::shared_ptr<arrow::Schema>& schema) const {
        if (!Object) {
            return false;
        }
        return Object->IsEqualSchema(schema);
    }

    bool IsEqualPointTo(const TPredicateContainer& item) const {
        if (!Object != !item.Object) {
            return false;
        }
        if (!Object) {
            return IsForwardInterval() == item.IsForwardInterval();
        }
        return Object->IsEqualTo(*item.Object);
    }

    NArrow::ECompareType GetCompareType() const {
        return CompareType;
    }

    const std::shared_ptr<NArrow::TSimpleRow>& GetReplaceKey() const {
        return ReplaceKey;
    }

    bool IsEmpty() const {
        return !Object;
    }

    template <class TArrayColumn>
    std::optional<typename TArrayColumn::value_type> Get(
        const ui32 colIndex, const ui32 rowIndex, const std::optional<typename TArrayColumn::value_type> defaultValue = {}) const {
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

    bool CrossRanges(const TPredicateContainer& ext) const;

    static TPredicateContainer BuildNullPredicateFrom() {
        return TPredicateContainer(NArrow::ECompareType::GREATER_OR_EQUAL);
    }

    static TConclusion<TPredicateContainer> BuildPredicateFrom(
        std::shared_ptr<NOlap::TPredicate> object, const std::shared_ptr<arrow::Schema>& pkSchema);

    static TPredicateContainer BuildNullPredicateTo() {
        return TPredicateContainer(NArrow::ECompareType::LESS_OR_EQUAL);
    }

    static TConclusion<TPredicateContainer> BuildPredicateTo(
        std::shared_ptr<NOlap::TPredicate> object, const std::shared_ptr<arrow::Schema>& pkSchema);

    NArrow::TColumnFilter BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const;
};

}   // namespace NKikimr::NOlap
