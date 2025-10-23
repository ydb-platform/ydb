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
    // NArrow::ECompareType CompareType;
    // mutable std::optional<std::vector<TString>> ColumnNames;
    // NArrow::NMerger::TSortableBatchPosition ReplaceKey;

    TPredicateContainer(std::optional<NOlap::TPredicate> object)
        : Object(object)
    // , CompareType(Object->GetCompareType())
    // , ReplaceKey(replaceKey)
    {
    }

    TPredicateContainer() {
    }

    static std::partial_ordering ComparePredicatesSamePrefix(const NOlap::TPredicate& l, const NOlap::TPredicate& r);

    static NArrow::NMerger::TSortableBatchPosition ExtractKey(const NOlap::TPredicate& predicate, const std::shared_ptr<arrow::Schema>& key) {
        // TODO: Not implemented (validation)
        // AFL_VERIFY(predicate.Batch);
        // const auto& batchFields = predicate.Batch->schema()->fields();
        // const auto& keyFields = key->fields();
        // AFL_VERIFY(batchFields.size() <= keyFields.size());
        // for (size_t i = 0; i < batchFields.size(); ++i) {
        //     Y_DEBUG_ABORT_UNLESS(batchFields[i]->type()->Equals(*keyFields[i]->type()));
        // }
        return predicate.Batch.TrimSortingKeys(key->num_fields());
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

    bool IsEmpty() const {
        return !Object;
    }

    // template <class TArrayColumn>
    // std::optional<typename TArrayColumn::value_type> Get(
    //     const ui32 colIndex, const ui32 rowIndex, const std::optional<typename TArrayColumn::value_type> defaultValue = {}) const {
    //     if (!Object) {
    //         return defaultValue;
    //     } else {
    //         return Object->Get<TArrayColumn>(colIndex, rowIndex, defaultValue);
    //     }
    // }

    TString DebugString() const;

    int MatchScalar(const ui32 columnIdx, const std::shared_ptr<arrow::Scalar>& s) const;

    std::vector<std::string> GetColumnNames() const;
    ui32 NumColumns() const {
        return Object ? Object->NumColumns() : 0;
    }

    bool IsForwardInterval() const;

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

    NArrow::TColumnFilter BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& data) const;

    size_t GetMemorySize() const {
        size_t res = sizeof(TPredicateContainer);
        res += Object ? sizeof(NOlap::TPredicate) : 0;
        // res += ReplaceKey ? ReplaceKey->GetMemorySize() : 0;
        // Column names are cached, may be changed
        return res;
    }
};

}   // namespace NKikimr::NOlap
