#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/validator_simple.h>
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <functional>

namespace NKikimr::NArrow {

class TSchemaSubset;
class TSchemaLite;
class TSchemaLiteView;

class TOrderedColumnIndexesImpl {
private:
    YDB_READONLY_DEF(std::vector<ui32>, ColumnIndexes);

public:
    TOrderedColumnIndexesImpl() = default;

    explicit TOrderedColumnIndexesImpl(const ui32 columnsCount);
    explicit TOrderedColumnIndexesImpl(const std::vector<ui32>& columnIndexes);
    explicit TOrderedColumnIndexesImpl(std::vector<ui32>&& columnIndexes);

    template <class TContainerWithIndexes>
    static std::vector<ui32> MergeColumnIdxs(const std::vector<TContainerWithIndexes>& sources) {
        class TIterator {
        private:
            std::vector<ui32>::const_iterator ItCurrent;
            std::vector<ui32>::const_iterator ItFinish;

        public:
            TIterator(const std::vector<ui32>& indexes)
                : ItCurrent(indexes.begin())
                , ItFinish(indexes.end()) {
            }

            bool operator<(const TIterator& item) const {
                return *ItCurrent > *item.ItCurrent;
            }

            bool IsValid() const {
                return ItCurrent != ItFinish;
            }

            ui32 operator*() const {
                return *ItCurrent;
            }

            bool Next() {
                return ++ItCurrent != ItFinish;
            }
        };

        std::vector<TIterator> heapToMerge;
        for (auto&& i : sources) {
            heapToMerge.emplace_back(TIterator(i.GetColumnIndexes()));
            if (!heapToMerge.back().IsValid()) {
                heapToMerge.pop_back();
            }
        }
        std::make_heap(heapToMerge.begin(), heapToMerge.end());
        std::vector<ui32> result;
        while (heapToMerge.size()) {
            std::pop_heap(heapToMerge.begin(), heapToMerge.end());
            if (result.empty() || result.back() != *heapToMerge.back()) {
                result.emplace_back(*heapToMerge.back());
            }
            if (!heapToMerge.back().Next()) {
                heapToMerge.pop_back();
            } else {
                std::push_heap(heapToMerge.begin(), heapToMerge.end());
            }
        }
        return result;
    }
};

template <class TDataContainer>
class TContainerWithIndexes: public TOrderedColumnIndexesImpl {
private:
    using TBase = TOrderedColumnIndexesImpl;
    YDB_ACCESSOR_DEF(std::shared_ptr<TDataContainer>, Container);

public:
    TContainerWithIndexes() = default;

    TContainerWithIndexes(const std::vector<ui32>& columnIndexes, const std::shared_ptr<TDataContainer>& container)
        : TBase(columnIndexes)
        , Container(container) {
        if (Container) {
            Y_ABORT_UNLESS((ui32)Container->num_columns() == columnIndexes.size());
        } else {
            Y_ABORT_UNLESS(!columnIndexes.size());
        }
    }

    explicit TContainerWithIndexes(const std::shared_ptr<TDataContainer>& container)
        : TBase(TSimpleValidator::CheckNotNull(container)->num_columns())
        , Container(container) {
    }

    TContainerWithIndexes<TDataContainer> BuildWithAnotherContainer(const std::shared_ptr<TDataContainer>& container) const {
        return TContainerWithIndexes<TDataContainer>(GetColumnIndexes(), container);
    }

    bool operator!() const {
        return !HasContainer();
    }

    bool HasContainer() const {
        return !!Container;
    }

    const TDataContainer* operator->() const {
        return Container.get();
    }

};

class TColumnOperator {
public:
    enum class EAbsentFieldPolicy {
        Error,
        Verify,
        Skip
    };

    enum class ECheckFieldTypesPolicy {
        Ignore,
        Error,
        Verify
    };

private:
    EAbsentFieldPolicy AbsentColumnPolicy = EAbsentFieldPolicy::Verify;
    ECheckFieldTypesPolicy DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Error;

public:
    TColumnOperator& VerifyOnDifferentFieldTypes() {
        DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Verify;
        return *this;
    };

    TColumnOperator& ErrorOnDifferentFieldTypes() {
        DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Error;
        return *this;
    };

    TColumnOperator& IgnoreOnDifferentFieldTypes() {
        DifferentColumnTypesPolicy = ECheckFieldTypesPolicy::Ignore;
        return *this;
    };

    TColumnOperator& ErrorIfAbsent() {
        AbsentColumnPolicy = EAbsentFieldPolicy::Error;
        return *this;
    }

    TColumnOperator& VerifyIfAbsent() {
        AbsentColumnPolicy = EAbsentFieldPolicy::Verify;
        return *this;
    }

    TColumnOperator& SkipIfAbsent() {
        AbsentColumnPolicy = EAbsentFieldPolicy::Skip;
        return *this;
    }

    TConclusion<TContainerWithIndexes<arrow::RecordBatch>> AdaptIncomingToDestinationExt(const std::shared_ptr<arrow::RecordBatch>& incoming,
        const TSchemaLiteView& dstSchema, const std::function<TConclusionStatus(const ui32, const i32)>& checker,
        const std::function<i32(const std::string&)>& nameResolver) const;

    std::shared_ptr<arrow::RecordBatch> Extract(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    std::shared_ptr<arrow::Table> Extract(
        const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::shared_ptr<arrow::Field>>& columns);
    std::shared_ptr<arrow::RecordBatch> Extract(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::shared_ptr<arrow::Field>>& columns);
    std::shared_ptr<arrow::RecordBatch> Extract(const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    std::shared_ptr<arrow::Table> Extract(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);

    TConclusion<TSchemaSubset> BuildSequentialSubset(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const NArrow::TSchemaLiteView& dstSchema);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::Table>> Adapt(
        const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<arrow::Schema>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Adapt(const std::shared_ptr<arrow::RecordBatch>& incoming,
        const std::shared_ptr<NArrow::TSchemaLite>& dstSchema, TSchemaSubset* subset = nullptr);
    TConclusion<std::shared_ptr<arrow::Table>> Adapt(
        const std::shared_ptr<arrow::Table>& incoming, const std::shared_ptr<NArrow::TSchemaLite>& dstSchema, TSchemaSubset* subset = nullptr);

    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(
        const std::shared_ptr<arrow::Table>& incoming, const std::vector<std::string>& columnNames);
    TConclusion<std::shared_ptr<arrow::RecordBatch>> Reorder(
        const std::shared_ptr<arrow::RecordBatch>& incoming, const std::vector<TString>& columnNames);
    TConclusion<std::shared_ptr<arrow::Table>> Reorder(const std::shared_ptr<arrow::Table>& incoming, const std::vector<TString>& columnNames);
};

}   // namespace NKikimr::NArrow
