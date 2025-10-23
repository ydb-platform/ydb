#pragma once

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/arrow_kernels/operations.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

struct TPredicate {
private:
    using EOperation = NKernels::EOperation;
    EOperation Operation{ EOperation::Unspecified };

public:
    static NArrow::NMerger::TSortableBatchPosition CutNulls(
        const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 rowIdx, const std::shared_ptr<arrow::Schema>& pkSchema);

    NArrow::NMerger::TSortableBatchPosition Batch;
    bool IsEqualSchema(const std::shared_ptr<arrow::Schema>& schema) const;
    bool IsEqualPointTo(const TPredicate& item) const;

    NArrow::ECompareType GetCompareType() const {
        if (Operation == EOperation::GreaterEqual) {
            return NArrow::ECompareType::GREATER_OR_EQUAL;
        } else if (Operation == EOperation::Greater) {
            return NArrow::ECompareType::GREATER;
        } else if (Operation == EOperation::LessEqual) {
            return NArrow::ECompareType::LESS_OR_EQUAL;
        } else if (Operation == EOperation::Less) {
            return NArrow::ECompareType::LESS;
        } else {
            Y_ABORT_UNLESS(false);
        }
    }

    // template <class TArrayColumn>
    // std::optional<typename TArrayColumn::value_type> Get(
    //     const ui32 colIndex, const ui32 rowIndex, const std::optional<typename TArrayColumn::value_type> defaultValue = {}) const {
    //     if (Batch.GetSorting()->GetColumns().size() <= colIndex) {
    //         return defaultValue;
    //     }
    //     const auto& column = Batch.GetSorting()->GetColumns[colIndex];
    //     if (rowIndex < Batch->num_rows()) {
    //         return static_cast<TArrayColumn&>(*column).Value(rowIndex);
    //     } else {
    //         return defaultValue;
    //     }
    // }

    bool Empty() const noexcept {
        return false;
    }
    bool Good() const {
        return true;
    }
    bool IsFrom() const noexcept {
        return Operation == EOperation::Greater || Operation == EOperation::GreaterEqual;
    }
    bool IsTo() const noexcept {
        return Operation == EOperation::Less || Operation == EOperation::LessEqual;
    }
    bool IsInclusive() const {
        return Operation == EOperation::GreaterEqual || Operation == EOperation::LessEqual;
    }

    std::vector<TString> ColumnNames() const;
    ui64 NumColumns() const {
        return Batch.GetSortFields().size();
    }

    std::string ToString() const {
        return Empty() ? "()" : Batch.DebugJson().GetStringRobust();
    }

    // static std::pair<NKernels::EOperation, NKernels::EOperation> DeserializePredicatesRange(const TSerializedTableRange& range,
    //     const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, const std::shared_ptr<arrow::Schema>& schema,
    //     NArrow::TArrowBatchBuilder& batchContainer);

    // constexpr TPredicate() noexcept = default;

    TPredicate(EOperation op, NArrow::NMerger::TSortableBatchPosition batch) noexcept;

    // TPredicate(EOperation op, const TString& serializedBatch, const std::shared_ptr<arrow::Schema>& schema);

    friend IOutputStream& operator<<(IOutputStream& out, const TPredicate& pred);

    // TPredicate BuildSame(const NArrow::NMerger::TSortableBatchPosition& batch) const {
    //     AFL_VERIFY(batch == Batch)("incoming", batch.DebugJson())("self", Batch.DebugJson());
    //     return TPredicate(Operation, batch);
    // }
};

}   // namespace NKikimr::NOlap
