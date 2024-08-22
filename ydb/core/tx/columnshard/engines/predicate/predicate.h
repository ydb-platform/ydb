#pragma once

#include <ydb/core/formats/arrow/program.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

struct TPredicate {
private:
    using EOperation = NArrow::EOperation;
    EOperation Operation{ EOperation::Unspecified };

public:
    static std::shared_ptr<arrow::RecordBatch> CutNulls(const std::shared_ptr<arrow::RecordBatch>& batch);

    std::shared_ptr<arrow::RecordBatch> Batch;

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

    template <class TArrayColumn>
    std::optional<typename TArrayColumn::value_type> Get(
        const ui32 colIndex, const ui32 rowIndex, const std::optional<typename TArrayColumn::value_type> defaultValue = {}) const {
        auto column = Batch->column(colIndex);
        if (!column) {
            return defaultValue;
        }
        if (rowIndex < Batch->num_rows()) {
            return static_cast<TArrayColumn&>(*column).Value(rowIndex);
        } else {
            return defaultValue;
        }
    }

    bool Empty() const noexcept {
        return Batch.get() == nullptr;
    }
    bool Good() const {
        return !Empty() && Batch->num_columns() && Batch->num_rows() == 1;
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

    std::string ToString() const {
        return Empty() ? "()" : Batch->schema()->ToString();
    }

    static std::pair<TPredicate, TPredicate> DeserializePredicatesRange(
        const TSerializedTableRange& range, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns);

    constexpr TPredicate() noexcept = default;

    TPredicate(EOperation op, std::shared_ptr<arrow::RecordBatch> batch) noexcept;

    TPredicate(EOperation op, const TString& serializedBatch, const std::shared_ptr<arrow::Schema>& schema);

    friend IOutputStream& operator<<(IOutputStream& out, const TPredicate& pred);
};

}   // namespace NKikimr::NOlap
