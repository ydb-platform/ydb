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

    TPredicate(EOperation op, NArrow::NMerger::TSortableBatchPosition batch) noexcept;

    friend IOutputStream& operator<<(IOutputStream& out, const TPredicate& pred);
};

}   // namespace NKikimr::NOlap
