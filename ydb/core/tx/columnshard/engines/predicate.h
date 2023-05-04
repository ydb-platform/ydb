#pragma once

#include "defs.h"

#include <ydb/core/formats/arrow/program.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

struct TPredicate {
    using EOperation = NArrow::EOperation;

    std::shared_ptr<arrow::RecordBatch> Batch;
    EOperation Operation{EOperation::Unspecified};
    bool Inclusive{false};

    bool Empty() const noexcept { return Batch.get() == nullptr; }
    bool Good() const { return !Empty() && Batch->num_columns() && Batch->num_rows() == 1; }
    bool IsFrom() const noexcept { return Operation == EOperation::Greater || Operation == EOperation::GreaterEqual; }
    bool IsTo() const noexcept { return Operation == EOperation::Less || Operation == EOperation::LessEqual; }

    TVector<TString> ColumnNames() const;

    std::string ToString() const {
        return Empty() ? "()" : Batch->schema()->ToString();
    }

    constexpr TPredicate() noexcept = default;

    TPredicate(EOperation op, std::shared_ptr<arrow::RecordBatch> batch, bool inclusive = false) noexcept;

    TPredicate(EOperation op, const TString& serializedBatch, const std::shared_ptr<arrow::Schema>& schema, bool inclusive);

    friend IOutputStream& operator << (IOutputStream& out, const TPredicate& pred);
};

} // namespace NKikimr::NOlap
