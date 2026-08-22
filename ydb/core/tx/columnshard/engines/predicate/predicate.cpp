#include "predicate.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/program/functions.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NOlap {

TPredicate::TPredicate(EOperation op, NArrow::NMerger::TSortableBatchPosition batch) noexcept
    : Operation(op)
    , Batch(std::move(batch))
{
    Y_ABORT_UNLESS(IsFrom() || IsTo());
}

std::vector<TString> TPredicate::ColumnNames() const {
    return std::vector<TString>(Batch.GetSorting()->GetFieldNames().begin(), Batch.GetSorting()->GetFieldNames().end());
}

NArrow::NMerger::TSortableBatchPosition TPredicate::CutNulls(
    const std::shared_ptr<arrow::RecordBatch>& batch, const ui64 rowIdx, const std::shared_ptr<arrow::Schema>& pkSchema) {
    AFL_VERIFY((i64)rowIdx < batch->num_rows())("idx", rowIdx)("count", batch->num_rows());
    std::vector<std::string> colsNotNull;
    for (const auto& field : pkSchema->field_names()) {
        if (batch->GetColumnByName(field)->IsNull(rowIdx)) {
            break;
        }
        colsNotNull.emplace_back(field);
    }
    AFL_VERIFY(colsNotNull.size());
    return NArrow::NMerger::TSortableBatchPosition(batch, rowIdx, colsNotNull, std::vector<std::string>(), false);
}

bool TPredicate::IsEqualSchema(const std::shared_ptr<arrow::Schema>& schema) const {
    AFL_VERIFY(schema);
    return Batch.IsSameSortingSchema(*schema);
}

bool TPredicate::IsEqualPointTo(const TPredicate& item) const {
    return Batch == item.Batch;
}

IOutputStream& operator<<(IOutputStream& out, const TPredicate& pred) {
    out << NArrow::NSSA::TSimpleFunction::GetFunctionName(pred.Operation) << " ";
    out << pred.Batch.DebugJson().GetStringRobust();
    return out;
}

}   // namespace NKikimr::NOlap
