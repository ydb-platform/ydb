#include "batch.h"

namespace NKikimr::NArrow::NConstruction {

std::shared_ptr<arrow::RecordBatch> TRecordBatchConstructor::BuildBatch(const ui32 numRows) const {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (auto&& i : Builders) {
        columns.emplace_back(i->BuildArray(numRows));
        fields.emplace_back(std::make_shared<arrow::Field>(i->GetFieldName(), columns.back()->type(), i->GetNullable()));
    }
    auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), numRows, columns);
    Y_ABORT_UNLESS(batch);
    Y_DEBUG_ABORT_UNLESS(batch->ValidateFull().ok());
    return batch;
}

}
