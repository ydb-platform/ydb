#pragma once
#include "array.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow::NConstruction {
class TRecordBatchConstructor {
private:
    const std::vector<IArrayBuilder::TPtr> Builders;
public:
    TRecordBatchConstructor(const std::vector<IArrayBuilder::TPtr> builders)
        : Builders(builders) {
        Y_ABORT_UNLESS(Builders.size());
    }

    std::shared_ptr<arrow::RecordBatch> BuildBatch(const ui32 numRows) const;
};
}
