#include "merged_column.h"

namespace NKikimr::NOlap::NCompaction {

void TMergedColumn::AppendSlice(const std::shared_ptr<arrow::Array>& data, const ui32 startIndex, const ui32 length) {
    RecordsCount += length;
    Y_ABORT_UNLESS(data);
    AFL_VERIFY(!Portion.AppendSlice(data, startIndex, length));
}

TColumnPortionResult TMergedColumn::BuildResult() {
    Portion.FlushBuffer();
    return Portion;
}

}
