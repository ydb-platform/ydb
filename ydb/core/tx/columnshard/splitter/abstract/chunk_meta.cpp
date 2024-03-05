#include "chunk_meta.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap {

TSimpleChunkMeta::TSimpleChunkMeta(const std::shared_ptr<arrow::Array>& column, const bool needMax, const bool isSortedColumn) {
    Y_ABORT_UNLESS(column);
    Y_ABORT_UNLESS(column->length());
    NumRows = column->length();
    RawBytes = NArrow::GetArrayDataSize(column);

    if (needMax) {
        std::pair<i32, i32> minMaxPos = {0, (column->length() - 1)};
        if (!isSortedColumn) {
            minMaxPos = NArrow::FindMinMaxPosition(column);
            Y_ABORT_UNLESS(minMaxPos.first >= 0);
            Y_ABORT_UNLESS(minMaxPos.second >= 0);
        }

        Max = NArrow::GetScalar(column, minMaxPos.second);

        Y_ABORT_UNLESS(Max);
    }
}

}
