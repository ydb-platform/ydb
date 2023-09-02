#include "chunk_meta.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap {

TSimpleChunkMeta::TSimpleChunkMeta(const std::shared_ptr<arrow::Array>& column, const bool needMinMax, const bool isSortedColumn) {
    Y_VERIFY(column);
    Y_VERIFY(column->length());
    NumRows = column->length();
    RawBytes = NArrow::GetArrayDataSize(column);

    if (needMinMax) {
        std::pair<i32, i32> minMaxPos = {0, (column->length() - 1)};
        if (!isSortedColumn) {
            minMaxPos = NArrow::FindMinMaxPosition(column);
            Y_VERIFY(minMaxPos.first >= 0);
            Y_VERIFY(minMaxPos.second >= 0);
        }

        Min = NArrow::GetScalar(column, minMaxPos.first);
        Max = NArrow::GetScalar(column, minMaxPos.second);

        Y_VERIFY(Min);
        Y_VERIFY(Max);
    }
}

}
