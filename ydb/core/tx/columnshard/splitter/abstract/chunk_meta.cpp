#include "chunk_meta.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap {

TSimpleChunkMeta::TSimpleChunkMeta(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column, const bool needMax, const bool isSortedColumn) {
    Y_ABORT_UNLESS(column);
    Y_ABORT_UNLESS(column->GetRecordsCount());
    NumRows = column->GetRecordsCount();
    RawBytes = column->GetRawSizeVerified();

    if (needMax) {
        if (!isSortedColumn) {
            Max = column->GetMaxScalar();
        } else {
            Max = column->GetScalar(column->GetRecordsCount() - 1);
        }
//        AFL_VERIFY(Max);
    }
}

}
