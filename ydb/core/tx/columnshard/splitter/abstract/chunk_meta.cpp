#include "chunk_meta.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/size_calcer.h>

namespace NKikimr::NOlap {

TSimpleChunkMeta::TSimpleChunkMeta(
    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column) {
    Y_ABORT_UNLESS(column);
    Y_ABORT_UNLESS(column->GetRecordsCount());
    RecordsCount = column->GetRecordsCount();
    RawBytes = column->GetRawSizeVerified();
}

}
