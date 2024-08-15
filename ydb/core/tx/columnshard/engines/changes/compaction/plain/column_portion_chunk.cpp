#include "column_portion_chunk.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/tx/columnshard/engines/changes/counters/general.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

ui32 TColumnPortion::AppendSlice(const std::shared_ptr<arrow::Array>& a, const ui32 startIndex, const ui32 length) {
    Y_ABORT_UNLESS(a);
    Y_ABORT_UNLESS(length);
    Y_ABORT_UNLESS(CurrentPortionRecords < ChunkContext.GetPortionRowsCountLimit());
    Y_ABORT_UNLESS(startIndex + length <= a->length());
    AFL_VERIFY(Type->id() == a->type_id())("own", Type->ToString())("a", a->type()->ToString());
    ui32 i = startIndex;
    const ui32 packedRecordSize = Context.GetColumnStat() ? Context.GetColumnStat()->GetPackedRecordSize() : 0;
    for (; i < startIndex + length; ++i) {
        ui64 recordSize = 0;
        AFL_VERIFY(NArrow::Append(*Builder, *a, i, &recordSize))("a", a->ToString())("a_type", a->type()->ToString())(
            "builder_type", Builder->type()->ToString());
        CurrentChunkRawSize += recordSize;
        PredictedPackedBytes += packedRecordSize ? packedRecordSize : (recordSize / 2);
        if (++CurrentPortionRecords == ChunkContext.GetPortionRowsCountLimit()) {
            FlushBuffer();
            ++i;
            break;
        }
        if (CurrentChunkRawSize >= Context.GetChunkRawBytesLimit() || PredictedPackedBytes >= Context.GetExpectedBlobPackedBytes()) {
            FlushBuffer();
        }
    }
    return startIndex + length - i;
}

bool TColumnPortion::FlushBuffer() {
    if (!Builder->length()) {
        return false;
    }
    auto newArrayChunk = NArrow::TStatusValidator::GetValid(Builder->Finish());
    Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(Context.GetSaver().Apply(newArrayChunk, Context.GetResultField()),
        std::make_shared<NArrow::NAccessor::TTrivialArray>(newArrayChunk), TChunkAddress(Context.GetColumnId(), 0), ColumnInfo));
    Builder = Context.MakeBuilder();
    CurrentChunkRawSize = 0;
    PredictedPackedBytes = 0;
    PackedSize += Chunks.back()->GetPackedSize();
    return true;
}

}   // namespace NKikimr::NOlap::NCompaction
