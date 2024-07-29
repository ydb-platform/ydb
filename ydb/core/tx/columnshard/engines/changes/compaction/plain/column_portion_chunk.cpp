#include "column_portion_chunk.h"
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/tx/columnshard/splitter/simple.h>
#include <ydb/core/tx/columnshard/engines/changes/counters/general.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

std::shared_ptr<arrow::Array> TColumnPortion::AppendBlob(const TString& data, const TColumnRecord& columnChunk, ui32& remained) {
//    if (CurrentPortionRecords + columnChunk.GetMeta().GetNumRows() <= Context.GetPortionRowsCountLimit() &&
//        columnChunk.GetMeta().GetRawBytes() < Context.GetChunkRawBytesLimit() &&
//        data.size() < Context.GetChunkPackedBytesLimit() &&
//        columnChunk.GetMeta().GetRawBytes() > Context.GetStorePackedChunkSizeLimit() && Context.GetSaver().IsHardPacker() &&
//        Context.GetUseWholeChunksOptimization())
//    {
//        NChanges::TGeneralCompactionCounters::OnFullBlobAppend(columnChunk.BlobRange.GetBlobSize());
//        FlushBuffer();
//        Chunks.emplace_back(std::make_shared<TChunkPreparation>(data, columnChunk, Context.GetSchemaInfo()));
//        PackedSize += Chunks.back()->GetPackedSize();
//        CurrentPortionRecords += columnChunk.GetMeta().GetNumRows();
//        return nullptr;
//    } else {
        NChanges::TGeneralCompactionCounters::OnSplittedBlobAppend(columnChunk.BlobRange.GetSize());
        auto batch = NArrow::TStatusValidator::GetValid(Context.GetLoader()->Apply(data));
        AFL_VERIFY(batch->num_columns() == 1);
        auto batchArray = batch->column(0);
        remained = AppendSlice(batchArray, 0, batch->num_rows());
        if (remained) {
            return batchArray;
        } else {
            return nullptr;
        }
//    }
}

ui32 TColumnPortion::AppendSlice(const std::shared_ptr<arrow::Array>& a, const ui32 startIndex, const ui32 length) {
    Y_ABORT_UNLESS(a);
    Y_ABORT_UNLESS(length);
    Y_ABORT_UNLESS(CurrentPortionRecords < Context.GetPortionRowsCountLimit());
    Y_ABORT_UNLESS(startIndex + length <= a->length());
    AFL_VERIFY(Type->id() == a->type_id())("own", Type->ToString())("a", a->type()->ToString());
    ui32 i = startIndex;
    const ui32 packedRecordSize = Context.GetColumnStat() ? Context.GetColumnStat()->GetPackedRecordSize() : 0;
    for (; i < startIndex + length; ++i) {
        ui64 recordSize = 0;
        AFL_VERIFY(NArrow::Append(*Builder, *a, i, &recordSize))("a", a->ToString())("a_type", a->type()->ToString())("builder_type", Builder->type()->ToString());
        CurrentChunkRawSize += recordSize;
        PredictedPackedBytes += packedRecordSize ? packedRecordSize : (recordSize / 2);
        if (++CurrentPortionRecords == Context.GetPortionRowsCountLimit()) {
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
    if (Builder->length()) {
        auto newArrayChunk = NArrow::TStatusValidator::GetValid(Builder->Finish());
        Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(Context.GetSaver().Apply(newArrayChunk, Context.GetResultField()), newArrayChunk, TChunkAddress(Context.GetColumnId(), 0), ColumnInfo));
        Builder = Context.MakeBuilder();
        CurrentChunkRawSize = 0;
        PredictedPackedBytes = 0;
        PackedSize += Chunks.back()->GetPackedSize();
        return true;
    } else {
        return false;
    }
}

}
