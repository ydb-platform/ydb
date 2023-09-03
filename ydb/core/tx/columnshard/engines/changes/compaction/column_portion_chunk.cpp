#include "column_portion_chunk.h"
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/tx/columnshard/splitter/simple.h>

namespace NKikimr::NOlap::NCompaction {

std::vector<NKikimr::NOlap::IPortionColumnChunk::TPtr> TChunkPreparation::DoInternalSplit(const TColumnSaver& saver, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const std::vector<ui64>& splitSizes) const {
    auto loader = SchemaInfo->GetColumnLoader(SchemaInfo->GetFieldByColumnIdVerified(Record.ColumnId)->name());
    auto rb = NArrow::TStatusValidator::GetValid(loader->Apply(Data));

    auto chunks = TSimpleSplitter(saver, counters).SplitBySizes(rb, Data, splitSizes);
    std::vector<IPortionColumnChunk::TPtr> newChunks;
    for (auto&& i : chunks) {
        Y_VERIFY(i.GetSlicedBatch()->num_columns() == 1);
        TColumnRecord newRecord(TChunkAddress(ColumnId, ChunkIdx), i.GetSlicedBatch()->column(0), SchemaInfo->GetIndexInfo());
        newChunks.emplace_back(std::make_shared<TChunkPreparation>(
            saver.Apply(i.GetSlicedBatch()), newRecord, SchemaInfo));
    }
    return newChunks;
}

std::shared_ptr<arrow::Array> TColumnPortion::AppendBlob(const TString& data, const TColumnRecord& columnChunk) {
    if (CurrentPortionRecords + columnChunk.GetMeta().GetNumRowsVerified() <= Context.GetPortionRowsCountLimit() &&
        columnChunk.GetMeta().GetRawBytesVerified() < Context.GetChunkRawBytesLimit() &&
        data.size() < Context.GetChunkPackedBytesLimit() &&
        columnChunk.GetMeta().GetRawBytesVerified() > Context.GetStorePackedChunkSizeLimit() && Context.GetSaver().IsHardPacker()) {
        FlushBuffer();
        Chunks.emplace_back(std::make_shared<TChunkPreparation>(data, columnChunk, Context.GetSchemaInfo()));
        PackedSize += Chunks.back()->GetPackedSize();
        CurrentPortionRecords += columnChunk.GetMeta().GetNumRowsVerified();
        return nullptr;
    } else {
        auto batch = NArrow::TStatusValidator::GetValid(Context.GetLoader()->Apply(data));
        return AppendSlice(batch);
    }
}

std::shared_ptr<arrow::Array> TColumnPortion::AppendSlice(const std::shared_ptr<arrow::Array>& a) {
    Y_VERIFY(a);
    ui32 i = 0;
    Y_VERIFY(CurrentPortionRecords < Context.GetPortionRowsCountLimit());
    for (; i < a->length(); ++i) {
        ui64 recordSize = 0;
        NArrow::Append(*Builder, *a, i, &recordSize);
        CurrentChunkRawSize += recordSize;
        PredictedPackedBytes += Context.GetColumnStat().GetPackedRecordSize();
        if (++CurrentPortionRecords == Context.GetPortionRowsCountLimit()) {
            FlushBuffer();
            ++i;
            break;
        }
        if (CurrentChunkRawSize >= Context.GetChunkRawBytesLimit() || PredictedPackedBytes >= Context.GetExpectedBlobPackedBytes()) {
            FlushBuffer();
        }
    }
    if (i == a->length()) {
        return nullptr;
    } else {
        return a->Slice(i, a->length() - i);
    }
}

void TColumnPortion::FlushBuffer() {
    if (Builder->length()) {
        auto newArrayChunk = NArrow::TStatusValidator::GetValid(Builder->Finish());
        Chunks.emplace_back(std::make_shared<TChunkPreparation>(Context.GetSaver().Apply(newArrayChunk, Context.GetField()), newArrayChunk, Context.GetColumnId(), Context.GetSchemaInfo()));
        Builder = Context.MakeBuilder();
        CurrentChunkRawSize = 0;
        PredictedPackedBytes = 0;
        PackedSize += Chunks.back()->GetPackedSize();
    }
}

}
