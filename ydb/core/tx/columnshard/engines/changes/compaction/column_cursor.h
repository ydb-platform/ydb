#pragma once
#include "merged_column.h"
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/portions/with_blobs.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NCompaction {

class TPortionColumnCursor {
private:
    std::vector<IPortionColumnChunk::TPtr> BlobChunks;
    std::vector<const TColumnRecord*> ColumnChunks;
    std::optional<ui32> RecordIndexStart;
    YDB_READONLY(ui32, RecordIndexFinish, 0);
    ui32 ChunkRecordIndexStartPosition = 0;
    ui32 ChunkIdx = 0;
    IPortionColumnChunk::TPtr CurrentBlobChunk;
    const TColumnRecord* CurrentColumnChunk = nullptr;
    std::shared_ptr<arrow::RecordBatch> CurrentBatch;
    std::shared_ptr<TColumnLoader> ColumnLoader;

    const arrow::RecordBatch& GetCurrentBatch();

    bool NextChunk();

public:
    ~TPortionColumnCursor() {
        AFL_VERIFY(!RecordIndexStart || ChunkIdx == ColumnChunks.size())("chunk", ChunkIdx)
            ("size", ColumnChunks.size())("start", RecordIndexStart.value_or(9999999))("finish", RecordIndexFinish)
            ("max", CurrentColumnChunk ? CurrentColumnChunk->GetMeta().GetNumRowsVerified() : -1)("current_start_position", ChunkRecordIndexStartPosition);
    }

    bool Next(const ui32 portionRecordIdx, TMergedColumn& column);

    bool Fetch(TMergedColumn& column);

    TPortionColumnCursor(const TPortionInfoWithBlobs& portionWithBlobs, const ui32 columnId, const std::shared_ptr<TColumnLoader> loader)
        : ColumnLoader(loader) {
        BlobChunks = portionWithBlobs.GetColumnChunks(columnId);
        ColumnChunks = portionWithBlobs.GetPortionInfo().GetColumnChunksPointers(columnId);
        Y_VERIFY(BlobChunks.size());
        Y_VERIFY(ColumnChunks.size() == BlobChunks.size());
        CurrentBlobChunk = BlobChunks.front();
        CurrentColumnChunk = ColumnChunks.front();
    }
};

}
