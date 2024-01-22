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
    std::vector<std::shared_ptr<IPortionDataChunk>> BlobChunks;
    std::vector<const TColumnRecord*> ColumnChunks;
    std::optional<ui32> RecordIndexStart;
    YDB_READONLY(ui32, RecordIndexFinish, 0);
    ui32 ChunkRecordIndexStartPosition = 0;
    ui32 ChunkIdx = 0;
    std::shared_ptr<IPortionDataChunk> CurrentBlobChunk;
    const TColumnRecord* CurrentColumnChunk = nullptr;
    ui32 CurrentChunkRecordsCount = 0;
    std::shared_ptr<arrow::Array> CurrentArray;
    std::shared_ptr<TColumnLoader> ColumnLoader;
    const ui64 PortionId;

    const std::shared_ptr<arrow::Array>& GetCurrentArray();

    bool NextChunk();

public:
    ~TPortionColumnCursor() {
        AFL_VERIFY(!RecordIndexStart || ChunkIdx == ColumnChunks.size())("chunk", ChunkIdx)
            ("size", ColumnChunks.size())("start", RecordIndexStart)("finish", RecordIndexFinish)
            ("max", CurrentBlobChunk->GetRecordsCount())("current_start_position", ChunkRecordIndexStartPosition);
    }

    bool Next(const ui32 portionRecordIdx, TMergedColumn& column);

    bool Fetch(TMergedColumn& column);

    TPortionColumnCursor(const std::vector<std::shared_ptr<IPortionDataChunk>>& columnChunks, const std::vector<const TColumnRecord*>& records, const std::shared_ptr<TColumnLoader>& loader, const ui64 portionId)
        : BlobChunks(columnChunks)
        , ColumnChunks(records)
        , ColumnLoader(loader)
        , PortionId(portionId) {
        AFL_VERIFY(ColumnLoader);
        Y_UNUSED(PortionId);
        Y_ABORT_UNLESS(BlobChunks.size());
        Y_ABORT_UNLESS(ColumnChunks.size() == BlobChunks.size());
        CurrentBlobChunk = BlobChunks.front();
        CurrentColumnChunk = ColumnChunks.front();
        CurrentChunkRecordsCount = CurrentBlobChunk->GetRecordsCountVerified();
    }
};

}
