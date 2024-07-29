#pragma once
#include "merged_column.h"
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/columnshard/engines/portions/column_record.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NCompaction {

class TPortionColumnCursor {
private:
    std::optional<NArrow::NAccessor::IChunkedArray::TFullDataAddress> CurrentChunk;
    std::shared_ptr<NArrow::NAccessor::IChunkedArray> BlobChunks;
    std::optional<ui32> RecordIndexStart;
    YDB_READONLY(ui32, RecordIndexFinish, 0);
public:
    ~TPortionColumnCursor() {
        AFL_VERIFY(!RecordIndexStart)("start", RecordIndexStart)("finish", RecordIndexFinish);
    }

    bool Next(const ui32 portionRecordIdx, TMergedColumn& column);

    bool Fetch(TMergedColumn& column);

    TPortionColumnCursor(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& columnChunks)
        : BlobChunks(columnChunks) {
    }
};

}
