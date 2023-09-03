#include "column_cursor.h"
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap::NCompaction {

bool TPortionColumnCursor::Fetch(TMergedColumn& column) {
    Y_VERIFY(ChunkIdx < ColumnChunks.size());
    Y_VERIFY(RecordIndexStart);
    ui32 currentStartPortionIdx = *RecordIndexStart;
    ui32 currentFinishPortionIdx = RecordIndexFinish;

    while (currentStartPortionIdx - ChunkRecordIndexStartPosition >= CurrentColumnChunk->GetMeta().GetNumRowsVerified()) {
        if (!NextChunk()) {
            return false;
        }
    }

    ui32 currentStart = currentStartPortionIdx - ChunkRecordIndexStartPosition;

    while (currentFinishPortionIdx - ChunkRecordIndexStartPosition >= CurrentColumnChunk->GetMeta().GetNumRowsVerified()) {
        const ui32 currentFinish = CurrentColumnChunk->GetMeta().GetNumRowsVerified();
        if (currentStart == 0) {
            column.AppendBlob(CurrentBlobChunk->GetData(), *CurrentColumnChunk);
        } else {
            column.AppendSlice(GetCurrentBatch().Slice(currentStart, currentFinish - currentStart));
        }
        currentStart = 0;
        if (!NextChunk()) {
            return false;
        }
    }

    const ui32 currentFinish = currentFinishPortionIdx - ChunkRecordIndexStartPosition;
    if (currentStart < currentFinish) {
        Y_VERIFY(currentFinish < CurrentColumnChunk->GetMeta().GetNumRowsVerified());
        column.AppendSlice(GetCurrentBatch().Slice(currentStart, currentFinish - currentStart));
    }

    RecordIndexStart.reset();
    RecordIndexFinish = 0;
    return true;
}

bool TPortionColumnCursor::Next(const ui32 portionRecordIdx, TMergedColumn& column) {
    Y_VERIFY(ChunkRecordIndexStartPosition <= portionRecordIdx);
    if (!RecordIndexStart) {
        RecordIndexStart = portionRecordIdx;
        RecordIndexFinish = portionRecordIdx + 1;
    } else if (RecordIndexFinish == portionRecordIdx) {
        RecordIndexFinish = portionRecordIdx + 1;
    } else {
        Fetch(column);
        RecordIndexStart = portionRecordIdx;
        RecordIndexFinish = portionRecordIdx + 1;
    }
    return true;
}

bool TPortionColumnCursor::NextChunk() {
    CurrentBatch = nullptr;
    if (++ChunkIdx == ColumnChunks.size()) {
        return false;
    } else {
        ChunkRecordIndexStartPosition += CurrentColumnChunk->GetMeta().GetNumRowsVerified();
        CurrentBlobChunk = BlobChunks[ChunkIdx];
        CurrentColumnChunk = ColumnChunks[ChunkIdx];
        return true;
    }
}

const arrow::RecordBatch& TPortionColumnCursor::GetCurrentBatch() {
    Y_VERIFY(ChunkIdx < ColumnChunks.size());
    Y_VERIFY(CurrentBlobChunk);

    if (!CurrentBatch) {
        CurrentBatch = NArrow::TStatusValidator::GetValid(ColumnLoader->Apply(CurrentBlobChunk->GetData()));
    }
    return *CurrentBatch;
}

}
