#include "column_cursor.h"
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap::NCompaction {

bool TPortionColumnCursor::Fetch(TMergedColumn& column) {
    Y_ABORT_UNLESS(RecordIndexStart);
    if (CurrentChunk && CurrentChunk->GetStartPosition() <= *RecordIndexStart && *RecordIndexStart < CurrentChunk->GetFinishPosition()) {
        
    } else {
        CurrentChunk = BlobChunks->GetChunk(CurrentChunk, *RecordIndexStart);
    }

    ui32 currentStart = *RecordIndexStart;
    while (RecordIndexFinish >= CurrentChunk->GetFinishPosition()) {
        column.AppendSlice(
            CurrentChunk->GetArray(), currentStart - CurrentChunk->GetStartPosition(), CurrentChunk->GetFinishPosition() - currentStart);
        currentStart = CurrentChunk->GetFinishPosition();
        if (currentStart < BlobChunks->GetRecordsCount()) {
            CurrentChunk = BlobChunks->GetChunk(CurrentChunk, currentStart);
        } else {
            CurrentChunk.reset();
            break;
        }
    }

    if (currentStart < RecordIndexFinish) {
        AFL_VERIFY(CurrentChunk);
        Y_ABORT_UNLESS(RecordIndexFinish < CurrentChunk->GetFinishPosition());
        column.AppendSlice(CurrentChunk->GetArray(), currentStart - CurrentChunk->GetStartPosition(), RecordIndexFinish - currentStart);
    }

    RecordIndexStart.reset();
    RecordIndexFinish = 0;
    return true;
}

bool TPortionColumnCursor::Next(const ui32 portionRecordIdx, TMergedColumn& column) {
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

}
