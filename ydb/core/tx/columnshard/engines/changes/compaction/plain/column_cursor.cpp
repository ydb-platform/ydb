#include "column_cursor.h"
#include <ydb/core/formats/arrow/common/validation.h>

namespace NKikimr::NOlap::NCompaction {

bool TPortionColumnCursor::Fetch(TMergedColumn& column) {
    Y_ABORT_UNLESS(RecordIndexStart);
    if (CurrentChunk && CurrentChunk->GetAddress().Contains(*RecordIndexStart)) {
        
    } else {
        CurrentChunk = BlobChunks->GetChunk(CurrentChunk, *RecordIndexStart);
    }

    ui32 currentStart = *RecordIndexStart;
    while (CurrentChunk->GetAddress().GetGlobalFinishPosition() <= RecordIndexFinish) {
        column.AppendSlice(CurrentChunk->GetArray(), CurrentChunk->GetAddress().GetLocalIndex(currentStart),
            CurrentChunk->GetAddress().GetGlobalFinishPosition() - currentStart);
        currentStart = CurrentChunk->GetAddress().GetGlobalFinishPosition();
        if (currentStart < BlobChunks->GetRecordsCount()) {
            CurrentChunk = BlobChunks->GetChunk(CurrentChunk, currentStart);
        } else {
            CurrentChunk.reset();
            break;
        }
    }

    if (currentStart < RecordIndexFinish) {
        AFL_VERIFY(CurrentChunk);
        Y_ABORT_UNLESS(RecordIndexFinish < CurrentChunk->GetAddress().GetGlobalFinishPosition());
        column.AppendSlice(
            CurrentChunk->GetArray(), CurrentChunk->GetAddress().GetLocalIndex(currentStart), RecordIndexFinish - currentStart);
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
