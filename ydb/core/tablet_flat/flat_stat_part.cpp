#include "flat_stat_part.h"

namespace NKikimr {
namespace NTable {

ui64 TPartSmallSizeHelper::CalcSize(TRowId begin, TRowId end)
{
    if (!Small || end <= begin) {
        return 0;
    }
    if (EndRow <= begin) {
        // Resynchronize so we start from an already known end
        BeginRow = EndRow;
        BeginPage = EndPage;
        Size = 0;
    }
    if (BeginRow != begin) {
        if (begin > BeginRow) {
            // Move starting position forward
            BeginPage = Small->Lower(begin, BeginPage, Max<ui32>());
        } else {
            // Move starting position backwards (shouldn't happen normally)
            BeginPage = Small->Lower(begin, 0, BeginPage);
        }
        BeginRow = begin;
        EndRow = begin;
        EndPage = BeginPage;
        Size = 0;
    } else if (end < EndRow) {
        // We seem to have some extra rows, dial back
        EndRow = BeginRow;
        EndPage = BeginPage;
        Size = 0;
    }
    Y_VERIFY(EndRow <= end);
    if (EndRow < end) {
        while (auto& rel = Small->Relation(EndPage)) {
            if (rel.Row < end) {
                Size += rel.Size;
                ++EndPage;
            } else {
                Y_VERIFY(rel.IsHead(), "Got unaligned NPage::TFrames head record");
                break;
            }
        }
        EndRow = end;
    }
    return Size;
}

ui64 TPartGroupSizeHelper::CalcSize(TRowId begin, TRowId end)
{
    if (end <= begin) {
        return 0;
    }
    if (EndRow <= begin) {
        // Start searching from an already known end
        BeginRow = EndRow = begin;
        Begin = End = Index.LookupRow(BeginRow, End);
        Size = 0;
        Y_VERIFY(Begin, "Unexpected failure to find an index record");
    } else if (BeginRow != begin) {
        // Start searching from a previous start
        BeginRow = EndRow = begin;
        Begin = End = Index.LookupRow(BeginRow, Begin);
        Size = 0;
        Y_VERIFY(Begin, "Unexpected failure to find an index record");
    } else if (EndRow > end) {
        // We seem to have some extra rows, dial back
        EndRow = BeginRow;
        End = Begin;
        Size = 0;
    }
    Y_VERIFY(EndRow <= end);
    if (EndRow < end) {
        while (End && End->GetRowId() < end) {
            Size += Part->GetPageSize(End->GetPageId(), GroupId);
            ++End;
        }
        EndRow = end;
    }
    return Size;
}

ui64 TPartDataSizeHelper::CalcSize(TRowId begin, TRowId end)
{
    if (begin >= PartEndRowId || end <= begin) {
        return 0;
    }
    ui64 size = SmallHelper.CalcSize(begin, end);
    for (auto& g : GroupHelpers) {
        size += g.CalcSize(begin, end);
    }
    return size;
}

}
}
