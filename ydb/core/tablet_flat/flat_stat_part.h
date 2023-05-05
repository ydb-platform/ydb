#pragma once

#include "flat_part_iface.h"
#include "flat_part_laid.h"
#include "flat_page_frames.h"
#include "util_basics.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/draft/holder_vector.h>

namespace NKikimr {
namespace NTable {

/**
 * Helper for calculating small blobs size between a pair of rows
 */
class TPartSmallSizeHelper {
public:
    TPartSmallSizeHelper(const TPart* part)
        : Small(part->Small.Get())
    {
    }

    /**
     * Returns size of small blobs in bytes between begin and end (not inclusive)
     */
    ui64 CalcSize(TRowId begin, TRowId end);

private:
    const NPage::TFrames* Small;
    TRowId BeginRow = 0;
    TRowId EndRow = 0;
    ui32 BeginPage = 0;
    ui32 EndPage = 0;
    ui64 Size = 0;
};

/**
 * Helper for calculating column group size between a pair of rows
 */
class TPartGroupSizeHelper {
public:
    TPartGroupSizeHelper(const TPart* part, NPage::TGroupId groupId)
        : Part(part)
        , GroupId(groupId)
        , Index(Part->GetGroupIndex(groupId))
        , Begin(Index->Begin())
        , End(Begin)
    {
        Y_VERIFY(Begin, "Cannot find the first index page");
    }

    /**
     * Returns size of group in bytes between begin and end (not inclusive)
     */
    ui64 CalcSize(TRowId begin, TRowId end);

private:
    const TPart* Part;
    const NPage::TGroupId GroupId;
    const NPage::TIndex& Index;
    NPage::TIndex::TIter Begin;
    NPage::TIndex::TIter End;
    TRowId BeginRow = 0;
    TRowId EndRow = 0;
    ui64 Size = 0;
};

/**
 * Helper for calculating upper bound size of part between a pair of rows
 */
class TPartDataSizeHelper {
public:
    TPartDataSizeHelper(const TPart* part)
        : PartEndRowId(part->Index.GetEndRowId())
        , SmallHelper(part)
    {
        GroupHelpers.reserve(part->Scheme->Groups.size());
        for (ui32 group : xrange(part->Scheme->Groups.size())) {
            GroupHelpers.emplace_back(part, NPage::TGroupId(group));
        }
    }

    /**
     * Returns size of part in bytes between begin and end (not inclusive)
     */
    ui64 CalcSize(TRowId begin, TRowId end);

private:
    const TRowId PartEndRowId;
    TPartSmallSizeHelper SmallHelper;
    TSmallVec<TPartGroupSizeHelper> GroupHelpers;
};

// Iterates over part index and calculates total row count and data size
// NOTE: we don't know row count for the last page so we also ignore its size
// This shouldn't be a problem for big parts with many pages
class TPartIndexIterator {
public:
    TPartIndexIterator(TIntrusiveConstPtr<TPart> part, TIntrusiveConstPtr<TKeyCellDefaults> keys)
        : Part(std::move(part))
        , KeyColumns(std::move(keys))
    {
        Pos = Part->Index->Begin();
        End = Part->Index->End();
        AltGroups.reserve(Part->Scheme->Groups.size() - 1);
        for (ui32 group : xrange(size_t(1), Part->Scheme->Groups.size())) {
            AltGroups.emplace_back(Part.Get(), NPage::TGroupId(group));
        }
        for (ui32 group : xrange(Part->HistoricIndexes.size())) {
            HistoryGroups.emplace_back(Part.Get(), NPage::TGroupId(group, true));
        }
        FillKey();
    }

    bool IsValid() const {
        return Pos != End;
    }

    void Next() {
        Y_VERIFY(IsValid());

        auto curPageId = Pos->GetPageId();
        LastSize = CurrentSize;
        LastRowId = Pos->GetRowId();
        CurrentSize += GetPageSize(curPageId);
        ++Pos;
        TRowId nextRowId = Pos ? Pos->GetRowId() : Max<TRowId>();
        for (auto& g : AltGroups) {
            while (g.Pos && g.Pos->GetRowId() < nextRowId) {
                // eagerly include all data up to the next row id
                CurrentSize += GetPageSize(g.Pos->GetPageId(), g.GroupId);
                ++g.Pos;
            }
        }
        // Include mvcc data
        if (!HistoryGroups.empty()) {
            auto& h = HistoryGroups[0];
            const auto& hscheme = Part->Scheme->HistoryGroup;
            Y_VERIFY_DEBUG(hscheme.ColsKeyIdx.size() == 3);
            while (h.Pos && h.Pos->Cell(hscheme.ColsKeyIdx[0]).AsValue<TRowId>() < nextRowId) {
                // eagerly include all history up to the next row id
                CurrentSize += GetPageSize(h.Pos->GetPageId(), h.GroupId);
                ++h.Pos;
            }
            TRowId nextHistoryRowId = h.Pos ? h.Pos->GetRowId() : Max<TRowId>();
            for (size_t index = 1; index < HistoryGroups.size(); ++index) {
                auto& g = HistoryGroups[index];
                while (g.Pos && g.Pos->GetRowId() < nextHistoryRowId) {
                    // eagerly include all data up to the next row id
                    CurrentSize += GetPageSize(g.Pos->GetPageId(), g.GroupId);
                    ++g.Pos;
                }
            }
        }
        FillKey();
    }

    TDbTupleRef GetCurrentKey() const {
        Y_VERIFY(KeyColumns->BasicTypes().size() == CurrentKey.size());
        return TDbTupleRef(KeyColumns->BasicTypes().data(), CurrentKey.data(), CurrentKey.size());
    }

    ui64 GetLastRowId() const {
        return LastRowId;
    }

    ui64 GetCurrentRowId() const {
        if (IsValid()) {
            return Pos->GetRowId();
        }
        if (TRowId endRowId = Part->Index.GetEndRowId(); endRowId != Max<TRowId>()) {
            // This would include the last page rows when known
            return endRowId;
        }
        return LastRowId;
    }

    ui64 GetLastDataSize() const {
        return LastSize;
    }

    ui64 GetCurrentDataSize() const {
        return CurrentSize;
    }

private:
    ui64 GetPageSize(TPageId pageId, NPage::TGroupId groupId = { }) const {
        return Part->GetPageSize(pageId, groupId);
    }

    void FillKey() {
        CurrentKey.clear();

        if (!IsValid())
            return;

        ui32 keyIdx = 0;
        // Add columns that are present in the part
        for (;keyIdx < Part->Scheme->Groups[0].KeyTypes.size(); ++keyIdx) {
            CurrentKey.push_back(Pos->Cell(Part->Scheme->Groups[0].ColsKeyIdx[keyIdx]));
        }

        // Extend with default values if needed
        for (;keyIdx < KeyColumns->Defs.size(); ++keyIdx) {
            CurrentKey.push_back(KeyColumns->Defs[keyIdx]);
        }
    }

private:
    struct TGroupState {
        NPage::TIndex::TIter Pos;
        const NPage::TGroupId GroupId;

        TGroupState(const TPart* part, NPage::TGroupId groupId)
            : Pos(part->GetGroupIndex(groupId)->Begin())
            , GroupId(groupId)
        { }
    };

private:
    TIntrusiveConstPtr<TPart> Part;
    TIntrusiveConstPtr<TKeyCellDefaults> KeyColumns;
    NPage::TIndex::TIter Pos;
    NPage::TIndex::TIter End;
    TSmallVec<TCell> CurrentKey;
    ui64 CurrentSize = 0;
    ui64 LastRowId = 0;
    ui64 LastSize = 0;
    TSmallVec<TGroupState> AltGroups;
    TSmallVec<TGroupState> HistoryGroups;
};

// This iterator skipps pages that are screened. Currently the logic is simple:
// if page start key is screened then we assume that the whole previous page is screened
// if page start key is not screened then the whole previous page is added to stats
class TScreenedPartIndexIterator {
public:
    TScreenedPartIndexIterator(TPartView partView, TIntrusiveConstPtr<TKeyCellDefaults> keyColumns,
                            TIntrusiveConstPtr<NPage::TFrames> small)
        : PartIter(partView.Part, keyColumns)
        , Screen(std::move(partView.Screen))
        , Small(std::move(small))
        , CurrentHole(TScreen::Iter(Screen, CurrentHoleIdx, 0, 1))
    {
    }

    bool IsValid() const {
        return PartIter.IsValid();
    }

    void Next() {
        Y_VERIFY(IsValid());

        PrevRowCount = CurrentRowCount;
        PrevSize = CurrentSize;
        PartIter.Next();

        ui64 rowCount = IncludedRows(PartIter.GetLastRowId(), PartIter.GetCurrentRowId());
        if (rowCount > 0) {
            // We try to count rows precisely, but data is only counted per-page
            CurrentRowCount += rowCount;
            CurrentSize += PartIter.GetCurrentDataSize() - PartIter.GetLastDataSize();
            if (Small) {
                CurrentSize += CalcSmallBytes();
            }
        }
    }

    TDbTupleRef GetCurrentKey() const {
        return PartIter.GetCurrentKey();
    }

    ui64 GetRowCountDelta() const {
        return CurrentRowCount - PrevRowCount;
    }

    ui64 GetDataSizeDelta() const {
        return CurrentSize - PrevSize;
    }

private:
    ui64 IncludedRows(TRowId beginRowId, TRowId endRowId) noexcept {
        if (!Screen) {
            // Include all rows
            return endRowId - beginRowId;
        }

        TRowId rowId = beginRowId;
        ui64 rowCount = 0;
        while (rowId < endRowId) {
            // Skip screen holes before the current rowId
            while (CurrentHole.End <= rowId) {
                CurrentHole = TScreen::Next(Screen, CurrentHoleIdx, 1);
            }
            TRowId next;
            if (rowId < CurrentHole.Begin) {
                // Skip rows before the next begin
                next = Min(CurrentHole.Begin, endRowId);
            } else {
                // Include rows before the next end
                next = Min(CurrentHole.End, endRowId);
                rowCount += next - rowId;
            }
            rowId = next;
        }

        return rowCount;
    }

    ui64 CalcSmallBytes() noexcept {
        ui64 bytes = 0;

        const auto row = PartIter.GetLastRowId();
        const auto end = PartIter.GetCurrentRowId();

        PrevSmallPage = Small->Lower(row, PrevSmallPage, Max<ui32>());

        while (auto &rel = Small->Relation(PrevSmallPage)) {
            if (rel.Row < end) {
                bytes += rel.Size, ++PrevSmallPage;
            } else if (!rel.IsHead()) {
                Y_FAIL("Got unaligned NPage::TFrames head record");
            } else {
                break;
            }
        }

        return bytes;
    }

private:
    TPartIndexIterator PartIter;
    TIntrusiveConstPtr<TScreen> Screen;
    TIntrusiveConstPtr<NPage::TFrames> Small;    /* Inverted index for small blobs   */
    size_t CurrentHoleIdx = 0;
    TScreen::THole CurrentHole;
    ui64 CurrentRowCount = 0;
    ui64 PrevRowCount = 0;
    ui64 CurrentSize = 0;
    ui64 PrevSize = 0;
    ui32 PrevSmallPage = 0;
};

}}
