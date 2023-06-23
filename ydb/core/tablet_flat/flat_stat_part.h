#pragma once

#include "flat_part_iface.h"
#include "flat_part_laid.h"
#include "flat_page_frames.h"
#include "util_basics.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/draft/holder_vector.h>

namespace NKikimr {
namespace NTable {

struct TPartDataSize {
    ui64 Size = 0;
    TVector<ui64> ByChannel = { };

    void Add(ui64 size, ui8 channel) {
        Size += size;
        if (!(channel < ByChannel.size())) {
            ByChannel.resize(channel + 1);
        }
        ByChannel[channel] += size;
    }
};

struct TPartDataStats {
    ui64 RowCount = 0;
    TPartDataSize DataSize = { };
};

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
// This iterator skipps pages that are screened. Currently the logic is simple:
// if page start key is screened then we assume that the whole previous page is screened
// if page start key is not screened then the whole previous page is added to stats
class TScreenedPartIndexIterator {
public:
    TScreenedPartIndexIterator(TPartView partView, TIntrusiveConstPtr<TKeyCellDefaults> keyColumns, 
            TIntrusiveConstPtr<NPage::TFrames> small, TIntrusiveConstPtr<NPage::TFrames> large)
        : Part(std::move(partView.Part))
        , KeyColumns(std::move(keyColumns))
        , Screen(std::move(partView.Screen))
        , Small(std::move(small))
        , Large(std::move(large))
        , CurrentHole(TScreen::Iter(Screen, CurrentHoleIdx, 0, 1))
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

    void Next(TPartDataStats& stats) {
        Y_VERIFY(IsValid());

        auto curPageId = Pos->GetPageId();
        LastRowId = Pos->GetRowId();
        ++Pos;
        ui64 rowCount = IncludedRows(GetLastRowId(), GetCurrentRowId());
        stats.RowCount += rowCount;

        if (rowCount) AddPageSize(stats.DataSize, curPageId);
        TRowId nextRowId = Pos ? Pos->GetRowId() : Max<TRowId>();
        for (auto& g : AltGroups) {
            while (g.Pos && g.Pos->GetRowId() < nextRowId) {
                // eagerly include all data up to the next row id
                if (rowCount) AddPageSize(stats.DataSize, g.Pos->GetPageId(), g.GroupId);
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
                if (rowCount) AddPageSize(stats.DataSize, h.Pos->GetPageId(), h.GroupId);
                ++h.Pos;
            }
            TRowId nextHistoryRowId = h.Pos ? h.Pos->GetRowId() : Max<TRowId>();
            for (size_t index = 1; index < HistoryGroups.size(); ++index) {
                auto& g = HistoryGroups[index];
                while (g.Pos && g.Pos->GetRowId() < nextHistoryRowId) {
                    // eagerly include all data up to the next row id
                    if (rowCount) AddPageSize(stats.DataSize, g.Pos->GetPageId(), g.GroupId);
                    ++g.Pos;
                }
            }
        }

        if (rowCount) {
            if (Small) {
                AddBlobsSize(stats.DataSize, Small.Get(), ELargeObj::Outer, PrevSmallPage);
            }
            if (Large) {
                AddBlobsSize(stats.DataSize, Large.Get(), ELargeObj::Extern, PrevLargePage);
            }
        }

        FillKey();
    }

    TDbTupleRef GetCurrentKey() const {
        Y_VERIFY(KeyColumns->BasicTypes().size() == CurrentKey.size());
        return TDbTupleRef(KeyColumns->BasicTypes().data(), CurrentKey.data(), CurrentKey.size());
    }

private:
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

private:
    void AddPageSize(TPartDataSize& stats, TPageId pageId, NPage::TGroupId groupId = { }) const {
        ui64 size = Part->GetPageSize(pageId, groupId);
        ui8 channel = Part->GetPageChannel(pageId, groupId);
        stats.Add(size, channel);
    }

private:
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

private:
    void AddBlobsSize(TPartDataSize& stats, const NPage::TFrames* frames, ELargeObj lob, ui32 &prevPage) noexcept {
        const auto row = GetLastRowId();
        const auto end = GetCurrentRowId();

        prevPage = frames->Lower(row, prevPage, Max<ui32>());

        while (auto &rel = frames->Relation(prevPage)) {
            if (rel.Row < end) {
                auto channel = Part->GetPageChannel(lob, prevPage);
                stats.Add(rel.Size, channel);
                ++prevPage;
            } else if (!rel.IsHead()) {
                Y_FAIL("Got unaligned NPage::TFrames head record");
            } else {
                break;
            }
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
    ui64 LastRowId = 0;
    TSmallVec<TGroupState> AltGroups;
    TSmallVec<TGroupState> HistoryGroups;
    TIntrusiveConstPtr<TScreen> Screen;
    TIntrusiveConstPtr<NPage::TFrames> Small;    /* Inverted index for small blobs   */
    TIntrusiveConstPtr<NPage::TFrames> Large;    /* Inverted index for large blobs   */
    size_t CurrentHoleIdx = 0;
    TScreen::THole CurrentHole;
    ui32 PrevSmallPage = 0;
    ui32 PrevLargePage = 0;
};

}}
