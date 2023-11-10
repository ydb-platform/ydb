#pragma once

#include "flat_part_iface.h"
#include "flat_part_index_iter.h"
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

// Iterates over part index and calculates total row count and data size
// NOTE: we don't know row count for the last page so we also ignore its size
// This shouldn't be a problem for big parts with many pages
// This iterator skipps pages that are screened. Currently the logic is simple:
// if page start key is screened then we assume that the whole previous page is screened
// if page start key is not screened then the whole previous page is added to stats
class TScreenedPartIndexIterator {
public:
    TScreenedPartIndexIterator(TPartView partView, IPages* env, TIntrusiveConstPtr<TKeyCellDefaults> keyColumns, 
            TIntrusiveConstPtr<NPage::TFrames> small, TIntrusiveConstPtr<NPage::TFrames> large)
        : Part(std::move(partView.Part))
        , Pos(Part.Get(), env, {})
        , KeyColumns(std::move(keyColumns))
        , Screen(std::move(partView.Screen))
        , Small(std::move(small))
        , Large(std::move(large))
        , CurrentHole(TScreen::Iter(Screen, CurrentHoleIdx, 0, 1))
    {
        AltGroups.reserve(Part->GroupsCount - 1);
        for (ui32 group : xrange(size_t(1), Part->GroupsCount)) {
            AltGroups.emplace_back(Part.Get(), env, NPage::TGroupId(group));
        }
        for (ui32 group : xrange(Part->HistoricGroupsCount)) {
            HistoryGroups.emplace_back(Part.Get(), env, NPage::TGroupId(group, true));
        }
    }

    EReady Start() {
        auto ready = Pos.Seek(0);
        if (ready != EReady::Page) {
            FillKey();
        }

        for (auto& g : AltGroups) {
            if (g.Pos.Seek(0) == EReady::Page) {
                ready = EReady::Page;
            }
        }
        for (auto& g : HistoryGroups) {
            if (g.Pos.Seek(0) == EReady::Page) {
                ready = EReady::Page;
            }
        }

        return ready;
    }

    bool IsValid() const {
        return Pos.IsValid();
    }

    EReady Next(TPartDataStats& stats) {
        Y_ABORT_UNLESS(IsValid());

        auto curPageId = Pos.GetPageId();
        LastRowId = Pos.GetRowId();
        auto ready = Pos.Next();
        if (ready == EReady::Page) {
            return ready;
        }
        ui64 rowCount = IncludedRows(GetLastRowId(), GetCurrentRowId());
        stats.RowCount += rowCount;

        if (rowCount) AddPageSize(stats.DataSize, curPageId);
        TRowId nextRowId = ready == EReady::Data ? Pos.GetRowId() : Max<TRowId>();
        for (auto& g : AltGroups) {
            while (g.Pos.IsValid() && g.Pos.GetRowId() < nextRowId) {
                // eagerly include all data up to the next row id
                if (rowCount) AddPageSize(stats.DataSize, g.Pos.GetPageId(), g.GroupId);
                if (g.Pos.Next() == EReady::Page) {
                    ready = EReady::Page;
                    break;
                }
            }
        }

        // Include mvcc data
        if (!HistoryGroups.empty()) {
            auto& h = HistoryGroups[0];
            const auto& hscheme = Part->Scheme->HistoryGroup;
            Y_DEBUG_ABORT_UNLESS(hscheme.ColsKeyIdx.size() == 3);
            while (h.Pos.IsValid() && h.Pos.GetRecord()->Cell(hscheme.ColsKeyIdx[0]).AsValue<TRowId>() < nextRowId) {
                // eagerly include all history up to the next row id
                if (rowCount) AddPageSize(stats.DataSize, h.Pos.GetPageId(), h.GroupId);
                if (h.Pos.Next() == EReady::Page) {
                    ready = EReady::Page;
                    break;
                }
            }
            TRowId nextHistoryRowId = h.Pos.IsValid() ? h.Pos.GetRowId() : Max<TRowId>();
            for (size_t index = 1; index < HistoryGroups.size(); ++index) {
                auto& g = HistoryGroups[index];
                while (g.Pos.IsValid() && g.Pos.GetRowId() < nextHistoryRowId) {
                    // eagerly include all data up to the next row id
                    if (rowCount) AddPageSize(stats.DataSize, g.Pos.GetPageId(), g.GroupId);
                    if (g.Pos.Next() == EReady::Page) {
                        ready = EReady::Page;
                        break;
                    }
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
        return ready;
    }

    TDbTupleRef GetCurrentKey() const {
        Y_ABORT_UNLESS(KeyColumns->BasicTypes().size() == CurrentKey.size());
        return TDbTupleRef(KeyColumns->BasicTypes().data(), CurrentKey.data(), CurrentKey.size());
    }

private:
    ui64 GetLastRowId() const {
        return LastRowId;
    }

    ui64 GetCurrentRowId() const {
        if (IsValid()) {
            return Pos.GetRowId();
        }
        if (TRowId endRowId = Pos.GetEndRowId(); endRowId != Max<TRowId>()) {
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
            CurrentKey.push_back(Pos.GetRecord()->Cell(Part->Scheme->Groups[0].ColsKeyIdx[keyIdx]));
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
                Y_ABORT("Got unaligned NPage::TFrames head record");
            } else {
                break;
            }
        }
    }

private:
    struct TGroupState {
        TPartIndexIt Pos;
        const NPage::TGroupId GroupId;

        TGroupState(const TPart* part, IPages* env, NPage::TGroupId groupId)
            : Pos(part, env, groupId)
            , GroupId(groupId)
        { }
    };

private:
    TIntrusiveConstPtr<TPart> Part;
    TPartIndexIt Pos;
    TIntrusiveConstPtr<TKeyCellDefaults> KeyColumns;
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
