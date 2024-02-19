#pragma once

#include "flat_part_iface.h"
#include "flat_part_laid.h"
#include "flat_page_frames.h"
#include "flat_stat_part_group_iter_iface.h"

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
// This iterator skips pages that are screened. Currently the logic is simple:
// if page start key is screened then we assume that the whole previous page is screened
// if page start key is not screened then the whole previous page is added to stats
class TStatsScreenedPartIterator {
    using TGroupId = NPage::TGroupId;
    using TFrames = NPage::TFrames;

public:
    TStatsScreenedPartIterator(TPartView partView, IPages* env, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults, 
            TIntrusiveConstPtr<TFrames> small, TIntrusiveConstPtr<TFrames> large)
        : Part(std::move(partView.Part))
        , KeyDefaults(std::move(keyDefaults))
        , Groups(::Reserve(Part->GroupsCount))
        , HistoricGroups(::Reserve(Part->HistoricGroupsCount))
        , Screen(std::move(partView.Screen))
        , Small(std::move(small))
        , Large(std::move(large))
        , CurrentHole(TScreen::Iter(Screen, CurrentHoleIdx, 0, 1))
    {
        for (ui32 groupIndex : xrange(Part->GroupsCount)) {
            Groups.push_back(CreateStatsPartGroupIterator(Part.Get(), env, TGroupId(groupIndex)));
        }
        for (ui32 groupIndex : xrange(Part->HistoricGroupsCount)) {
            HistoricGroups.push_back(CreateStatsPartGroupIterator(Part.Get(), env, TGroupId(groupIndex, true)));
        }
    }

    EReady Start() {
        auto ready = EReady::Data;

        for (auto& iter : Groups) {
            if (iter->Start() == EReady::Page) {
                ready = EReady::Page;
            }
        }
        for (auto& iter : HistoricGroups) {
            if (iter->Start() == EReady::Page) {
                ready = EReady::Page;
            }
        }

        if (ready != EReady::Page) {
            FillKey();
        }

        return ready;
    }

    bool IsValid() const {
        return Groups[0]->IsValid();
    }

    EReady Next(TPartDataStats& stats) {
        Y_ABORT_UNLESS(IsValid());

        auto curPageId = Groups[0]->GetPageId();
        LastRowId = Groups[0]->GetRowId();
        auto ready = Groups[0]->Next();
        if (ready == EReady::Page) {
            return ready;
        }

        ui64 rowCount = CountUnscreenedRows(GetLastRowId(), GetCurrentRowId());
        stats.RowCount += rowCount;
        if (rowCount) {
            AddPageSize(stats.DataSize, curPageId, TGroupId(0));
        }
        
        TRowId nextRowId = ready == EReady::Data ? Groups[0]->GetRowId() : Max<TRowId>();
        for (auto groupIndex : xrange<ui32>(1, Groups.size())) {
            while (Groups[groupIndex]->IsValid() && Groups[groupIndex]->GetRowId() < nextRowId) {
                // eagerly include all data up to the next row id
                if (rowCount) {
                    AddPageSize(stats.DataSize, Groups[groupIndex]->GetPageId(), TGroupId(groupIndex));
                }
                if (Groups[groupIndex]->Next() == EReady::Page) {
                    ready = EReady::Page;
                    break;
                }
            }
        }

        if (HistoricGroups) {
            Y_DEBUG_ABORT_UNLESS(Part->Scheme->HistoryGroup.ColsKeyIdx.size() == 3);
            while (HistoricGroups[0]->IsValid() && (!HistoricGroups[0]->GetKeyCellsCount() || HistoricGroups[0]->GetKeyCell(0).AsValue<TRowId>() < nextRowId)) {
                // eagerly include all history up to the next row id
                if (rowCount) {
                    AddPageSize(stats.DataSize, HistoricGroups[0]->GetPageId(), TGroupId(0, true));
                }
                if (HistoricGroups[0]->Next() == EReady::Page) {
                    ready = EReady::Page;
                    break;
                }
            }
            TRowId nextHistoryRowId = HistoricGroups[0]->IsValid() ? HistoricGroups[0]->GetRowId() : Max<TRowId>();
            for (auto groupIndex : xrange<ui32>(1, Groups.size())) {
                while (HistoricGroups[groupIndex]->IsValid() && HistoricGroups[groupIndex]->GetRowId() < nextHistoryRowId) {
                    // eagerly include all data up to the next row id
                    if (rowCount) {
                        AddPageSize(stats.DataSize, HistoricGroups[groupIndex]->GetPageId(), TGroupId(groupIndex, true));
                    }
                    if (HistoricGroups[groupIndex]->Next() == EReady::Page) {
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
        Y_ABORT_UNLESS(KeyDefaults->BasicTypes().size() == CurrentKey.size());
        return TDbTupleRef(KeyDefaults->BasicTypes().data(), CurrentKey.data(), CurrentKey.size());
    }

private:
    ui64 GetLastRowId() const {
        return LastRowId;
    }

    ui64 GetCurrentRowId() const {
        if (IsValid()) {
            return Groups[0]->GetRowId();
        }
        if (TRowId endRowId = Groups[0]->GetEndRowId(); endRowId != Max<TRowId>()) {
            // This would include the last page rows when known
            return endRowId;
        }
        return LastRowId;
    }

    void AddPageSize(TPartDataSize& stats, TPageId pageId, TGroupId groupId) const {
        // TODO: move to IStatsPartGroupIterator
        ui64 size = Part->GetPageSize(pageId, groupId);
        ui8 channel = Part->GetPageChannel(groupId);
        stats.Add(size, channel);
    }

    void FillKey() {
        CurrentKey.clear();

        if (!IsValid())
            return;

        ui32 keyIdx = 0;
        // Add columns that are present in the part
        if (ui32 keyCellsCount = Groups[0]->GetKeyCellsCount()) {
            for (;keyIdx < keyCellsCount; ++keyIdx) {
                CurrentKey.push_back(Groups[0]->GetKeyCell(keyIdx));
            }
        }

        // Extend with default values if needed
        for (;keyIdx < KeyDefaults->Defs.size(); ++keyIdx) {
            CurrentKey.push_back(KeyDefaults->Defs[keyIdx]);
        }
    }

    ui64 CountUnscreenedRows(TRowId beginRowId, TRowId endRowId) noexcept {
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

    void AddBlobsSize(TPartDataSize& stats, const TFrames* frames, ELargeObj lob, ui32 &prevPage) noexcept {
        const auto row = GetLastRowId();
        const auto end = GetCurrentRowId();

        prevPage = frames->Lower(row, prevPage, Max<ui32>());

        while (auto &rel = frames->Relation(prevPage)) {
            if (rel.Row < end) {
                auto channel = Part->GetPageChannel(lob, prevPage);
                stats.Add(rel.Size, channel);
                ++prevPage;
            } else if (!rel.IsHead()) {
                Y_ABORT("Got unaligned TFrames head record");
            } else {
                break;
            }
        }
    }

private:
    TIntrusiveConstPtr<TPart> Part;
    TIntrusiveConstPtr<TKeyCellDefaults> KeyDefaults;
    TSmallVec<TCell> CurrentKey;
    ui64 LastRowId = 0;
    
    TVector<THolder<IStatsPartGroupIterator>> Groups;
    TVector<THolder<IStatsPartGroupIterator>> HistoricGroups;
    TIntrusiveConstPtr<TScreen> Screen;
    TIntrusiveConstPtr<TFrames> Small;    /* Inverted index for small blobs   */
    TIntrusiveConstPtr<TFrames> Large;    /* Inverted index for large blobs   */
    size_t CurrentHoleIdx = 0;
    TScreen::THole CurrentHole;
    ui32 PrevSmallPage = 0;
    ui32 PrevLargePage = 0;
};

}}
