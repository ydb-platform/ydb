#pragma once

#include "flat_part_iface.h"
#include "flat_part_laid.h"
#include "flat_page_frames.h"
#include "flat_stat_part_group_iter_iface.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/draft/holder_vector.h>

namespace NKikimr {
namespace NTable {

// Iterates over part index and calculates total row count and data size
// This iterator skips pages that are screened. Currently the logic is simple:
// if page start key is screened then we assume that the whole previous page is screened
// if page start key is not screened then the whole previous page is added to stats
class TStatsScreenedPartIterator {
    using TGroupId = NPage::TGroupId;
    using TFrames = NPage::TFrames;

public:
    TStatsScreenedPartIterator(TPartView partView, IPages* env, TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults, 
            TIntrusiveConstPtr<TFrames> small, TIntrusiveConstPtr<TFrames> large,
            ui64 rowCountResolution, ui64 dataSizeResolution)
        : Part(std::move(partView.Part))
        , KeyDefaults(std::move(keyDefaults))
        , Groups(::Reserve(Part->GroupsCount))
        , HistoricGroups(::Reserve(Part->HistoricGroupsCount))
        , Screen(std::move(partView.Screen))
        , Small(std::move(small))
        , Large(std::move(large))
        , CurrentHole(TScreen::Iter(Screen, CurrentHoleIdx, 0, 1))
    {
        TVector<TRowId> splitPoints;
        if (Screen) {
            splitPoints.reserve(Screen->Size() * 2);
            for (auto hole : *Screen) {
                for (auto splitPoint : {hole.Begin, hole.End}) {
                    Y_DEBUG_ABORT_UNLESS(splitPoints.empty() || splitPoints.back() <= splitPoint);
                    if (0 < splitPoint && splitPoint < Part->Stat.Rows - 1 && (splitPoints.empty() || splitPoints.back() < splitPoint)) {
                        splitPoints.push_back(splitPoint);
                    }
                }
            }
        }

        for (bool historic : {false, true}) {
            for (ui32 groupIndex : xrange(historic ? Part->HistoricGroupsCount : Part->GroupsCount)) {
                ui64 groupRowCountResolution, groupDataSizeResolution;
                if (groupIndex == 0 && (Part->GroupsCount > 1 || Small || Large)) {
                    // make steps as small as possible because they will affect groups resolution
                    groupRowCountResolution = groupDataSizeResolution = 0;
                } else {
                    groupRowCountResolution = rowCountResolution;
                    groupDataSizeResolution = dataSizeResolution;
                }

                (historic ? HistoricGroups : Groups).push_back(
                    CreateStatsPartGroupIterator(Part.Get(), env, TGroupId(groupIndex, historic), 
                        groupRowCountResolution, groupDataSizeResolution, 
                        historic || groupRowCountResolution == 0 ? TVector<TRowId>() : splitPoints));
            }
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

    EReady Next(TDataStats& stats) {
        Y_ABORT_UNLESS(IsValid());

        LastRowId = Groups[0]->GetRowId();
        auto ready = Groups[0]->Next();
        if (ready == EReady::Page) {
            Y_DEBUG_ABORT("Shouldn't really happen");
            return ready;
        }

        ui64 rowCount = CountUnscreenedRows(GetLastRowId(), GetCurrentRowId());
        stats.RowCount += rowCount;
        if (rowCount) {
            Groups[0]->AddLastDeltaDataSize(stats.DataSize);
        }
        
        TRowId nextRowId = ready == EReady::Data ? Groups[0]->GetRowId() : Max<TRowId>();
        for (auto groupIndex : xrange<ui32>(1, Groups.size())) {
            while (Groups[groupIndex]->IsValid() && Groups[groupIndex]->GetRowId() < nextRowId) {
                // eagerly include all data up to the next row id
                if (Groups[groupIndex]->Next() == EReady::Page) {
                    Y_DEBUG_ABORT("Shouldn't really happen");
                    ready = EReady::Page;
                    break;
                }
                if (rowCount) {
                    Groups[groupIndex]->AddLastDeltaDataSize(stats.DataSize);
                }
            }
        }

        if (HistoricGroups) {
            Y_DEBUG_ABORT_UNLESS(Part->Scheme->HistoryGroup.ColsKeyIdx.size() == 3);
            while (HistoricGroups[0]->IsValid() && (!HistoricGroups[0]->GetKeyCellsCount() || HistoricGroups[0]->GetKeyCell(0).AsValue<TRowId>() < nextRowId)) {
                // eagerly include all history up to the next row id
                if (HistoricGroups[0]->Next() == EReady::Page) {
                    Y_DEBUG_ABORT("Shouldn't really happen");
                    ready = EReady::Page;
                    break;
                }
                if (rowCount) {
                    HistoricGroups[0]->AddLastDeltaDataSize(stats.DataSize);
                }
            }
            TRowId nextHistoryRowId = HistoricGroups[0]->IsValid() ? HistoricGroups[0]->GetRowId() : Max<TRowId>();
            for (auto groupIndex : xrange<ui32>(1, Groups.size())) {
                while (HistoricGroups[groupIndex]->IsValid() && HistoricGroups[groupIndex]->GetRowId() < nextHistoryRowId) {
                    // eagerly include all data up to the next row id
                    if (HistoricGroups[groupIndex]->Next() == EReady::Page) {
                        Y_DEBUG_ABORT("Shouldn't really happen");
                        ready = EReady::Page;
                        break;
                    }
                    if (rowCount) {
                        HistoricGroups[groupIndex]->AddLastDeltaDataSize(stats.DataSize);
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

    void FillKey() {
        CurrentKey.clear();

        if (!IsValid())
            return;

        // Add columns that are present in the part
        Groups[0]->GetKeyCells(CurrentKey);

        // Extend with default values if needed
        for (ui32 index = CurrentKey.size(); index < KeyDefaults->Defs.size(); ++index) {
            CurrentKey.push_back(KeyDefaults->Defs[index]);
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

    void AddBlobsSize(TChanneledDataSize& stats, const TFrames* frames, ELargeObj lob, ui32 &prevPage) noexcept {
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
    
    TVector<THolder<IStatsPartGroupIter>> Groups;
    TVector<THolder<IStatsPartGroupIter>> HistoricGroups;
    TIntrusiveConstPtr<TScreen> Screen;
    TIntrusiveConstPtr<TFrames> Small;    /* Inverted index for small blobs   */
    TIntrusiveConstPtr<TFrames> Large;    /* Inverted index for large blobs   */
    size_t CurrentHoleIdx = 0;
    TScreen::THole CurrentHole;
    ui32 PrevSmallPage = 0;
    ui32 PrevLargePage = 0;
};

}}
