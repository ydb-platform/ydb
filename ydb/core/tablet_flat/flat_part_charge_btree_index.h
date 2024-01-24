#pragma once

#include "flat_table_part.h"
#include "flat_part_iface.h"
#include "flat_part_charge_iface.h"

namespace NKikimr::NTable {

class TChargeBTreeIndex : public ICharge {
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;
    using TRecIdx = NPage::TRecIdx;
    using TGroupId = NPage::TGroupId;
    using TChild = TBtreeIndexNode::TChild;

    // TODO: store PageId only instead of TChild?
    struct TNodeState : TBtreeIndexNode, TChild {
        TRowId BeginRowId;
        TRowId EndRowId;

        TNodeState(TSharedData data, TChild meta, TRowId beginRowId, TRowId endRowId)
            : TBtreeIndexNode(data) 
            , TChild(meta)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
        {
        }
    };

public:
    TChargeBTreeIndex(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory = false)
        : Part(&part)
        , Scheme(*Part->Scheme)
        , Env(env) {
        Y_UNUSED(part);
        Y_UNUSED(tags);
        Y_UNUSED(includeHistory);
    }

public:
    TResult Do(TCells key1, TCells key2, TRowId row1, TRowId row2, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        bool ready = true;

        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];

        if (meta.LevelCount == 0) {
            ready &= HasDataPage(meta.PageId, { });
            return { ready, true };
        }

        if (Y_UNLIKELY(row1 >= meta.RowCount)) {
            return { true, true }; // already out of bounds, nothing to precharge
        }
        if (Y_UNLIKELY(row1 > row2)) {
            row2 = row1; // will not go further than row1
        }
        TRowId sliceRow2 = row2;
        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
        }

        TVector<TNodeState> level(Reserve(3)), nextLevel(Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](const auto& tryLoadNext) {
            for (ui32 i : xrange<ui32>(level.size())) {
                if (level[i].PageId == key1PageId) {
                    TRecIdx pos = level[i].Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                    key1PageId = level[i].GetShortChild(pos).PageId;
                    if (pos) {
                        // move row1 to the first key >= key1
                        row1 = Max(row1, level[i].GetShortChild(pos - 1).RowCount);
                    }
                }
                if (level[i].PageId == key2PageId) {
                    TRecIdx pos = level[i].Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                    key2PageId = level[i].GetShortChild(pos).PageId;
                    // move row2 to the first key > key2
                    row2 = Min(row2, level[i].GetShortChild(pos).RowCount);
                    // always charge row2, no matter what keys are
                    row2 = Max(row2, row1);
                }
                
                if (level[i].EndRowId <= row1 || level[i].BeginRowId > row2) {
                    continue;
                }

                TRecIdx from = 0, to = level[i].GetKeysCount();
                if (level[i].BeginRowId < row1) {
                    from = level[i].Seek(row1);
                }
                if (level[i].EndRowId > row2 + 1) {
                    to = level[i].Seek(row2);
                }
                for (TRecIdx j : xrange(from, to + 1)) {
                    ready &= tryLoadNext(level[i], j);
                }
            }
        };

        const auto tryLoadNode = [&](TNodeState& current, TRecIdx pos) -> bool {
            return TryLoadNode(current, pos, nextLevel);
        };

        const auto hasDataPage = [&](TNodeState& current, TRecIdx pos) -> bool {
            return HasDataPage(current.GetShortChild(pos).PageId, { });
        };

        ready &= TryLoadRoot(meta, level);

        for (ui32 height = 1; height < meta.LevelCount && ready; height++) {
            iterateLevel(tryLoadNode);
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (!ready) {
            // some index pages are missing, do not continue
            return {false, false};
        }

        iterateLevel(hasDataPage);

        return {ready, row2 == sliceRow2};
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId row1, TRowId row2, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        bool ready = true;
        
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];

        if (meta.LevelCount == 0) {
            ready &= HasDataPage(meta.PageId, { });
            return { ready, true };
        }

        if (Y_UNLIKELY(row1 >= meta.RowCount)) {
            row1 = meta.RowCount - 1; // start from the last row
        }
        if (Y_UNLIKELY(row2 > row1)) {
            row2 = row1; // will not go further than row1
        }
        TRowId sliceRow2 = row2;
        if (Y_UNLIKELY(key1 && key2 && Compare(key2, key1, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
        }

        // level contains nodes in reverse order
        TVector<TNodeState> level(Reserve(3)), nextLevel(Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](const auto& tryLoadNext) {
            for (ui32 i : xrange<ui32>(level.size())) {
                if (level[i].PageId == key1PageId) {
                    TRecIdx pos = level[i].SeekReverse(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                    key1PageId = level[i].GetShortChild(pos).PageId;
                    // move row1 to the first key <= key1
                    row1 = Min(row1, level[i].GetShortChild(pos).RowCount - 1);
                }
                if (level[i].PageId == key2PageId) {
                    TRecIdx pos = level[i].SeekReverse(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                    key2PageId = level[i].GetShortChild(pos).PageId;
                    // move row2 to the first key > key2
                    if (pos) {
                        row2 = Max(row2, level[i].GetShortChild(pos - 1).RowCount - 1);
                        // always charge row1, no matter what keys are
                        row2 = Min(row2, row1);
                    }
                }
                
                if (level[i].EndRowId <= row2 || level[i].BeginRowId > row1) {
                    continue;
                }

                TRecIdx from = level[i].GetKeysCount(), to = 0;
                if (level[i].EndRowId > row1 + 1) {
                    from = level[i].Seek(row1);
                }
                if (level[i].BeginRowId < row2) {
                    to = level[i].Seek(row2);
                }
                for (TRecIdx j = from + 1; j > to; j--) {
                    ready &= tryLoadNext(level[i], j - 1);
                }
            }
        };

        const auto tryLoadNode = [&](TNodeState& current, TRecIdx pos) -> bool {
            return TryLoadNode(current, pos, nextLevel);
        };

        const auto hasDataPage = [&](TNodeState& current, TRecIdx pos) -> bool {
            return HasDataPage(current.GetShortChild(pos).PageId, { });
        };

        ready &= TryLoadRoot(meta, level);

        for (ui32 height = 1; height < meta.LevelCount && ready; height++) {
            iterateLevel(tryLoadNode);
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (!ready) {
            // some index pages are missing, do not continue
            return {false, false};
        }

        iterateLevel(hasDataPage);

        return {ready, row2 == sliceRow2};
    }

private:
    bool HasDataPage(TPageId pageId, TGroupId groupId) const noexcept {
        return Env->TryGetPage(Part, pageId, groupId);
    }

    bool TryLoadRoot(const TBtreeIndexMeta& meta, TVector<TNodeState>& level) const noexcept {
        auto page = Env->TryGetPage(Part, meta.PageId);
        if (!page) {
            return false;
        }

        level.emplace_back(*page, meta, 0, meta.RowCount);
        return true;
    }

    bool TryLoadNode(TNodeState& current, TRecIdx pos, TVector<TNodeState>& level) const noexcept {
        Y_ABORT_UNLESS(pos < current.GetChildrenCount(), "Should point to some child");

        auto child = current.GetChild(pos);

        auto page = Env->TryGetPage(Part, child.PageId);
        if (!page) {
            return false;
        }

        TRowId beginRowId = pos ? current.GetChild(pos - 1).RowCount : current.BeginRowId;
        TRowId endRowId = child.RowCount;

        level.emplace_back(*page, child, beginRowId, endRowId);
        return true;
    }

    int Compare(TCells left, TCells right, const TKeyCellDefaults &keyDefaults) const noexcept
    {
        Y_DEBUG_ABORT_UNLESS(left, "Empty keys should be handled separately");
        Y_DEBUG_ABORT_UNLESS(right, "Empty keys should be handled separately");

        for (TPos it = 0; it < Min(left.size(), right.size(), keyDefaults.Size()); it++) {
            if (int cmp = CompareTypedCells(left[it], right[it], keyDefaults.Types[it])) {
                return cmp;
            }
        }

        return left.size() == right.size() 
            ? 0
            // Missing point cells are filled with a virtual +inf
            : (left.size() > right.size() ? -1 : 1);
    }

private:
    const TPart* const Part;
    const TPartScheme &Scheme;
    IPages* const Env;
};

}
