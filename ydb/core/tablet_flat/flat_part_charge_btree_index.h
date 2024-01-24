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
    struct TChildState : TChild {
        TRowId BeginRowId;
        TRowId EndRowId;

        TChildState(TChild meta, TRowId beginRowId, TRowId endRowId)
            : TChild(meta)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
        {
        }
    };

    struct TNodeState : TChildState, TBtreeIndexNode {
        TNodeState(TSharedData data, TChild meta, TRowId beginRowId, TRowId endRowId)
            : TChildState(meta, beginRowId, endRowId)
            , TBtreeIndexNode(data)
        {
        }

        TNodeState(TSharedData data, TChildState child)
            : TChildState(child)
            , TBtreeIndexNode(data)
        {
        }
    };

public:
    TChargeBTreeIndex(IPages *env, const TPart &part, TTagsRef tags, bool includeHistory)
        : Part(&part)
        , Scheme(*Part->Scheme)
        , Env(env) 
    {    
        Y_UNUSED(includeHistory);
        // if (includeHistory && Part->HistoricGroupsCount) {
        //     HistoryIndex.emplace(Part, Env, TGroupId(0, true));
        // }

        TDynBitMap seen;
        for (TTag tag : tags) {
            if (const auto* col = Scheme.FindColumnByTag(tag)) {
                if (col->Group != 0 && !seen.Get(col->Group)) {
                    NPage::TGroupId groupId(col->Group);
                    Groups.push_back(groupId);
                    // if (HistoryIndex) {
                    //     NPage::TGroupId historyGroupId(col->Group, true);
                    //     HistoryGroups.emplace_back(TPartIndexIt(Part, Env, historyGroupId), historyGroupId);
                    // }
                    seen.Set(col->Group);
                }
            }
        }
    }

public:
    TResult Do(TCells key1, TCells key2, TRowId row1, TRowId row2, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        Cerr << "Do " << " " << row1 << " " << row2 << Endl;

        bool ready = true;
        bool chargeGroups = true; // false value means that row1, row2 are invalid and shouldn't be used

        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];

        if (Y_UNLIKELY(row1 >= meta.RowCount)) {
            return {ready, true}; // already out of bounds, nothing to precharge
        }
        if (Y_UNLIKELY(row1 > row2)) {
            row2 = row1; // will not go further than row1
            chargeGroups = false;
        }
        TRowId sliceRow2 = row2;
        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            chargeGroups = false;
        }

        TVector<TNodeState> level, nextLevel(Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](const auto& tryHandle) {
            const TRowId levelRow1 = row1, levelRow2 = Max(row2, row1); // tryHandle may update them, copy for simplicity
            for (const auto &node : level) {
                if (node.EndRowId <= levelRow1 || node.BeginRowId > levelRow2) {
                    continue;
                }

                TRecIdx from = 0, to = node.GetKeysCount();
                if (node.BeginRowId < levelRow1) {
                    from = node.Seek(levelRow1);
                }
                if (node.EndRowId > levelRow2 + 1) {
                    to = node.Seek(levelRow2);
                }
                for (TRecIdx pos : xrange(from, to + 1)) {
                    auto child = node.GetChild(pos);
                    TRowId beginRowId = pos ? node.GetChild(pos - 1).RowCount : node.BeginRowId;
                    TRowId endRowId = child.RowCount;
                    ready &= tryHandle(TChildState(child, beginRowId, endRowId));
                }
            }
        };

        const auto tryHandleNode = [&](TChildState child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) { // update row1, row2
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key1PageId = node.GetShortChild(pos).PageId;
                        if (pos) {
                            row1 = Max(row1, node.GetShortChild(pos - 1).RowCount); // move row1 to the first key >= key1
                        }
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key2PageId = node.GetShortChild(pos).PageId;
                        row2 = Min(row2, node.GetShortChild(pos).RowCount); // move row2 to the first key > key2
                        if (node.GetShortChild(pos).RowCount <= row1) {
                            chargeGroups = false; // key2 is before current slice
                        }
                    }
                    return true;
                } else { // skip unloaded page rows
                    if (child.PageId == key1PageId) {
                        row1 = Max(row1, child.EndRowId);
                    }
                    if (child.PageId == key2PageId) {
                        if (child.BeginRowId) {
                            row2 = Min(row2, child.BeginRowId - 1);
                        } else {
                            chargeGroups = false;
                        }
                    }
                    return false;
                }
            } else {
                return TryLoadNode(child, nextLevel);
            }
        };

        const auto tryHandleDataPage = [&](TChildState child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                const auto page = TryGetDataPage(child.PageId, { });
                if (page) { // update row1, row2
                    auto data = NPage::TDataPage(page);
                    if (child.PageId == key1PageId) {
                        TRowId key1RowId = data.BaseRow() + data.LookupKey(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults).Off();
                        Cerr << "key1RowId " << key1RowId << Endl;
                        row1 = Max(row1, key1RowId);
                    }
                    if (child.PageId == key2PageId) {
                        TRowId key2RowId = data.BaseRow() + data.LookupKey(key2, Scheme.Groups[0], ESeek::Upper, &keyDefaults).Off();
                        Cerr << "key2RowId " << key2RowId << Endl;
                        if (key2RowId > 0) {
                            row2 = Min(row2, key2RowId - 1);
                        } else {
                            chargeGroups = false;
                        }
                    }
                    return true;
                } else { // skip unloaded page rows
                    if (child.PageId == key1PageId) {
                        row1 = Max(row1, child.EndRowId);
                    }
                    if (child.PageId == key2PageId) {
                        if (child.BeginRowId) {
                            row2 = Min(row2, child.BeginRowId - 1);
                        } else {
                            chargeGroups = false;
                        }
                    }
                    return false;
                }
            } else {
                return HasDataPage(child.PageId, { });
            }
        };

        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= tryHandleNode(TChildState(meta, 0, meta.RowCount));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (!ready) { // some index pages are missing, do not continue
            ready &= DoPrechargeGroups(chargeGroups, row1, row2); // precharge groups using the latest row bounds
            return {ready, false};
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        bool overshot = row2 == sliceRow2;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(TChildState(meta, 0, meta.RowCount));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoPrechargeGroups(chargeGroups, row1, row2); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId row1, TRowId row2, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        bool ready = true;
        
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];

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
                        // // always charge row1, no matter what keys are
                        // row2 = Min(row2, row1);
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

        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= TryLoadRoot(meta, level);
            } else {
                iterateLevel(tryLoadNode);
                level.swap(nextLevel);
                nextLevel.clear();
            }
        }

        if (!ready) {
            // some index pages are missing, do not continue

            // precharge groups using the latest row bounds
            for (auto groupId : Groups) {
                DoPrechargeGroup(groupId, row2, row1);
            }

            return {ready, false};
        }

        if (meta.LevelCount == 0) {
            ready &= HasDataPage(meta.PageId, { });
        } else {
            iterateLevel(hasDataPage);
        }

        // precharge groups using the latest row bounds
        for (auto groupId : Groups) {
            DoPrechargeGroup(groupId, row2, row1);
        }

        return {ready, row2 == sliceRow2};
    }

private:
    bool DoPrechargeGroups(bool chargeGroups, TRowId row1, TRowId row2) const noexcept {
        Cerr << "Groups " << chargeGroups << " " << row1 << " " << row2 << Endl;
        bool ready = true;
        
        if (chargeGroups && row1 <= row2) {
            for (auto groupId : Groups) {
                ready &= DoPrechargeGroup(groupId, row1, row2);
            }
        }

        return ready;
    }

    bool DoPrechargeGroup(TGroupId groupId, TRowId row1, TRowId row2) const noexcept {
        bool ready = true;

        const auto& meta = groupId.IsHistoric() ? Part->IndexPages.BTreeHistoric[groupId.Index] : Part->IndexPages.BTreeGroups[groupId.Index];

        Y_ABORT_UNLESS(row2 < meta.RowCount);
        
        TVector<TNodeState> level(Reserve(3)), nextLevel(Reserve(3));

        const auto iterateLevel = [&](const auto& tryLoadNext) {
            for (ui32 i : xrange<ui32>(level.size())) {
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
            return HasDataPage(current.GetShortChild(pos).PageId, groupId);
        };

        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= TryLoadRoot(meta, level);
            } else {
                iterateLevel(tryLoadNode);
                level.swap(nextLevel);
                nextLevel.clear();
            }
        }

        if (!ready) {
            // some index pages are missing, do not continue
            return ready;
        }

        if (meta.LevelCount == 0) {
            ready &= HasDataPage(meta.PageId, groupId);
        } else {
            iterateLevel(hasDataPage);
        }

        return ready;
    }

private:
    const TSharedData* TryGetDataPage(TPageId pageId, TGroupId groupId) const noexcept {
        return Env->TryGetPage(Part, pageId, groupId);
    };

    bool HasDataPage(TPageId pageId, TGroupId groupId) const noexcept {
        return bool(Env->TryGetPage(Part, pageId, groupId));
    }

    bool TryLoadRoot(const TBtreeIndexMeta& meta, TVector<TNodeState>& level) const noexcept {
        auto page = Env->TryGetPage(Part, meta.PageId);
        if (!page) {
            return false;
        }

        level.emplace_back(*page, meta, 0, meta.RowCount);
        return true;
    }

    bool TryLoadNode(TChildState& child, TVector<TNodeState>& level) const noexcept {
        auto page = Env->TryGetPage(Part, child.PageId);
        if (!page) {
            return false;
        }

        level.emplace_back(*page, child);
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
    TSmallVec<TGroupId> Groups;
};

}
