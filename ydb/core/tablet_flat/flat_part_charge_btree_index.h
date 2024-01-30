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

        TDynBitMap seen;
        for (TTag tag : tags) {
            if (const auto* col = Scheme.FindColumnByTag(tag)) {
                if (col->Group != 0 && !seen.Get(col->Group)) {
                    NPage::TGroupId groupId(col->Group);
                    Groups.push_back(groupId);
                    seen.Set(col->Group);
                }
            }
        }
    }

public:
    TResult Do(TCells key1, TCells key2, TRowId beginRowId, TRowId endRowId, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        endRowId++; // current interface accepts inclusive row2 bound
        Y_ABORT_UNLESS(beginRowId < endRowId);

        bool ready = true;
        bool chargeGroups = bool(Groups); // false value means that beginRowId, endRowId are invalid and shouldn't be used

        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];
        Y_ABORT_UNLESS(endRowId <= meta.RowCount);

        TRowId sliceEndRowId = endRowId;
        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            chargeGroups = false;
        }

        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            // tryHandleChild may update them, copy for simplicity
            // always load beginRowId regardless of keys
            const TRowId levelBeginRowId = beginRowId, levelEndRowId = Max(endRowId, beginRowId + 1);
            for (const auto &node : level) {
                if (node.EndRowId <= levelBeginRowId || node.BeginRowId >= levelEndRowId) {
                    continue;
                }

                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId < levelBeginRowId) {
                    from = node.Seek(levelBeginRowId);
                }
                if (node.EndRowId > levelEndRowId) {
                    to = node.Seek(levelEndRowId - 1) + 1;
                }
                for (TRecIdx pos : xrange(from, to)) {
                    auto child = node.GetChild(pos);
                    TRowId beginRowId = pos ? node.GetChild(pos - 1).RowCount : node.BeginRowId;
                    TRowId endRowId = child.RowCount;
                    ready &= tryHandleChild(TChildState(child, beginRowId, endRowId));
                }
            }
        };

        const auto tryHandleNode = [&](TChildState child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) { // update beginRowId, endRowId
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key1PageId = node.GetShortChild(pos).PageId;
                        if (pos) {
                            beginRowId = Max(beginRowId, node.GetShortChild(pos - 1).RowCount); // move beginRowId to the first key >= key1
                        }
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key2PageId = node.GetShortChild(pos).PageId;
                        endRowId = Min(endRowId, node.GetShortChild(pos).RowCount + 1); // move endRowId - 1 to the first key > key2
                        if (node.GetShortChild(pos).RowCount <= beginRowId) {
                            chargeGroups = false; // key2 is before current slice
                        }
                    }
                    return true;
                } else { // skip unloaded page rows
                    if (child.PageId == key1PageId) {
                        beginRowId = Max(beginRowId, child.EndRowId);
                    }
                    if (child.PageId == key2PageId) {
                        endRowId = Min(endRowId, child.BeginRowId);
                    }
                    return false;
                }
            } else {
                return TryLoadNode(child, nextLevel);
            }
        };

        const auto tryHandleDataPage = [&](TChildState child) -> bool {
            if (chargeGroups && (child.PageId == key1PageId || child.PageId == key2PageId)) {
                const auto page = TryGetDataPage(child.PageId, { });
                if (page) { // update beginRowId, endRowId
                    auto data = NPage::TDataPage(page);
                    if (child.PageId == key1PageId) {
                        TRowId key1RowId = data.BaseRow() + data.LookupKey(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults).Off();
                        beginRowId = Max(beginRowId, key1RowId);
                    }
                    if (child.PageId == key2PageId) {
                        TRowId key2RowId = data.BaseRow() + data.LookupKey(key2, Scheme.Groups[0], ESeek::Upper, &keyDefaults).Off();
                        endRowId = Min(endRowId, key2RowId);
                    }
                    return true;
                } else { // skip unloaded page rows
                    if (child.PageId == key1PageId) {
                        beginRowId = Max(beginRowId, child.EndRowId);
                    }
                    if (child.PageId == key2PageId) {
                        endRowId = Min(endRowId, child.BeginRowId);
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
            ready &= DoPrechargeGroups(chargeGroups, beginRowId, endRowId); // precharge groups using the latest row bounds
            return {ready, false};
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        bool overshot = endRowId == sliceEndRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(TChildState(meta, 0, meta.RowCount));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoPrechargeGroups(chargeGroups, beginRowId, endRowId); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId endRowId, TRowId beginRowId, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        endRowId++; // current interface accepts inclusive row1 bound
        Y_ABORT_UNLESS(beginRowId < endRowId);
        
        bool ready = true;
        bool chargeGroups = bool(Groups); // false value means that beginRowId, endRowId are invalid and shouldn't be used
        
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];
        Y_ABORT_UNLESS(endRowId <= meta.RowCount);

        TRowId sliceBeginRowId = beginRowId;
        if (Y_UNLIKELY(key1 && key2 && Compare(key2, key1, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            chargeGroups = false;
        }

        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            // tryHandleChild may update them, copy for simplicity
            // always load endRowId - 1 regardless of keys
            const TRowId levelBeginRowId = Min(beginRowId, endRowId - 1), levelEndRowId = endRowId;
            for (const auto &node : level) {
                if (node.EndRowId <= levelBeginRowId || node.BeginRowId >= levelEndRowId) {
                    continue;
                }

                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId < levelBeginRowId) {
                    from = node.Seek(levelBeginRowId);
                }
                if (node.EndRowId > levelEndRowId) {
                    to = node.Seek(levelEndRowId - 1) + 1;
                }
                for (TRecIdx pos : xrange(from, to)) {
                    auto child = node.GetChild(pos);
                    TRowId beginRowId = pos ? node.GetChild(pos - 1).RowCount : node.BeginRowId;
                    TRowId endRowId = child.RowCount;
                    ready &= tryHandleChild(TChildState(child, beginRowId, endRowId));
                }
            }
        };

        const auto tryHandleNode = [&](TChildState child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) { // update beginRowId, endRowId
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.SeekReverse(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key1PageId = node.GetShortChild(pos).PageId;
                        endRowId = Min(endRowId, node.GetShortChild(pos).RowCount); // move endRowId - 1 to the last key <= key1
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key2PageId = node.GetShortChild(pos).PageId;
                        if (pos) {
                            beginRowId = Max(beginRowId, node.GetShortChild(pos - 1).RowCount - 1); // move beginRowId to the last key < key2
                            if (node.GetShortChild(pos - 1).RowCount >= endRowId) {
                                chargeGroups = false; // key2 is after current slice
                            }
                        }                        
                    }
                    return true;
                } else { // skip unloaded page rows
                    if (child.PageId == key1PageId) {
                        endRowId = Min(endRowId, child.BeginRowId);
                    }
                    if (child.PageId == key2PageId) {
                        beginRowId = Max(beginRowId, child.EndRowId);
                    }
                    return false;
                }
            } else {
                return TryLoadNode(child, nextLevel);
            }
        };

        const auto tryHandleDataPage = [&](TChildState child) -> bool {
            if (chargeGroups && (child.PageId == key1PageId || child.PageId == key2PageId)) {
                const auto page = TryGetDataPage(child.PageId, { });
                if (page) { // update beginRowId, endRowId
                    auto data = NPage::TDataPage(page);
                    if (child.PageId == key1PageId) {
                        auto iter = data.LookupKeyReverse(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults);
                        if (iter) {
                            TRowId key1RowId = data.BaseRow() + iter.Off();
                            endRowId = Min(endRowId, key1RowId + 1);
                        } else {
                            endRowId = Min(endRowId, child.BeginRowId);
                        }
                    }
                    if (child.PageId == key2PageId) {
                        auto iter = data.LookupKeyReverse(key2, Scheme.Groups[0], ESeek::Upper, &keyDefaults);
                        if (iter) {
                            TRowId key2RowId = data.BaseRow() + iter.Off();
                            beginRowId = Max(beginRowId, key2RowId + 1);
                        } else {
                            beginRowId = Max(beginRowId, child.BeginRowId);
                        }
                    }
                    return true;
                } else { // skip unloaded page rows
                    if (child.PageId == key1PageId) {
                        endRowId = Min(endRowId, child.BeginRowId);
                    }
                    if (child.PageId == key2PageId) {
                        beginRowId = Max(beginRowId, child.EndRowId);
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
            ready &= DoPrechargeGroups(chargeGroups, beginRowId, endRowId); // precharge groups using the latest row bounds
            return {ready, false};
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        bool overshot = beginRowId == sliceBeginRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(TChildState(meta, 0, meta.RowCount));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoPrechargeGroups(chargeGroups, beginRowId, endRowId); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

private:
    bool DoPrechargeGroups(bool chargeGroups, TRowId beginRowId, TRowId endRowId) const noexcept {
        bool ready = true;
        
        if (chargeGroups && beginRowId < endRowId) {
            for (auto groupId : Groups) {
                ready &= DoPrechargeGroup(groupId, beginRowId, endRowId);
            }
        }

        return ready;
    }

    bool DoPrechargeGroup(TGroupId groupId, TRowId beginRowId, TRowId endRowId) const noexcept {
        bool ready = true;

        const auto& meta = groupId.IsHistoric() ? Part->IndexPages.BTreeHistoric[groupId.Index] : Part->IndexPages.BTreeGroups[groupId.Index];

        TVector<TNodeState> level, nextLevel(::Reserve(3));

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            for (const auto &node : level) {
                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId < beginRowId) {
                    from = node.Seek(beginRowId);
                }
                if (node.EndRowId > endRowId) {
                    to = node.Seek(endRowId - 1) + 1;
                }
                for (TRecIdx pos : xrange(from, to)) {
                    auto child = node.GetChild(pos);
                    TRowId beginRowId = pos ? node.GetChild(pos - 1).RowCount : node.BeginRowId;
                    TRowId endRowId = child.RowCount;
                    ready &= tryHandleChild(TChildState(child, beginRowId, endRowId));
                }
            }
        };

        const auto tryHandleNode = [&](TChildState child) -> bool {
            return TryLoadNode(child, nextLevel);
        };

        const auto tryHandleDataPage = [&](TChildState child) -> bool {
            return HasDataPage(child.PageId, groupId);
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
            return ready;
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(TChildState(meta, 0, meta.RowCount));
        } else {
            iterateLevel(tryHandleDataPage);
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

    bool TryLoadNode(TChildState& child, TVector<TNodeState>& level) const noexcept {
        auto page = Env->TryGetPage(Part, child.PageId);
        if (!page) {
            return false;
        }

        level.emplace_back(*page, child);
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
