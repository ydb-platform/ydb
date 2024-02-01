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

    struct TChildState {
        TPageId PageId;
        TRowId BeginRowId;
        TRowId EndRowId;

        TChildState(TPageId pageId, TRowId beginRowId, TRowId endRowId)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
        {
        }
    };

    struct TNodeState : TChildState, TBtreeIndexNode {
        TNodeState(TSharedData data, TPageId pageId, TRowId beginRowId, TRowId endRowId)
            : TChildState(pageId, beginRowId, endRowId)
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

        bool ready = true, overshot = true;
        bool chargeGroups = bool(Groups); // false value means that beginRowId, endRowId are invalid and shouldn't be used
        ui64 chargeGroupsItemsLimit = itemsLimit; // pessimistic items limit for groups

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

            const TChild* firstChild = nullptr;
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
                    auto child = node.GetChildRef(pos);
                    auto prevChild = pos ? node.GetChildRef(pos - 1) : nullptr;
                    TRowId beginRowId = prevChild ? prevChild->RowCount : node.BeginRowId;
                    TRowId endRowId = child->RowCount;
                    ready &= tryHandleChild(TChildState(child->PageId, beginRowId, endRowId));
                    if (itemsLimit || bytesLimit) {
                        if (!firstChild) {
                            // do not apply limit on the first child because beginRowId/key1 position is uncertain
                            firstChild = child;
                        } else {
                            if (itemsLimit) {
                                ui64 items = child->GetNonErasedRowCount() - firstChild->GetNonErasedRowCount();
                                if (LimitExceeded(items, itemsLimit)) {
                                    overshot = false;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.PageId == key1PageId) {
                if (chargeGroups && chargeGroupsItemsLimit) {
                    // TODO: use erased count
                    ui64 unloadedItems = child.EndRowId - child.BeginRowId;
                    if (unloadedItems < chargeGroupsItemsLimit) {
                        chargeGroupsItemsLimit -= unloadedItems;
                    } else {
                        chargeGroups = false;
                    }
                }
                beginRowId = Max(beginRowId, child.EndRowId);
            }
            if (child.PageId == key2PageId) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
        };

        const auto tryHandleNode = [&](TChildState child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) {
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
                        auto& key2Child = node.GetShortChild(pos);
                        key2PageId = key2Child.PageId;
                        endRowId = Min(endRowId, key2Child.RowCount + 1); // move endRowId - 1 to the first key > key2
                        if (key2Child.RowCount <= beginRowId) {
                            chargeGroups = false; // key2 is before current slice
                        }
                    }
                    return true;
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                return TryLoadNode(child, nextLevel);
            }
        };

        const auto tryHandleDataPage = [&](TChildState child) -> bool {
            if (chargeGroups && (child.PageId == key1PageId || child.PageId == key2PageId)) {
                const auto page = TryGetDataPage(child.PageId, { });
                if (page) {
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
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                return HasDataPage(child.PageId, { });
            }
        };

        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= tryHandleNode(TChildState(meta.PageId, 0, meta.RowCount));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (!ready) { // some index pages are missing, do not continue
            ready &= DoPrechargeGroups(chargeGroups, beginRowId, endRowId, chargeGroupsItemsLimit, bytesLimit); // precharge groups using the latest row bounds
            return {ready, false};
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        overshot &= endRowId == sliceEndRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(TChildState(meta.PageId, 0, meta.RowCount));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoPrechargeGroups(chargeGroups, beginRowId, endRowId, chargeGroupsItemsLimit, bytesLimit); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId endRowId, TRowId beginRowId, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        endRowId++; // current interface accepts inclusive row1 bound
        Y_ABORT_UNLESS(beginRowId < endRowId);
        
        bool ready = true, overshot = true;
        bool chargeGroups = bool(Groups); // false value means that beginRowId, endRowId are invalid and shouldn't be used
        ui64 chargeGroupsItemsLimit = itemsLimit; // pessimistic items limit for groups

        const auto& meta = Part->IndexPages.BTreeGroups[0];
        Y_ABORT_UNLESS(endRowId <= meta.RowCount);

        TRowId sliceBeginRowId = beginRowId;
        if (Y_UNLIKELY(key1 && key2 && Compare(key2, key1, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            chargeGroups = false;
        }

        // level's nodes is in reverse order
        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            // tryHandleChild may update them, copy for simplicity
            // always load endRowId - 1 regardless of keys
            const TRowId levelBeginRowId = Min(beginRowId, endRowId - 1), levelEndRowId = endRowId;
            
            const TChild* lastChild = nullptr;
            const TChild* prevLastChild = nullptr;
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
                for (TRecIdx posExt = to; posExt > from; posExt--) {
                    auto child = node.GetChildRef(posExt - 1);
                    auto prevChild = posExt - 1 ? node.GetChildRef(posExt - 2) : nullptr;
                    TRowId beginRowId = prevChild ? prevChild->RowCount : node.BeginRowId;
                    TRowId endRowId = child->RowCount;
                    if (itemsLimit || bytesLimit) {
                        if (!lastChild) {
                            // do not apply limit on the last child because endRowId/key1 position is uncertain
                            lastChild = child;
                        } else {
                            if (!prevLastChild) {
                                prevLastChild = child;
                            }
                            if (itemsLimit) {
                                ui64 items = prevLastChild->GetNonErasedRowCount() - child->GetNonErasedRowCount();
                                if (LimitExceeded(items, itemsLimit)) {
                                    overshot = false;
                                    return;
                                }
                            }
                        }
                    }
                    ready &= tryHandleChild(TChildState(child->PageId, beginRowId, endRowId));
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.PageId == key1PageId) {
                if (chargeGroups && chargeGroupsItemsLimit) {
                    // TODO: use erased count
                    ui64 unloadedItems = child.EndRowId - child.BeginRowId;
                    if (unloadedItems < chargeGroupsItemsLimit) {
                        chargeGroupsItemsLimit -= unloadedItems;
                    } else {
                        chargeGroups = false;
                    }
                }
                endRowId = Min(endRowId, child.BeginRowId);
            }
            if (child.PageId == key2PageId) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
        };

        const auto tryHandleNode = [&](TChildState child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.SeekReverse(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        auto& key1Child = node.GetShortChild(pos);
                        key1PageId = key1Child.PageId;
                        endRowId = Min(endRowId, key1Child.RowCount); // move endRowId - 1 to the last key <= key1
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key2PageId = node.GetShortChild(pos).PageId;
                        if (pos) {
                            auto& prevKey2Child = node.GetShortChild(pos - 1);
                            beginRowId = Max(beginRowId, prevKey2Child.RowCount - 1); // move beginRowId to the last key < key2
                            if (prevKey2Child.RowCount >= endRowId) {
                                chargeGroups = false; // key2 is after current slice
                            }
                        }                        
                    }
                    return true;
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                return TryLoadNode(child, nextLevel);
            }
        };

        const auto tryHandleDataPage = [&](TChildState child) -> bool {
            if (chargeGroups && (child.PageId == key1PageId || child.PageId == key2PageId)) {
                const auto page = TryGetDataPage(child.PageId, { });
                if (page) {
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
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                return HasDataPage(child.PageId, { });
            }
        };

        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= tryHandleNode(TChildState(meta.PageId, 0, meta.RowCount));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (!ready) { // some index pages are missing, do not continue
            ready &= DoPrechargeGroupsReverse(chargeGroups, beginRowId, endRowId, chargeGroupsItemsLimit, bytesLimit); // precharge groups using the latest row bounds
            return {ready, false};
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        overshot &= beginRowId == sliceBeginRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(TChildState(meta.PageId, 0, meta.RowCount));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoPrechargeGroupsReverse(chargeGroups, beginRowId, endRowId, chargeGroupsItemsLimit, bytesLimit); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

private:
    bool DoPrechargeGroups(bool chargeGroups, TRowId beginRowId, TRowId endRowId, ui64 itemsLimit, ui64 bytesLimit) const noexcept {
        bool ready = true;
        
        if (chargeGroups && beginRowId < endRowId) {
            if (itemsLimit && endRowId - beginRowId - 1 >= itemsLimit) {
                endRowId = beginRowId + itemsLimit + 1;
            }

            for (auto groupId : Groups) {
                ready &= DoPrechargeGroup(groupId, beginRowId, endRowId, bytesLimit);
            }
        }

        return ready;
    }

    bool DoPrechargeGroupsReverse(bool chargeGroups, TRowId beginRowId, TRowId endRowId, ui64 itemsLimit, ui64 bytesLimit) const noexcept {
        bool ready = true;
        
        if (chargeGroups && beginRowId < endRowId) {
            if (itemsLimit && endRowId - beginRowId - 1 >= itemsLimit) {
                beginRowId = endRowId - itemsLimit - 1;
            }

            for (auto groupId : Groups) {
                ready &= DoPrechargeGroup(groupId, beginRowId, endRowId, bytesLimit);
            }
        }

        return ready;
    }

private:
    bool DoPrechargeGroup(TGroupId groupId, TRowId beginRowId, TRowId endRowId, ui64 bytesLimit) const noexcept {
        bool ready = true;

        Y_UNUSED(bytesLimit);

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
                    auto child = node.GetShortChild(pos);
                    auto prevChild = pos ? node.GetShortChildRef(pos - 1) : nullptr;
                    TRowId beginRowId = prevChild ? prevChild->RowCount : node.BeginRowId;
                    TRowId endRowId = child.RowCount;
                    ready &= tryHandleChild(TChildState(child.PageId, beginRowId, endRowId));
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
                ready &= tryHandleNode(TChildState(meta.PageId, 0, meta.RowCount));
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
            ready &= tryHandleDataPage(TChildState(meta.PageId, 0, meta.RowCount));
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

    bool LimitExceeded(ui64 value, ui64 limit) const noexcept {
        return limit && value > limit;
    }

private:
    const TPart* const Part;
    const TPartScheme &Scheme;
    IPages* const Env;
    TSmallVec<TGroupId> Groups;
};

}
