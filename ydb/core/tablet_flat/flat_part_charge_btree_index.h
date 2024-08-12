#pragma once

#include "flat_table_part.h"
#include "flat_part_iface.h"
#include "flat_part_charge_iface.h"
#include "flat_page_data.h"

namespace NKikimr::NTable {

class TChargeBTreeIndex : public ICharge {
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;
    using TRecIdx = NPage::TRecIdx;
    using TGroupId = NPage::TGroupId;
    using TChild = TBtreeIndexNode::TChild;
    using TShortChild = TBtreeIndexNode::TShortChild;

    struct TChildState {
        TPageId PageId;
        TRowId BeginRowId, EndRowId;
        TRowId PrevItems, Items;
        ui64 PrevBytes, Bytes;

        TChildState(TPageId pageId, TRowId beginRowId, TRowId endRowId, TRowId prevItems, TRowId items, ui64 prevDataSize, ui64 dataSize)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , PrevItems(prevItems)
            , Items(items)
            , PrevBytes(prevDataSize)
            , Bytes(dataSize)
        {
        }
    };

    struct TNodeState : TChildState, TBtreeIndexNode {
        TNodeState(TSharedData data, TPageId pageId, TRowId beginRowId, TRowId endRowId, TRowId prevItems, TRowId items, ui64 prevDataSize, ui64 dataSize)
            : TChildState(pageId, beginRowId, endRowId, prevItems, items, prevDataSize, dataSize)
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
        , IncludeHistory(includeHistory &&  Part->HistoricGroupsCount)
    {    
        TDynBitMap seen;
        for (TTag tag : tags) {
            if (const auto* col = Scheme.FindColumnByTag(tag)) {
                if (col->Group != 0 && !seen.Get(col->Group)) {
                    Groups.push_back(col->Group);
                    seen.Set(col->Group);
                }
            }
        }
    }

public:
    TResult Do(TCells key1, TCells key2, TRowId beginRowId, TRowId endRowId, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        endRowId++; // current interface accepts inclusive row2 bound

        bool ready = true, overshot = true, hasValidRowsRange = Groups || IncludeHistory;
        const TRowId sliceBeginRowId = beginRowId, sliceEndRowId = endRowId;
        const auto& meta = Part->IndexPages.GetBTree({});
        Y_ABORT_UNLESS(beginRowId < endRowId);
        Y_ABORT_UNLESS(endRowId <= meta.GetRowCount());

        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            hasValidRowsRange = false;
        }

        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageId key1PageId = key1 ? meta.GetPageId() : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.GetPageId() : Max<TPageId>();
        TChildState firstChild = BuildRootChildState(meta);

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            // tryHandleChild may update them, copy for simplicity
            const TRowId levelBeginRowId = beginRowId, levelEndRowId = endRowId;
            
            for (const auto &node : level) {
                if (node.EndRowId <= levelBeginRowId || node.BeginRowId >= levelEndRowId) {
                    continue;
                }
                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId <= levelBeginRowId) {
                    from = node.Seek(levelBeginRowId);
                    if (firstChild.PageId != Max<TPageId>()) { // still valid and should be updated
                        auto& child = node.GetChild(from);
                        auto prevChild = from ? node.GetChildRef(from - 1) : nullptr;
                        firstChild = BuildChildState(node, child, prevChild);
                    }
                }
                if (node.EndRowId > levelEndRowId) {
                    to = node.Seek(levelEndRowId - 1) + 1;
                }
                
                for (TRecIdx pos : xrange(from, to)) {
                    auto child = node.GetChild(pos);
                    auto prevChild = pos ? node.GetChildRef(pos - 1) : nullptr;
                    auto childState = BuildChildState(node, child, prevChild);
                    if (LimitExceeded(firstChild.Items, childState.PrevItems, itemsLimit) || LimitExceeded(firstChild.Bytes, childState.PrevBytes, bytesLimit)) {
                        endRowId = Min(endRowId, childState.BeginRowId);
                        return;
                    }
                    ready &= tryHandleChild(childState);
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.PageId == firstChild.PageId) {
                firstChild.PageId = Max<TPageId>(); // mark first child unloaded
            }
            if (child.PageId == key1PageId) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
            if (child.PageId == key2PageId) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
        };

        const auto tryHandleNode = [&](const TChildState& child) -> bool {
            if (child.PageId == firstChild.PageId || child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        auto& key1Child = node.GetChild(pos);
                        key1PageId = key1Child.GetPageId();
                        if (pos) {
                            beginRowId = Max(beginRowId, node.GetChild(pos - 1).GetRowCount()); // move beginRowId to the first key >= key1
                        }
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        auto& key2Child = node.GetChild(pos);
                        key2PageId = key2Child.GetPageId();
                        endRowId = Min(endRowId, key2Child.GetRowCount() + 1); // move endRowId - 1 to the first key > key2
                        if (key2Child.GetRowCount() <= sliceBeginRowId) {
                            hasValidRowsRange = false; // key2 is before current slice
                            endRowId = Max(endRowId, sliceBeginRowId + 1); // always load sliceBeginRowId regardless of key2
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

        const auto tryHandleDataPage = [&](const TChildState& child) -> bool {
            if (hasValidRowsRange && (child.PageId == key1PageId || child.PageId == key2PageId)) {
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

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
            if (firstChild.PageId == Max<TPageId>()) { // first child is unloaded, consider all first's child rows are needed for next levels
                firstChild.Items = firstChild.PrevItems;
                firstChild.Bytes = firstChild.PrevBytes;
            }
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        overshot &= endRowId == sliceEndRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoGroupsAndHistory(hasValidRowsRange, beginRowId, endRowId, firstChild, itemsLimit, bytesLimit); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId endRowId, TRowId beginRowId, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        endRowId++; // current interface accepts inclusive row1 bound
        
        bool ready = true, overshot = true, hasValidRowsRange = Groups || IncludeHistory;
        const TRowId sliceBeginRowId = beginRowId, sliceEndRowId = endRowId;
        const auto& meta = Part->IndexPages.GetBTree({});
        Y_ABORT_UNLESS(beginRowId < endRowId);
        Y_ABORT_UNLESS(endRowId <= meta.GetRowCount());

        if (Y_UNLIKELY(key1 && key2 && Compare(key2, key1, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            hasValidRowsRange = false;
        }

        // level's nodes is in reverse order
        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageId key1PageId = key1 ? meta.GetPageId() : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.GetPageId() : Max<TPageId>();
        TChildState lastChild = BuildRootChildState(meta);

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            // tryHandleChild may update them, copy for simplicity
            const TRowId levelBeginRowId = beginRowId, levelEndRowId = endRowId;

            for (const auto &node : level) {
                if (node.EndRowId <= levelBeginRowId || node.BeginRowId >= levelEndRowId) {
                    continue;
                }
                TRecIdx from = 0, to = node.GetKeysCount();
                if (node.BeginRowId < levelBeginRowId) {
                    from = node.Seek(levelBeginRowId);
                }
                if (node.EndRowId >= levelEndRowId) {
                    to = node.Seek(levelEndRowId - 1);
                    if (lastChild.PageId != Max<TPageId>()) { // still valid and should be updated
                        auto& child = node.GetChild(to);
                        auto prevChild = to ? node.GetChildRef(to - 1) : nullptr;
                        lastChild = BuildChildState(node, child, prevChild);
                    }
                }

                for (TRecIdx posExt = to + 1; posExt > from; posExt--) {
                    auto& child = node.GetChild(posExt - 1);
                    auto prevChild = posExt - 1 ? node.GetChildRef(posExt - 2) : nullptr;
                    auto childState = BuildChildState(node, child, prevChild);
                    if (LimitExceeded(childState.Items, lastChild.PrevItems, itemsLimit) || LimitExceeded(childState.Bytes, lastChild.PrevBytes, bytesLimit)) {
                        beginRowId = Max(beginRowId, childState.EndRowId);
                        return;
                    }
                    ready &= tryHandleChild(childState);
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.PageId == lastChild.PageId) {
                lastChild.PageId = Max<TPageId>(); // mark last child unloaded
            }
            if (child.PageId == key1PageId) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
            if (child.PageId == key2PageId) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
        };

        const auto tryHandleNode = [&](const TChildState& child) -> bool {
            if (child.PageId == lastChild.PageId || child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.SeekReverse(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        auto& key1Child = node.GetChild(pos);
                        key1PageId = key1Child.GetPageId();
                        endRowId = Min(endRowId, key1Child.GetRowCount()); // move endRowId - 1 to the last key <= key1
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key2PageId = node.GetChild(pos).GetPageId();
                        if (pos) {
                            auto& prevKey2Child = node.GetChild(pos - 1);
                            beginRowId = Max(beginRowId, prevKey2Child.GetRowCount() - 1); // move beginRowId to the last key < key2
                            if (prevKey2Child.GetRowCount() >= sliceEndRowId) {
                                hasValidRowsRange = false; // key2 is after current slice
                                beginRowId = Min(beginRowId, sliceEndRowId - 1); // always load endRowId - 1 regardless of keys 
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

        const auto tryHandleDataPage = [&](const TChildState& child) -> bool {
            if (hasValidRowsRange && (child.PageId == key1PageId || child.PageId == key2PageId)) {
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

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
            if (lastChild.PageId == Max<TPageId>()) { // last child is unloaded, consider all last's child rows are needed for next levels
                lastChild.PrevItems = lastChild.Items;
                lastChild.PrevBytes = lastChild.Bytes;
            }
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        overshot &= beginRowId == sliceBeginRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoGroupsAndHistoryReverse(hasValidRowsRange, beginRowId, endRowId, lastChild, itemsLimit, bytesLimit); // precharge groups using the latest row bounds

        return {ready, overshot};
    }

private:
    bool DoGroupsAndHistory(bool hasValidRowsRange, TRowId beginRowId, TRowId endRowId, const TChildState& firstChild, ui64 itemsLimit, ui64 bytesLimit) const noexcept {
        bool ready = true;
        
        if (!hasValidRowsRange) {
            return ready;
        }
        if (beginRowId >= endRowId) {
            return ready;
        }

        if (itemsLimit) {
            // TODO: items limit should be applied on items not rows, but it requires iteration via first and last data pages
            TRowId limitFromRowId = firstChild.PageId == Max<TPageId>() ? firstChild.BeginRowId : beginRowId;
            if (endRowId - limitFromRowId - 1 > itemsLimit) {
                endRowId = limitFromRowId + itemsLimit + 1;
            }
            if (beginRowId >= endRowId) {
                return ready;
            }
        }

        if (IncludeHistory && (!bytesLimit || firstChild.PageId != Max<TPageId>())) {
            ready &= DoHistory(beginRowId, endRowId);
        }

        for (auto groupIndex : Groups) {
            ready &= DoGroup(TGroupId(groupIndex), beginRowId, endRowId, firstChild.BeginRowId, bytesLimit);
        }

        return ready;
    }

    bool DoGroupsAndHistoryReverse(bool hasValidRowsRange, TRowId beginRowId, TRowId endRowId, const TChildState& lastChild, ui64 itemsLimit, ui64 bytesLimit) const noexcept {
        bool ready = true;
        
        if (!hasValidRowsRange) {
            return ready;
        }
        if (beginRowId >= endRowId) {
            return ready;
        }

        if (itemsLimit) {
            // TODO: items limit should be applied on items not rows, but it requires iteration via first and last data pages
            TRowId limitToRowId = lastChild.PageId == Max<TPageId>() ? lastChild.EndRowId : endRowId;
            if (limitToRowId - beginRowId - 1 >= itemsLimit) {
                beginRowId = limitToRowId - itemsLimit - 1;
            }
            if (beginRowId >= endRowId) {
                return ready;
            }
        }

        if (IncludeHistory && (!bytesLimit || lastChild.PageId != Max<TPageId>())) {
            ready &= DoHistory(beginRowId, endRowId);
        }
            
        for (auto groupIndex : Groups) {
            ready &= DoGroupReverse(TGroupId(groupIndex), beginRowId, endRowId, lastChild.EndRowId, bytesLimit);
        }

        return ready;
    }

private:
    bool DoGroup(TGroupId groupId, TRowId beginRowId, TRowId endRowId, TRowId firstChildBeginRowId, ui64 bytesLimit) const noexcept {
        bool ready = true;
        const auto& meta = Part->IndexPages.GetBTree(groupId);

        TVector<TNodeState> level, nextLevel(::Reserve(3));
        ui64 firstChildPrevBytes = bytesLimit ? GetPrevBytes(meta, firstChildBeginRowId) : 0;

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
                    auto& child = node.GetShortChild(pos);
                    auto prevChild = pos ? node.GetShortChildRef(pos - 1) : nullptr;
                    auto childState = BuildChildState(node, child, prevChild);
                    if (LimitExceeded(firstChildPrevBytes, childState.PrevBytes, bytesLimit)) {
                        return;
                    }
                    ready &= tryHandleChild(childState);
                }
            }
        };

        const auto tryHandleNode = [&](const TChildState& child) -> bool {
            return TryLoadNode(child, nextLevel);
        };

        const auto tryHandleDataPage = [&](const TChildState& child) -> bool {
            return HasDataPage(child.PageId, groupId);
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        return ready;
    }

    bool DoGroupReverse(TGroupId groupId, TRowId beginRowId, TRowId endRowId, TRowId lastChildEndRowId, ui64 bytesLimit) const noexcept {
        bool ready = true;
        const auto& meta = Part->IndexPages.GetBTree(groupId);

        // level's nodes is in reverse order
        TVector<TNodeState> level, nextLevel(::Reserve(3));
        ui64 lastChildBytes = bytesLimit ? GetBytes(meta, lastChildEndRowId - 1) : 0;

        const auto iterateLevel = [&](const auto& tryHandleChild) {
            for (const auto &node : level) {
                TRecIdx from = 0, to = node.GetKeysCount();
                if (node.BeginRowId < beginRowId) {
                    from = node.Seek(beginRowId);
                }
                if (node.EndRowId > endRowId) {
                    to = node.Seek(endRowId - 1);
                }
                for (TRecIdx posExt = to + 1; posExt > from; posExt--) {
                    auto& child = node.GetShortChild(posExt - 1);
                    auto prevChild = posExt - 1 ? node.GetShortChildRef(posExt - 2) : nullptr;
                    auto childState = BuildChildState(node, child, prevChild);
                    if (LimitExceeded(childState.Bytes, lastChildBytes, bytesLimit)) {
                        return;
                    }
                    ready &= tryHandleChild(childState);
                }
            }
        };

        const auto tryHandleNode = [&](const TChildState& child) -> bool {
            return TryLoadNode(child, nextLevel);
        };

        const auto tryHandleDataPage = [&](const TChildState& child) -> bool {
            return HasDataPage(child.PageId, groupId);
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        return ready;
    }

private:
    bool DoHistory(TRowId keyBeginRowId, TRowId keyEndRowId) const noexcept {
        bool ready = true;

        // Minimum key is (startRowId, max, max)
        ui64 startStep = Max<ui64>();
        ui64 startTxId = Max<ui64>();
        TCell key1Cells[3] = {
            TCell::Make(keyBeginRowId),
            TCell::Make(startStep),
            TCell::Make(startTxId),
        };
        TCells key1{ key1Cells, 3 };

        // Maximum key is (endRowId, 0, 0)
        ui64 endStep = 0;
        ui64 endTxId = 0;
        TCell key2Cells[3] = {
            TCell::Make(keyEndRowId - 1),
            TCell::Make(endStep),
            TCell::Make(endTxId),
        };
        TCells key2{ key2Cells, 3 };

        // Directly use the history group scheme and key defaults with correct sort order
        const auto& scheme = Part->Scheme->HistoryGroup;
        Y_DEBUG_ABORT_UNLESS(scheme.ColsKeyIdx.size() == 3);
        const TKeyCellDefaults* keyDefaults = Part->Scheme->HistoryKeys.Get();

        const TGroupId groupId(0, true);
        const auto& meta = Part->IndexPages.GetBTree(TGroupId{0, true});
        TRowId beginRowId = 0, endRowId = meta.GetRowCount();
        
        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageId key1PageId = meta.GetPageId(), key2PageId = meta.GetPageId();

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
                    auto& child = node.GetShortChild(pos);
                    auto prevChild = pos ? node.GetShortChildRef(pos - 1) : nullptr;
                    auto childState = BuildChildState(node, child, prevChild);
                    ready &= tryHandleChild(childState);
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.PageId == key1PageId) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
            if (child.PageId == key2PageId) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
        };

        const auto tryHandleNode = [&](const TChildState& child) -> bool {
            if (child.PageId == key1PageId || child.PageId == key2PageId) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.PageId == key1PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key1, scheme.ColsKeyIdx, keyDefaults);
                        auto& key1Child = node.GetShortChild(pos);
                        key1PageId = key1Child.GetPageId();
                        if (pos) {
                            beginRowId = Max(beginRowId, node.GetShortChild(pos - 1).GetRowCount()); // move beginRowId to the first key >= key1
                        }
                    }
                    if (child.PageId == key2PageId) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, scheme.ColsKeyIdx, keyDefaults);
                        auto& key2Child = node.GetShortChild(pos);
                        key2PageId = key2Child.GetPageId();
                        endRowId = Min(endRowId, key2Child.GetRowCount()); // move endRowId to the first key > key2
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

        const auto tryHandleDataPage = [&](const TChildState& child) -> bool {
            if (Groups && (child.PageId == key1PageId || child.PageId == key2PageId)) {
                const auto page = TryGetDataPage(child.PageId, groupId);
                if (page) {
                    auto data = NPage::TDataPage(page);
                    if (child.PageId == key1PageId) {
                        TRowId key1RowId = data.BaseRow() + data.LookupKey(key1, scheme, ESeek::Lower, keyDefaults).Off();
                        beginRowId = Max(beginRowId, key1RowId);
                    }
                    if (child.PageId == key2PageId) {
                        TRowId key2RowId = data.BaseRow() + data.LookupKey(key2, scheme, ESeek::Upper, keyDefaults).Off();
                        endRowId = Min(endRowId, key2RowId);
                    }
                    return true;
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                return HasDataPage(child.PageId, groupId);
            }
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta));
            } else {
                iterateLevel(tryHandleNode);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta));
        } else {
            iterateLevel(tryHandleDataPage);
        }

        ready &= DoHistoricGroups(beginRowId, endRowId); // precharge historic groups using the latest row bounds

        return ready;
    }

    bool DoHistoricGroups(TRowId beginRowId, TRowId endRowId) const noexcept {
        bool ready = true;
        
        if (beginRowId < endRowId) {
            for (auto groupIndex : Groups) {
                ready &= DoGroup(TGroupId(groupIndex, true), beginRowId, endRowId, 0, 0);
            }
        }

        return ready;
    }

private:
    ui64 GetPrevBytes(const TBtreeIndexMeta& meta, TRowId rowId) const {
        TPageId pageId = meta.GetPageId();
        ui64 result = 0;

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            auto page = Env->TryGetPage(Part, pageId, {});
            if (!page) {
                return result;
            }
            auto node = TBtreeIndexNode(*page);
            auto pos = node.Seek(rowId);
            pageId = node.GetShortChild(pos).GetPageId();
            if (pos) {
                result = node.GetShortChild(pos - 1).GetDataSize();
            }
        }

        return result;
    }

    ui64 GetBytes(TBtreeIndexMeta meta, TRowId rowId) const {
        TPageId pageId = meta.GetPageId();
        ui64 result = meta.GetDataSize();

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            auto page = Env->TryGetPage(Part, pageId, {});
            if (!page) {
                return result;
            }
            auto node = TBtreeIndexNode(*page);
            auto pos = node.Seek(rowId);
            pageId = node.GetShortChild(pos).GetPageId();
            result = node.GetShortChild(pos).GetDataSize();
        }

        return result;
    }

private:
    const TSharedData* TryGetDataPage(TPageId pageId, TGroupId groupId) const noexcept {
        return Env->TryGetPage(Part, pageId, groupId);
    };

    bool HasDataPage(TPageId pageId, TGroupId groupId) const noexcept {
        return bool(Env->TryGetPage(Part, pageId, groupId));
    }

    bool TryLoadNode(const TChildState& child, TVector<TNodeState>& level) const noexcept {
        auto page = Env->TryGetPage(Part, child.PageId, {});
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

    TChildState BuildRootChildState(const TBtreeIndexMeta& meta) const noexcept {
        return TChildState(meta.GetPageId(),
            0, meta.GetRowCount(),
            0, meta.GetNonErasedRowCount(),
            0, meta.GetDataSize());
    }

    TChildState BuildChildState(const TNodeState& parent, TChild child, const TChild* prevChild) const noexcept {
        return TChildState(child.GetPageId(),
            prevChild ? prevChild->GetRowCount() : parent.BeginRowId, child.GetRowCount(),
            prevChild ? prevChild->GetNonErasedRowCount() : parent.PrevItems, child.GetNonErasedRowCount(),
            prevChild ? prevChild->GetDataSize() : parent.PrevBytes, child.GetDataSize());
    }

    TChildState BuildChildState(const TNodeState& parent, TShortChild child, const TShortChild* prevChild) const noexcept {
        return TChildState(child.GetPageId(),
            prevChild ? prevChild->GetRowCount() : parent.BeginRowId, child.GetRowCount(),
            prevChild ? prevChild->GetRowCount() : parent.BeginRowId, child.GetRowCount(),
            prevChild ? prevChild->GetDataSize() : parent.PrevBytes, child.GetDataSize());
    }

    bool LimitExceeded(ui64 prev, ui64 current, ui64 limit) const noexcept {
        return limit && current > prev && current - prev > limit;
    }

private:
    const TPart* const Part;
    const TPartScheme &Scheme;
    IPages* const Env;
    TSmallVec<ui32> Groups;
    bool IncludeHistory;
};

}
