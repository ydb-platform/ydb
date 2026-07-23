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
        TPageRef Ref;
        TRowId BeginRowId, EndRowId;
        TRowId PrevItems, Items;
        ui64 PrevBytes, Bytes;

        TChildState(TPageRef ref, TRowId beginRowId, TRowId endRowId, TRowId prevItems, TRowId items, ui64 prevDataSize, ui64 dataSize)
            : Ref(std::move(ref))
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , PrevItems(prevItems)
            , Items(items)
            , PrevBytes(prevDataSize)
            , Bytes(dataSize)
        {
        }

        static TChildState Invalid(TRowId beginRowId, TRowId endRowId, TRowId prevItems, TRowId items,
                                   ui64 prevDataSize, ui64 dataSize) {
            return TChildState(NPage::TPageLocation::Max(), beginRowId, endRowId, prevItems, items, prevDataSize,
                               dataSize);
        }

        bool IsValid() const noexcept {
            return Ref.IsValid();
        }
    };

    struct TNodeState : TChildState, TBtreeIndexNode {
        TNodeState(TSharedData data, TChildState child)
            : TChildState(std::move(child))
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
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const override {
        endRowId++; // current interface accepts inclusive row2 bound

        bool ready = true, overshot = true, hasValidRowsRange = Groups || IncludeHistory;
        const TRowId sliceBeginRowId = beginRowId, sliceEndRowId = endRowId;
        const auto& meta = Part->IndexPages.GetBTree({});
        Y_ENSURE(beginRowId < endRowId);
        Y_ENSURE(endRowId <= meta.GetRowCount());

        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            hasValidRowsRange = false;
        }

        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageRef key1Ref = key1 ? BuildRootChildState(meta).Ref : NPage::TPageLocation::Max();
        TPageRef key2Ref = key2 ? BuildRootChildState(meta).Ref : NPage::TPageLocation::Max();
        TChildState firstChild = BuildRootChildState(meta);

        const auto iterateLevel = [&](const auto& tryHandleChild, bool isLeafLevel, bool isDataBelow) {
            // tryHandleChild may update them, copy for simplicity
            const TRowId levelBeginRowId = beginRowId, levelEndRowId = endRowId;

            for (const auto &node : level) {
                if (node.EndRowId <= levelBeginRowId || node.BeginRowId >= levelEndRowId) {
                    continue;
                }
                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId <= levelBeginRowId) {
                    from = node.Seek(levelBeginRowId);
                    if (firstChild.IsValid()) { // still valid and should be updated
                        firstChild = BuildChildState(node, from, isLeafLevel);
                    }
                }
                if (node.EndRowId > levelEndRowId) {
                    to = node.Seek(levelEndRowId - 1) + 1;
                }

                for (TRecIdx pos : xrange(from, to)) {
                    auto childState = BuildChildState(node, pos, isLeafLevel);
                    if (LimitExceeded(firstChild.Items, childState.PrevItems, itemsLimit) || LimitExceeded(firstChild.Bytes, childState.PrevBytes, bytesLimit)) {
                        endRowId = Min(endRowId, childState.BeginRowId);
                        return;
                    }
                    ready &= tryHandleChild(childState, isDataBelow);
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.Ref == firstChild.Ref) {
                firstChild.Ref = NPage::TPageLocation::Max(); // mark first child unloaded
            }
            if (child.Ref == key1Ref) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
            if (child.Ref == key2Ref) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
        };

        const auto tryHandleNode = [&](const TChildState& child, bool isDataBelow) -> bool {
            if (child.Ref == firstChild.Ref || child.Ref == key1Ref || child.Ref == key2Ref) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.Ref == key1Ref) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key1Ref = node.GetChild( pos, isDataBelow);
                        if (pos) {
                            beginRowId = Max(beginRowId, node.GetChildRowCount(pos - 1)); // move beginRowId to the first key >= key1
                        }
                    }
                    if (child.Ref == key2Ref) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        TRowId key2ChildRowCount = node.GetChildRowCount(pos);
                        key2Ref = node.GetChild( pos, isDataBelow);
                        endRowId = Min(endRowId, key2ChildRowCount + 1); // move endRowId - 1 to the first key > key2
                        if (key2ChildRowCount <= sliceBeginRowId) {
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

        // NOTE: tryHandleDataPage() will be called for each data page, which falls
        //       within the requested range of keys/rows. It will count
        //       the size of each data page in the number of bytes precharged.
        ui64 prechargedBytes = 0;

        const auto tryHandleDataPage = [&](const TChildState& child, bool /*isDataBelow*/ = false) -> bool {
            prechargedBytes += child.Bytes - child.PrevBytes;

            if (hasValidRowsRange && (child.Ref == key1Ref || child.Ref == key2Ref)) {
                auto location = NTable::ResolvePageLocation(Part, child.Ref, {});
                const auto page = TryGetDataPage(location, { });
                if (page) {
                    auto data = NPage::TDataPage(page);
                    if (child.Ref == key1Ref) {
                        TRowId key1RowId = data.BaseRow() + data.LookupKey(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults).Off();
                        beginRowId = Max(beginRowId, key1RowId);
                    }
                    if (child.Ref == key2Ref) {
                        TRowId key2RowId = data.BaseRow() + data.LookupKey(key2, Scheme.Groups[0], ESeek::Upper, &keyDefaults).Off();
                        endRowId = Min(endRowId, key2RowId);
                    }
                    return true;
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                auto location = NTable::ResolvePageLocation(Part, child.Ref, {});
                return HasDataPage(location, { });
            }
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            const bool isDataBelow = (height + 1 == meta.LevelCount);
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta), isDataBelow);
            } else {
                iterateLevel(tryHandleNode, /*isLeafLevel=*/false, isDataBelow);
            }
            level.swap(nextLevel);
            nextLevel.clear();
            if (!firstChild.IsValid()) { // first child is unloaded, consider all first's child rows are needed for next levels
                firstChild.Items = firstChild.PrevItems;
                firstChild.Bytes = firstChild.PrevBytes;
            }
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        overshot &= endRowId == sliceEndRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta), 0);
        } else {
            iterateLevel(tryHandleDataPage, /*isLeafLevel=*/true, 0);
        }

        // NOTE: Column groups are precharged for the same set of rows, but the overall
        //       size limit (bytesLimit) is applied independently to each group
        //       (and the main column group). Applying the requested size limit
        //       across all groups might result in too few pages from the main group
        //       being placed into the cache. This is a trade-off between using more
        //       memory and allowing reads to be handled faster.
        //
        //       To avoid confusing the caller, return back only those bytes,
        //       which were precharged from the main column group and ignore
        //       all other column groups.
        ready &= DoGroupsAndHistory(hasValidRowsRange, beginRowId, endRowId, firstChild, itemsLimit, bytesLimit); // precharge groups using the latest row bounds

        return {
            .Ready = ready,
            .Overshot = overshot,
            .ItemsPrecharged = (endRowId >= beginRowId) ? endRowId - beginRowId : 0,
            .BytesPrecharged = prechargedBytes
        };
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId endRowId, TRowId beginRowId,
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const override {
        endRowId++; // current interface accepts inclusive row1 bound

        bool ready = true, overshot = true, hasValidRowsRange = Groups || IncludeHistory;
        const TRowId sliceBeginRowId = beginRowId, sliceEndRowId = endRowId;
        const auto& meta = Part->IndexPages.GetBTree({});
        Y_ENSURE(beginRowId < endRowId);
        Y_ENSURE(endRowId <= meta.GetRowCount());

        if (Y_UNLIKELY(key1 && key2 && Compare(key2, key1, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
            hasValidRowsRange = false;
        }

        // level's nodes is in reverse order
        TVector<TNodeState> level, nextLevel(::Reserve(3));
        TPageRef key1Ref = key1 ? BuildRootChildState(meta).Ref : NPage::TPageLocation::Max();
        TPageRef key2Ref = key2 ? BuildRootChildState(meta).Ref : NPage::TPageLocation::Max();
        TChildState lastChild = BuildRootChildState(meta);
        const auto iterateLevel = [&](const auto& tryHandleChild, bool isLeafLevel, bool isDataBelow) {
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
                    if (lastChild.IsValid()) { // still valid and should be updated
                        lastChild = BuildChildState(node, to, isLeafLevel);
                    }
                }

                for (TRecIdx posExt = to + 1; posExt > from; posExt--) {
                    auto childState = BuildChildState(node, posExt - 1, isLeafLevel);
                    if (LimitExceeded(childState.Items, lastChild.PrevItems, itemsLimit) || LimitExceeded(childState.Bytes, lastChild.PrevBytes, bytesLimit)) {
                        beginRowId = Max(beginRowId, childState.EndRowId);
                        return;
                    }
                    ready &= tryHandleChild(childState, isDataBelow);
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.Ref == lastChild.Ref) {
                lastChild.Ref = NPage::TPageLocation::Max(); // mark last child unloaded
            }
            if (child.Ref == key1Ref) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
            if (child.Ref == key2Ref) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
        };

        const auto tryHandleNode = [&](const TChildState& child, bool isDataBelow) -> bool {
            if (child.Ref == lastChild.Ref || child.Ref == key1Ref || child.Ref == key2Ref) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.Ref == key1Ref) {
                        TRecIdx pos = node.SeekReverse(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key1Ref = node.GetChild( pos, isDataBelow);
                        endRowId = Min(endRowId, node.GetChildRowCount(pos)); // move endRowId - 1 to the last key <= key1
                    }
                    if (child.Ref == key2Ref) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                        key2Ref = node.GetChild( pos, isDataBelow);
                        if (pos) {
                            auto prevKey2ChildRowCount = node.GetChildRowCount(pos - 1);
                            beginRowId = Max(beginRowId, prevKey2ChildRowCount - 1); // move beginRowId to the last key < key2
                            if (prevKey2ChildRowCount >= sliceEndRowId) {
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

        // NOTE: tryHandleDataPage() will be called for each data page, which falls
        //       within the requested range of keys/rows. It will count
        //       the size of each data page in the number of bytes precharged.
        ui64 prechargedBytes = 0;

        const auto tryHandleDataPage = [&](const TChildState& child, bool /*isDataBelow*/ = false) -> bool {
            prechargedBytes += child.Bytes - child.PrevBytes;

            if (hasValidRowsRange && (child.Ref == key1Ref || child.Ref == key2Ref)) {
                auto location = NTable::ResolvePageLocation(Part, child.Ref, {});
                const auto page = TryGetDataPage(location, { });
                if (page) {
                    auto data = NPage::TDataPage(page);
                    if (child.Ref == key1Ref) {
                        auto iter = data.LookupKeyReverse(key1, Scheme.Groups[0], ESeek::Lower, &keyDefaults);
                        if (iter) {
                            TRowId key1RowId = data.BaseRow() + iter.Off();
                            endRowId = Min(endRowId, key1RowId + 1);
                        } else {
                            endRowId = Min(endRowId, child.BeginRowId);
                        }
                    }
                    if (child.Ref == key2Ref) {
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
                auto location = NTable::ResolvePageLocation(Part, child.Ref, {});
                return HasDataPage(location, { });
            }
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            const bool isDataBelow = (height + 1 == meta.LevelCount);
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta), isDataBelow);
            } else {
                iterateLevel(tryHandleNode, /*isLeafLevel=*/false, isDataBelow);
            }
            level.swap(nextLevel);
            nextLevel.clear();
            if (!lastChild.IsValid()) { // last child is unloaded, consider all last's child rows are needed for next levels
                lastChild.PrevItems = lastChild.Items;
                lastChild.PrevBytes = lastChild.Bytes;
            }
        }

        // flat index doesn't treat key placement within data page, so let's do the same
        // TODO: remove it later
        overshot &= beginRowId == sliceBeginRowId;

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta), 0);
        } else {
            iterateLevel(tryHandleDataPage, /*isLeafLevel=*/true, 0);
        }

        // NOTE: Column groups are precharged for the same set of rows, but the overall
        //       size limit (bytesLimit) is applied independently to each group
        //       (and the main column group). Applying the requested size limit
        //       across all groups might result in too few pages from the main group
        //       being placed into the cache. This is a trade-off between using more
        //       memory and allowing reads to be handled faster.
        //
        //       To avoid confusing the caller, return back only those bytes,
        //       which were precharged from the main column group and ignore
        //       all other column groups.
        ready &= DoGroupsAndHistoryReverse(hasValidRowsRange, beginRowId, endRowId, lastChild, itemsLimit, bytesLimit); // precharge groups using the latest row bounds

        return {
            .Ready = ready,
            .Overshot = overshot,
            .ItemsPrecharged = (endRowId >= beginRowId) ? endRowId - beginRowId : 0,
            .BytesPrecharged = prechargedBytes
        };
    }

private:
    bool DoGroupsAndHistory(bool hasValidRowsRange, TRowId beginRowId, TRowId endRowId, const TChildState& firstChild, ui64 itemsLimit, ui64 bytesLimit) const {
        bool ready = true;

        if (!hasValidRowsRange) {
            return ready;
        }
        if (beginRowId >= endRowId) {
            return ready;
        }

        bool firstChildLoaded = firstChild.IsValid();

        if (itemsLimit) {
            // TODO: items limit should be applied on items not rows, but it requires iteration via first and last data pages
            TRowId limitFromRowId = !firstChildLoaded ? firstChild.BeginRowId : beginRowId;
            if (endRowId - limitFromRowId - 1 > itemsLimit) {
                endRowId = limitFromRowId + itemsLimit + 1;
            }
            if (beginRowId >= endRowId) {
                return ready;
            }
        }

        if (IncludeHistory && (!bytesLimit || firstChildLoaded)) {
            ready &= DoHistory(beginRowId, endRowId);
        }

        for (auto groupIndex : Groups) {
            ready &= DoGroup(TGroupId(groupIndex), beginRowId, endRowId, firstChild.BeginRowId, bytesLimit);
        }

        return ready;
    }

    bool DoGroupsAndHistoryReverse(bool hasValidRowsRange, TRowId beginRowId, TRowId endRowId, const TChildState& lastChild, ui64 itemsLimit, ui64 bytesLimit) const {
        bool ready = true;

        if (!hasValidRowsRange) {
            return ready;
        }
        if (beginRowId >= endRowId) {
            return ready;
        }

        bool lastChildLoaded = lastChild.IsValid();

        if (itemsLimit) {
            // TODO: items limit should be applied on items not rows, but it requires iteration via first and last data pages
            TRowId limitToRowId = !lastChildLoaded ? lastChild.EndRowId : endRowId;
            if (limitToRowId - beginRowId - 1 >= itemsLimit) {
                beginRowId = limitToRowId - itemsLimit - 1;
            }
            if (beginRowId >= endRowId) {
                return ready;
            }
        }

        if (IncludeHistory && (!bytesLimit || lastChildLoaded)) {
            ready &= DoHistory(beginRowId, endRowId);
        }

        for (auto groupIndex : Groups) {
            ready &= DoGroupReverse(TGroupId(groupIndex), beginRowId, endRowId, lastChild.EndRowId, bytesLimit);
        }

        return ready;
    }

private:
    bool DoGroup(TGroupId groupId, TRowId beginRowId, TRowId endRowId, TRowId firstChildBeginRowId, ui64 bytesLimit) const {
        bool ready = true;
        const auto& meta = Part->IndexPages.GetBTree(groupId);

        TVector<TNodeState> level, nextLevel(::Reserve(3));
        ui64 firstChildPrevBytes = bytesLimit ? GetPrevBytes(meta, groupId, firstChildBeginRowId) : 0;

        const auto iterateLevel = [&](const auto& tryHandleChild, bool isLeafLevel, bool isDataBelow) {
            for (const auto &node : level) {
                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId < beginRowId) {
                    from = node.Seek(beginRowId);
                }
                if (node.EndRowId > endRowId) {
                    to = node.Seek(endRowId - 1) + 1;
                }

                for (TRecIdx pos : xrange(from, to)) {
                    auto childState = BuildChildState(node, pos, isLeafLevel);
                    if (LimitExceeded(firstChildPrevBytes, childState.PrevBytes, bytesLimit)) {
                        return;
                    }
                    ready &= tryHandleChild(childState, isDataBelow);
                }
            }
        };

        const auto tryHandleNode = [&](const TChildState& child, bool /*isDataBelow*/) -> bool {
            return TryLoadNode(child, nextLevel);
        };

        const auto tryHandleDataPage = [&](const TChildState& child, bool /*isDataBelow*/ = false) -> bool {
            auto location = NTable::ResolvePageLocation(Part, child.Ref, groupId);
            return HasDataPage(location, groupId);
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            const bool isDataBelow = (height + 1 == meta.LevelCount);
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta), isDataBelow);
            } else {
                iterateLevel(tryHandleNode, /*isLeafLevel=*/false, isDataBelow);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta), 0);
        } else {
            iterateLevel(tryHandleDataPage, /*isLeafLevel=*/true, 0);
        }

        return ready;
    }

    bool DoGroupReverse(TGroupId groupId, TRowId beginRowId, TRowId endRowId, TRowId lastChildEndRowId, ui64 bytesLimit) const {
        bool ready = true;
        const auto& meta = Part->IndexPages.GetBTree(groupId);

        // level's nodes is in reverse order
        TVector<TNodeState> level, nextLevel(::Reserve(3));
        ui64 lastChildBytes = bytesLimit ? GetBytes(meta, groupId, lastChildEndRowId - 1) : 0;

        const auto iterateLevel = [&](const auto& tryHandleChild, bool isLeafLevel, bool isDataBelow) {
            for (const auto &node : level) {
                TRecIdx from = 0, to = node.GetKeysCount();
                if (node.BeginRowId < beginRowId) {
                    from = node.Seek(beginRowId);
                }
                if (node.EndRowId > endRowId) {
                    to = node.Seek(endRowId - 1);
                }
                for (TRecIdx posExt = to + 1; posExt > from; posExt--) {
                    auto childState = BuildChildState(node, posExt - 1, isLeafLevel);
                    if (LimitExceeded(childState.Bytes, lastChildBytes, bytesLimit)) {
                        return;
                    }
                    ready &= tryHandleChild(childState, isDataBelow);
                }
            }
        };

        const auto tryHandleNode = [&](const TChildState& child, bool /*isDataBelow*/) -> bool {
            return TryLoadNode(child, nextLevel);
        };

        const auto tryHandleDataPage = [&](const TChildState& child, bool /*isDataBelow*/ = false) -> bool {
            auto location = NTable::ResolvePageLocation(Part, child.Ref, groupId);
            return HasDataPage(location, groupId);
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            const bool isDataBelow = (height + 1 == meta.LevelCount);
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta), isDataBelow);
            } else {
                iterateLevel(tryHandleNode, /*isLeafLevel=*/false, isDataBelow);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta), 0);
        } else {
            iterateLevel(tryHandleDataPage, /*isLeafLevel=*/true, 0);
        }

        return ready;
    }

private:
    bool DoHistory(TRowId keyBeginRowId, TRowId keyEndRowId) const {
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
        TPageRef key1Ref = BuildRootChildState(meta).Ref;
        TPageRef key2Ref = BuildRootChildState(meta).Ref;

        const auto iterateLevel = [&](const auto& tryHandleChild, bool isLeafLevel, bool isDataBelow) {
            for (const auto &node : level) {
                TRecIdx from = 0, to = node.GetChildrenCount();
                if (node.BeginRowId < beginRowId) {
                    from = node.Seek(beginRowId);
                }
                if (node.EndRowId > endRowId) {
                    to = node.Seek(endRowId - 1) + 1;
                }
                for (TRecIdx pos : xrange(from, to)) {
                    auto childState = BuildChildState(node, pos, isLeafLevel);
                    ready &= tryHandleChild(childState, isDataBelow);
                }
            }
        };

        const auto skipUnloadedRows = [&](const TChildState& child) {
            if (child.Ref == key1Ref) {
                beginRowId = Max(beginRowId, child.EndRowId);
            }
            if (child.Ref == key2Ref) {
                endRowId = Min(endRowId, child.BeginRowId);
            }
        };

        const auto tryHandleNode = [&](const TChildState& child, bool isDataBelow) -> bool {
            if (child.Ref == key1Ref || child.Ref == key2Ref) {
                if (TryLoadNode(child, nextLevel)) {
                    const auto& node = nextLevel.back();
                    if (child.Ref == key1Ref) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key1, scheme.ColsKeyIdx, keyDefaults);
                        key1Ref = node.GetChild( pos, isDataBelow);
                        if (pos) {
                            beginRowId = Max(beginRowId, node.GetChildRowCount(pos - 1)); // move beginRowId to the first key >= key1
                        }
                    }
                    if (child.Ref == key2Ref) {
                        TRecIdx pos = node.Seek(ESeek::Lower, key2, scheme.ColsKeyIdx, keyDefaults);
                        TRowId key2ChildRowCount = node.GetChildRowCount(pos);
                        key2Ref = node.GetChild( pos, isDataBelow);
                        endRowId = Min(endRowId, key2ChildRowCount); // move endRowId to the first key > key2
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

        const auto tryHandleDataPage = [&](const TChildState& child, bool /*isDataBelow*/ = false) -> bool {
            if (Groups && (child.Ref == key1Ref || child.Ref == key2Ref)) {
                auto location = NTable::ResolvePageLocation(Part, child.Ref, groupId);
                const auto page = TryGetDataPage(location, groupId);
                if (page) {
                    auto data = NPage::TDataPage(page);
                    if (child.Ref == key1Ref) {
                        TRowId key1RowId = data.BaseRow() + data.LookupKey(key1, scheme, ESeek::Lower, keyDefaults).Off();
                        beginRowId = Max(beginRowId, key1RowId);
                    }
                    if (child.Ref == key2Ref) {
                        TRowId key2RowId = data.BaseRow() + data.LookupKey(key2, scheme, ESeek::Upper, keyDefaults).Off();
                        endRowId = Min(endRowId, key2RowId);
                    }
                    return true;
                } else {
                    skipUnloadedRows(child);
                    return false;
                }
            } else {
                auto location = NTable::ResolvePageLocation(Part, child.Ref, groupId);
                return HasDataPage(location, groupId);
            }
        };

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            const bool isDataBelow = (height + 1 == meta.LevelCount);
            if (height == 0) {
                ready &= tryHandleNode(BuildRootChildState(meta), isDataBelow);
            } else {
                iterateLevel(tryHandleNode, /*isLeafLevel=*/false, isDataBelow);
            }
            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (meta.LevelCount == 0) {
            ready &= tryHandleDataPage(BuildRootChildState(meta), false);
        } else {
            iterateLevel(tryHandleDataPage, /*isLeafLevel=*/true, false);
        }

        ready &= DoHistoricGroups(beginRowId, endRowId); // precharge historic groups using the latest row bounds

        return ready;
    }

    bool DoHistoricGroups(TRowId beginRowId, TRowId endRowId) const {
        bool ready = true;

        if (beginRowId < endRowId) {
            for (auto groupIndex : Groups) {
                ready &= DoGroup(TGroupId(groupIndex, true), beginRowId, endRowId, 0, 0);
            }
        }

        return ready;
    }

private:
    ui64 GetPrevBytes(const TBtreeIndexMeta& meta, TGroupId groupId, TRowId rowId) const {
        auto* indexPages = Part->GetPageCollection(0);
        auto* groupPages = Part->GetPageCollection(groupId.Index);
        auto location = GetBTreeRootLocation(meta, indexPages, groupPages);
        ui64 result = 0;

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            bool isDataPage = (height + 1 == meta.LevelCount);
            auto page = Env->TryGetPage(Part, location, {});
            if (!page) {
                return result;
            }
            auto node = TBtreeIndexNode(*page);
            auto pos = node.Seek(rowId);
            location = ResolvePageLocation(Part, node.GetChild( pos, isDataPage), isDataPage ? groupId : TGroupId{});
            if (pos) {
                result = node.GetChildDataSize(pos - 1);
            }
        }

        return result;
    }

    ui64 GetBytes(TBtreeIndexMeta meta, TGroupId groupId, TRowId rowId) const {
        auto* indexPages = Part->GetPageCollection(0);
        auto* groupPages = Part->GetPageCollection(groupId.Index);
        auto location = GetBTreeRootLocation(meta, indexPages, groupPages);
        ui64 result = meta.GetDataSize();

        for (ui32 height = 0; height < meta.LevelCount; height++) {
            bool isDataPage = (height + 1 == meta.LevelCount);
            auto page = Env->TryGetPage(Part, location, {});
            if (!page) {
                return result;
            }
            auto node = TBtreeIndexNode(*page);
            auto pos = node.Seek(rowId);
            location = ResolvePageLocation(Part, node.GetChild( pos, isDataPage), isDataPage ? groupId : TGroupId{});
            result = node.GetChildDataSize(pos);
        }

        return result;
    }

private:
    const TSharedData* TryGetDataPage(const NPage::TPageLocation& location, TGroupId groupId) const {
        return Env->TryGetPage(Part, location, groupId);
    }

    bool HasDataPage(const NPage::TPageLocation& location, TGroupId groupId) const {
        return bool(Env->TryGetPage(Part, location, groupId));
    }

    bool TryLoadNode(const TChildState& child, TVector<TNodeState>& level) const {
        auto location = NTable::ResolvePageLocation(Part, child.Ref, {});
        auto page = Env->TryGetPage(Part, location, {});
        if (!page) {
            return false;
        }

        level.emplace_back(*page, child);
        return true;
    }

    int Compare(TCells left, TCells right, const TKeyCellDefaults &keyDefaults) const
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

    TChildState BuildRootChildState(const TBtreeIndexMeta& meta) const {
        if (meta.HasRootV2()) {
            return TChildState(meta.RootV2,
                0, meta.GetRowCount(),
                0, meta.GetNonErasedRowCount(),
                0, meta.GetDataSize());
        }
        return TChildState(meta.RootV1PageId(),
            0, meta.GetRowCount(),
            0, meta.GetNonErasedRowCount(),
            0, meta.GetDataSize());
    }

    // Version-aware child state builder
    TChildState BuildChildState(const TNodeState& parent, TRecIdx curPos, bool isDataPage) const {
        auto prevPos = curPos - 1;
        bool hasPrev = prevPos != Max<TRecIdx>();
        TRowId beginRowId = hasPrev ? parent.GetChildRowCount(prevPos) : parent.BeginRowId;
        TRowId endRowId = parent.GetChildRowCount(curPos);
        TRowId prevItems = hasPrev ? parent.GetChildNonErasedRowCount(prevPos) : parent.PrevItems;
        TRowId items = parent.GetChildNonErasedRowCount(curPos);
        ui64 prevBytes = hasPrev ? parent.GetChildDataSize(prevPos) : parent.PrevBytes;
        ui64 bytes = parent.GetChildDataSize(curPos);
        return TChildState(parent.GetChild(curPos, isDataPage), beginRowId, endRowId, prevItems, items, prevBytes, bytes);
    }

    bool LimitExceeded(ui64 prev, ui64 current, ui64 limit) const {
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
