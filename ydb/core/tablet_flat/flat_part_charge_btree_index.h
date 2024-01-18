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
    using TColumns = TBtreeIndexNode::TColumns;
    using TCellsIterable = TBtreeIndexNode::TCellsIterable;

    struct TNodeState {
        TChild Meta;
        TBtreeIndexNode Node;
        TRowId BeginRowId;
        TRowId EndRowId;

        TNodeState(TChild meta, TBtreeIndexNode node, TRowId beginRowId, TRowId endRowId)
            : Meta(meta)
            , Node(node)
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
        Y_UNUSED(env);
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
        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
        }

        TVector<TNodeState> level(Reserve(3)), nextLevel(Reserve(3));
        TPageId key1PageId = key1 ? meta.PageId : Max<TPageId>();
        TPageId key2PageId = key2 ? meta.PageId : Max<TPageId>();

        const auto iterateLevel = [&](std::function<bool(TNodeState& current, TRecIdx pos)> tryLoadNext) {
            for (ui32 i : xrange<ui32>(level.size())) {
                if (level[i].Meta.PageId == key1PageId) {
                    TRecIdx pos = level[i].Node.Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                    key1PageId = level[i].Node.GetShortChild(pos).PageId;
                    if (pos) {
                        // move row1 to the first key >= key1
                        row1 = Max(row1, level[i].Node.GetShortChild(pos - 1).RowCount);
                    }
                }
                if (level[i].Meta.PageId == key2PageId) {
                    TRecIdx pos = level[i].Node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                    key2PageId = level[i].Node.GetShortChild(pos).PageId;
                    // move row2 to the first key > key2
                    row2 = Min(row2, level[i].Node.GetShortChild(pos).RowCount);
                }
                
                if (level[i].EndRowId <= row1 || level[i].BeginRowId > row2) {
                    continue;
                }

                TRecIdx from = 0, to = level[i].Node.GetKeysCount();
                if (level[i].BeginRowId < row1) {
                    from = level[i].Node.Seek(row1);
                }
                if (level[i].EndRowId > row2) {
                    to = level[i].Node.Seek(row2);
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
            return HasDataPage(current.Node.GetShortChild(pos).PageId, { });
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

        // TODO: overshot for keys search
        return {ready, false};
    }

    TResult DoReverse(TCells key1, TCells key2, TRowId row1, TRowId row2, 
            const TKeyCellDefaults &keyDefaults, ui64 itemsLimit, ui64 bytesLimit) const noexcept override {
        bool ready = true;
        
        Y_UNUSED(key1);
        Y_UNUSED(key2);
        Y_UNUSED(keyDefaults);
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];

        if (Y_UNLIKELY(row1 >= meta.RowCount)) {
            row1 = meta.RowCount - 1; // start from the last row
        }
        if (Y_UNLIKELY(row1 < row2)) {
            row2 = row1; // will not go further than row1
        }

        // level contains nodes in reverse order
        TVector<TNodeState> level, nextLevel(Reserve(3));
        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= TryLoadRoot(meta, nextLevel);
            } else {
                for (ui32 i : xrange<ui32>(level.size())) {
                    TRecIdx from = level[i].Node.GetKeysCount(), to = 0;
                    if (level[i].EndRowId > row1) {
                        Y_DEBUG_ABORT_UNLESS(i == 0);
                        from = level[i].Node.Seek(row1);
                    }
                    if (level[i].BeginRowId < row2) {
                        Y_DEBUG_ABORT_UNLESS(i == level.size() - 1);
                        to = level[i].Node.Seek(row2);
                    }
                    for (TRecIdx j = from + 1; j > to; j--) {
                        ready &= TryLoadNode(level[i], j - 1, nextLevel);
                    }
                }
            }

            level.swap(nextLevel);
            nextLevel.clear();
        }

        if (!ready) {
            // some index pages are missing, do not continue
            return {false, false};
        }

        if (meta.LevelCount == 0) {
            ready &= HasDataPage(meta.PageId, { });
        } else {
            for (ui32 i : xrange<ui32>(level.size())) {
                TRecIdx from = level[i].Node.GetKeysCount(), to = 0;
                if (level[i].EndRowId > row1) {
                    Y_DEBUG_ABORT_UNLESS(i == 0);
                    from = level[i].Node.Seek(row1);
                }
                if (level[i].BeginRowId < row2) {
                    Y_DEBUG_ABORT_UNLESS(i == level.size() - 1);
                    to = level[i].Node.Seek(row2);
                }
                for (TRecIdx j = from + 1; j > to; j--) {
                    ready &= HasDataPage(level[i].Node.GetShortChild(j - 1).PageId, { });
                }
            }
        }

        // TODO: overshot for keys search
        return {ready, false};
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

        level.emplace_back(meta, TBtreeIndexNode(*page), 0, meta.RowCount);
        return true;
    }

    bool TryLoadNode(TNodeState& current, TRecIdx pos, TVector<TNodeState>& level) const noexcept {
        Y_ABORT_UNLESS(pos < current.Node.GetChildrenCount(), "Should point to some child");

        auto child = current.Node.GetChild(pos);

        auto page = Env->TryGetPage(Part, child.PageId);
        if (!page) {
            return false;
        }

        TRowId beginRowId = pos ? current.Node.GetChild(pos - 1).RowCount : current.BeginRowId;
        TRowId endRowId = child.RowCount;

        level.emplace_back(child, TBtreeIndexNode(*page), beginRowId, endRowId);
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
