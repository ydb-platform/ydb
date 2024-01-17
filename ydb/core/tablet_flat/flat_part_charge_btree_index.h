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
        TCellsIterable BeginKey;
        TCellsIterable EndKey;

        TNodeState(TChild meta, TBtreeIndexNode node, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, TCellsIterable endKey)
            : Meta(meta)
            , Node(node)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , EndKey(endKey)
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

        Y_UNUSED(key1);
        Y_UNUSED(key2);
        Y_UNUSED(keyDefaults);
        Y_UNUSED(itemsLimit);
        Y_UNUSED(bytesLimit);

        const auto& meta = Part->IndexPages.BTreeGroups[0];

        if (Y_UNLIKELY(row1 >= meta.RowCount)) {
            return { true, true }; // already out of bounds, nothing to precharge
        }
        if (Y_UNLIKELY(row1 > row2)) {
            row2 = row1; // will not go further than row1
        }
        if (Y_UNLIKELY(key1 && key2 && Compare(key1, key2, keyDefaults) > 0)) {
            key2 = key1; // will not go further than key1
        }

        TVector<TNodeState> level, nextLevel(Reserve(3));
        for (ui32 height = 0; height < meta.LevelCount && ready; height++) {
            if (height == 0) {
                ready &= TryLoadRoot(meta, nextLevel);
            } else {
                for (ui32 i : xrange<ui32>(level.size())) {
                    TRecIdx from = 0, to = level[i].Node.GetKeysCount();
                    if (level[i].BeginRowId < row1) {
                        Y_DEBUG_ABORT_UNLESS(i == 0);
                        from = level[i].Node.Seek(row1);
                    }
                    if (level[i].EndRowId > row2) {
                        Y_DEBUG_ABORT_UNLESS(i == level.size() - 1);
                        to = level[i].Node.Seek(row2);
                    }
                    for (TRecIdx j : xrange(from, to + 1)) {
                        ready &= TryLoadNode(level[i], j, nextLevel);
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
            if (key1) {
                for (auto it = level.begin(); it != level.end(); it++) {
                    if (!it->BeginKey.Count() || it->BeginKey.CompareTo(key1, &keyDefaults) <= 0) {
                        // Y_VERIFY_DEBUG_S(std::distance(level.begin(), it) < 1, "Should have key2 node at the begin");
                        TRecIdx pos = it->Node.Seek(ESeek::Lower, key1, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);

                        // shrink row1 to the first key >= key1
                        row1 = Max(row1, pos 
                            ? it->Node.GetShortChild(pos - 1).RowCount 
                            : it->BeginRowId);

                        // break; //???
                    }
                }
            }
            if (key2) {
                for (auto it = level.rbegin(); it != level.rend(); it++) {
                    if (!it->EndKey.Count() || it->EndKey.CompareTo(key2, &keyDefaults) > 0) {
                        // Y_VERIFY_DEBUG_S(std::distance(level.rbegin(), it) < 2, "Should have key2 node at the end");
                        TRecIdx pos = it->Node.Seek(ESeek::Lower, key2, Scheme.Groups[0].ColsKeyIdx, &keyDefaults);
                            
                        // shrink row2 to the first key > key2
                        row2 = Min(row2, it->Node.GetShortChild(pos).RowCount);

                        // break; //???
                    }
                }
            }

            for (ui32 i : xrange<ui32>(level.size())) {
                if (level[i].EndRowId <= row1 || level[i].BeginRowId > row2) {
                    continue; // TODO: ???
                }

                TRecIdx from = 0, to = level[i].Node.GetKeysCount();
                if (level[i].BeginRowId < row1) {
                    from = level[i].Node.Seek(row1);
                }
                if (level[i].EndRowId > row2) {
                    to = level[i].Node.Seek(row2);
                }
                for (TRecIdx j : xrange(from, to + 1)) {
                    ready &= HasDataPage(level[i].Node.GetShortChild(j).PageId, { });
                }
            }
        }

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
        const static TCellsIterable EmptyKey = TCellsIterable(static_cast<const char*>(nullptr), TColumns());

        auto page = Env->TryGetPage(Part, meta.PageId);
        if (!page) {
            return false;
        }

        level.emplace_back(meta, TBtreeIndexNode(*page), 0, meta.RowCount, EmptyKey, EmptyKey);
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

        TCellsIterable beginKey = pos ? current.Node.GetKeyCellsIterable(pos - 1, Scheme.Groups[0].ColsKeyIdx) : current.BeginKey;
        TCellsIterable endKey = pos < current.Node.GetKeysCount() ? current.Node.GetKeyCellsIterable(pos, Scheme.Groups[0].ColsKeyIdx) : current.EndKey;
        
        level.emplace_back(child, TBtreeIndexNode(*page), beginRowId, endRowId, beginKey, endKey);
        return true;
    }

    int Compare(TCells left, TCells right, const TKeyCellDefaults &keyDefaults) const noexcept
    {
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
