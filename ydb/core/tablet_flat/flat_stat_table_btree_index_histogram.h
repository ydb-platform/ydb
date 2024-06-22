#pragma once

#include "flat_stat_table.h"
#include "flat_table_subset.h"

namespace NKikimr::NTable {

namespace {

using TGroupId = NPage::TGroupId;
using TFrames = NPage::TFrames;
using TBtreeIndexNode = NPage::TBtreeIndexNode;
using TChild = TBtreeIndexNode::TChild;
using TColumns = TBtreeIndexNode::TColumns;
using TCells = NPage::TCells;
using TCellsIterable = TBtreeIndexNode::TCellsIterable;
using TCellsIter = TBtreeIndexNode::TCellsIter;

const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());

class TTableHistogramBuilderBtreeIndex {
    struct TNodeState : public TIntrusiveListItem<TNodeState> {
        const TPart* Part;
        TPageId PageId;
        ui32 Level;
        TRowId BeginRowId, EndRowId;
        ui64 BeginDataSize, EndDataSize;
        TCellsIterable BeginKey, EndKey;

        TNodeState(const TPart* part, TPageId pageId, ui32 level, TRowId beginRowId, TRowId endRowId, TRowId beginDataSize, TRowId endDataSize, TCellsIterable beginKey, TCellsIterable endKey)
            : Part(part)
            , PageId(pageId)
            , Level(level)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginDataSize(beginDataSize)
            , EndDataSize(endDataSize)
            , BeginKey(beginKey)
            , EndKey(endKey)
        {
        }

        ui64 GetDataSize() const noexcept {
            return EndDataSize - BeginDataSize;
        }

        TRowId GetRowCount() const noexcept {
            return EndRowId - BeginRowId;
        }
    };

    struct TNodeEvent {
        TCellsIterable Key;
        bool IsBegin;
        TNodeState* Node;
    };

    struct TNodeEventKeyGreater {
        const TKeyCellDefaults& KeyDefaults;

        bool operator ()(const TNodeEvent& a, const TNodeEvent& b) const noexcept {
            if (a.Key && b.Key) {
                auto cmp = CompareKeys(a.Key, b.Key, KeyDefaults);
                if (cmp != 0) {
                    return cmp > 0;
                }
                return GetKind(a) > GetKind(b);
            }

            return GetCategory(a) > GetCategory(b);
        }

        ui8 GetKind(const TNodeEvent& a) const noexcept {
            // end first
            return a.IsBegin ? 1 : -1;
        }

        ui8 GetCategory(const TNodeEvent& a) const noexcept {
            if (a.Key) {
                return 0;
            }
            return a.IsBegin ? -1 : 1;
        }
    };

public:
    TTableHistogramBuilderBtreeIndex(const TSubset& subset, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler)
        : Subset(subset)
        , KeyDefaults(*Subset.Scheme->Keys)
        , RowCountResolution(rowCountResolution)
        , DataSizeResolution(dataSizeResolution)
        , Env(env)
        , YieldHandler(yieldHandler)
        , NodeEvents(TNodeEventKeyGreater{KeyDefaults})
    {
    }

    bool Build(TStats& stats) {
        bool ready = true;

        for (auto index : xrange(Subset.Flatten.size())) {
            auto& part = Subset.Flatten[index];
            auto& meta = part->IndexPages.GetBTree({});
            LoadedStateNodes.emplace_back(part.Part.Get(), meta.GetPageId(), meta.LevelCount, 0, meta.GetRowCount(), 0, meta.GetDataSize(), EmptyKey, EmptyKey);
            ready &= SlicePart(*part.Slices, LoadedStateNodes.back());
        }

        if (!ready) {
            return false;
        }

        BuildIterate(stats);

        LoadedBTreeNodes.clear();
        LoadedStateNodes.clear();

        return ready;
    }

private:
    bool SlicePart(const TSlices& slices, TNodeState& node) {
        YieldHandler();

        auto it = slices.LookupBackward(slices.end(), node.EndRowId - 1);
        
        if (it == slices.end() || node.EndRowId <= it->BeginRowId() || it->EndRowId() <= node.BeginRowId) {
            // skip the node
            return true;
        }

        if (it->BeginRowId() <= node.BeginRowId && node.EndRowId <= it->EndRowId()) {
            // take the node
            AddNodeEvents(node);
            return true;
        }

        // split the node

        if (node.Level == 0) {
            // can't split, decide by node.EndRowId - 1
            if (it->Has(node.EndRowId - 1)) {
                AddNodeEvents(node);
            }
            return true;
        }

        bool ready = true;

        const auto addNode = [&](TNodeState& child) {
            ready &= SlicePart(slices, child);
        };
        if (!TryLoadNode(node, addNode)) {
            return false;
        }

        return ready;
    }

    bool BuildIterate(TStats& stats) {
        Y_UNUSED(stats);
        Y_UNUSED(RowCountResolution, DataSizeResolution);

        // ui64 nextBucket = RowCountResolution;
        THashSet<TNodeState*> currentNodes;

        while (NodeEvents) {
            auto event = NodeEvents.top();
            NodeEvents.pop();

            if (event.IsBegin) {

            } else {

            }
        }

        return true;
    }

    void AddBucket(THistogram& histogram, TCellsIterable key, ui64 value) {
        TVector<TCell> splitKeyCells;

        // Add columns that are present in the part
        auto iter = key.Iter();
        for (TPos pos : xrange(iter.Count())) {
            Y_UNUSED(pos);
            splitKeyCells.push_back(iter.Next());
        }

        // Extend with default values if needed
        for (TPos index = splitKeyCells.size(); index < KeyDefaults.Defs.size(); ++index) {
            splitKeyCells.push_back(KeyDefaults.Defs[index]);
        }

        TString serializedSplitKey = TSerializedCellVec::Serialize(splitKeyCells);

        histogram.push_back({serializedSplitKey, value});
    }

    bool TryLoadNode(const TNodeState& parent, const auto& addNode) {
        Y_ABORT_UNLESS(parent.Level);

        auto page = Env->TryGetPage(parent.Part, parent.PageId, {});
        if (!page) {
            return false;
        }

        LoadedBTreeNodes.emplace_back(*page);
        auto &bTreeNode = LoadedBTreeNodes.back();
        auto& groupInfo = parent.Part->Scheme->GetLayout({});

        for (auto pos : xrange(bTreeNode.GetChildrenCount())) {
            auto& child = bTreeNode.GetChild(pos);

            LoadedStateNodes.emplace_back(parent.Part, child.GetPageId(), parent.Level - 1,
                pos ? bTreeNode.GetChild(pos - 1).GetRowCount() : parent.BeginRowId, child.GetRowCount(),
                pos ? bTreeNode.GetChild(pos - 1).GetTotalDataSize() : parent.BeginDataSize, child.GetTotalDataSize(),
                pos ? bTreeNode.GetKeyCellsIterable(pos - 1, groupInfo.ColsKeyData) : parent.BeginKey,
                pos < bTreeNode.GetKeysCount() ? bTreeNode.GetKeyCellsIterable(pos, groupInfo.ColsKeyData) : parent.EndKey);

            addNode(LoadedStateNodes.back());
        }

        return true;
    }

    void AddNodeEvents(TNodeState& node) {
        NodeEvents.push(TNodeEvent{node.BeginKey, true, &node});
        NodeEvents.push(TNodeEvent{node.EndKey, false, &node});
    }
    
private:
    static int CompareKeys(const TCellsIterable& left_, const TCellsIterable& right_, const TKeyCellDefaults& keyDefaults) {
        Y_DEBUG_ABORT_UNLESS(left_);
        Y_DEBUG_ABORT_UNLESS(right_);

        auto left = left_.Iter(), right = right_.Iter();
        size_t end = Max(left.Count(), right.Count());
        Y_DEBUG_ABORT_UNLESS(end <= keyDefaults.Size(), "Key schema is smaller than compared keys");
        
        for (size_t pos = 0; pos < end; ++pos) {
            const auto& leftCell = pos < left.Count() ? left.Next() : keyDefaults.Defs[pos];
            const auto& rightCell = pos < right.Count() ? right.Next() : keyDefaults.Defs[pos];
            if (int cmp = CompareTypedCells(leftCell, rightCell, keyDefaults.Types[pos])) {
                return cmp;
            }
        }

        return 0;
    }

private:
    const TSubset& Subset;
    const TKeyCellDefaults& KeyDefaults;
    ui64 RowCountResolution, DataSizeResolution;
    IPages* const Env;
    TBuildStatsYieldHandler YieldHandler;
    TDeque<TBtreeIndexNode> LoadedBTreeNodes; // keep nodes to use TCellsIterable key refs
    TDeque<TNodeState> LoadedStateNodes; // keep nodes to use TIntrusiveList
    TPriorityQueue<TNodeEvent, TVector<TNodeEvent>, TNodeEventKeyGreater> NodeEvents;
};

}

inline bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    TTableHistogramBuilderBtreeIndex builder(subset, rowCountResolution, dataSizeResolution, env, yieldHandler);

    return builder.Build(stats);
}

}
