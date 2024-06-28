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

const ui8 INITIAL_STATE = 0;
const ui8 OPENED_STATE = 1;
const ui8 CLOSED_STATE = 2;
const ui8 IGNORED_STATE = 3;

class TTableHistogramBuilderBtreeIndex {
    struct TNodeState : public TIntrusiveListItem<TNodeState> {
        const TPart* Part;
        TPageId PageId;
        ui32 Level;
        TRowId BeginRowId, EndRowId;
        ui64 BeginDataSize, EndDataSize;
        TCellsIterable BeginKey, EndKey;
        ui8 State = 0;

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

        TRowId GetRowCount() const noexcept {
            return EndRowId - BeginRowId;
        }

        ui64 GetDataSize() const noexcept {
            return EndDataSize - BeginDataSize;
        }

        void Open(ui64& openedRowCount, ui64& openedDataSize) noexcept {
            Y_ABORT_UNLESS(State == INITIAL_STATE);
            
            State = OPENED_STATE;
            openedRowCount += GetRowCount();
            openedDataSize += GetDataSize();
        }

        void Close(ui64& openedRowCount, ui64& closedRowCount, ui64& openedDataSize, ui64& closedDataSize) noexcept {
            Y_ABORT_UNLESS(State == OPENED_STATE || State == IGNORED_STATE);

            if (State == OPENED_STATE) {
                State = CLOSED_STATE;
                ui64 rowCount = GetRowCount();
                ui64 dataSize = GetDataSize();
                Y_ABORT_UNLESS(openedRowCount >= rowCount);
                Y_ABORT_UNLESS(openedDataSize >= dataSize);
                openedRowCount -= rowCount;
                openedDataSize -= dataSize;
                closedRowCount += rowCount;
                closedDataSize += dataSize;
            }
        }

        void Ignore(ui64& openedRowCount, ui64& openedDataSize) noexcept {
            Y_ABORT_UNLESS(State == OPENED_STATE);
            
            State = IGNORED_STATE;
            ui64 rowCount = GetRowCount();
            ui64 dataSize = GetDataSize();
            Y_ABORT_UNLESS(openedRowCount >= rowCount);
            Y_ABORT_UNLESS(openedDataSize >= dataSize);
            openedRowCount -= rowCount;
            openedDataSize -= dataSize;
        }
    };

    struct TNodeEvent {
        TCellsIterable Key;
        bool IsBegin;
        TNodeState* Node;
    };

    struct TNodeEventKeyGreater {
        const TKeyCellDefaults& KeyDefaults;

        // returns that a > b
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

        i8 GetKind(const TNodeEvent& a) const noexcept {
            Y_ABORT_UNLESS(a.Key);
            return a.IsBegin ? 1 : -1; // end first
        }

        i8 GetCategory(const TNodeEvent& a) const noexcept {
            if (a.Key) {
                return 0;
            }
            return a.IsBegin ? -1 : 1;
        }
    };

    struct TNodeRowCountLess {
        bool operator ()(const TNodeState* a, const TNodeState* b) const noexcept {
            return a->GetRowCount() < b->GetRowCount();
        }
    };

    struct TNodeDataSizeLess {
        bool operator ()(const TNodeState* a, const TNodeState* b) const noexcept {
            return a->GetDataSize() < b->GetDataSize();
        }
    };

public:
    TTableHistogramBuilderBtreeIndex(const TSubset& subset, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler)
        : Subset(subset)
        , KeyDefaults(*Subset.Scheme->Keys)
        , RowCountResolution(rowCountResolution)
        , DataSizeResolution(dataSizeResolution)
        , RowCountResolutionGap(RowCountResolution / 2)
        , DataSizeResolutionGap(DataSizeResolution / 2)
        , Env(env)
        , YieldHandler(yieldHandler)
        , NodeEventKeyGreater{KeyDefaults}
        , NodeEvents(NodeEventKeyGreater)
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

        ready &= BuildIterate(stats);

        NodeEvents.clear();
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
        // The idea is the following:
        // - we move some key pointer through all parts simultaneously
        //   keeping all nodes that have current key pointer in opened heap (sorted by size)
        //   all nodes before current key pointer are considered as closed
        // - we keep invariant that size of closed and opened nodes <= next requested histogram bucket value
        //   if it false we load opened nodes and split them using current key pointer

        ui64 nextRowCount = RowCountResolution, closedRowCount = 0, openedRowCount = 0;
        ui64 nextDataSize = DataSizeResolution, closedDataSize = 0, openedDataSize = 0;
        TPriorityQueue<TNodeState*, TVector<TNodeState*>, TNodeRowCountLess> openedSortedByRowCount;
        TPriorityQueue<TNodeState*, TVector<TNodeState*>, TNodeDataSizeLess> openedSortedByDataSize;
        
        while (NodeEvents) {
            YieldHandler();

            auto event = NodeEvents.top();
            ui64 rowCount = closedRowCount + openedRowCount / 2; // right before new opens
            ui64 dataSize = closedDataSize + openedDataSize / 2; // right before new opens

            Y_ABORT_UNLESS(NodeEvents && !NodeEventKeyGreater(NodeEvents.top(), event), "Should pop top");
            while (NodeEvents && !NodeEventKeyGreater(NodeEvents.top(), event)) {
                auto& newEvent = NodeEvents.top();
                if (newEvent.IsBegin) {
                    newEvent.Node->Open(openedRowCount, openedDataSize);
                    openedSortedByRowCount.push(newEvent.Node);
                    openedSortedByDataSize.push(newEvent.Node);
                } else {
                    newEvent.Node->Close(openedRowCount, closedRowCount, openedDataSize, closedDataSize);
                }
            
                NodeEvents.pop();
            }

            const auto addEvent = [&](TNodeEvent newEvent) {
                if (NodeEventKeyGreater(newEvent, event)) {
                    NodeEvents.push(newEvent);
                } else {
                    if (newEvent.IsBegin) {
                        newEvent.Node->Open(openedRowCount, openedDataSize);
                        openedSortedByRowCount.push(newEvent.Node);
                        openedSortedByDataSize.push(newEvent.Node);
                    } else {
                        newEvent.Node->Close(openedRowCount, closedRowCount, openedDataSize, closedDataSize);
                    }
                }
            };

            const auto addNode = [&](TNodeState& child) {
                TNodeEvent childBegin{child.BeginKey, true, &child};
                TNodeEvent childEnd{child.EndKey, false, &child};

                addEvent(childBegin);
                addEvent(childEnd);
            };

            while (nextRowCount != Max<ui64>() && closedRowCount + openedRowCount > nextRowCount + RowCountResolutionGap && openedSortedByRowCount) {
                auto node = openedSortedByRowCount.top();
                openedSortedByRowCount.pop();

                if (node->State != OPENED_STATE) {
                    Y_ABORT_UNLESS(node->State == CLOSED_STATE || node->State == IGNORED_STATE);
                    continue;
                }

                if (node->Level) {
                    node->Ignore(openedRowCount, openedDataSize);
                    if (!TryLoadNode(*node, addNode)) {
                        return false;
                    }
                } // else: leaf nodes will be closed later
            }

            while (nextDataSize != Max<ui64>() && closedDataSize + openedDataSize > nextDataSize + DataSizeResolutionGap && openedSortedByDataSize) {
                auto node = openedSortedByDataSize.top();
                openedSortedByDataSize.pop();

                if (node->State != OPENED_STATE) {
                    Y_ABORT_UNLESS(node->State == CLOSED_STATE || node->State == IGNORED_STATE);
                    continue;
                }

                if (node->Level) {
                    node->Ignore(openedRowCount, openedDataSize);
                    if (!TryLoadNode(*node, addNode)) {
                        return false;
                    }
                } // else: leaf nodes will be closed later
            }

            if (!event.IsBegin) { // add only newly closed nodes
                rowCount = closedRowCount + openedRowCount / 2;
                dataSize = closedDataSize + openedDataSize / 2;
            }
            rowCount = Min(rowCount, stats.RowCount);
            dataSize = Min(dataSize, stats.DataSize.Size);

            if (event.Key) {
                // we search value in interval [nextRowCount - RowCountResolutionGap, nextRowCount + RowCountResolutionGap]
                if (nextRowCount != Max<ui64>()) {
                    if (closedRowCount + openedRowCount > nextRowCount + RowCountResolutionGap || closedRowCount > nextRowCount - RowCountResolutionGap) {
                        if (stats.RowCountHistogram.empty() || stats.RowCountHistogram.back().Value < rowCount) {
                            AddBucket(stats.RowCountHistogram, event.Key, rowCount);
                            nextRowCount = Max(rowCount + 1, nextRowCount + RowCountResolution);
                            if (nextRowCount + RowCountResolutionGap > stats.RowCount) {
                                nextRowCount = Max<ui64>();
                                if (nextDataSize == Max<ui64>()) break;
                            }
                        }
                    }
                }
                // we search value in interval [nextDataSize - DataSizeResolutionGap, nextDataSize + DataSizeResolutionGap]
                if (nextDataSize != Max<ui64>()) {
                    if (closedDataSize + openedDataSize > nextDataSize + DataSizeResolutionGap || closedDataSize > nextDataSize - DataSizeResolutionGap) {
                        if (stats.DataSizeHistogram.empty() || stats.DataSizeHistogram.back().Value < dataSize) {
                            AddBucket(stats.DataSizeHistogram, event.Key, dataSize);
                            nextDataSize = Max(dataSize + 1, nextDataSize + DataSizeResolution);
                            if (nextDataSize + DataSizeResolutionGap > stats.DataSize.Size) {
                                nextDataSize = Max<ui64>();
                                if (nextRowCount == Max<ui64>()) break;
                            }
                        }
                    }
                }
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
        Y_ABORT_UNLESS(left_);
        Y_ABORT_UNLESS(right_);

        auto left = left_.Iter(), right = right_.Iter();
        size_t end = Max(left.Count(), right.Count());
        Y_ABORT_UNLESS(end <= keyDefaults.Size(), "Key schema is smaller than compared keys");
        
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
    ui64 RowCountResolutionGap, DataSizeResolutionGap;
    IPages* const Env;
    TBuildStatsYieldHandler YieldHandler;
    TDeque<TBtreeIndexNode> LoadedBTreeNodes; // keep nodes to use TCellsIterable key refs
    TDeque<TNodeState> LoadedStateNodes; // keep nodes to use TIntrusiveList
    TNodeEventKeyGreater NodeEventKeyGreater;
    TPriorityQueue<TNodeEvent, TVector<TNodeEvent>, TNodeEventKeyGreater> NodeEvents;
};

}

inline bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    TTableHistogramBuilderBtreeIndex builder(subset, rowCountResolution, dataSizeResolution, env, yieldHandler);

    if (!builder.Build(stats)) {
        return false;
    }

    return true;
}

}
