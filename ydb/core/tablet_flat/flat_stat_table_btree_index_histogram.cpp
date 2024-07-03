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

enum class ENodeState : ui8 {
    Initial,
    Opened,
    Closed,
    Ignored,
};

class TTableHistogramBuilderBtreeIndex {
    struct TNodeState {
        const TPart* Part;
        TPageId PageId;
        ui32 Level;
        TRowId BeginRowId, EndRowId;
        ui64 BeginDataSize, EndDataSize;
        TCellsIterable BeginKey, EndKey;
        ENodeState State = ENodeState::Initial;

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
            Y_ABORT_UNLESS(State == ENodeState::Initial);
            
            State = ENodeState::Opened;
            openedRowCount += GetRowCount();
            openedDataSize += GetDataSize();
        }

        void Close(ui64& openedRowCount, ui64& closedRowCount, ui64& openedDataSize, ui64& closedDataSize) noexcept {
            Y_ABORT_UNLESS(State == ENodeState::Opened || State == ENodeState::Ignored);

            if (State == ENodeState::Opened) {
                State = ENodeState::Closed;
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
            Y_ABORT_UNLESS(State == ENodeState::Opened);
            
            State = ENodeState::Ignored;
            ui64 rowCount = GetRowCount();
            ui64 dataSize = GetDataSize();
            Y_ABORT_UNLESS(openedRowCount >= rowCount);
            Y_ABORT_UNLESS(openedDataSize >= dataSize);
            openedRowCount -= rowCount;
            openedDataSize -= dataSize;
        }
    };

    struct TEvent {
        TCellsIterable Key;
        bool IsBegin;
        TNodeState* Node;
    };

    struct TNodeEventKeyGreater {
        const TKeyCellDefaults& KeyDefaults;

        // returns that a > b
        bool operator ()(const TEvent& a, const TEvent& b) const noexcept {
            return Compare(a, b) > 0;
        }

        i8 Compare(const TEvent& a, const TEvent& b) const noexcept {
            // events go in order:
            // - Key = {}, IsBegin = true
            // - ...
            // - Key = {'c'}, IsBegin = false
            // - Key = {'c'}, IsBegin = true
            // - ...
            // - Key = {'d'}, IsBegin = false
            // - Key = {'d'}, IsBegin = true
            // - ...
            // - Key = {}, IsBegin = false

            if (a.Key && b.Key) { // compare by keys
                auto cmp = CompareKeys(a.Key, b.Key, KeyDefaults);
                if (cmp != 0) {
                    return cmp;
                }
                // keys are the same, compare by begin flag, end events first:
                return Compare(a.IsBegin ? 1 : -1, b.IsBegin ? 1 : -1);
            }

            // category = -1 for Key = { }, IsBegin = true
            // category =  0 for Key = {*}, IsBegin = *
            // category = -1 for Key = { }, IsBegin = false
            return Compare(GetCategory(a), GetCategory(b));
        }

    private:
        static i8 GetCategory(const TEvent& a) noexcept {
            if (a.Key) {
                return 0;
            }
            return a.IsBegin ? -1 : 1;
        }

        static i8 Compare(i8 a, i8 b) noexcept {
            if (a < b) return -1;
            if (a > b) return 1;
            return 0;
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
        , FutureEvents(NodeEventKeyGreater)
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

        FutureEvents.clear();
        LoadedBTreeNodes.clear();
        LoadedStateNodes.clear();

        return ready;
    }

private:
    bool SlicePart(const TSlices& slices, TNodeState& node) {
        YieldHandler();

        // TODO: avoid binary search for each call (we may intersect slices with nodes in linear time actually)
        auto it = slices.LookupBackward(slices.end(), node.EndRowId - 1);
        
        if (it == slices.end() || node.EndRowId <= it->BeginRowId() || it->EndRowId() <= node.BeginRowId) {
            // skip the node
            return true;
        }

        if (it->BeginRowId() <= node.BeginRowId && node.EndRowId <= it->EndRowId()) {
            // take the node
            AddFutureEvents(node);
            return true;
        }

        // split the node

        if (node.Level == 0) {
            // can't split, decide by node.EndRowId - 1
            // TODO: decide by non-empty slice and node intersection, but this requires size calculation changes too
            if (it->Has(node.EndRowId - 1)) {
                AddFutureEvents(node);
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
        // - move a key pointer through all parts simultaneously
        //   keeping all nodes that contain current key pointer in opened heaps (sorted by size descending)
        //   all nodes that ended before current key pointer are considered as closed
        // - keep an invariant that size of closed and opened nodes don't exceed next histogram bucket values
        //   otherwise, load opened nodes
        // - because histogram is approximate each its value is allowed to be in range
        //   [next value - gap, next value + gap]

        // next histogram keys are been looking for:
        ui64 nextHistogramRowCount = RowCountResolution, nextHistogramDataSize = DataSizeResolution;

        // closed nodes stats:
        ui64 closedRowCount = 0, closedDataSize = 0;

        // opened nodes stats and heaps:
        ui64 openedRowCount = 0, openedDataSize = 0;
        TPriorityQueue<TNodeState*, TVector<TNodeState*>, TNodeRowCountLess> openedSortedByRowCount;
        TPriorityQueue<TNodeState*, TVector<TNodeState*>, TNodeDataSizeLess> openedSortedByDataSize;

        // will additionally save list of all nodes that start at current key pointer:
        TVector<TNodeState*> currentKeyPointerOpens;
        
        while (FutureEvents && (nextHistogramRowCount != Max<ui64>() || nextHistogramDataSize != Max<ui64>())) {
            YieldHandler();

            auto currentKeyPointer = FutureEvents.top();
            currentKeyPointerOpens.clear();

            auto processEvent = [&](const TEvent& event) {
                Y_DEBUG_ABORT_UNLESS(NodeEventKeyGreater.Compare(event, currentKeyPointer) <= 0, "Can't process future events");
                if (event.IsBegin) {
                    event.Node->Open(openedRowCount, openedDataSize);
                    openedSortedByRowCount.push(event.Node);
                    openedSortedByDataSize.push(event.Node);
                } else {
                    event.Node->Close(openedRowCount, closedRowCount, openedDataSize, closedDataSize);
                }
            };

            // process all events with the same key and type as current key pointer:
            do {
                const TEvent& event = FutureEvents.top();
                processEvent(event);
                if (event.IsBegin) {
                    currentKeyPointerOpens.push_back(event.Node);
                }
                FutureEvents.pop();
            } while (FutureEvents && NodeEventKeyGreater.Compare(FutureEvents.top(), currentKeyPointer) == 0);

            const auto addEvent = [&](TEvent event) {
                // TODO: skip all closed nodes and don't process them here
                // TODO: don't compare each node key and replace it with parentNode.Seek(currentKeyPointer)
                auto cmp = NodeEventKeyGreater.Compare(event, currentKeyPointer);
                if (cmp <= 0) { // event happened
                    processEvent(event);
                    if (cmp == 0) {
                        currentKeyPointerOpens.push_back(event.Node);
                    }
                } else { // event didn't happen yet
                    FutureEvents.push(event);
                }
            };
            const auto addNode = [&](TNodeState& node) {
                addEvent(TEvent{node.BeginKey, true, &node});
                addEvent(TEvent{node.EndKey, false, &node});
            };

            // may safely skip current key pointer and go further only if at the next iteration
            // sizes of closed and opened nodes don't exceed next histogram bucket values (plus their gaps)
            // otherwise, load opened nodes right now
            // in that case, next level nodes will be converted to begin and end events and then 
            // either processed or been postponed to future events according to current key pointer position
            while (nextHistogramRowCount != Max<ui64>() && closedRowCount + openedRowCount > nextHistogramRowCount + RowCountResolutionGap && openedSortedByRowCount) {
                auto node = openedSortedByRowCount.top();
                openedSortedByRowCount.pop();

                if (node->State != ENodeState::Opened) {
                    // may have already closed or ignored nodes in the heap, just skip them
                    Y_ABORT_UNLESS(node->State == ENodeState::Closed || node->State == ENodeState::Ignored);
                    continue;
                }

                if (node->Level) {
                    node->Ignore(openedRowCount, openedDataSize);
                    
                    if (!TryLoadNode(*node, addNode)) {
                        return false;
                    }
                } // else: leaf nodes will be closed later
            }
            while (nextHistogramDataSize != Max<ui64>() && closedDataSize + openedDataSize > nextHistogramDataSize + DataSizeResolutionGap && openedSortedByDataSize) {
                auto node = openedSortedByDataSize.top();
                openedSortedByDataSize.pop();

                if (node->State != ENodeState::Opened) {
                    // may have already closed or ignored nodes in the heap, just skip them
                    Y_ABORT_UNLESS(node->State == ENodeState::Closed || node->State == ENodeState::Ignored);
                    continue;
                }

                if (node->Level) {
                    node->Ignore(openedRowCount, openedDataSize);
                    if (!TryLoadNode(*node, addNode)) {
                        return false;
                    }
                } // else: leaf nodes will be closed later
            }

            // add current key pointer if we either:
            // - failed to split opened nodes and may exceed next histogram bucket values (plus their gaps)
            // - have enough closed nodes (more than next histogram bucket values (minus their gaps))
            // current key pointer value:
            // - includes size of all closed nodes
            // - includes half of size of all opened nodes (as they exact position is unknown)
            // - ignores all nodes that start at current key pointer
            if (currentKeyPointer.Key) {
                if (nextHistogramRowCount != Max<ui64>()) {
                    if (closedRowCount + openedRowCount > nextHistogramRowCount + RowCountResolutionGap || closedRowCount > nextHistogramRowCount - RowCountResolutionGap) {
                        ui64 currentKeyRowCountOpens = 0;
                        for (auto* node : currentKeyPointerOpens) {
                            if (node->State == ENodeState::Opened) {
                                currentKeyRowCountOpens += node->GetRowCount();
                            }
                        }
                        Y_ABORT_UNLESS(currentKeyRowCountOpens <= openedRowCount);
                        ui64 currentKeyPointerRowCount = closedRowCount + (openedRowCount - currentKeyRowCountOpens) / 2;
                        currentKeyPointerRowCount = Min(currentKeyPointerRowCount, stats.RowCount);
                        if (stats.RowCountHistogram.empty() || stats.RowCountHistogram.back().Value < currentKeyPointerRowCount) {
                            AddKey(stats.RowCountHistogram, currentKeyPointer.Key, currentKeyPointerRowCount);
                            nextHistogramRowCount = Max(currentKeyPointerRowCount + 1, nextHistogramRowCount + RowCountResolution);
                            if (nextHistogramRowCount + RowCountResolutionGap > stats.RowCount) {
                                nextHistogramRowCount = Max<ui64>();
                            }
                        }
                    }
                }
                if (nextHistogramDataSize != Max<ui64>()) {
                    if (closedDataSize + openedDataSize > nextHistogramDataSize + DataSizeResolutionGap || closedDataSize > nextHistogramDataSize - DataSizeResolutionGap) {
                        ui64 currentKeyDataSizeOpens = 0;
                        for (auto* node : currentKeyPointerOpens) {
                            if (node->State == ENodeState::Opened) {
                                currentKeyDataSizeOpens += node->GetDataSize();
                            }
                        }
                        Y_ABORT_UNLESS(currentKeyDataSizeOpens <= openedDataSize);
                        ui64 currentKeyPointerDataSize = closedDataSize + (openedDataSize - currentKeyDataSizeOpens) / 2;
                        currentKeyPointerDataSize = Min(currentKeyPointerDataSize, stats.DataSize.Size);
                        if (stats.DataSizeHistogram.empty() || stats.DataSizeHistogram.back().Value < currentKeyPointerDataSize) {
                            AddKey(stats.DataSizeHistogram, currentKeyPointer.Key, currentKeyPointerDataSize);
                            nextHistogramDataSize = Max(currentKeyPointerDataSize + 1, nextHistogramDataSize + DataSizeResolution);
                            if (nextHistogramDataSize + DataSizeResolutionGap > stats.DataSize.Size) {
                                nextHistogramDataSize = Max<ui64>();
                            }
                        }
                    }
                }
            }
        }

        return true;
    }

    void AddKey(THistogram& histogram, TCellsIterable key, ui64 value) {
        TVector<TCell> keyCells;

        // add columns that are present in the part:
        auto iter = key.Iter();
        for (TPos pos : xrange(iter.Count())) {
            Y_UNUSED(pos);
            keyCells.push_back(iter.Next());
        }

        // extend with default values if needed:
        for (TPos index = keyCells.size(); index < KeyDefaults.Defs.size(); ++index) {
            keyCells.push_back(KeyDefaults.Defs[index]);
        }

        TString serializedKey = TSerializedCellVec::Serialize(keyCells);

        histogram.push_back({serializedKey, value});
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

    void AddFutureEvents(TNodeState& node) {
        FutureEvents.push(TEvent{node.BeginKey, true, &node});
        FutureEvents.push(TEvent{node.EndKey, false, &node});
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
    TDeque<TBtreeIndexNode> LoadedBTreeNodes; // keep nodes to use TCellsIterable references
    TDeque<TNodeState> LoadedStateNodes; // keep nodes to use their references
    TNodeEventKeyGreater NodeEventKeyGreater;
    TPriorityQueue<TEvent, TVector<TEvent>, TNodeEventKeyGreater> FutureEvents;
};

}

bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    TTableHistogramBuilderBtreeIndex builder(subset, rowCountResolution, dataSizeResolution, env, yieldHandler);

    if (!builder.Build(stats)) {
        return false;
    }

    return true;
}

}
