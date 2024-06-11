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
public:
    struct TNodeState : public TIntrusiveListItem<TNodeState> {
        TPageId PageId;
        ui32 Level;
        TRowId BeginRowId, EndRowId;
        TCellsIterable BeginKey, EndKey;
        ui64 BeginSize, EndSize;

        TNodeState(TPageId pageId, ui32 level, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, TCellsIterable endKey, TRowId beginSize, TRowId endSize)
            : PageId(pageId)
            , Level(level)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , EndKey(endKey)
            , BeginSize(beginSize)
            , EndSize(endSize)
        {
        }

        ui64 GetSize() const noexcept {
            return EndSize - BeginSize;
        }
    };

    struct TGetRowCount {
        static ui64 Get(const TChild& child) noexcept {
            return child.RowCount;
        }
    };

    struct TGetDataSize {
        static ui64 Get(const TChild& child) noexcept {
            return child.GetTotalDataSize();
        }
    };

private:
    struct TPartNodes {
        TPartNodes(const TPart* part, size_t index) 
            : Part(part)
            , Index(index)
        {
        }

        const TPart* GetPart() const noexcept {
            return Part;
        }

        size_t GetIndex() const noexcept {
            return Index;
        }

        size_t GetCount() const noexcept {
            return Count;
        }

        ui64 GetSize() const noexcept {
            return Size;
        }

        const TIntrusiveList<TNodeState>& GetNodes() const noexcept {
            return Nodes;
        }

        TNodeState* PopFront() noexcept {
            auto result = Nodes.PopFront();
            
            Count--;
            Size -= result->GetSize();
            
            return result;
        }

        TNodeState* PopBack() noexcept {
            auto result = Nodes.PopBack();
            
            Count--;
            Size -= result->GetSize();
            
            return result;
        }

        void PushFront(TNodeState* item) noexcept {
            Count++;
            Size += item->GetSize();
            Nodes.PushFront(item);
        }

        void PushBack(TNodeState* item) noexcept {
            Count++;
            Size += item->GetSize();
            Nodes.PushBack(item);
        }

        bool operator < (const TPartNodes& other) const noexcept {
            return Size < other.Size;
        }

    private:
        const TPart* Part;
        size_t Index;
        size_t Count = 0;
        ui64 Size = 0;
        TIntrusiveList<TNodeState> Nodes;
    };

public:
    TTableHistogramBuilderBtreeIndex(const TSubset& subset, IPages* env, ui32 histogramBucketsCount, TBuildStatsYieldHandler yieldHandler)
        : Subset(subset)
        , KeyDefaults(*Subset.Scheme->Keys)
        , Env(env)
        , HistogramBucketsCount(histogramBucketsCount)
        , YieldHandler(yieldHandler)
    {
    }

    template <typename TGetSize>
    bool Build(THistogram& histogram, ui64 statTotalSize) {
        if (!HistogramBucketsCount) {
            return true;
        }

        Resolution = statTotalSize / HistogramBucketsCount;
        StatTotalSize = statTotalSize;
        
        bool ready = true;
        ui64 endSize = 0;
        TVector<TPartNodes> parts;

        for (auto index : xrange(Subset.Flatten.size())) {
            auto& part = Subset.Flatten[index];
            auto& meta = part->IndexPages.GetBTree({});
            parts.emplace_back(part.Part.Get(), index);
            LoadedStateNodes.emplace_back(meta.PageId, meta.LevelCount, 0, meta.RowCount, EmptyKey, EmptyKey, 0, TGetSize::Get(meta));
            ready &= SlicePart<TGetSize>(parts.back(), *part.Slices, LoadedStateNodes.back());
            endSize += parts.back().GetSize();
        }

        if (!ready) {
            return false;
        }

        if (endSize) {
            ready &= BuildHistogramRecursive<TGetSize>(histogram, parts, 0, endSize, 0);
        }

        LoadedBTreeNodes.clear();
        LoadedStateNodes.clear();

        return ready;
    }

private:
    template <typename TGetSize>
    bool SlicePart(TPartNodes& part, const TSlices& slices, TNodeState& node) {
        YieldHandler();

        auto it = slices.LookupBackward(slices.end(), node.EndRowId - 1);
        
        if (it == slices.end() || node.EndRowId <= it->BeginRowId() || it->EndRowId() <= node.BeginRowId) {
            // skip the node
            return true;
        }

        if (it->BeginRowId() <= node.BeginRowId && node.EndRowId <= it->EndRowId()) {
            // take the node
            part.PushBack(&node);
            return true;
        }

        // split the node

        if (node.Level == 0) {
            // can't split, decide by node.EndRowId - 1
            if (it->Has(node.EndRowId - 1)) {
                part.PushBack(&node);
            }
            return true;
        }

        bool ready = true;

        const auto addNode = [&](TNodeState& child) {
            ready &= SlicePart<TGetSize>(part, slices, child);
        };
        if (!TryLoadNode<TGetSize>(part.GetPart(), node, addNode)) {
            return false;
        }

        return ready;
    }

    template <typename TGetSize>
    bool BuildHistogramRecursive(THistogram& histogram, TVector<TPartNodes>& parts, ui64 beginSize, ui64 endSize, ui32 depth) {
        const static ui32 MaxDepth = 100;

        YieldHandler();

#ifndef NDEBUG
        {
            Y_DEBUG_ABORT_UNLESS(beginSize < endSize);
            ui64 size = 0;
            for (const auto& part : parts) {
                size += part.GetSize();
            }
            Y_DEBUG_ABORT_UNLESS(size == endSize - beginSize);
        }
#endif

        if (SafeDiff(endSize, beginSize) <= Resolution || depth > MaxDepth) {
            Y_DEBUG_ABORT_UNLESS(depth <= MaxDepth, "Shouldn't normally happen");
            return true;
        }

        auto biggestPart = std::max_element(parts.begin(), parts.end());
        if (Y_UNLIKELY(biggestPart == parts.end())) {
            Y_DEBUG_ABORT("Invalid part states");
            return true;
        }
        Y_ABORT_UNLESS(biggestPart->GetCount());

        if (biggestPart->GetCount() == 1 && biggestPart->GetNodes().Front()->Level > 0) {
            const auto addNode = [&biggestPart](TNodeState& child) {
                biggestPart->PushBack(&child);
            };
            if (!TryLoadNode<TGetSize>(biggestPart->GetPart(), *biggestPart->PopFront(), addNode)) {
                return false;
            }
        }
        TCellsIterable splitKey = biggestPart->GetCount() > 1
            ? FindMedianPartKey(*biggestPart)
            : FindMedianTableKey(parts);

        if (!splitKey) {
            return true;
        }

        ui64 leftSize = 0, middleSize = 0, rightSize = 0;
        TVector<TPartNodes> leftParts, middleParts, rightParts;

        for (auto& part : parts) {
            auto& leftNodes = PushNextPartNodes(part, leftParts);
            auto& middleNodes = PushNextPartNodes(part, middleParts);
            auto& rightNodes = PushNextPartNodes(part, rightParts);

            while (part.GetCount()) {
                auto& node = *part.PopFront();
                if (node.EndKey && CompareKeys(node.EndKey, splitKey) <= 0) {
                    leftNodes.PushBack(&node);
                } else if (node.BeginKey && CompareKeys(node.BeginKey, splitKey) >= 0) {
                    rightNodes.PushBack(&node);
                } else {
                    middleNodes.PushBack(&node);
                }
            }

            Y_DEBUG_ABORT_UNLESS(middleNodes.GetCount() <= 1);
            leftSize += leftNodes.GetSize();
            middleSize += middleNodes.GetSize();
            rightSize += rightNodes.GetSize();
        }
        
        if (middleSize > Resolution / 2) {
            std::make_heap(middleParts.begin(), middleParts.end());

            while (middleSize > Resolution / 2 && middleParts.size()) {
                std::pop_heap(middleParts.begin(), middleParts.end());
                auto& middleNodes = middleParts.back();
                auto& leftNodes = GetNextPartNodes(middleNodes, leftParts);
                auto& rightNodes = GetNextPartNodes(middleNodes, rightParts);
                TIntrusiveList<TNodeState> rightNodesBuffer;
                
                leftSize -= leftNodes.GetSize();
                middleSize -= middleNodes.GetSize();
                rightSize -= rightNodes.GetSize();

                auto count = middleNodes.GetCount();
                bool hasChanges = false;
                for (auto index : xrange(count)) {
                    Y_UNUSED(index);
                    auto& node = *middleNodes.PopFront();
                    if (!node.Level) { // can't be splitted, return as-is
                        middleNodes.PushBack(&node);
                        continue;
                    }
                    const auto addNode = [&](TNodeState& node) {
                        if (node.EndKey && CompareKeys(node.EndKey, splitKey) <= 0) {
                            leftNodes.PushBack(&node);
                        } else if (node.BeginKey && CompareKeys(node.BeginKey, splitKey) >= 0) {
                            rightNodesBuffer.PushBack(&node);
                        } else {
                            middleNodes.PushBack(&node);
                        }
                    };
                    if (!TryLoadNode<TGetSize>(middleNodes.GetPart(), node, addNode)) {
                        return false;
                    }
                    hasChanges = true;
                }

                while (!rightNodesBuffer.Empty()) { // reverse right part new nodes
                    rightNodes.PushFront(rightNodesBuffer.PopBack());
                }

                Y_DEBUG_ABORT_UNLESS(middleNodes.GetCount() <= 1);
                leftSize += leftNodes.GetSize();
                middleSize += middleNodes.GetSize();
                rightSize += rightNodes.GetSize();

                if (hasChanges) { // return updated nodes to the heap 
                    std::push_heap(middleParts.begin(), middleParts.end());
                } else { // can't be splitted, ignore
                    middleParts.pop_back();
                }
            }
        }

        if (middleSize == 0 && (leftSize == 0 || rightSize == 0)) {
            // no progress, don't continue
            return true;
        }

        bool ready = true;

        if (leftSize) {
            ready &= BuildHistogramRecursive<TGetSize>(histogram, leftParts, beginSize, beginSize + leftSize, depth + 1);
        }
        
        ui64 splitSize = beginSize + leftSize + middleSize / 2;
        // Note: due to different calculation approaches splitSize may exceed StatTotalSize, ignore them
        if (beginSize < splitSize && splitSize < Min(endSize, StatTotalSize)) {
            AddBucket(histogram, splitKey, splitSize);
        }

        if (rightSize) {
            ready &= BuildHistogramRecursive<TGetSize>(histogram, rightParts, SafeDiff(endSize, rightSize), endSize, depth + 1);
        }

        return ready;
    }

    TCellsIterable FindMedianPartKey(const TPartNodes& part) {
        Y_ABORT_UNLESS(part.GetCount() > 1, "It's impossible to split part with only one node");

        TCellsIterable splitKey = EmptyKey;
        ui64 splitSize = 0, currentSize = 0;
        const ui64 middleSize = part.GetSize() / 2;
        
        for (const auto& node : part.GetNodes()) {
            if (currentSize) { // can't split with the first key, skip it
                if (!splitSize || AbsDifference(currentSize, middleSize) < AbsDifference(splitSize, middleSize)) {
                    splitKey = node.BeginKey;
                    splitSize = currentSize;
                }
            }

            currentSize += node.GetSize();
        }

        Y_ABORT_UNLESS(splitKey);

        return splitKey;
    }

    TCellsIterable FindMedianTableKey(const TVector<TPartNodes>& parts) {
        TVector<TCellsIterable> keys;
        for (const auto& part : parts) {
            for (const auto& node : part.GetNodes()) {
                if (node.BeginKey) {
                    keys.push_back(node.BeginKey);
                }
            }
        }

        auto median = keys.begin() + (keys.size() + 1) / 2;

        if (median == keys.end()) {
            return EmptyKey;
        }

        // Note: may work badly in case when all begin keys are the same
        // however such cases are rare and don't worth optimizing with sort+unique complex code
        // also this method is only called when we couldn't split the biggest part
        std::nth_element(keys.begin(), median, keys.end(), [this](const TCellsIterable& left, const TCellsIterable& right) {
            return CompareKeys(left, right) < 0;
        });
        
        return *median;
    }

    void AddBucket(THistogram& histogram, TCellsIterable key, ui64 size) {
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

        histogram.push_back({serializedSplitKey, size});
    }

    template <typename TGetSize>
    bool TryLoadNode(const TPart* part, const TNodeState& parent, const auto& addNode) {
        Y_ABORT_UNLESS(parent.Level);

        auto page = Env->TryGetPage(part, parent.PageId, {});
        if (!page) {
            return false;
        }

        LoadedBTreeNodes.emplace_back(*page);
        auto &bTreeNode = LoadedBTreeNodes.back();
        auto& groupInfo = part->Scheme->GetLayout({});

        for (auto pos : xrange(bTreeNode.GetChildrenCount())) {
            auto& child = bTreeNode.GetChild(pos);

            LoadedStateNodes.emplace_back(child.PageId, parent.Level - 1,
                pos ? bTreeNode.GetChild(pos - 1).RowCount : parent.BeginRowId, child.RowCount,
                pos ? bTreeNode.GetKeyCellsIterable(pos - 1, groupInfo.ColsKeyData) : parent.BeginKey,
                pos < bTreeNode.GetKeysCount() ? bTreeNode.GetKeyCellsIterable(pos, groupInfo.ColsKeyData) : parent.EndKey,
                pos ? TGetSize::Get(bTreeNode.GetChild(pos - 1)) : parent.BeginSize, TGetSize::Get(child));

            addNode(LoadedStateNodes.back());
        }

        return true;
    }

    TPartNodes& PushNextPartNodes(const TPartNodes& part, TVector<TPartNodes>& list) const {
        Y_ABORT_UNLESS(part.GetIndex() == list.size());
        list.emplace_back(part.GetPart(), part.GetIndex());
        return list.back();
    }

    TPartNodes& GetNextPartNodes(const TPartNodes& part, TVector<TPartNodes>& list) const {
        Y_ABORT_UNLESS(part.GetPart() == list[part.GetIndex()].GetPart());
        return list[part.GetIndex()];
    }

private:
    int CompareKeys(const TCellsIterable& left_, const TCellsIterable& right_) const {
        Y_DEBUG_ABORT_UNLESS(left_);
        Y_DEBUG_ABORT_UNLESS(right_);

        auto left = left_.Iter(), right = right_.Iter();
        size_t end = Max(left.Count(), right.Count());
        Y_DEBUG_ABORT_UNLESS(end <= KeyDefaults.Size(), "Key schema is smaller than compared keys");

        
        for (size_t pos = 0; pos < end; ++pos) {
            const auto& leftCell = pos < left.Count() ? left.Next() : KeyDefaults.Defs[pos];
            const auto& rightCell = pos < right.Count() ? right.Next() : KeyDefaults.Defs[pos];
            if (int cmp = CompareTypedCells(leftCell, rightCell, KeyDefaults.Types[pos])) {
                return cmp;
            }
        }

        return 0;
    }

    ui64 AbsDifference(ui64 a, ui64 b) const {
        return static_cast<ui64>(std::abs(static_cast<i64>(a) - static_cast<i64>(b)));
    }

    ui64 SafeDiff(ui64 a, ui64 b) const {
        return a - Min(a, b);
    }

private:
    const TSubset& Subset;
    const TKeyCellDefaults& KeyDefaults;
    IPages* const Env;
    ui32 HistogramBucketsCount;
    TBuildStatsYieldHandler YieldHandler;
    ui64 Resolution, StatTotalSize;
    TDeque<TBtreeIndexNode> LoadedBTreeNodes; // keep nodes to use TCellsIterable key refs
    TDeque<TNodeState> LoadedStateNodes; // keep nodes to use TIntrusiveList
};

}

inline bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui32 histogramBucketsCount, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    bool ready = true;
    
    TTableHistogramBuilderBtreeIndex builder(subset, env, histogramBucketsCount, yieldHandler);

    ready &= builder.Build<TTableHistogramBuilderBtreeIndex::TGetRowCount>(stats.RowCountHistogram, stats.RowCount);
    ready &= builder.Build<TTableHistogramBuilderBtreeIndex::TGetDataSize>(stats.DataSizeHistogram, stats.DataSize.Size);

    return ready;
}

}
