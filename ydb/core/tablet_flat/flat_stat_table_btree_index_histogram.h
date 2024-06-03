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
        TCellsIterable BeginKey, EndKey;
        ui64 BeginSize, EndSize;

        TNodeState(TPageId pageId, TCellsIterable beginKey, TCellsIterable endKey, TRowId beginSize, TRowId endSize)
            : PageId(pageId)
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

        TPartNodes ForkNew() const noexcept {
            return TPartNodes(Part, Index);
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
    TTableHistogramBuilderBtreeIndex(const TSubset& subset, IPages* env)
        : Subset(subset)
        , KeyDefaults(*Subset.Scheme->Keys)
        , Env(env)
    {
    }

    template <typename TGetSize>
    bool Build(THistogram& histogram, ui64 resolution, ui64 totalSize) {
        Resolution = resolution;

        TVector<TPartNodes> parts;

        for (auto index : xrange(Subset.Flatten.size())) {
            auto& part = Subset.Flatten[index];
            auto& meta = part->IndexPages.GetBTree({});
            parts.emplace_back(part.Part.Get(), index);
            LoadedStateNodes.emplace_back(meta.PageId, EmptyKey, EmptyKey, 0, TGetSize::Get(meta));
            parts.back().PushBack(&LoadedStateNodes.back());
        }

        auto ready = BuildHistogramRecursive<TGetSize>(histogram, parts, 0, totalSize, 0);

        LoadedBTreeNodes.clear();
        LoadedStateNodes.clear();

        return ready;
    }

private:
    template <typename TGetSize>
    bool BuildHistogramRecursive(THistogram& histogram, TVector<TPartNodes>& parts, ui64 beginSize, ui64 endSize, ui32 depth) {
        const static ui32 MaxDepth = 100;
        bool ready = true;

        if (SafeDiff(endSize, beginSize) <= Resolution || depth > MaxDepth) {
            return true;
        }

        auto biggestPart = std::max_element(parts.begin(), parts.end());
        if (Y_UNLIKELY(biggestPart == parts.end())) {
            Y_DEBUG_ABORT("Invalid part states");
            return true;
        }
        Y_ABORT_UNLESS(biggestPart->GetCount());

        // FIXME: load the biggest node if its size more than a half
        if (biggestPart->GetCount() == 1) {
            if (!TryLoadNode<TGetSize>(biggestPart->GetPart(), *biggestPart->PopFront(), [&biggestPart](TNodeState& child) {
                biggestPart->PushBack(&child);
            })) {
                return false;
            }
        }
        TCellsIterable splitKey = FindMiddlePartKey(*biggestPart);

        if (Y_UNLIKELY(!splitKey)) {
            // Note: an extremely rare scenario when we can't split biggest SST
            // this means that we have a lot of parts which total size exceed resolution

            // TODO: pick some
            Y_DEBUG_ABORT("Unimplemented");
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

            leftSize += leftNodes.GetSize();
            middleSize += middleNodes.GetSize();
            rightSize += rightNodes.GetSize();
            
            Y_DEBUG_ABORT_UNLESS(middleNodes.GetCount() <= 1);
        }
        
        if (middleSize > Resolution / 2) {
            std::make_heap(middleParts.begin(), middleParts.end());

            while (middleSize > Resolution / 2 && middleParts.size()) {
                std::pop_heap(middleParts.begin(), middleParts.end());
                auto& middleNodes = middleParts.back();
                auto& leftNodes = GetNextPartNodes(middleNodes, leftParts);
                auto& rightNodes = GetNextPartNodes(middleNodes, rightParts);
                auto rightBuffer = middleNodes.ForkNew();
                
                leftSize -= leftNodes.GetSize();
                middleSize -= middleNodes.GetSize();
                rightSize -= rightNodes.GetSize();

                auto count = middleNodes.GetCount();
                for (auto index : xrange(count)) {
                    Y_UNUSED(index);
                    if (!TryLoadNode<TGetSize>(middleNodes.GetPart(), *middleNodes.PopFront(), [&](TNodeState& node) {
                        if (node.EndKey && CompareKeys(node.EndKey, splitKey) <= 0) {
                            leftNodes.PushBack(&node);
                        } else if (node.BeginKey && CompareKeys(node.BeginKey, splitKey) >= 0) {
                            rightBuffer.PushBack(&node);
                        } else {
                            middleNodes.PushBack(&node);
                        }
                    })) {
                        return false;
                    }
                }

                while (rightBuffer.GetCount()) { // should be reversed
                    rightNodes.PushFront(rightBuffer.PopBack());
                }

                leftSize += leftNodes.GetSize();
                middleSize += middleNodes.GetSize();
                rightSize += rightNodes.GetSize();

                Y_DEBUG_ABORT_UNLESS(middleNodes.GetCount() <= 1);

                // TODO: add back to heap

            }
        }

        // TODO: don't copy nodes?

        ready &= BuildHistogramRecursive<TGetSize>(histogram, leftParts, beginSize, beginSize + leftSize, depth + 1);
        
        ui64 splitValue = beginSize + leftSize + middleSize / 2;
        // TODO:
        // splitValue = Min(splitValue, endSize);
        // splitValue = Max(splitValue, beginSize);
        AddBucket(histogram, splitKey, splitValue);

        ready &= BuildHistogramRecursive<TGetSize>(histogram, rightParts, SafeDiff(endSize, rightSize), endSize, depth + 1);

        return ready;
    }

    TCellsIterable FindMiddlePartKey(const TPartNodes& part) {
        Y_ABORT_UNLESS(part.GetCount());
        
        TCellsIterable splitKey = EmptyKey;
        ui64 splitSize = 0, currentSize = 0;
        const ui64 middleSize = part.GetSize() / 2;
        
        for (const auto& node : part.GetNodes()) {
            if (!splitKey || AbsDifference(currentSize, middleSize) < AbsDifference(splitSize, middleSize)) {
                splitKey = node.BeginKey;
                splitSize = currentSize;
            }

            currentSize += node.GetSize();
        }

        return splitKey;
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

    template <typename TGetSize>
    bool TryLoadNode(const TPart* part, const TNodeState& node, const auto& addNode) {
        // TODO: track level
        auto page = Env->TryGetPage(part, node.PageId, {});
        if (!page) {
            return false;
        }

        LoadedBTreeNodes.emplace_back(*page);
        auto &bTreeNode = LoadedBTreeNodes.back();
        auto& groupInfo = part->Scheme->GetLayout({});

        ui64 currentBeginSize = node.BeginSize;
        for (auto pos : xrange(bTreeNode.GetChildrenCount())) {
            auto& child = bTreeNode.GetChild(pos);

            LoadedStateNodes.emplace_back(child.PageId, 
                pos ? bTreeNode.GetKeyCellsIterable(pos - 1, groupInfo.ColsKeyData) : node.BeginKey,
                pos < bTreeNode.GetKeysCount() ? bTreeNode.GetKeyCellsIterable(pos, groupInfo.ColsKeyData) : node.EndKey,
                currentBeginSize, TGetSize::Get(child));

            currentBeginSize = LoadedStateNodes.back().EndSize;

            addNode(LoadedStateNodes.back());
        }

        return true;
    }

    TPartNodes& PushNextPartNodes(const TPartNodes& part, TVector<TPartNodes>& list) const {
        Y_ABORT_UNLESS(part.GetIndex() == list.size());
        list.push_back(part.ForkNew());
        return list.back();
    }

    TPartNodes& GetNextPartNodes(const TPartNodes& part, TVector<TPartNodes>& list) const {
        Y_ABORT_UNLESS(part.GetPart() == list[part.GetIndex()].GetPart());
        return list[part.GetIndex()];
    }

private:
    int CompareKeys(const TCellsIterable& left_, const TCellsIterable& right_) const {
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
    ui64 Resolution;
    TDeque<TBtreeIndexNode> LoadedBTreeNodes; // keep nodes to use TCellsIterable key refs
    TDeque<TNodeState> LoadedStateNodes; // keep nodes to use TIntrusiveList
};

}

inline bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env) {
    bool ready = true;
    
    TTableHistogramBuilderBtreeIndex builder(subset, env);

    ready &= builder.Build<TTableHistogramBuilderBtreeIndex::TGetRowCount>(stats.RowCountHistogram, rowCountResolution, stats.RowCount);
    ready &= builder.Build<TTableHistogramBuilderBtreeIndex::TGetDataSize>(stats.DataSizeHistogram, dataSizeResolution, stats.DataSize.Size);

    return ready;
}

}