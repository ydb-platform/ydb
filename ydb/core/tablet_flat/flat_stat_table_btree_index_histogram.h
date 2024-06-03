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
    struct TNodeState {
        TPageId PageId;
        TCellsIterable BeginKey, EndKey;
        TRowId BeginRowId, EndRowId;
        ui64 BeginDataSize, EndDataSize;

        TNodeState(TPageId pageId, TCellsIterable beginKey, TCellsIterable endKey, TRowId beginRowId, TRowId endRowId, ui64 beginDataSize, ui64 endDataSize)
            : PageId(pageId)
            , BeginKey(beginKey)
            , EndKey(endKey)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginDataSize(beginDataSize)
            , EndDataSize(endDataSize)
        {
        }
    };

    struct TGetRowCount {
        static ui64 Get(const TNodeState& node) noexcept {
            return node.EndRowId - node.BeginRowId;
        }
    };

    struct TGetDataSize {
        static ui64 Get(const TNodeState& node) noexcept {
            return node.EndDataSize - node.BeginDataSize;
        }
    };

private:
    struct TPartNodes {
        const TPart* Part;
        TVector<TNodeState> Nodes;
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
        LoadedNodes.clear();

        TVector<TPartNodes> parts;

        for (const auto& part : Subset.Flatten) {
            auto& meta = part->IndexPages.GetBTree({});
            parts.emplace_back(part.Part.Get(), TVector<TNodeState>{
                {meta.PageId, EmptyKey, EmptyKey, 0, meta.RowCount, 0, meta.GetTotalDataSize()}
            });
        }

        auto ready = BuildHistogramRecursive<TGetSize>(histogram, parts, 0, totalSize, 0);

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

        size_t biggestPartIndex = Max<size_t>();
        ui64 biggestPartSize = 0;
        FindBiggestPart<TGetSize>(parts, biggestPartIndex, biggestPartSize);
        if (Y_UNLIKELY(biggestPartIndex == Max<size_t>())) {
            Y_DEBUG_ABORT("Invalid part states");
            return true;
        }
        auto& biggestPart = parts[biggestPartIndex];

        // FIXME: load the biggest node if its size more than a half
        if (biggestPart.Nodes.size() == 1) {
            auto node = biggestPart.Nodes.front();
            biggestPart.Nodes.clear();
            if (!TryLoadNode(biggestPart.Part, node, biggestPart.Nodes)) {
                return false;
            }
        }
        TCellsIterable splitKey = FindMiddlePartKey<TGetSize>(biggestPart, biggestPartSize);

        if (Y_UNLIKELY(!splitKey)) {
            // Note: an extremely rare scenario when we can't split biggest SST
            // this means that we have a lot of parts which total size exceed resolution

            // TODO: pick some
            Y_DEBUG_ABORT("Unimplemented");
            return true;
        }

        ui64 leftSize = 0, middleSize = 0, rightSize = 0;
        TVector<TNodeState> middleNodes;

        TVector<TPartNodes> leftParts, middleParts, rightParts;
        for (const auto& part : parts) {
            leftParts.emplace_back(part.Part, TVector<TNodeState>());
            middleParts.emplace_back(part.Part, TVector<TNodeState>());
            rightParts.emplace_back(part.Part, TVector<TNodeState>());

            for (const auto& node : part.Nodes) {
                if (node.EndKey && CompareKeys(node.EndKey, splitKey) <= 0) {
                    leftSize += TGetSize::Get(node);
                    leftParts.back().Nodes.push_back(node);
                } else if (node.BeginKey && CompareKeys(node.BeginKey, splitKey) >= 0) {
                    rightSize += TGetSize::Get(node);
                    rightParts.back().Nodes.push_back(node);
                } else {
                    middleSize += TGetSize::Get(node);
                    middleParts.back().Nodes.push_back(node);
                }
            }
        }
        
        // TODO: clear parts

        // TODO: process middleNodes
        Y_ABORT_UNLESS(middleSize <= Resolution / 2);

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

    template <typename TGetSize>
    void FindBiggestPart(const TVector<TPartNodes>& parts, size_t& biggestPartIndex, ui64& biggestPartSize) {
        for (auto index : xrange(parts.size())) {
            ui64 size = GetPartSize<TGetSize>(parts[index]);
            if (size > biggestPartSize) {
                biggestPartSize = size;
                biggestPartIndex = index;
            }
        }
    }

    template <typename TGetSize>
    ui64 GetPartSize(const TPartNodes part) {
        ui64 size = 0;
        for (const auto& node : part.Nodes) {
            size += TGetSize::Get(node);
        }
        return size;
    }

    template <typename TGetSize>
    TCellsIterable FindMiddlePartKey(const TPartNodes& part, ui64& partSize) {
        Y_ABORT_UNLESS(part.Nodes);
        
        TCellsIterable splitKey = EmptyKey;
        ui64 splitSize = 0, currentSize = 0;
        const ui64 middleSize = partSize / 2;
        
        for (const auto& node : part.Nodes) {
            if (!splitKey || AbsDifference(currentSize, middleSize) < AbsDifference(splitSize, middleSize)) {
                splitKey = node.BeginKey;
                splitSize = currentSize;
            }

            currentSize += TGetSize::Get(node);
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

    bool TryLoadNode(const TPart* part, const TNodeState& node, TVector<TNodeState>& list) {
        auto page = Env->TryGetPage(part, node.PageId, {});
        if (!page) {
            return false;
        }

        LoadedNodes.emplace_back(*page);
        auto &loadedNode = LoadedNodes.back();
        auto& groupInfo = part->Scheme->GetLayout({});

        TRowId currentBeginRowId = node.BeginRowId;
        ui64 currentBeginDataSize = node.BeginDataSize;
        for (auto pos : xrange(loadedNode.GetChildrenCount())) {
            auto& child = loadedNode.GetChild(pos);

            list.emplace_back(child.PageId, 
                pos ? loadedNode.GetKeyCellsIterable(pos - 1, groupInfo.ColsKeyData) : node.BeginKey,
                pos < loadedNode.GetKeysCount() ? loadedNode.GetKeyCellsIterable(pos, groupInfo.ColsKeyData) : node.EndKey,
                currentBeginRowId, child.RowCount, 
                currentBeginDataSize, child.GetTotalDataSize());

            currentBeginRowId = list.back().EndRowId;
            currentBeginDataSize = list.back().EndDataSize;
        }

        return true;
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
    TDeque<TBtreeIndexNode> LoadedNodes; // keep nodes to use TCellsIterable key refs
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