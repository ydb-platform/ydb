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

const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());

class TTableHistogramBuilderBtreeIndex {
public:
    struct TNodeState {
        TPageId PageId;
        TRowId BeginRowId;
        TRowId EndRowId;
        TCellsIterable BeginKey;
        TCellsIterable EndKey;
        ui64 BeginDataSize;
        ui64 EndDataSize;

        TNodeState(TPageId pageId, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, TCellsIterable endKey, ui64 beginDataSize, ui64 endDataSize)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , EndKey(endKey)
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

    TTableHistogramBuilderBtreeIndex(const TSubset& subset, IPages* env)
        : Subset(subset)
        , Env(env)
    {
    }

    template <typename TGetSize>
    bool Build(THistogram& histogram, ui64 resolution, ui64 totalSize) {
        Resolution = resolution;

        TVector<TVector<TNodeState>> parts;

        for (const auto& part : Subset.Flatten) {
            auto& meta = part->IndexPages.GetBTree({});
            parts.emplace_back();
            parts.back().emplace_back(meta.PageId, 0, meta.RowCount, EmptyKey, EmptyKey, 0, meta.GetTotalDataSize());
        }

        auto ready = BuildHistogramRecursive<TGetSize>(histogram, parts, 0, totalSize, 0);

        // Note: some values may exceed total due to different calculation approaches
        for (auto& bucket : histogram) {
            bucket.Value = Min(bucket.Value, totalSize);
        }

        return ready;
    }

private:
    template <typename TGetSize>
    bool BuildHistogramRecursive(THistogram& histogram, TVector<TVector<TNodeState>>& parts, ui64 beginSize, ui64 endSize, ui32 depth) {
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

        TCellsIterable splitKey = FindMiddlePartKey<TGetSize>(parts[biggestPartIndex], biggestPartSize);

        if (Y_UNLIKELY(!splitKey)) {
            // Note: an extremely rare scenario when we can't split biggest SST
            // this means that we have a lot of parts which total size exceed resolution

            // TODO: pick some
            Y_DEBUG_ABORT("Unimplemented");
            return true;
        }

        ui64 splitSize = 0; // TODO: beginSize?
        ui64 leftSize = 0, rightSize = 0;

        TVector<TVector<TNodeState>> leftParts, rightParts;
        // for (const auto& part : parts) {

        // }

        ready &= BuildHistogramRecursive<TGetSize>(histogram, leftParts, beginSize, beginSize + leftSize, depth + 1);
        
        

        ready &= BuildHistogramRecursive<TGetSize>(histogram, rightParts, SafeDiff(endSize, rightSize), endSize, depth + 1);

        return ready;
    }

    template <typename TGetSize>
    void FindBiggestPart(const TVector<TVector<TNodeState>>& parts, size_t& biggestPartIndex, ui64& biggestPartSize) {
        for (auto index : xrange(parts.size())) {
            ui64 size = 0;
            for (const auto& node : parts[index]) {
                size += TGetSize::Get(node);
            }
            if (size > biggestPartSize) {
                biggestPartSize = size;
                biggestPartIndex = index;
            }
        }
    }

    template <typename TGetSize>
    TCellsIterable FindMiddlePartKey(TVector<TNodeState>& part, ui64& partSize) {
        Y_ABORT_UNLESS(part);
        
        TCellsIterable splitKey = EmptyKey;
        ui64 splitSize = 0, currentSize = 0;
        const ui64 middleSize = partSize / 2;
        
        for (const auto& node : part) {
            if (!splitKey || AbsDifference(currentSize, middleSize) < AbsDifference(splitSize, middleSize)) {
                splitKey = node.BeginKey;
                splitSize = currentSize;
            }

            currentSize += TGetSize::Get(node);
        }

        // TODO: load node
        Y_UNUSED(Env);

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
        for (TPos index = splitKeyCells.size(); index < Subset.Scheme->Keys->Defs.size(); ++index) {
            splitKeyCells.push_back(Subset.Scheme->Keys->Defs[index]);
        }

        TString serializedSplitKey = TSerializedCellVec::Serialize(splitKeyCells);

        histogram.push_back({serializedSplitKey, value});
    }

private:
    ui64 AbsDifference(ui64 a, ui64 b) {
        return static_cast<ui64>(std::abs(static_cast<i64>(a) - static_cast<i64>(b)));
    }

    ui64 SafeDiff(ui64 a, ui64 b) {
        return a - Min(a, b);
    }

private:
    const TSubset& Subset;
    IPages* const Env;
    ui64 Resolution;
};

}

bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env) {
    bool ready = true;
    
    TTableHistogramBuilderBtreeIndex builder(subset, env);

    ready &= builder.Build<TTableHistogramBuilderBtreeIndex::TGetRowCount>(stats.RowCountHistogram, rowCountResolution, stats.RowCount);
    ready &= builder.Build<TTableHistogramBuilderBtreeIndex::TGetDataSize>(stats.DataSizeHistogram, dataSizeResolution, stats.DataSize.Size);

    return ready;
}

}