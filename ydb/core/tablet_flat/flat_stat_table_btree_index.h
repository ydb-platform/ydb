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

};

struct TGetDataSize {

};

ui64 GetPrevDataSize(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
    auto& meta = part->IndexPages.GetBTree(groupId);

    if (rowId == 0) {
        return 0;
    }
    if (rowId >= meta.RowCount) {
        return meta.DataSize;
    }

    TPageId pageId = meta.PageId;
    ui64 prevDataSize = 0;

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return prevDataSize;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(rowId);

        pageId = node.GetShortChild(pos).PageId;
        if (pos) {
            prevDataSize = node.GetShortChild(pos - 1).DataSize;
        }
    }

    return prevDataSize;
}

ui64 GetPrevHistoricDataSize(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, TRowId& historicRowId, bool& ready) {
    Y_ABORT_UNLESS(groupId == TGroupId(0, true));

    auto& meta = part->IndexPages.GetBTree(groupId);

    if (rowId == 0) {
        return 0;
    }
    if (rowId >= part->IndexPages.GetBTree({}).RowCount) {
        historicRowId = meta.RowCount;
        return meta.DataSize;
    }

    TPageId pageId = meta.PageId;
    ui64 prevDataSize = 0;
    historicRowId = 0;

    // Minimum key is (startRowId, max, max)
    ui64 startStep = Max<ui64>();
    ui64 startTxId = Max<ui64>();
    TCell key1Cells[3] = {
        TCell::Make(rowId),
        TCell::Make(startStep),
        TCell::Make(startTxId),
    };
    TCells key1{ key1Cells, 3 };

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return prevDataSize;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(ESeek::Lower, key1, part->Scheme->HistoryGroup.ColsKeyIdx, part->Scheme->HistoryKeys.Get());

        pageId = node.GetShortChild(pos).PageId;
        if (pos) {
            const auto& prevChild = node.GetShortChild(pos - 1);
            prevDataSize = prevChild.DataSize;
            historicRowId = prevChild.RowCount;
        }
    }

    return prevDataSize;
}

void AddBlobsSize(const TPart* part, TChanneledDataSize& stats, const TFrames* frames, ELargeObj lob, TRowId beginRowId, TRowId endRowId) noexcept {
    ui32 page = frames->Lower(beginRowId, 0, Max<ui32>());

    while (auto &rel = frames->Relation(page)) {
        if (rel.Row < endRowId) {
            auto channel = part->GetPageChannel(lob, page);
            stats.Add(rel.Size, channel);
            ++page;
        } else if (!rel.IsHead()) {
            Y_ABORT("Got unaligned TFrames head record");
        } else {
            break;
        }
    }
}

bool AddDataSize(const TPartView& part, TStats& stats, IPages* env) {
    bool ready = true;

    if (!part.Slices || part.Slices->empty()) {
        return true;
    }

    if (part->GroupsCount) { // main group
        TGroupId groupId{};
        auto channel = part->GetGroupChannel(groupId);
        
        for (const auto& slice : *part.Slices) {
            stats.RowCount += slice.EndRowId() - slice.BeginRowId();
            
            ui64 beginDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.BeginRowId(), env, ready);
            ui64 endDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.EndRowId(), env, ready);
            if (ready && endDataSize > beginDataSize) {
                stats.DataSize.Add(endDataSize - beginDataSize, channel);
            }

            if (part->Small) {
                AddBlobsSize(part.Part.Get(), stats.DataSize, part->Small.Get(), ELargeObj::Outer, slice.BeginRowId(), slice.EndRowId());
            }
            if (part->Large) {
                AddBlobsSize(part.Part.Get(), stats.DataSize, part->Large.Get(), ELargeObj::Extern, slice.BeginRowId(), slice.EndRowId());
            }
        }
    }

    for (ui32 groupIndex : xrange<ui32>(1, part->GroupsCount)) {
        TGroupId groupId{groupIndex};
        auto channel = part->GetGroupChannel(groupId);
        for (const auto& slice : *part.Slices) {
            ui64 beginDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.BeginRowId(), env, ready);
            ui64 endDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.EndRowId(), env, ready);
            if (ready && endDataSize > beginDataSize) {
                stats.DataSize.Add(endDataSize - beginDataSize, channel);
            }
        }
    }

    TVector<std::pair<TRowId, TRowId>> historicSlices;

    if (part->HistoricGroupsCount) { // main historic group
        TGroupId groupId{0, true};
        auto channel = part->GetGroupChannel(groupId);
        for (const auto& slice : *part.Slices) {
            TRowId beginRowId, endRowId;
            bool readySlice = true;
            ui64 beginDataSize = GetPrevHistoricDataSize(part.Part.Get(), groupId, slice.BeginRowId(), env, beginRowId, readySlice);
            ui64 endDataSize = GetPrevHistoricDataSize(part.Part.Get(), groupId, slice.EndRowId(), env, endRowId, readySlice);
            ready &= readySlice;
            if (ready && endDataSize > beginDataSize) {
                stats.DataSize.Add(endDataSize - beginDataSize, channel);
            }
            if (readySlice && endRowId > beginRowId) {
                historicSlices.emplace_back(beginRowId, endRowId);
            }
        }
    }

    for (ui32 groupIndex : xrange<ui32>(1, part->HistoricGroupsCount)) {
        TGroupId groupId{groupIndex, true};
        auto channel = part->GetGroupChannel(groupId);
        for (const auto& slice : historicSlices) {
            ui64 beginDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.first, env, ready);
            ui64 endDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.second, env, ready);
            if (ready && endDataSize > beginDataSize) {
                stats.DataSize.Add(endDataSize - beginDataSize, channel);
            }
        }
    }

    return ready;
}

void BuildHistogramRecursive(const TVector<TVector<TNodeState>>& parts, THistogram& histogram, ui64 resolution, ui64 beginValue, ui64 endValue, ui32 deep, IPages* env) {
    const static ui32 MaxDeep = 100;
    if (beginValue > endValue || endValue - beginValue <= resolution || deep > MaxDeep) {
        return;
    }

    size_t biggestPartIndex = Max<size_t>();
    ui64 biggestPartSize = 0;
    for (auto index : xrange(parts.size())) {
        ui64 size = 0;
        for (const auto& node : parts[index]) {
            size += node.EndRowId - node.BeginRowId;
        }
        if (size > biggestPartSize) {
            biggestPartSize = size;
            biggestPartIndex = index;
        }
    }

    if (Y_UNLIKELY(biggestPartIndex == Max<size_t>())) {
        Y_DEBUG_ABORT("Invalid part states");
    }


}

void BuildHistogram(const TSubset& subset, THistogram& histogram, ui64 resolution, ui64 total, IPages* env) {
    const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());
    
    TVector<TVector<TNodeState>> parts;

    for (const auto& part : subset.Flatten) {
        auto& meta = part->IndexPages.GetBTree({});
        parts.emplace_back();
        parts.back().emplace_back(meta.PageId, 0, meta.RowCount, EmptyKey, EmptyKey, 0, meta.GetTotalDataSize());
    }

    BuildHistogramRecursive(parts, histogram, resolution, 0, total, env);

    for (auto& bucket : histogram) {
        bucket.Value = Min(bucket.Value, total);
    }
}

}

inline bool BuildStatsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env) {
    stats.Clear();

    bool ready = true;
    for (const auto& part : subset.Flatten) {
        stats.IndexSize.Add(part->IndexesRawSize, part->Label.Channel());
        ready &= AddDataSize(part, stats, env);
    }

    if (!ready) {
        return false;
    }

    BuildHistogram(subset, stats.RowCountHistogram, rowCountResolution, stats.RowCount, env);
    Y_UNUSED(dataSizeResolution);

    return true;
}

}
