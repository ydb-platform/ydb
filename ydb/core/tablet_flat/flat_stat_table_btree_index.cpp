#include "flat_stat_table.h"
#include "flat_table_subset.h"
#include "flat_stat_table_btree_index.h"

namespace NKikimr::NTable {

namespace {

using TGroupId = NPage::TGroupId;
using TFrames = NPage::TFrames;
using TBtreeIndexNode = NPage::TBtreeIndexNode;
using TChild = TBtreeIndexNode::TChild;
using TColumns = TBtreeIndexNode::TColumns;
using TCells = NPage::TCells;

ui64 GetPrevDataSize(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
    auto& meta = part->IndexPages.GetBTree(groupId);

    if (rowId == 0) {
        return 0;
    }
    if (rowId >= meta.GetRowCount()) {
        return meta.GetDataSize();
    }

    TPageId pageId = meta.GetPageId();
    ui64 prevDataSize = 0;

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return prevDataSize;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(rowId);

        pageId = node.GetShortChild(pos).GetPageId();
        if (pos) {
            prevDataSize = node.GetShortChild(pos - 1).GetDataSize();
        }
    }

    return prevDataSize;
}

ui64 GetPrevHistoricDataSize(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, TRowId& historicRowId, bool& ready) {
    Y_ABORT_UNLESS(groupId == TGroupId(0, true));

    auto& meta = part->IndexPages.GetBTree(groupId);

    if (rowId == 0) {
        historicRowId = 0;
        return 0;
    }
    if (rowId >= part->IndexPages.GetBTree({}).GetRowCount()) {
        historicRowId = meta.GetRowCount();
        return meta.GetDataSize();
    }

    TPageId pageId = meta.GetPageId();
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

        pageId = node.GetShortChild(pos).GetPageId();
        if (pos) {
            const auto& prevChild = node.GetShortChild(pos - 1);
            prevDataSize = prevChild.GetDataSize();
            historicRowId = prevChild.GetRowCount();
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

bool AddDataSize(const TPartView& part, TStats& stats, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    bool ready = true;

    if (!part.Slices || part.Slices->empty()) {
        return true;
    }

    if (part->GroupsCount) { // main group
        TGroupId groupId{};
        auto channel = part->GetGroupChannel(groupId);
        
        for (const auto& slice : *part.Slices) {
            yieldHandler();

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
            yieldHandler();
            
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
            yieldHandler();
            
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
            yieldHandler();
            
            ui64 beginDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.first, env, ready);
            ui64 endDataSize = GetPrevDataSize(part.Part.Get(), groupId, slice.second, env, ready);
            if (ready && endDataSize > beginDataSize) {
                stats.DataSize.Add(endDataSize - beginDataSize, channel);
            }
        }
    }

    return ready;
}

}

bool BuildStatsBTreeIndex(const TSubset& subset, TStats& stats, ui32 histogramBucketsCount, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    stats.Clear();

    bool ready = true;
    for (const auto& part : subset.Flatten) {
        stats.IndexSize.Add(part->IndexesRawSize, part->Label.Channel());
        stats.ByKeyFilterSize += part->ByKey ? part->ByKey->Raw.size() : 0;
        ready &= AddDataSize(part, stats, env, yieldHandler);
    }

    if (!ready) {
        return false;
    }

    ready &= BuildStatsHistogramsBTreeIndex(subset, stats, 
        stats.RowCount / histogramBucketsCount, stats.DataSize.Size / histogramBucketsCount, 
        env, yieldHandler);

    return ready;
}

}
