#include "flat_part_index_iter_bree_index.h"
#include "flat_stat_table.h"
#include "flat_table_subset.h"
#include "library/cpp/int128/int128.h"

namespace NKikimr {
namespace NTable {

namespace {

using TGroupId = NPage::TGroupId;
using TFrames = NPage::TFrames;
using TBtreeIndexNode = NPage::TBtreeIndexNode;
using TChild = TBtreeIndexNode::TChild;
using TCells = NPage::TCells;

TChild GetPrevChild(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
    auto& meta = part->IndexPages.GetBTree(groupId);

    TPageId pageId = meta.PageId;
    TChild result{0, 0, 0, 0, 0};

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return result;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(rowId);
        pageId = node.GetShortChild(pos).PageId;
        if (pos) {
            if (node.IsShortChildFormat()) {
                auto& child = node.GetShortChild(pos - 1);
                result = {child.PageId, child.RowCount, child.DataSize, 0, 0};
            } else {
                result = node.GetChild(pos - 1);
            }
        }
    }

    return result;
}

TChild GetChild(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
    auto& meta = part->IndexPages.GetBTree(groupId);

    TPageId pageId = meta.PageId;
    TChild result = meta;

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return result;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(rowId);
        pageId = node.GetShortChild(pos).PageId;
        if (node.IsShortChildFormat()) {
            auto& child = node.GetShortChild(pos);
            result = {child.PageId, child.RowCount, child.DataSize, 0, 0};
        } else {
            result = node.GetChild(pos);
        }
    }

    return result;
}

TChild GetPrevHistoryChild(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
    auto& meta = part->IndexPages.GetBTree(groupId);

    TPageId pageId = meta.PageId;
    TChild result{0, 0, 0, 0, 0};

    // Minimum key is (rowId, max, max)
    ui64 startStep = Max<ui64>();
    ui64 startTxId = Max<ui64>();
    TCell keyCells[3] = {
        TCell::Make(rowId),
        TCell::Make(startStep),
        TCell::Make(startTxId),
    };
    TCells key{ keyCells, 3 };

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return result;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(ESeek::Lower, key, part->Scheme->HistoryGroup.ColsKeyIdx, part->Scheme->HistoryKeys.Get());
        pageId = node.GetShortChild(pos).PageId;
        if (pos) {
            if (node.IsShortChildFormat()) {
                auto& child = node.GetShortChild(pos - 1);
                result = {child.PageId, child.RowCount, child.DataSize, 0, 0};
            } else {
                result = node.GetChild(pos - 1);
            }
        }
    }

    return result;
}

TChild GetHistoryChild(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
    auto& meta = part->IndexPages.GetBTree(groupId);

    TPageId pageId = meta.PageId;
    TChild result = meta;

    // Maximum key is (rowId, 0, 0)
    ui64 endStep = 0;
    ui64 endTxId = 0;
    TCell keyCells[3] = {
        TCell::Make(rowId),
        TCell::Make(endStep),
        TCell::Make(endTxId),
    };
    TCells key{ keyCells, 3 };

    for (ui32 height = 0; height < meta.LevelCount; height++) {
        auto page = env->TryGetPage(part, pageId, {});
        if (!page) {
            ready = false;
            return result;
        }
        auto node = TBtreeIndexNode(*page);
        auto pos = node.Seek(ESeek::Lower, key, part->Scheme->HistoryGroup.ColsKeyIdx, part->Scheme->HistoryKeys.Get());
        pageId = node.GetShortChild(pos).PageId;
        if (node.IsShortChildFormat()) {
            auto& child = node.GetShortChild(pos);
            result = {child.PageId, child.RowCount, child.DataSize, 0, 0};
        } else {
            result = node.GetChild(pos);
        }
    }

    return result;
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

void AddSliceDataSize(TStats& stats, ui8 channel, const TChild& prevChild, const TChild& lastChild, TRowId beginRowId, TRowId endRowId) {
    Y_DEBUG_ABORT_UNLESS(lastChild.DataSize > prevChild.DataSize);
    Y_DEBUG_ABORT_UNLESS(lastChild.RowCount > prevChild.RowCount);

    if (Y_LIKELY(lastChild.DataSize > prevChild.DataSize && lastChild.RowCount > prevChild.RowCount)) {
        TRowId sliceRows = endRowId - beginRowId;
        TRowId countedRows = lastChild.RowCount - prevChild.RowCount;
        Y_DEBUG_ABORT_UNLESS(sliceRows <= countedRows);

        if (sliceRows == countedRows) {
            stats.DataSize.Add(lastChild.DataSize - prevChild.DataSize, channel);
        } else {
            ui128 countedDataSize = lastChild.DataSize - prevChild.DataSize;
            ui64 sliceDataSize = static_cast<ui64>(countedDataSize * sliceRows / countedRows);
            stats.DataSize.Add(sliceDataSize, channel);
        }
    }
}

ui64 GetBeginDataSize(TPartGroupBtreeIndexIter iter, TRowId rowId, bool& ready) {
    auto seek = iter.Seek(rowId);
    if (seek == EReady::Page) {
        ready = false;
        return 0;
    }

    auto node = iter.GetNode();
    if (node.BeginRowId == rowId) {
        return node.B
    }
}

bool AddDataSize(const TPartView& part, TStats& stats, IPages* env) {
    bool ready = true;

    if (!part.Slices || part.Slices->empty()) {
        return true;
    }

    { // main group
        TGroupId groupId{};
        auto channel = part->GetGroupChannel(groupId);
        TPartGroupBtreeIndexIter iter(part.Part.Get(), env, groupId);

        for (const auto& slice : *part.Slices) {
            iter.Seek(slice.BeginRowId())

            auto prevChild = GetPrevChild(part.Part.Get(), groupId, slice.BeginRowId(), env, ready);
            auto lastChild = GetChild(part.Part.Get(), groupId, slice.EndRowId() - 1, env, ready);
            if (!ready) {
                continue;
            }

            stats.RowCount += slice.EndRowId() - slice.BeginRowId();
            
            AddSliceDataSize(stats, channel, prevChild, lastChild, slice.BeginRowId(), slice.EndRowId());

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
            auto prevChild = GetPrevChild(part.Part.Get(), groupId, slice.BeginRowId(), env, ready);
            auto lastChild = GetChild(part.Part.Get(), groupId, slice.EndRowId() - 1, env, ready);
            if (!ready) {
                continue;
            }

            AddSliceDataSize(stats, channel, prevChild, lastChild, slice.BeginRowId(), slice.EndRowId());
        }
    }

    if (part->HistoricGroupsCount) { // main history group
        TGroupId groupId{0, true};
        auto channel = part->GetGroupChannel(groupId);
        for (const auto& slice : *part.Slices) {
            auto prevChild = GetPrevHistoryChild(part.Part.Get(), groupId, slice.BeginRowId(), env, ready);
            auto lastChild = GetHistoryChild(part.Part.Get(), groupId, slice.EndRowId() - 1, env, ready);
            if (!ready) {
                continue;
            }

            // TODO: don't count twice
            AddSliceDataSize(stats, channel, prevChild, lastChild, prevChild.RowCount, lastChild.RowCount);
        }
    }

    return ready;
}

}

inline bool BuildStatsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler) {
    stats.Clear();

    Y_UNUSED(rowCountResolution, dataSizeResolution);

    bool ready = true;
    for (const auto& part : subset.Flatten) {
        stats.IndexSize.Add(part->IndexesRawSize, part->Label.Channel());
        ready &= AddDataSize(part, stats, env);
    }

    if (!ready) {
        return false;
    }

    // TODO: build histogram here

    return true;
}

}}
