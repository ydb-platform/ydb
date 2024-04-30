#include "flat_stat_table.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

namespace {
    using TGroupId = NPage::TGroupId;
    using TFrames = NPage::TFrames;
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TChild = TBtreeIndexNode::TChild;

    TChild GetPrevChild(const TPart* part, TGroupId groupId, TRowId rowId, IPages* env, bool& ready) {
        auto& meta = part->IndexPages.GetBTree(groupId);

        if (rowId >= meta.RowCount) {
            return meta;
        }

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

        for (ui32 groupIndex : xrange(part->GroupsCount)) {
            auto channel = part->GetGroupChannel(TGroupId(groupIndex));
            for (const auto& slice : *part.Slices) {
                auto beginChild = GetPrevChild(part.Part.Get(), {}, slice.BeginRowId(), env, ready);
                auto endChild = GetPrevChild(part.Part.Get(), {}, slice.EndRowId(), env, ready);
                if (ready) {
                    stats.RowCount += endChild.RowCount - beginChild.RowCount;
                    stats.DataSize.Add(endChild.DataSize - beginChild.DataSize, channel);
                }

                if (part->Small) {
                    AddBlobsSize(part.Part.Get(), stats.DataSize, part->Small.Get(), ELargeObj::Outer, slice.BeginRowId(), slice.EndRowId());
                }
                if (part->Large) {
                    AddBlobsSize(part.Part.Get(), stats.DataSize, part->Large.Get(), ELargeObj::Extern, slice.BeginRowId(), slice.EndRowId());
                }
            }
        }

        return ready;
    }

}

inline bool BuildStatsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env) {
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
