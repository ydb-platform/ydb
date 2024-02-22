#pragma once

#include "flat_stat_part_group_iter_iface.h"
#include "flat_page_btree_index.h"
#include "flat_table_part.h"

namespace NKikimr::NTable {

class TStatsPartGroupBtreeIndexIterator : public IStatsPartGroupIterator {
    using TCells = NPage::TCells;
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TGroupId = NPage::TGroupId;
    using TRecIdx = NPage::TRecIdx;
    using TChild = TBtreeIndexNode::TChild;
    using TColumns = TBtreeIndexNode::TColumns;
    using TCellsIterable = TBtreeIndexNode::TCellsIterable;
    using TCellsIter = TBtreeIndexNode::TCellsIter;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;

    struct TNodeState {
        TPageId PageId;
        TRowId BeginRowId;
        TRowId EndRowId;
        TCellsIterable BeginKey;
        ui64 DataSize;

        TNodeState(TPageId pageId, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, ui64 dataSize)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , DataSize(dataSize)
        {
        }
    };

public:
    TStatsPartGroupBtreeIndexIterator(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(part->Scheme->GetLayout(groupId))
        , Meta(groupId.IsHistoric() ? part->IndexPages.BTreeHistoric[groupId.Index] : part->IndexPages.BTreeGroups[groupId.Index])
        , GroupChannel(Part->GetGroupChannel(GroupId))
        , NodeIndex(0)
    {
    }
    
    EReady Start() override {
        const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());

        bool ready = true;
        TVector<TNodeState> nextNodes;
        Nodes.emplace_back(Meta.PageId, 0, GetEndRowId(), EmptyKey, Meta.DataSize);

        for (ui32 height = 0; height < Meta.LevelCount; height++) {
            for (auto &nodeState : Nodes) {
                auto page = Env->TryGetPage(Part, nodeState.PageId);
                if (!page) {
                    ready = false;
                    continue;
                }
                TBtreeIndexNode node(*page);

                for (TRecIdx pos : xrange<TRecIdx>(0, node.GetChildrenCount())) {
                    auto& child = node.GetShortChild(pos);

                    TRowId beginRowId = pos ? node.GetShortChild(pos - 1).RowCount : nodeState.BeginRowId;
                    TRowId endRowId = child.RowCount;
                    TCellsIterable beginKey = pos ? node.GetKeyCellsIterable(pos - 1, GroupInfo.ColsKeyIdx) : nodeState.BeginKey;
                    ui64 dataSize = child.DataSize;

                    nextNodes.emplace_back(child.PageId, beginRowId, endRowId, beginKey, dataSize);
                }
            }

            Nodes.swap(nextNodes);
            nextNodes.clear();
        }

        if (!ready) {
            Nodes.clear(); // some invalid subset
            return EReady::Page;
        }

        return DataOrGone();
    }

    EReady Next() override {
        Y_ABORT_UNLESS(IsValid());

        NodeIndex++;
        
        Y_DEBUG_ABORT_UNLESS(NodeIndex == Nodes.size() || Nodes[NodeIndex - 1].EndRowId == Nodes[NodeIndex].BeginRowId);

        return DataOrGone();
    }

    void AddLastDeltaDataSize(TChanneledDataSize& dataSize) override {
        Y_DEBUG_ABORT_UNLESS(NodeIndex);
        ui64 delta = Nodes[NodeIndex - 1].DataSize;
        if (NodeIndex > 1) {
            Y_DEBUG_ABORT_UNLESS(delta >= Nodes[NodeIndex - 2].DataSize);
            delta -= Nodes[NodeIndex - 2].DataSize;
        }
        ui8 channel = Part->GetGroupChannel(GroupId);
        dataSize.Add(delta, channel);
    }

public:
    bool IsValid() const override {
        return NodeIndex < Nodes.size();
    }

    TRowId GetEndRowId() const override {
        return Meta.RowCount;
    }

    TPageId GetPageId() const override {
        return GetCurrentNode().PageId;
    }

    TRowId GetRowId() const override {
        return GetCurrentNode().BeginRowId;
    }

    TPos GetKeyCellsCount() const override {
        return GetCurrentNode().BeginKey.Count();
    }

    TCell GetKeyCell(TPos index) const override {
        return GetCurrentNode().BeginKey.Iter().At(index);
    }

private:
    EReady DataOrGone() const {
        return IsValid() ? EReady::Data : EReady::Gone;
    }

    const TNodeState& GetCurrentNode() const {
        Y_ABORT_UNLESS(IsValid());
        return Nodes[NodeIndex];
    }

private:
    const TPart* const Part;
    IPages* const Env;
    const TGroupId GroupId;
    const TPartScheme::TGroupInfo& GroupInfo;
    const TBtreeIndexMeta Meta;
    ui8 GroupChannel;
    ui32 NodeIndex;
    TVector<TNodeState> Nodes;
};

}
