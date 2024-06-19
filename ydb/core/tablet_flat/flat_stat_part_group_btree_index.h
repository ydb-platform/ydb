#pragma once

#include "flat_stat_part_group_iter_iface.h"
#include "flat_page_btree_index.h"
#include "flat_table_part.h"

namespace NKikimr::NTable {

class TStatsPartGroupBtreeIndexIter : public IStatsPartGroupIter {
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
        ui64 BeginDataSize;
        ui64 EndDataSize;

        TNodeState(TPageId pageId, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, ui64 beginDataSize, ui64 endDataSize)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , BeginDataSize(beginDataSize)
            , EndDataSize(endDataSize)
        {
        }
    };

public:
    TStatsPartGroupBtreeIndexIter(const TPart* part, IPages* env, TGroupId groupId,
            ui64 rowCountResolution, ui64 dataSizeResolution, const TVector<TRowId>& splitPoints)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(part->Scheme->GetLayout(groupId))
        , Meta(groupId.IsHistoric() ? part->IndexPages.BTreeHistoric[groupId.Index] : part->IndexPages.BTreeGroups[groupId.Index])
        , GroupChannel(Part->GetGroupChannel(GroupId))
        , NodeIndex(0)
        , RowCountResolution(rowCountResolution)
        , DataSizeResolution(dataSizeResolution)
        , SplitPoints(splitPoints) // make copy for Start
    {
        Y_DEBUG_ABORT_UNLESS(std::is_sorted(SplitPoints.begin(), SplitPoints.end()));
    }
    
    EReady Start() override {
        const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());

        bool ready = true;
        TVector<TNodeState> nextNodes;
        Nodes.emplace_back(Meta.GetPageId(), 0, GetEndRowId(), EmptyKey, 0, Meta.GetDataSize());

        for (ui32 height = 0; height < Meta.LevelCount; height++) {
            bool hasChanges = false;
            size_t splitPointIndex = 0;

            for (auto &nodeState : Nodes) {
                while (splitPointIndex < SplitPoints.size() && SplitPoints[splitPointIndex] < nodeState.BeginRowId) {
                    splitPointIndex++;
                }
                if (splitPointIndex < SplitPoints.size() && SplitPoints[splitPointIndex] < nodeState.EndRowId) {
                    // split node and go deeper
                } else if (nodeState.EndRowId - nodeState.BeginRowId <= RowCountResolution
                        && nodeState.EndDataSize - nodeState.BeginDataSize <= DataSizeResolution) {
                    nextNodes.push_back(nodeState); // lift current node on the next level as-is
                    continue; // don't go deeper
                }

                auto page = Env->TryGetPage(Part, nodeState.PageId, {});
                if (!page) {
                    ready = false;
                    continue; // continue requesting other nodes
                }
                TBtreeIndexNode node(*page);

                for (TRecIdx pos : xrange<TRecIdx>(0, node.GetChildrenCount())) {
                    auto& child = node.GetShortChild(pos);

                    TPageId pageId = child.GetPageId();
                    TRowId beginRowId = pos ? node.GetShortChild(pos - 1).GetRowCount() : nodeState.BeginRowId;
                    TRowId endRowId = child.GetRowCount();
                    TCellsIterable beginKey = pos ? node.GetKeyCellsIterable(pos - 1, GroupInfo.ColsKeyIdx) : nodeState.BeginKey;
                    ui64 beginDataSize = pos ? node.GetShortChild(pos - 1).GetDataSize() : nodeState.BeginDataSize;
                    ui64 endDataSize = child.GetDataSize();

                    nextNodes.emplace_back(pageId, beginRowId, endRowId, beginKey, beginDataSize, endDataSize);
                    hasChanges = true;
                }
            }

            Nodes.swap(nextNodes);
            nextNodes.clear();

            if (!hasChanges) {
                break; // don't go deeper
            }
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
        Y_DEBUG_ABORT_UNLESS(Nodes[NodeIndex - 1].EndDataSize >= Nodes[NodeIndex - 1].BeginDataSize);
        ui64 delta = Nodes[NodeIndex - 1].EndDataSize - Nodes[NodeIndex - 1].BeginDataSize;
        ui8 channel = Part->GetGroupChannel(GroupId);
        dataSize.Add(delta, channel);
    }

public:
    bool IsValid() const override {
        return NodeIndex < Nodes.size();
    }

    TRowId GetEndRowId() const override {
        return Meta.GetRowCount();
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

    void GetKeyCells(TSmallVec<TCell>& keyCells) const override {
        keyCells.clear();

        auto iter = GetCurrentNode().BeginKey.Iter();
        for (TPos pos : xrange(iter.Count())) {
            Y_UNUSED(pos);
            keyCells.push_back(iter.Next());
        }
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
    ui64 RowCountResolution;
    ui64 DataSizeResolution;
    TVector<TRowId> SplitPoints;
};

}
