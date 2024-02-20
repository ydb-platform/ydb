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
        TCellsIterable EndKey;
        std::optional<TBtreeIndexNode> Node;
        std::optional<TRecIdx> Pos;
        ui64 DataSize;

        TNodeState(TPageId pageId, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, TCellsIterable endKey, ui64 dataSize)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , EndKey(endKey)
            , DataSize(dataSize)
        {
        }

        bool IsLastPos() const noexcept {
            Y_ABORT_UNLESS(Node);
            Y_ABORT_UNLESS(Pos);
            return *Pos == Node->GetKeysCount();
        }

        bool IsFirstPos() const noexcept {
            Y_ABORT_UNLESS(Node);
            Y_ABORT_UNLESS(Pos);
            return *Pos == 0;
        }
    };

    struct TSeekRowId {
        TSeekRowId(TRowId rowId)
            : RowId(rowId)
        {}

        bool BelongsTo(const TNodeState& state) const noexcept {
            return TBtreeIndexNode::Has(RowId, state.BeginRowId, state.EndRowId);
        }

        TRecIdx Do(const TNodeState& state) const noexcept {
            return state.Node->Seek(RowId, state.Pos);
        }

        const TRowId RowId;
    };

public:
    TStatsPartGroupBtreeIndexIterator(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(part->Scheme->GetLayout(groupId))
        , Meta(groupId.IsHistoric() ? part->IndexPages.BTreeHistoric[groupId.Index] : part->IndexPages.BTreeGroups[groupId.Index])
        , State(Reserve(Meta.LevelCount + 1))
        , GroupChannel(Part->GetGroupChannel(GroupId))
        , PrevDataSize(0)
        , PrevPrevDataSize(0)
    {
        const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());
        State.emplace_back(Meta.PageId, 0, GetEndRowId(), EmptyKey, EmptyKey, Meta.DataSize);
    }
    
    EReady Start() override {
        return DoSeek<TSeekRowId>({0});
    }

    EReady Next() override {
        Y_ABORT_UNLESS(!IsExhausted());

        PrevPrevDataSize = PrevDataSize;
        PrevDataSize = State.back().DataSize;

        if (Meta.LevelCount == 0) {
            return Exhaust();
        }

        if (IsLeaf()) {
            do {
                State.pop_back();
            } while (State.size() > 1 && State.back().IsLastPos());
            if (State.back().IsLastPos()) {
                return Exhaust();
            }
            PushNextState(*State.back().Pos + 1);
        }

        for (ui32 level : xrange<ui32>(State.size() - 1, Meta.LevelCount)) {
            if (!TryLoad(State[level])) {
                // exiting with an intermediate state
                Y_DEBUG_ABORT_UNLESS(!IsLeaf() && !IsExhausted());
                return EReady::Page;
            }
            PushNextState(0);
        }

        // State.back() points to the target data page
        Y_ABORT_UNLESS(IsLeaf());
        return EReady::Data;
    }

    void AddLastDeltaDataSize(TChanneledDataSize& dataSize) override {
        Y_ABORT_UNLESS(IsExhausted() || IsLeaf());
        Y_DEBUG_ABORT_UNLESS(PrevDataSize >= PrevPrevDataSize);
        ui64 delta = PrevDataSize - PrevPrevDataSize;
        dataSize.Add(delta, GroupChannel);
    }

public:
    bool IsValid() const override {
        Y_DEBUG_ABORT_UNLESS(IsLeaf() || IsExhausted());
        return IsLeaf();
    }

    TRowId GetEndRowId() const override {
        return Meta.RowCount;
    }

    TPageId GetPageId() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().PageId;
    }

    TRowId GetRowId() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginRowId;
    }

    TPos GetKeyCellsCount() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginKey.Count();
    }

    TCell GetKeyCell(TPos index) const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginKey.Iter().At(index);
    }

private:
    template<typename TSeek>
    EReady DoSeek(TSeek seek) {
        while (State.size() > 1 && !seek.BelongsTo(State.back())) {
            State.pop_back();
        }

        if (IsExhausted()) {
            // don't use exhausted state as an initial one
            State[0].Pos = { };
        }

        for (ui32 level : xrange<ui32>(State.size() - 1, Meta.LevelCount)) {
            auto &state = State[level];
            Y_DEBUG_ABORT_UNLESS(seek.BelongsTo(state));
            if (!TryLoad(state)) {
                // exiting with an intermediate state
                Y_DEBUG_ABORT_UNLESS(!IsLeaf() && !IsExhausted());
                return EReady::Page;
            }
            auto pos = seek.Do(state);
            
            PushNextState(pos);
        }

        // State.back() points to the target data page
        Y_ABORT_UNLESS(IsLeaf());
        Y_DEBUG_ABORT_UNLESS(seek.BelongsTo(State.back()));
        return EReady::Data;
    }

    bool IsRoot() const noexcept {
        return State.size() == 1;
    }
    
    bool IsExhausted() const noexcept {
        return State[0].Pos == Max<TRecIdx>();
    }

    bool IsLeaf() const noexcept {
        // Note: it is possible to have 0 levels in B-Tree
        // so we may have exhausted state with leaf (data) node
        return State.size() == Meta.LevelCount + 1 && !IsExhausted();
    }

    EReady Exhaust() {
        while (State.size() > 1) {
            State.pop_back();
        }
        State[0].Pos = Max<TRecIdx>();
        return EReady::Gone;
    }

    void PushNextState(TRecIdx pos) {
        TNodeState& current = State.back();
        Y_ABORT_UNLESS(pos < current.Node->GetChildrenCount(), "Should point to some child");
        current.Pos.emplace(pos);

        auto& child = current.Node->GetShortChild(pos);

        TRowId beginRowId = pos ? current.Node->GetShortChild(pos - 1).RowCount : current.BeginRowId;
        TRowId endRowId = child.RowCount;
        
        TCellsIterable beginKey = pos ? current.Node->GetKeyCellsIterable(pos - 1, GroupInfo.ColsKeyIdx) : current.BeginKey;
        TCellsIterable endKey = pos < current.Node->GetKeysCount() ? current.Node->GetKeyCellsIterable(pos, GroupInfo.ColsKeyIdx) : current.EndKey;
        
        ui64 dataSize = child.DataSize;

        State.emplace_back(child.PageId, beginRowId, endRowId, beginKey, endKey, dataSize);
    }

    bool TryLoad(TNodeState& state) {
        if (state.Node) {
            return true;
        }

        auto page = Env->TryGetPage(Part, state.PageId);
        if (page) {
            state.Node.emplace(*page);
            return true;
        }
        return false;
    }

private:
    const TPart* const Part;
    IPages* const Env;
    const TGroupId GroupId;
    const TPartScheme::TGroupInfo& GroupInfo;
    const TBtreeIndexMeta Meta;
    TVector<TNodeState> State;
    ui8 GroupChannel;
    ui64 PrevDataSize, PrevPrevDataSize;
};

}
