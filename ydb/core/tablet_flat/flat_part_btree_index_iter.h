#pragma once

#include "flat_part_iface.h"
#include "flat_page_index.h"
#include "flat_table_part.h"


namespace NKikimr::NTable {

class TPartBtreeIndexIt {
    using TCells = NPage::TCells;
    using TBtreeIndexNode = NPage::TBtreeIndexNode;
    using TGroupId = NPage::TGroupId;
    using TRecIdx = NPage::TRecIdx;
    using TChild = TBtreeIndexNode::TChild;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;

    struct TNodeState {
        TChild Meta;
        TRowId BeginRowId;
        TRowId EndRowId;
        // TCells BeginKey;
        // TCells EndKey;
        std::optional<TBtreeIndexNode> Node;
        std::optional<TRecIdx> Pos;

        TNodeState(TChild meta, TRowId beginRowId, TRowId endRowId)
            : Meta(meta)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
        { 
        }

        bool HasRow(TRowId rowId) const {
            return BeginRowId <= rowId && rowId < EndRowId;
        }
    };

public:
    TPartBtreeIndexIt(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , Meta(groupId.IsMain() ? part->IndexPages.BTreeGroups[groupId.Index] : part->IndexPages.BTreeHistoric[groupId.Index])
    {
        State.emplace_back(Meta, 0, Meta.Count);
    }
    
    EReady Seek(TRowId rowId) {
        if (rowId >= Meta.Count) {
            return Exhaust();
        }

        while (State.size() > 1 && !State.back().HasRow(rowId)) {
            State.pop_back();
        }

        if (IsExhausted()) {
            // don't use exhausted state as an initial one
            State[0].Pos = { };
        }

        for (size_t level : xrange(State.size() - 1, Meta.LevelsCount)) {
            auto &state = State[level];
            Y_ABORT_UNLESS(state.HasRow(rowId));
            if (!TryLoad(state)) {
                // exiting with an intermediate state
                Y_DEBUG_ABORT_UNLESS(!IsLeaf() && !IsExhausted());
                return EReady::Page;
            }
            auto pos = state.Node->Seek(rowId, state.Pos);
            state.Pos.emplace(pos);
            
            auto child = state.Node->GetChild(pos);
            TRowId firstRowId = pos ? state.Node->GetChild(pos - 1).Count : state.BeginRowId;
            TRowId lastRowId = child.Count;
            State.emplace_back(child, firstRowId, lastRowId);
        }

        // State.back() points to the target data page
        Y_ABORT_UNLESS(IsLeaf());
        Y_ABORT_UNLESS(State.back().HasRow(rowId));
        return EReady::Data;
    }

    // EReady Next() {
    //     Y_DEBUG_ABORT_UNLESS(IsLeaf());
    //     return Exhaust();
    // }

    // EReady Prev() {
    //     Y_DEBUG_ABORT_UNLESS(IsLeaf());
    //     return Exhaust();
    // }

public:
    bool IsValid() const {
        Y_DEBUG_ABORT_UNLESS(IsLeaf() || IsExhausted());
        return IsLeaf();
    }

    TPageId GetPageId() const {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().Meta.PageId;
    }

    TRowId GetRowId() const {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginRowId;
    }

    TRowId GetNextRowId() const {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().EndRowId;
    }

private:
    bool IsRoot() const noexcept {
        return State.size() == 1;
    }
    
    bool IsExhausted() const noexcept {
        return State[0].Pos == Max<TRecIdx>();
    }

    bool IsLeaf() const noexcept {
        // Note: it is possible to have 0 levels in B-Tree
        // so we may have exhausted state with leaf (data) node
        return State.size() == Meta.LevelsCount + 1 && !IsExhausted();
    }

    EReady Exhaust() {
        while (State.size() > 1) {
            State.pop_back();
        }
        State[0].Pos = Max<TRecIdx>();
        return EReady::Gone;
    }

    bool TryLoad(TNodeState& state) {
        if (state.Node) {
            return true;
        }

        auto page = Env->TryGetPage(Part, state.Meta.PageId);
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
    const TBtreeIndexMeta Meta;
    TVector<TNodeState> State;
};

}
