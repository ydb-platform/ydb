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
    using TColumns = TBtreeIndexNode::TColumns;
    using TCellsIterable = TBtreeIndexNode::TCellsIterable;
    using TCellsIter = TBtreeIndexNode::TCellsIter;
    using TBtreeIndexMeta = NPage::TBtreeIndexMeta;

    struct TNodeState {
        TChild Meta;
        TRowId BeginRowId;
        TRowId EndRowId;
        TCellsIterable BeginKey;
        TCellsIterable EndKey;
        std::optional<TBtreeIndexNode> Node;
        std::optional<TRecIdx> Pos;

        TNodeState(TChild meta, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, TCellsIterable endKey)
            : Meta(meta)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , EndKey(endKey)
        { 
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

    struct TSeekKey {
        TSeekKey(ESeek seek, TCells key, TColumns columns, const TKeyCellDefaults *keyDefaults)
            : Seek(seek)
            , Key(key)
            , Columns(columns)
            , KeyDefaults(keyDefaults)
        {}

        bool BelongsTo(const TNodeState& state) const noexcept {
            return TBtreeIndexNode::Has(Seek, Key, state.BeginKey, state.EndKey, KeyDefaults);
        }

        TRecIdx Do(const TNodeState& state) const noexcept {
            return state.Node->Seek(Seek, Key, Columns, KeyDefaults);
        }

        const ESeek Seek;
        const TCells Key;
        const TColumns Columns;
        const TKeyCellDefaults* const KeyDefaults;
    };

    struct TSeekKeyReverse {
        TSeekKeyReverse(ESeek seek, TCells key, TColumns columns, const TKeyCellDefaults *keyDefaults)
            : Seek(seek)
            , Key(key)
            , Columns(columns)
            , KeyDefaults(keyDefaults)
        {}

        bool BelongsTo(const TNodeState& state) const noexcept {
            return TBtreeIndexNode::HasReverse(Seek, Key, state.BeginKey, state.EndKey, KeyDefaults);
        }

        TRecIdx Do(const TNodeState& state) const noexcept {
            return state.Node->SeekReverse(Seek, Key, Columns, KeyDefaults);
        }

        const ESeek Seek;
        const TCells Key;
        const TColumns Columns;
        const TKeyCellDefaults* const KeyDefaults;
    };

public:
    TPartBtreeIndexIt(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(part->Scheme->GetLayout(groupId))
        , Meta(groupId.IsMain() ? part->IndexPages.BTreeGroups[groupId.Index] : part->IndexPages.BTreeHistoric[groupId.Index])
    {
        const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());
        State.emplace_back(Meta, 0, GetEndRowId(), EmptyKey, EmptyKey);
    }
    
    EReady Seek(TRowId rowId) {
        if (rowId >= GetEndRowId()) {
            return Exhaust();
        }

        return DoSeek<TSeekRowId>({rowId});
    }

    /**
     * Searches for the first page that may contain given key with specified seek mode
     *
     * Result is approximate and may be off by one page
     */
    EReady Seek(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) {
        if (!key) {
            // Special treatment for an empty key
            switch (seek) {
                case ESeek::Lower:
                    return Seek(0);
                case ESeek::Exact:
                case ESeek::Upper:
                    return Seek(GetEndRowId());
            }
        }

        return DoSeek<TSeekKey>({seek, key, GroupInfo.ColsKeyIdx, keyDefaults});
    }

    /**
     * Searches for the first page (in reverse) that may contain given key with specified seek mode
     *
     * Result is approximate and may be off by one page
     */
    EReady SeekReverse(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) {
        if (!key) {
            // Special treatment for an empty key
            switch (seek) {
                case ESeek::Lower:
                    return Seek(GetEndRowId() - 1);
                case ESeek::Exact:
                case ESeek::Upper:
                    return Seek(GetEndRowId());
            }
        }

        return DoSeek<TSeekKeyReverse>({seek, key, GroupInfo.ColsKeyIdx, keyDefaults});
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

    TRowId GetEndRowId() const {
        return Meta.Count;
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

        for (size_t level : xrange(State.size() - 1, Meta.LevelsCount)) {
            auto &state = State[level];
            Y_DEBUG_ABORT_UNLESS(seek.BelongsTo(state));
            if (!TryLoad(state)) {
                // exiting with an intermediate state
                Y_DEBUG_ABORT_UNLESS(!IsLeaf() && !IsExhausted());
                return EReady::Page;
            }
            auto pos = seek.Do(state);
            
            PushNextState(state, pos);
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
        return State.size() == Meta.LevelsCount + 1 && !IsExhausted();
    }

    EReady Exhaust() {
        while (State.size() > 1) {
            State.pop_back();
        }
        State[0].Pos = Max<TRecIdx>();
        return EReady::Gone;
    }

    void PushNextState(TNodeState& current, TRecIdx pos) {
        Y_ABORT_UNLESS(pos < current.Node->GetChildrenCount(), "Should point to some child");
        current.Pos.emplace(pos);

        auto child = current.Node->GetChild(pos);

        TRowId beginRowId = pos ? current.Node->GetChild(pos - 1).Count : current.BeginRowId;
        TRowId endRowId = child.Count;
        
        TCellsIterable beginKey = pos ? current.Node->GetKeyCellsIterable(pos - 1, GroupInfo.ColsKeyIdx) : current.BeginKey;
        TCellsIterable endKey = pos < current.Node->GetKeysCount() ? current.Node->GetKeyCellsIterable(pos, GroupInfo.ColsKeyIdx) : current.EndKey;
        
        State.emplace_back(child, beginRowId, endRowId, beginKey, endKey);
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
    const TPartScheme::TGroupInfo& GroupInfo;
    const TBtreeIndexMeta Meta;
    TVector<TNodeState> State;
};

}
