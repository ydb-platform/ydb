#pragma once

#include "flat_part_iface.h"
#include "flat_table_part.h"
#include "flat_part_index_iter_iface.h"

namespace NKikimr::NTable {

class TPartGroupBtreeIndexIter : public IPartGroupIndexIter {
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

        TNodeState(TPageId pageId, TRowId beginRowId, TRowId endRowId, TCellsIterable beginKey, TCellsIterable endKey)
            : PageId(pageId)
            , BeginRowId(beginRowId)
            , EndRowId(endRowId)
            , BeginKey(beginKey)
            , EndKey(endKey)
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
    TPartGroupBtreeIndexIter(const TPart* part, IPages* env, TGroupId groupId)
        : Part(part)
        , Env(env)
        , GroupId(groupId)
        , GroupInfo(Part->Scheme->GetLayout(GroupId))
        , Meta(Part->IndexPages.GetBTree(GroupId))
        , State(Reserve(Meta.LevelCount + 1))
    {
        const static TCellsIterable EmptyKey(static_cast<const char*>(nullptr), TColumns());
        State.emplace_back(Meta.GetPageId(), 0, GetEndRowId(), EmptyKey, EmptyKey);
    }
    
    EReady Seek(TRowId rowId) override {
        if (rowId >= GetEndRowId()) {
            return Exhaust();
        }

        return DoSeek<TSeekRowId>({rowId});
    }

    EReady SeekLast() override {
        if (Y_UNLIKELY(GetEndRowId() == 0)) {
            Y_DEBUG_ABORT("TPart can't be empty");
            return Exhaust();
        }
        return Seek(GetEndRowId() - 1);
    }

    /**
     * Searches for the first page that may contain given key with specified seek mode
     *
     * Result is approximate and may be off by one page
     */
    EReady Seek(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) override {
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
    EReady SeekReverse(ESeek seek, TCells key, const TKeyCellDefaults *keyDefaults) override {
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

    EReady Next() override {
        Y_ABORT_UNLESS(!IsExhausted());

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

    EReady Prev() override {
        Y_ABORT_UNLESS(!IsExhausted());

        if (Meta.LevelCount == 0) {
            return Exhaust();
        }

        if (IsLeaf()) {
            do {
                State.pop_back();
            } while (State.size() > 1 && State.back().IsFirstPos());
            if (State.back().IsFirstPos()) {
                return Exhaust();
            }
            PushNextState(*State.back().Pos - 1);
        }

        for (ui32 level : xrange<ui32>(State.size() - 1, Meta.LevelCount)) {
            if (!TryLoad(State[level])) {
                // exiting with an intermediate state
                Y_DEBUG_ABORT_UNLESS(!IsLeaf() && !IsExhausted());
                return EReady::Page;
            }
            PushNextState(State[level].Node->GetKeysCount());
        }

        // State.back() points to the target data page
        Y_ABORT_UNLESS(IsLeaf());
        return EReady::Data;
    }

public:
    bool IsValid() const override {
        Y_DEBUG_ABORT_UNLESS(IsLeaf() || IsExhausted());
        return IsLeaf();
    }

    TRowId GetEndRowId() const override {
        return Meta.GetRowCount();
    }

    TPageId GetPageId() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().PageId;
    }

    TRowId GetRowId() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginRowId;
    }

    TRowId GetNextRowId() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().EndRowId;
    }

    TPos GetKeyCellsCount() const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginKey.Count();
    }

    TCell GetKeyCell(TPos index) const override {
        Y_ABORT_UNLESS(IsLeaf());
        return State.back().BeginKey.Iter().At(index);
    }

    void GetKeyCells(TSmallVec<TCell>& keyCells) const override {
        keyCells.clear();

        Y_ABORT_UNLESS(IsLeaf());

        auto iter = State.back().BeginKey.Iter();
        for (TPos pos : xrange(iter.Count())) {
            Y_UNUSED(pos);
            keyCells.push_back(iter.Next());
        }
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

        TPageId pageId = child.GetPageId();
        TRowId beginRowId = pos ? current.Node->GetShortChild(pos - 1).GetRowCount() : current.BeginRowId;
        TRowId endRowId = child.GetRowCount();
        TCellsIterable beginKey = pos ? current.Node->GetKeyCellsIterable(pos - 1, GroupInfo.ColsKeyIdx) : current.BeginKey;
        TCellsIterable endKey = pos < current.Node->GetKeysCount() ? current.Node->GetKeyCellsIterable(pos, GroupInfo.ColsKeyIdx) : current.EndKey;
        
        State.emplace_back(pageId, beginRowId, endRowId, beginKey, endKey);
    }

    bool TryLoad(TNodeState& state) {
        if (state.Node) {
            return true;
        }

        auto page = Env->TryGetPage(Part, state.PageId, {});
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
