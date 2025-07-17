#pragma once

#include "range_tree_base.h"

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NIntervalTree {

template <class TKey, class TValue>
struct TRangeTreapDefaultTypeTraits {
    using TKeyView = const TKey*;

    static bool Less(const TValue& a, const TValue& b) {
        return a < b;
    }

    static bool Equal(const TValue& a, const TValue& b) {
        return a == b;
    }
};

template <class TKey, class TValue, class TComparator, class TTypeTraits = TRangeTreapDefaultTypeTraits<TKey, TValue>>
class TRangeTreap: public TRangeTreeBase<TKey, typename TTypeTraits::TKeyView> {
private:
    using TBase = TRangeTreeBase<TKey, typename TTypeTraits::TKeyView>;

private:
    class TNode: public TIntrusiveListItem<TNode> {
    public:
        TNode* Parent = nullptr;
        THolder<TNode> Left;
        THolder<TNode> Right;
        ui64 Prio = -1;
        TKey LeftKey;
        TKey RightKey;
        TKey MaxRightKey;
        TValue Value;
        EPrefixMode LeftMode;
        EPrefixMode RightMode;
        EPrefixMode MaxRightMode;
        bool MaxRightTrivial;

        void SetLeft(THolder<TNode> child) noexcept {
            if (child) {
                child->Parent = this;
            }
            Left = std::move(child);
        }

        void SetRight(THolder<TNode> child) noexcept {
            if (child) {
                child->Parent = this;
            }
            Right = std::move(child);
        }

        THolder<TNode> RemoveLeft() noexcept {
            if (Left) {
                Left->Parent = nullptr;
            }
            return std::move(Left);
        }

        THolder<TNode> RemoveRight() noexcept {
            if (Right) {
                Right->Parent = nullptr;
            }
            return std::move(Right);
        }

        TBase::TBorder LeftBorder() const noexcept {
            return typename TBase::TBorder(LeftKey, LeftMode);
        }

        TBase::TBorder RightBorder() const noexcept {
            return typename TBase::TBorder(RightKey, RightMode);
        }

        TBase::TBorder MaxRightBorder() const noexcept {
            return typename TBase::TBorder(MaxRightKey, MaxRightMode);
        }

        TBase::TRange ToRange() const noexcept {
            return typename TBase::TRange(LeftKey, TBase::TBorder::IsInclusive(LeftMode), RightKey, TBase::TBorder::IsInclusive(RightMode));
        }
    };

public:
    /**
         * Clears the tree
         */
    void Clear() noexcept {
        Values.clear();
        Root.Reset();
        TBase::Size_ = 0;
    }

    /**
         * Adds mapping from the given range to the given value
         */
    void AddRange(const TBase::TRange& range, TValue value) {
        AddRange(range.ToOwnedRange(), std::move(value));
    }

    /**
         * Adds mapping from the given range to the given value
         */
    void AddRange(TBase::TOwnedRange range, TValue value) {
        Comparator.ValidateKey(range.LeftKey);
        Comparator.ValidateKey(range.RightKey);

        auto leftBorder = TBase::TBorder::MakeLeft(range.LeftKey, range.LeftInclusive);
        auto rightBorder = TBase::TBorder::MakeRight(range.RightKey, range.RightInclusive);

        DoInsert(&Root, leftBorder, rightBorder, std::move(range.LeftKey), std::move(range.RightKey), std::move(value), RandomNumber<ui64>());
    }

    /**
         * Removes all ranges with the given value
         */
    void RemoveRanges(const TValue& value) {
        auto it = Values.find(value);
        if (it != Values.end()) {
            while (!it->second.Empty()) {
                DoRemove(it->second.PopFront());
            }
            Values.erase(it);
        }
    }

    /**
         * Calls callback(range, value) for each range in the tree
         *
         * Order is sorted by (range.Left, value, range.Right) tuples.
         */
    template <class TCallback>
    void EachRange(TCallback&& callback) const {
        if (Root) {
            DoEachRange(Root.Get(), callback);
        }
    }

    /**
         * Calls callback for each range with the given point
         */
    template <class TCallback>
    void EachIntersection(TTypeTraits::TKeyView point, TCallback&& callback) const {
        EachIntersection(typename TBase::TRange(point, true, point, true), callback);
    }

    /**
         * Calls callback for each range intersecting with the query
         */
    template <class TCallback>
    void EachIntersection(const TBase::TRange& range, TCallback&& callback) const {
        if (Root) {
            DoEachIntersection(Root.Get(), TBase::TBorder::MakeLeft(range.LeftKey, range.LeftInclusive),
                TBase::TBorder::MakeRight(range.RightKey, range.RightInclusive), callback);
        }
    }

private:
    /**
         * Inserts a new (leftKey, value, rightKey, prio) node into subtree rooted at tptr
         *
         * If subtree already contains an existing (leftKey, value), then attempts to
         * extend its RightKey instead of inserting a new node.
         */
    void DoInsert(THolder<TNode>* tptr, const TBase::TBorder& leftKey, const TBase::TBorder& rightKey, TKey leftOwnedKey,
        TKey rightOwnedKey, TValue value, ui64 prio)
    {
        THolder<TNode> l, r;

        TNode* t;
        TNode* parent = nullptr;
        while ((t = tptr->Get()) != nullptr) {
            if (prio < t->Prio) {
                // New node must be a parent of t, split it first
                if (TNode* p = FindOrSplit(std::move(*tptr), l, r, leftKey, value)) {
                    // We found an existing node for leftKey
                    ++TBase::Stats_.Updates;
                    if (ExtendRightKey(p, rightKey, std::move(rightOwnedKey))) {
                        ExtendMaxRightKeys(p->Parent, p->RightKey, p->RightMode);
                    }
                    return;
                }

                Y_DEBUG_ABORT_UNLESS(!tptr->Get());
                break;
            }

            int cmp = CompareBorders(leftKey, t->LeftBorder());
            if (cmp == 0 && TTypeTraits::Equal(value, t->Value)) {
                // Current subtree is already equal to leftKey
                ++TBase::Stats_.Updates;
                if (ExtendRightKey(t, rightKey, std::move(rightOwnedKey))) {
                    ExtendMaxRightKeys(parent, t->RightKey, t->RightMode);
                }
                return;
            }

            parent = t;
            tptr = (cmp < 0 || cmp == 0 && TTypeTraits::Less(value, t->Value)) ? &t->Left : &t->Right;
        }

        // Make a new subtree root with l and r as children
        tptr->Reset((t = new TNode));
        Values[value].PushBack(t);
        t->Parent = parent;
        t->SetLeft(std::move(l));
        t->SetRight(std::move(r));
        t->Prio = prio;
        t->Value = std::move(value);
        t->LeftKey = std::move(leftOwnedKey);
        t->RightKey = std::move(rightOwnedKey);
        t->LeftMode = leftKey.Mode;
        t->RightMode = rightKey.Mode;
        ++TBase::Stats_.Inserts;
        ++TBase::Size_;
        RecomputeMaxRight(t);

        // The original t was split into l and r, that means their
        // max(MaxRightKey) was already accomodated into original t's
        // parent, and new max(MaxRightKey) cannot be smaller that that.
        // We only need to update parents' MaxRightKey when the new
        // RightKey is larger than what we previously had.
        if (t->MaxRightTrivial) {
            ExtendMaxRightKeys(parent, t->RightKey, t->RightMode);
        }
    }

    /**
         * Split subtree t into l and r subtrees, such that l < key and key < r
         *
         * Does nothing and returns existing node if (key, value) already exist in subtree t.
         */
    TNode* FindOrSplit(THolder<TNode>&& t, THolder<TNode>& l, THolder<TNode>& r, const TBase::TBorder& key, const TValue& value)
    {
        if (!t) {
            return nullptr;
        }

        int cmp = CompareBorders(key, t->LeftBorder());
        if (cmp == 0 && TTypeTraits::Equal(value, t->Value)) {
            // Key is already in the tree, return pointer to an existing node
            return t.Get();
        }

        if (cmp < 0 || cmp == 0 && TTypeTraits::Less(value, t->Value)) {
            // We must split the left subtree
            if (t->Left) {
                THolder<TNode> tmp;
                if (TNode* found = FindOrSplit(std::move(t->Left), l, tmp, key, value)) {
                    return found;
                }
                t->SetLeft(std::move(tmp));
                RecomputeMaxRight(t.Get());
            }
            t->Parent = nullptr;
            r = std::move(t);
        } else {
            // We must split the right subtree
            if (t->Right) {
                THolder<TNode> tmp;
                if (TNode* found = FindOrSplit(std::move(t->Right), tmp, r, key, value)) {
                    return found;
                }
                t->SetRight(std::move(tmp));
                RecomputeMaxRight(t.Get());
            }
            t->Parent = nullptr;
            l = std::move(t);
        }

        return nullptr;
    }

private:
    /**
         * Removes node t from the tree
         */
    void DoRemove(TNode* t) {
        Y_DEBUG_ABORT_UNLESS(t, "Trying to remove a nullptr node");
        TNode* p = t->Parent;
        if (p) {
            if (p->Left == t) {
                return DoRemove(&p->Left);
            } else {
                Y_DEBUG_ABORT_UNLESS(p->Right == t, "Node has corrupted parent link");
                return DoRemove(&p->Right);
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(Root.Get() == t, "Node has corrupted parent link");
            return DoRemove(&Root);
        }
    }

    /**
         * Removes the node linked by tptr from the tree
         */
    void DoRemove(THolder<TNode>* tptr) {
        THolder<TNode> d = std::move(*tptr);
        Y_DEBUG_ABORT_UNLESS(d, "Cannot remove a null node");
        ++TBase::Stats_.Deletes;
        --TBase::Size_;

        if (d->Left || d->Right) {
            *tptr = Merge(d->RemoveLeft(), d->RemoveRight());
            tptr->Get()->Parent = d->Parent;
        }

        // Recompute all parents of the deleted node
        if (TNode* p = std::exchange(d->Parent, nullptr)) {
            RecomputeMaxRights(p);
        }
    }

    /**
         * Merges two subtrees l and r (where l < r)
         */
    THolder<TNode> Merge(THolder<TNode> l, THolder<TNode> r) {
        Y_DEBUG_ABORT_UNLESS(!l || l->Parent == nullptr);
        Y_DEBUG_ABORT_UNLESS(!r || r->Parent == nullptr);
        if (!l || !r) {
            return l ? std::move(l) : std::move(r);
        } else if (l->Prio <= r->Prio) {
            l->SetRight(Merge(l->RemoveRight(), std::move(r)));
            RecomputeMaxRight(l.Get());
            return std::move(l);
        } else {
            r->SetLeft(Merge(std::move(l), r->RemoveLeft()));
            RecomputeMaxRight(r.Get());
            return std::move(r);
        }
    }

private:
    /**
         * Recomputes MaxRightKey for subtree root t
         *
         * Returns the node which was the source of the new MaxRightKey
         */
    TNode* RecomputeMaxRight(TNode* t) {
        TNode* source = t;
        t->MaxRightKey = t->RightKey;
        t->MaxRightMode = t->RightMode;
        t->MaxRightTrivial = true;
        if (t->Left && CompareBorders(t->MaxRightBorder(), t->Left->MaxRightBorder()) < 0) {
            source = t->Left.Get();
            t->MaxRightKey = t->Left->MaxRightKey;
            t->MaxRightMode = t->Left->MaxRightMode;
            t->MaxRightTrivial = false;
        }
        if (t->Right && CompareBorders(t->MaxRightBorder(), t->Right->MaxRightBorder()) < 0) {
            source = t->Right.Get();
            t->MaxRightKey = t->Right->MaxRightKey;
            t->MaxRightMode = t->Right->MaxRightMode;
            t->MaxRightTrivial = false;
        }
        return source;
    }

    /**
         * Recomputes MaxRightKey for subtree root t and its parents
         */
    void RecomputeMaxRights(TNode* t) {
        while (t) {
            RecomputeMaxRight(t);
            t = t->Parent;
        }
    }

    /**
         * Extends RightKey of node t if necessary
         *
         * Returns true when t->MaxRightKey is modified
         */
    bool ExtendRightKey(TNode* t, const TBase::TBorder& rightKey, TKey rightOwnedKey) {
        if (CompareBorders(t->RightBorder(), rightKey) < 0) {
            t->RightKey = rightOwnedKey;
            t->RightMode = rightKey.Mode;
            if (t->MaxRightTrivial) {
                t->MaxRightKey = t->RightKey;
                t->MaxRightMode = t->RightMode;
                return true;
            } else {
                int cmp = CompareBorders(t->MaxRightBorder(), t->RightBorder());
                if (cmp <= 0) {
                    t->MaxRightKey = t->RightKey;
                    t->MaxRightMode = t->RightMode;
                    t->MaxRightTrivial = true;
                }
                return cmp < 0;
            }
        } else {
            return false;
        }
    }

    /**
         * Extends MaxRightKey of node t
         *
         * Returns true when t->MaxRightKey is modified
         */
    bool ExtendMaxRightKey(TNode* t, const TKey& rightKey, EPrefixMode rightMode) {
        int cmp = CompareBorders(t->MaxRightBorder(), typename TBase::TBorder(rightKey, rightMode));
        if (cmp < 0) {
            t->MaxRightKey = rightKey;
            t->MaxRightMode = rightMode;
            t->MaxRightTrivial = false;
            return true;
        } else {
            return false;
        }
    }

    /**
         * Extends MaxRightKey of node t and all its parents
         */
    void ExtendMaxRightKeys(TNode* t, const TKey& rightKey, EPrefixMode rightMode) {
        while (t && ExtendMaxRightKey(t, rightKey, rightMode)) {
            t = t->Parent;
        }
    }

private:
    template <class TCallback>
    void DoEachRange(const TNode* t, TCallback&& callback) const {
        if (t->Left) {
            DoEachRange(t->Left.Get(), callback);
        }
        { callback(t->ToRange(), t->Value); }
        if (t->Right) {
            DoEachRange(t->Right.Get(), callback);
        }
    }

    template <class TCallback>
    void DoEachIntersection(
        const TNode* t, const TBase::TBorder& leftBorder, const TBase::TBorder& rightBorder, TCallback&& callback, bool wentLeft = false) const {
        int cmp;

        if (wentLeft) {
            int cmp = CompareBorders(t->MaxRightBorder(), leftBorder);
            if (cmp < 0) {
                // There is no intersection with this whole subtree
                return;
            }
        }

        if (t->Left) {
            // Descend into the left subtree
            // Note it will terminate on subtrees that have small MaxRightBorder
            DoEachIntersection(t->Left.Get(), leftBorder, rightBorder, callback, true);
        }

        cmp = CompareBorders(rightBorder, t->LeftBorder());
        if (cmp < 0) {
            // There is no intersection with this node or the right subtree
            return;
        }

        // N.B. we avoid comparison with RightKey when it is equal to MaxRightKey.
        if ((wentLeft && t->MaxRightTrivial) || CompareBorders(leftBorder, t->RightBorder()) <= 0)
        {
            callback(t->ToRange(), t->Value);
        }

        if (t->Right) {
            // Descend into the right subtree
            DoEachIntersection(t->Right.Get(), leftBorder, rightBorder, callback);
        }
    }

public:
    /**
         * Returns height of tree, for tests only, O(n) complexity
         */
    size_t Height() const {
        return Root ? DoCalcHeight(Root.Get()) : 0;
    }

private:
    size_t DoCalcHeight(const TNode* t) const {
        size_t children = Max(t->Left ? DoCalcHeight(t->Left.Get()) : 0, t->Right ? DoCalcHeight(t->Right.Get()) : 0);
        return 1 + children;
    }

public:
    /**
         * Validates all invariants for the tree, used for tests
         */
    void Validate() const {
        if (Root) {
            Y_ENSURE(Root->Parent == nullptr, "Root must not have a parent");
            DoValidate(Root.Get());
        }
    }

    TComparator& MutableComparator() {
        return Comparator;
    }

    const TComparator& GetComparator() const {
        return Comparator;
    }

private:
    /**
         * Validates all invariants for subtree t
         */
    std::tuple<TNode*, TNode*> DoValidate(TNode* t) const {
        int cmp;
        TNode* leftMost = t;
        TNode* rightMost = t;
        typename TBase::TBorder maxRightBorder = t->RightBorder();
        bool maxRightTrivial = true;

        if (auto* l = t->Left.Get()) {
            Y_ENSURE(l->Parent == t, "Left child parent is incorrect");
            Y_ENSURE(l->Prio >= t->Prio, "Left child prio is incorrect");
            cmp = this->CompareBorders(l->LeftBorder(), t->LeftBorder());
            Y_ENSURE(cmp < 0 || cmp == 0 && TTypeTraits::Less(l->Value, t->Value), "Left child must be smaller than t");
            TNode* leftRightMost;
            std::tie(leftMost, leftRightMost) = DoValidate(l);
            cmp = this->CompareBorders(leftRightMost->LeftBorder(), t->LeftBorder());
            Y_ENSURE(
                cmp < 0 || cmp == 0 && TTypeTraits::Less(leftRightMost->Value, t->Value), "Left child rightmost node must be smaller than t");
            cmp = this->CompareBorders(maxRightBorder, l->MaxRightBorder());
            if (cmp < 0) {
                maxRightBorder = l->MaxRightBorder();
                maxRightTrivial = false;
            }
        }

        if (auto* r = t->Right.Get()) {
            Y_ENSURE(r->Parent == t, "Right child parent is incorrect");
            Y_ENSURE(r->Prio >= t->Prio, "Right child prio is incorrect");
            cmp = this->CompareBorders(t->LeftBorder(), r->LeftBorder());
            Y_ENSURE(cmp < 0 || cmp == 0 && TTypeTraits::Less(t->Value, r->Value), "Right child must be bigger than t");
            TNode* rightLeftMost;
            std::tie(rightLeftMost, rightMost) = DoValidate(r);
            cmp = this->CompareBorders(t->LeftBorder(), rightLeftMost->LeftBorder());
            Y_ENSURE(
                cmp < 0 || cmp == 0 && TTypeTraits::Less(t->Value, rightLeftMost->Value), "Right child leftmost node must be bigger than t");
            cmp = this->CompareBorders(maxRightBorder, r->MaxRightBorder());
            if (cmp < 0) {
                maxRightBorder = r->MaxRightBorder();
                maxRightTrivial = false;
            }
        }

        cmp = this->CompareBorders(maxRightBorder, t->MaxRightBorder());
        Y_ENSURE(cmp == 0, "Subtree must have max right key equal to the calculated max");
        Y_ENSURE(maxRightTrivial == t->MaxRightTrivial, "Subtree must have correct MaxRightTrivial flag (computed="
                                                            << int(maxRightTrivial) << ", stored=" << int(t->MaxRightTrivial) << ")");

        return { leftMost, rightMost };
    }

    int CompareBorders(const TBase::TBorder& a, const TBase::TBorder& b) const {
        ++TBase::Stats_.Comparisons;
        return Comparator.Compare(a, b);
    }

private:
    TComparator Comparator;
    THolder<TNode> Root;
    THashMap<TValue, TIntrusiveList<TNode>> Values;
};
}   // namespace NIntervalTree

namespace NDataShard {

using TRangeTreeBase = NIntervalTree::TRangeTreeBase<TOwnedCellVec, TConstArrayRef<NKikimr::TCell>>;

template <class TValue>
class TKeyComparator {
private:
    TVector<NScheme::TTypeInfo> KeyTypes;

public:
    int Compare(const TRangeTreeBase::TBorder& lhs, const TRangeTreeBase::TBorder& rhs) const {
        return ComparePrefixBorders(KeyTypes, lhs.Key, lhs.Mode, rhs.Key, rhs.Mode);
    }

    void ValidateKey(const TOwnedCellVec& key) const {
        Y_ENSURE(key.size() <= KeyTypes.size(), "Range key is too large");
    }

    void SetKeyTypes(const TVector<NScheme::TTypeInfo>& keyTypes) {
        Y_ENSURE(keyTypes.size() >= KeyTypes.size(), "Number of key columns must not decrease over time");
        KeyTypes = keyTypes;
    }

    ui64 KeyColumns() const {
        return KeyTypes.size();
    }
};

template <class TValue>
struct TRangeTreapDefaultTypeTraits {
    using TKeyView = TConstArrayRef<NKikimr::TCell>;

    static bool Less(const TValue& a, const TValue& b) {
        return a < b;
    }

    static bool Equal(const TValue& a, const TValue& b) {
        return a == b;
    }
};

template <class TValue, class TTypeTraits = TRangeTreapDefaultTypeTraits<TValue>>
using TRangeTreap = NKikimr::NIntervalTree::TRangeTreap<TOwnedCellVec, TValue, TKeyComparator<TValue>, TTypeTraits>;

}   // namespace NDataShard
}   // namespace NKikimr
