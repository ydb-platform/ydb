#pragma once

#include "range.h"

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NRangeTreap {

template <class TValue>
struct TDefaultValueTraits {
    static bool Less(const TValue& a, const TValue& b) {
        return a < b;
    }

    static bool Equal(const TValue& a, const TValue& b) {
        return a == b;
    }

    using TValueHash = THash<TValue>;
};

template <class TKey, class TValue, class TKeyView = TKey, class TValueTraits = TDefaultValueTraits<TValue>,
    class TBorderComparator = TDefaultBorderComparator<TKey, TKeyView>>
class TRangeTreap {
public:
    using TBorder = TBorder<TKeyView>;
    using TOwnedRange = TRange<TKey>;
    using TRange = TRange<TKeyView>;

    struct TStats {
        size_t Comparisons = 0;
        size_t Inserts = 0;
        size_t Updates = 0;
        size_t Deletes = 0;
    };

public:
    size_t Size() const noexcept {
        return Size_;
    }

    const TStats& Stats() const noexcept {
        return Stats_;
    }

    void ResetStats() noexcept {
        Stats_ = {};
    }

private:
    size_t Size_ = 0;
    mutable TStats Stats_;

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
        EBorderMode LeftMode;
        EBorderMode RightMode;
        EBorderMode MaxRightMode;
        bool MaxRightTrivial;

        TNode(TNode* parent, THolder<TNode> left, THolder<TNode> right, const ui64 prio, TKey leftKey, TKey rightKey, TValue value,
            const EBorderMode leftMode, const EBorderMode rightMode, TKey maxRightKey, const EBorderMode maxRightMode,
            const bool maxRightTrivial)
            : Parent(parent)
            , Prio(prio)
            , LeftKey(std::move(leftKey))
            , RightKey(std::move(rightKey))
            , MaxRightKey(std::move(maxRightKey))
            , Value(std::move(value))
            , LeftMode(leftMode)
            , RightMode(rightMode)
            , MaxRightMode(maxRightMode)
            , MaxRightTrivial(maxRightTrivial)
        {
            SetLeft(std::move(left));
            SetRight(std::move(right));
        }

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

        TBorder LeftBorder() const noexcept {
            return TBorder(LeftKey, LeftMode);
        }

        TBorder RightBorder() const noexcept {
            return TBorder(RightKey, RightMode);
        }

        TBorder MaxRightBorder() const noexcept {
            return TBorder(MaxRightKey, MaxRightMode);
        }

        TRange ToRange() const noexcept {
            return TRange(LeftKey, TBorderModeTraits::IsInclusive(LeftMode), RightKey, TBorderModeTraits::IsInclusive(RightMode));
        }
    };

public:
    /**
      * Clears the tree
      */
    void Clear() noexcept {
        Values.clear();
        Root.Reset();
        Size_ = 0;
    }

    /**
      * Adds mapping from the given range to the given value
      */
    void AddRange(TOwnedRange range, TValue value) {
        Comparator.ValidateKey(range.LeftKey);
        Comparator.ValidateKey(range.RightKey);

        auto leftBorder = TBorder::MakeLeft(range.LeftKey, range.LeftInclusive);
        auto rightBorder = TBorder::MakeRight(range.RightKey, range.RightInclusive);

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
    void EachIntersection(TKeyView point, TCallback&& callback) const {
        EachIntersection(TRange(point, true, point, true), callback);
    }

    /**
      * Calls callback for each range intersecting with the query
      */
    template <class TCallback>
    void EachIntersection(const TRange& range, TCallback&& callback) const {
        if (Root) {
            DoEachIntersection(Root.Get(), TBorder::MakeLeft(range.LeftKey, range.LeftInclusive),
                TBorder::MakeRight(range.RightKey, range.RightInclusive), callback);
        }
    }

private:
    /**
      * Inserts a new (leftKey, value, rightKey, prio) node into subtree rooted at tptr
      *
      * If subtree already contains an existing (leftKey, value), then attempts to
      * extend its RightKey instead of inserting a new node.
      */
    void DoInsert(
        THolder<TNode>* tptr, const TBorder& leftKey, const TBorder& rightKey, TKey leftOwnedKey, TKey rightOwnedKey, TValue value, ui64 prio)
    {
        THolder<TNode> l, r;

        TNode* t;
        TNode* parent = nullptr;
        while ((t = tptr->Get()) != nullptr) {
            if (prio < t->Prio) {
                // New node must be a parent of t, split it first
                if (TNode* p = FindOrSplit(std::move(*tptr), l, r, leftKey, value)) {
                    // We found an existing node for leftKey
                    ++Stats_.Updates;
                    if (ExtendRightKey(p, rightKey, std::move(rightOwnedKey))) {
                        ExtendMaxRightKeys(p->Parent, p->RightKey, p->RightMode);
                    }
                    return;
                }

                Y_DEBUG_ABORT_UNLESS(!tptr->Get());
                break;
            }

            int cmp = CompareBorders(leftKey, t->LeftBorder());
            if (cmp == 0 && TValueTraits::Equal(value, t->Value)) {
                // Current subtree is already equal to leftKey
                ++Stats_.Updates;
                if (ExtendRightKey(t, rightKey, std::move(rightOwnedKey))) {
                    ExtendMaxRightKeys(parent, t->RightKey, t->RightMode);
                }
                return;
            }

            parent = t;
            tptr = (cmp < 0 || cmp == 0 && TValueTraits::Less(value, t->Value)) ? &t->Left : &t->Right;
        }

        // Make a new subtree root with l and r as children
        TKey rightKeyCopy = rightOwnedKey;
        tptr->Reset((t = new TNode(
                         /*Parent=*/parent, /*Left=*/std::move(l), /*Right=*/std::move(r), /*Prio=*/prio, /*LeftKey=*/std::move(leftOwnedKey),
                         /*RightKey=*/std::move(rightOwnedKey), /*Value=*/std::move(value), /*LeftMode=*/leftKey.GetMode(),
                         /*RightMode=*/rightKey.GetMode(), /*MaxRightKey=*/std::move(rightKeyCopy), /*MaxRightMode=*/rightKey.GetMode(),
                         /*MaxRightTrivial=*/true)));
        Values[t->Value].PushBack(t);
        ++Stats_.Inserts;
        ++Size_;
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
    TNode* FindOrSplit(THolder<TNode>&& t, THolder<TNode>& l, THolder<TNode>& r, const TBorder& key, const TValue& value)
    {
        if (!t) {
            return nullptr;
        }

        int cmp = CompareBorders(key, t->LeftBorder());
        if (cmp == 0 && TValueTraits::Equal(value, t->Value)) {
            // Key is already in the tree, return pointer to an existing node
            return t.Get();
        }

        if (cmp < 0 || cmp == 0 && TValueTraits::Less(value, t->Value)) {
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
        ++Stats_.Deletes;
        --Size_;

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
    bool ExtendRightKey(TNode* t, const TBorder& rightKey, TKey rightOwnedKey) {
        if (CompareBorders(t->RightBorder(), rightKey) < 0) {
            t->RightKey = rightOwnedKey;
            t->RightMode = rightKey.GetMode();
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
    bool ExtendMaxRightKey(TNode* t, const TKey& rightKey, EBorderMode rightMode) {
        int cmp = CompareBorders(t->MaxRightBorder(), TBorder(rightKey, rightMode));
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
    void ExtendMaxRightKeys(TNode* t, const TKey& rightKey, EBorderMode rightMode) {
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
        const TNode* t, const TBorder& leftBorder, const TBorder& rightBorder, TCallback&& callback, bool wentLeft = false) const {
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

    TBorderComparator& MutableComparator() {
        return Comparator;
    }

    const TBorderComparator& GetComparator() const {
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
        TBorder maxRightBorder = t->RightBorder();
        bool maxRightTrivial = true;

        if (auto* l = t->Left.Get()) {
            Y_ENSURE(l->Parent == t, "Left child parent is incorrect");
            Y_ENSURE(l->Prio >= t->Prio, "Left child prio is incorrect");
            cmp = this->CompareBorders(l->LeftBorder(), t->LeftBorder());
            Y_ENSURE(cmp < 0 || cmp == 0 && TValueTraits::Less(l->Value, t->Value), "Left child must be smaller than t");
            TNode* leftRightMost;
            std::tie(leftMost, leftRightMost) = DoValidate(l);
            cmp = this->CompareBorders(leftRightMost->LeftBorder(), t->LeftBorder());
            Y_ENSURE(
                cmp < 0 || cmp == 0 && TValueTraits::Less(leftRightMost->Value, t->Value), "Left child rightmost node must be smaller than t");
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
            Y_ENSURE(cmp < 0 || cmp == 0 && TValueTraits::Less(t->Value, r->Value), "Right child must be bigger than t");
            TNode* rightLeftMost;
            std::tie(rightLeftMost, rightMost) = DoValidate(r);
            cmp = this->CompareBorders(t->LeftBorder(), rightLeftMost->LeftBorder());
            Y_ENSURE(
                cmp < 0 || cmp == 0 && TValueTraits::Less(t->Value, rightLeftMost->Value), "Right child leftmost node must be bigger than t");
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

    int CompareBorders(const TBorder& a, const TBorder& b) const {
        ++Stats_.Comparisons;
        return Comparator.Compare(a, b);
    }

private:
    TBorderComparator Comparator;
    THolder<TNode> Root;
    THashMap<TValue, TIntrusiveList<TNode>, typename TValueTraits::TValueHash> Values;
};
}   // namespace NRangeTreap
}   // namespace NKikimr
