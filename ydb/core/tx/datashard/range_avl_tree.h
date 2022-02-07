#pragma once

#include "range_tree_base.h"

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/random/random.h>

namespace NKikimr {
namespace NDataShard {

    template<class TValue>
    class TRangeAvlTree : public TRangeTreeBase {
    public:
        TRangeAvlTree() = default;

        ~TRangeAvlTree() noexcept {
            delete Root;
        }

    private:
        class TNode : public TIntrusiveListItem<TNode> {
        public:
            TNode* Parent = nullptr;
            TNode* Left = nullptr;
            TNode* Right = nullptr;
            i64 Height = 0;
            TOwnedCellVec LeftKey;
            TOwnedCellVec RightKey;
            TOwnedCellVec MaxRightKey;
            TValue Value;
            EPrefixMode LeftMode;
            EPrefixMode RightMode;
            EPrefixMode MaxRightMode;
            bool MaxRightTrivial;

            ~TNode() noexcept {
                if (Left) {
                    delete Left;
                }
                if (Right) {
                    delete Right;
                }
            }

            TBorder LeftBorder() const noexcept {
                return TBorder{ LeftKey, LeftMode };
            }

            TBorder RightBorder() const noexcept {
                return TBorder{ RightKey, RightMode };
            }

            TBorder MaxRightBorder() const noexcept {
                return TBorder{ MaxRightKey, MaxRightMode };
            }

            TRange ToRange() const noexcept {
                return TRange(LeftKey, TBorder::IsInclusive(LeftMode), RightKey, TBorder::IsInclusive(RightMode));
            }
        };

    private:
        /**
         * Recomputes height for t only
         */
        void RecomputeHeight(TNode* t) noexcept {
            i64 lheight = t->Left ? t->Left->Height : 0;
            i64 rheight = t->Right ? t->Right->Height : 0;
            t->Height = Max(lheight, rheight) + 1;
        }

        /**
         * Recomputes heights for t and all its parents
         */
        void RecomputeHeights(TNode* t) noexcept {
            while (t) {
                i64 lheight = t->Left ? t->Left->Height : 0;
                i64 rheight = t->Right ? t->Right->Height : 0;
                i64 height = Max(lheight, rheight) + 1;

                if (t->Height == height) {
                    return;
                }

                t->Height = height;
                t = t->Parent;
            }
        }

        /**
         * Recomputes MaxRightKey for node t
         */
        void RecomputeMaxRightKey(TNode* t) noexcept {
            t->MaxRightKey = t->RightKey;
            t->MaxRightMode = t->RightMode;
            t->MaxRightTrivial = true;
            if (t->Left && CompareBorders(t->MaxRightBorder(), t->Left->MaxRightBorder()) < 0) {
                t->MaxRightKey = t->Left->MaxRightKey;
                t->MaxRightMode = t->Left->MaxRightMode;
                t->MaxRightTrivial = false;
            }
            if (t->Right && CompareBorders(t->MaxRightBorder(), t->Right->MaxRightBorder()) < 0) {
                t->MaxRightKey = t->Right->MaxRightKey;
                t->MaxRightMode = t->Right->MaxRightMode;
                t->MaxRightTrivial = false;
            }
        }

        /**
         * Extends MaxRightKey for node t
         *
         * Returns true when t->MaxRightKey is modified
         */
        bool ExtendMaxRightKey(TNode* t, const TOwnedCellVec& rightKey, EPrefixMode rightMode) noexcept {
            if (CompareBorders(t->MaxRightBorder(), TBorder{ rightKey, rightMode }) < 0) {
                t->MaxRightKey = rightKey;
                t->MaxRightMode = rightMode;
                t->MaxRightTrivial = false;
                return true;
            } else {
                return false;
            }
        }

        /**
         * Extends MaxRightKey for node t and all parents
         */
        void ExtendMaxRightKeys(TNode* t, const TOwnedCellVec& rightKey, EPrefixMode rightMode) noexcept {
            while (t && ExtendMaxRightKey(t, rightKey, rightMode)) {
                t = t->Parent;
            }
        }

        /**
         * Extends RightKey for node t when necessary
         *
         * Updates MaxRightKey for all parents accordingly
         */
        void ExtendRightKey(TNode* t, const TBorder& rightKey, TOwnedCellVec ownedRightKey) {
            if (CompareBorders(t->RightBorder(), rightKey) < 0) {
                t->RightKey = ownedRightKey;
                t->RightMode = rightKey.Mode;
                if (t->MaxRightTrivial) {
                    t->MaxRightKey = t->RightKey;
                    t->MaxRightMode = t->RightMode;
                    ExtendMaxRightKeys(t->Parent, t->RightKey, t->RightMode);
                } else {
                    int cmp = CompareBorders(t->MaxRightBorder(), t->RightBorder());
                    if (cmp <= 0) {
                        t->MaxRightKey = t->RightKey;
                        t->MaxRightTrivial = true;
                        t->MaxRightMode = t->RightMode;
                    }
                    if (cmp < 0) {
                        ExtendMaxRightKeys(t->Parent, t->RightKey, t->RightMode);
                    }
                }
            }
        }

        TNode* FindFirstUnbalancedGrandParent(TNode* t) noexcept {
            if (t == nullptr || t->Parent == nullptr) {
                return nullptr;
            }

            TNode* gp = t->Parent->Parent;
            while (gp) {
                i64 lheight = gp->Left ? gp->Left->Height : 0;
                i64 rheight = gp->Right ? gp->Right->Height : 0;
                i64 balance = lheight - rheight;

                if (balance < -1 || balance > +1) {
                    return t;
                }

                t = t->Parent;
                gp = gp->Parent;
            }

            return nullptr;
        }

        TNode* FindFirstUnbalancedAndRecomputeMaxRightKey(TNode* t) noexcept {
            while (t) {
                i64 lheight = t->Left ? t->Left->Height : 0;
                i64 rheight = t->Right ? t->Right->Height : 0;
                i64 balance = lheight - rheight;

                if (balance < -1 || balance > +1) {
                    // Note: we don't recompute MaxRightKey for unbalanced node
                    // It's going to be rebalanced and recomputed anyway
                    return t;
                }

                RecomputeMaxRightKey(t);
                t = t->Parent;
            }

            return nullptr;
        }

        /**
         * Rebalances node n, which must have an unbalanced grandparent
         */
        TNode* Rebalance(TNode* n) noexcept {
            TNode* a;
            TNode* b;
            TNode* c;
            TNode* t1;
            TNode* t2;
            TNode* t3;
            TNode* t4;
            TNode* keep;

            TNode* p = n->Parent;
            TNode* gp = p->Parent;
            TNode* ggp = gp->Parent;

            if (gp->Right == p) {
                if (p->Right == n) {
                    // Right Right Case
                    a = gp;
                    b = p;
                    c = n;
                    t1 = gp->Left;
                    t2 = p->Left;
                    t3 = n->Left;
                    t4 = n->Right;
                    keep = n;
                } else {
                    // Right Left Case
                    a = gp;
                    b = n;
                    c = p;
                    t1 = gp->Left;
                    t2 = n->Left;
                    t3 = n->Right;
                    t4 = p->Right;
                    keep = nullptr;
                }
            } else {
                if (p->Right == n) {
                    // Left Right Case
                    a = p;
                    b = n;
                    c = gp;
                    t1 = p->Left;
                    t2 = n->Left;
                    t3 = n->Right;
                    t4 = gp->Right;
                    keep = nullptr;
                } else {
                    // Left Left Case
                    a = n;
                    b = p;
                    c = gp;
                    t1 = n->Left;
                    t2 = n->Right;
                    t3 = p->Right;
                    t4 = gp->Right;
                    keep = n;
                }
            }

            if (ggp == nullptr) {
                Root = b;
            } else if (ggp->Left == gp) {
                ggp->Left = b;
            } else {
                ggp->Right = b;
            }
            b->Parent = ggp;

            b->Left = a;
            a->Parent = b;

            b->Right = c;
            c->Parent = b;

            a->Left = t1;
            if (t1 != nullptr) {
                t1->Parent = a;
            }

            a->Right = t2;
            if (t2 != nullptr) {
                t2->Parent = a;
            }

            c->Left = t3;
            if (t3 != nullptr) {
                t3->Parent = c;
            }

            c->Right = t4;
            if (t4 != nullptr) {
                t4->Parent = c;
            }

            if (a != keep) {
                RecomputeHeight(a);
                RecomputeMaxRightKey(a);
            }

            if (c != keep) {
                RecomputeHeight(c);
                RecomputeMaxRightKey(c);
            }

            RecomputeHeight(b);
            RecomputeMaxRightKey(b);

            RecomputeHeights(ggp);
            return ggp;
        }

    private:
        void DoInsert(
                const TBorder& leftKey,
                const TBorder& rightKey,
                TOwnedCellVec ownedLeftKey,
                TOwnedCellVec ownedRightKey,
                TValue value)
        {
            TNode* current = Root;
            TNode* parent = nullptr;
            bool isLeft = true;
            while (current) {
                int cmp = CompareBorders(leftKey, current->LeftBorder());
                if (cmp == 0 && value == current->Value) {
                    ++Stats_.Updates;
                    ExtendRightKey(current, rightKey, std::move(ownedRightKey));
                    return;
                }

                if (cmp < 0 || cmp == 0 && value < current->Value) {
                    isLeft = true;
                    parent = current;
                    current = current->Left;
                } else {
                    isLeft = false;
                    parent = current;
                    current = current->Right;
                }
            }

            THolder<TNode> p(new TNode);
            p->Parent = parent;
            p->Height = 1;
            p->LeftKey = std::move(ownedLeftKey);
            p->RightKey = std::move(ownedRightKey);
            p->MaxRightKey = p->RightKey;
            p->Value = value;
            p->LeftMode = leftKey.Mode;
            p->RightMode = rightKey.Mode;
            p->MaxRightMode = p->RightMode;
            p->MaxRightTrivial = true;

            TNode* inserted = p.Get();

            if (parent) {
                if (isLeft) {
                    parent->Left = p.Release();
                } else {
                    parent->Right = p.Release();
                }

                RecomputeHeights(parent);
                ExtendMaxRightKeys(parent, inserted->RightKey, inserted->RightMode);

                if (TNode* ub = FindFirstUnbalancedGrandParent(inserted)) {
                    Rebalance(ub);
                }
            } else {
                Root = p.Release();
            }

            Values[value].PushBack(inserted);
            ++Size_;
            ++Stats_.Inserts;
        }

        void DoRemove(TNode* n) noexcept {
            TNode* fixfrom;

            if (n->Right && (!n->Left || n->Left->Height <= n->Right->Height)) {
                TNode* replacement = n->Right;
                while (replacement->Left) {
                    replacement = replacement->Left;
                }

                if (replacement->Parent == n) {
                    fixfrom = replacement;
                } else {
                    fixfrom = replacement->Parent;
                }

                DoRemoveNode(replacement, replacement->Right);
                DoReplaceNode(n, replacement);
            } else if (n->Left) {
                TNode* replacement = n->Left;
                while (replacement->Right) {
                    replacement = replacement->Right;
                }

                if (replacement->Parent == n) {
                    fixfrom = replacement;
                } else {
                    fixfrom = replacement->Parent;
                }

                DoRemoveNode(replacement, replacement->Left);
                DoReplaceNode(n, replacement);
            } else {
                fixfrom = n->Parent;

                DoRemoveNode(n, nullptr);
            }

            delete n;
            --Size_;
            ++Stats_.Deletes;

            if (fixfrom == nullptr) {
                return;
            }

            RecomputeHeights(fixfrom);

            while (TNode* ub = FindFirstUnbalancedAndRecomputeMaxRightKey(fixfrom)) {
                i64 lheight = ub->Left ? ub->Left->Height : 0;
                i64 rheight = ub->Right ? ub->Right->Height : 0;

                if (rheight >= lheight) {
                    ub = ub->Right;
                    lheight = ub->Left ? ub->Left->Height : 0;
                    rheight = ub->Right ? ub->Right->Height : 0;
                } else {
                    ub = ub->Left;
                    lheight = ub->Left ? ub->Left->Height : 0;
                    rheight = ub->Right ? ub->Right->Height : 0;
                }

                if (rheight >= lheight) {
                    ub = ub->Right;
                } else {
                    ub = ub->Left;
                }

                fixfrom = Rebalance(ub);
            }
        }

        void DoRemoveNode(TNode* n, TNode* filler) noexcept {
            TNode* parent = n->Parent;
            if (parent) {
                if (parent->Left == n) {
                    parent->Left = filler;
                } else {
                    parent->Right = filler;
                }
            } else {
                Root = filler;
            }

            if (filler) {
                filler->Parent = parent;
            }

            n->Left = nullptr;
            n->Right = nullptr;
        }

        void DoReplaceNode(TNode* n, TNode* replacement) noexcept {
            TNode* parent = n->Parent;
            TNode* left = n->Left;
            TNode* right = n->Right;

            replacement->Left = left;
            if (left) {
                left->Parent = replacement;
            }

            replacement->Right = right;
            if (right) {
                right->Parent = replacement;
            }

            replacement->Parent = parent;

            if (parent) {
                if (parent->Left == n) {
                    parent->Left = replacement;
                } else {
                    parent->Right = replacement;
                }
            } else {
                Root = replacement;
            }

            replacement->Height = n->Height;
            n->Left = nullptr;
            n->Right = nullptr;
        }

    private:
        /**
         * Validates all invariants for subtree t
         */
        std::tuple<TNode*, TNode*> DoValidate(TNode* t) const noexcept {
            int cmp;
            TNode* leftMost = t;
            TNode* rightMost = t;
            TBorder maxRightBorder = t->RightBorder();
            bool maxRightTrivial = true;
            i64 maxHeight = 1;

            if (auto* l = t->Left) {
                Y_VERIFY(l->Parent == t, "Left child parent is incorrect");
                cmp = this->CompareBorders(l->LeftBorder(), t->LeftBorder());
                Y_VERIFY(cmp < 0 || cmp == 0 && l->Value < t->Value, "Left child must be smaller than t");
                TNode* leftRightMost;
                std::tie(leftMost, leftRightMost) = DoValidate(l);
                cmp = this->CompareBorders(leftRightMost->LeftBorder(), t->LeftBorder());
                Y_VERIFY(cmp < 0 || cmp == 0 && leftRightMost->Value < t->Value, "Left child rightmost node must be smaller than t");
                cmp = this->CompareBorders(maxRightBorder, l->MaxRightBorder());
                if (cmp < 0) {
                    maxRightBorder = l->MaxRightBorder();
                    maxRightTrivial = false;
                }
                maxHeight = Max(maxHeight, l->Height + 1);
            }

            if (auto* r = t->Right) {
                Y_VERIFY(r->Parent == t, "Right child parent is incorrect");
                cmp = this->CompareBorders(t->LeftBorder(), r->LeftBorder());
                Y_VERIFY(cmp < 0 || cmp == 0 && t->Value < r->Value, "Right child must be bigger than t");
                TNode* rightLeftMost;
                std::tie(rightLeftMost, rightMost) = DoValidate(r);
                cmp = this->CompareBorders(t->LeftBorder(), rightLeftMost->LeftBorder());
                Y_VERIFY(cmp < 0 || cmp == 0 && t->Value < rightLeftMost->Value, "Right child leftmost node must be bigger than t");
                cmp = this->CompareBorders(maxRightBorder, r->MaxRightBorder());
                if (cmp < 0) {
                    maxRightBorder = r->MaxRightBorder();
                    maxRightTrivial = false;
                }
                maxHeight = Max(maxHeight, r->Height + 1);
            }

            Y_VERIFY(t->Height == maxHeight, "Subtree height is incorrect");

            cmp = this->CompareBorders(maxRightBorder, t->MaxRightBorder());
            Y_VERIFY(cmp == 0, "Subtree must have max right key equal to the calculated max");
            Y_VERIFY(maxRightTrivial == t->MaxRightTrivial,
                "Subtree must have correct MaxRightTrivial flag (computed=%d, stored=%d)",
                int(maxRightTrivial), int(t->MaxRightTrivial));

            return { leftMost, rightMost };
        }

    public:
        /**
         * Validates all invariants for the tree, used for tests
         */
        void Validate() const noexcept {
            if (Root) {
                Y_VERIFY(Root->Parent == nullptr, "Root must not have a parent");
                DoValidate(Root);
            }
        }

    public:
        void Clear() noexcept {
            Values.clear();
            if (Root) {
                delete std::exchange(Root, nullptr);
            }
            Size_ = 0;
        }

        /**
         * Adds mapping from the given range to the given value
         */
        void AddRange(const TRange& range, TValue value) {
            AddRange(range.ToOwnedRange(), std::move(value));
        }

        /**
         * Adds mapping from the given range to the given value
         */
        void AddRange(TOwnedRange range, TValue value) {
            Y_VERIFY(range.LeftKey.size() <= KeyTypes.size(), "Range left key is too large");
            Y_VERIFY(range.RightKey.size() <= KeyTypes.size(), "Range right key is too large");

            auto leftBorder = TBorder::MakeLeft(range.LeftKey, range.LeftInclusive);
            auto rightBorder = TBorder::MakeRight(range.RightKey, range.RightInclusive);

            DoInsert(
                leftBorder,
                rightBorder,
                std::move(range.LeftKey),
                std::move(range.RightKey),
                value);
        }

        /**
         * Removes all ranges with the given value
         */
        void RemoveRanges(TValue value) {
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
        template<class TCallback>
        void EachRange(TCallback&& callback) const {
            if (Root) {
                DoEachRange(Root, callback);
            }
        }

        /**
         * Calls callback for each range with the given point
         */
        template<class TCallback>
        void EachIntersection(TConstArrayRef<TCell> point, TCallback&& callback) const {
            EachIntersection(TRange(point, true, point, true), callback);
        }

        /**
         * Calls callback for each range intersecting with the query
         */
        template<class TCallback>
        void EachIntersection(const TRange& range, TCallback&& callback) const {
            if (Root) {
                DoEachIntersection(
                    Root,
                    TBorder::MakeLeft(range.LeftKey, range.LeftInclusive),
                    TBorder::MakeRight(range.RightKey, range.RightInclusive),
                    callback);
            }
        }

        size_t Height() const {
            return Root ? Root->Height : 0;
        }

    private:
        template<class TCallback>
        void DoEachRange(const TNode* t, TCallback&& callback) const {
            if (t->Left) {
                DoEachRange(t->Left, callback);
            }
            {
                callback(t->ToRange(), t->Value);
            }
            if (t->Right) {
                DoEachRange(t->Right, callback);
            }
        }

        template<class TCallback>
        void DoEachIntersection(
                const TNode* t,
                const TBorder& leftBorder,
                const TBorder& rightBorder,
                TCallback&& callback,
                bool wentLeft = false) const
        {
            int cmp;

            if (wentLeft) {
                cmp = CompareBorders(t->MaxRightBorder(), leftBorder);
                if (cmp < 0) {
                    // There is no intersection with this whole subtree
                    return;
                }
            }

            if (t->Left) {
                // Descend into the left subtree
                // Note it will terminate on subtrees that have small MaxRightBorder
                DoEachIntersection(t->Left, leftBorder, rightBorder, callback, true);
            }

            cmp = CompareBorders(rightBorder, t->LeftBorder());
            if (cmp < 0) {
                // There is no intersection with this node or the right subtree
                return;
            }

            // N.B. we avoid comparison with RightKey when it is equal to MaxRightKey.
            if ((wentLeft && t->MaxRightTrivial) ||
                (cmp = CompareBorders(leftBorder, t->RightBorder())) <= 0)
            {
                callback(t->ToRange(), t->Value);
            }

            if (t->Right) {
                // Descend into the right subtree
                DoEachIntersection(t->Right, leftBorder, rightBorder, callback);
            }
        }

    private:
        TNode* Root = nullptr;
        THashMap<TValue, TIntrusiveList<TNode>> Values;
    };

} // namespace NDataShard
} // namespace NKikimr
