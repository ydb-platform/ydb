#pragma once
#include <ydb/library/union_copy_set/union_copy_set.h>
#include <util/random/random.h>
#include <compare>
#include <memory>

namespace NKikimr {

    /**
     * Intersection tree with unique hashable values
     */
    template<class TKey, class TValue, class TCompare = std::compare_three_way>
    class TIntersectionTree
        : private TCompare
    {
    public:
        /**
         * A key that partitions a range into left and right subranges
         */
        struct TPartitionKey {
            TKey Key;
            // When true keys equal to Key go into the left subtree
            // A range like `[a, b]` translates into partition keys `{a, false}` and `{b, true}`
            bool EqualGoesLeft;

            bool IsLeftInclusive() const {
                return !EqualGoesLeft;
            }

            bool IsRightInclusive() const {
                return EqualGoesLeft;
            }

            friend IOutputStream& operator<<(IOutputStream& out, const TPartitionKey& key) {
                return out << "{ " << key.Key << ", " << (key.EqualGoesLeft ? "true" : "false") << " }";
            }

            friend auto operator<=>(const TPartitionKey& a, const TPartitionKey& b)
                requires (std::same_as<TCompare, std::compare_three_way>)
            {
                return CompareKeys(TCompare{}, a, b);
            }
        };

    private:
        static std::partial_ordering CompareKeys(const TCompare& cmp, const TKey& a, const TKey& b) {
            return cmp(a, b);
        }

        static std::partial_ordering CompareKeys(const TCompare& cmp, const TPartitionKey& a, const TPartitionKey& b) {
            std::partial_ordering order = CompareKeys(cmp, a.Key, b.Key);
            if (order != std::partial_ordering::equivalent) {
                return order;
            }
            // A range like `[k, k]` partitions key space into `[-inf, k), [k, k], (k, +inf]`, where
            // the left border (`{k, false}`) must be less than the right border (`{k, true}`).
            // This means `{k, false} < {k, true}` or `false < true`.
            if (!a.EqualGoesLeft) {
                return b.EqualGoesLeft ? std::partial_ordering::less : std::partial_ordering::equivalent;
            } else {
                return b.EqualGoesLeft ? std::partial_ordering::equivalent : std::partial_ordering::greater;
            }
        }

    private:
        struct TValueNode;

        using TValueMap = THashMap<TValue, TValueNode>;
        using TValueSet = TUnionCopySet<TValueNode>;

        struct TValueNode : public TValueSet::TItem {
            TValue Value;
            TPartitionKey LeftKey;
            TPartitionKey RightKey;

            TValueNode(const TValue& value, const TKey& leftKey, const TKey& rightKey)
                : Value(value)
                , LeftKey{leftKey, false}
                , RightKey{rightKey, true}
            {}
        };

        /**
         * A node in the tree
         */
        struct TNode {
            TNode* Parent = nullptr;
            std::unique_ptr<TNode> Left;
            std::unique_ptr<TNode> Right;
            const ui32 Prio;
            ui32 UseCount = 0;
            i32 LeftDelta = 0;
            i32 RightDelta = 0;
            i32 MaxLeftCount = 0;
            i32 MaxRightCount = 0;
            const TPartitionKey Key;
            TValueSet LeftValues;
            TValueSet RightValues;

            TNode(const TPartitionKey& key, ui32 prio)
                : Prio(prio)
                , Key(key)
            {}

            friend IOutputStream& operator<<(IOutputStream& out, const TNode& node) {
                out << "{ Key = " << node.Key << ", Prio = " << node.Prio;
                out << ", LeftDelta = " << node.LeftDelta << ", MaxLeftCount = " << node.MaxLeftCount << ", Left = ";
                if (node.Left) {
                    out << *node.Left;
                } else {
                    out << "nullptr";
                }
                out << ", RightDelta = " << node.RightDelta << ", MaxRightCount = " << node.MaxRightCount << ", Right = ";
                if (node.Right) {
                    out << *node.Right;
                } else {
                    out << "nullptr";
                }
                return out << " }";
            }

            void SetLeft(std::unique_ptr<TNode> child, i32 delta, TValueSet&& values) {
                if (child) {
                    child->Parent = this;
                }
                Left = std::move(child);
                LeftDelta += delta;
                LeftValues += std::move(values);
                RecomputeMaxLeftCount();
            }

            void SetRight(std::unique_ptr<TNode> child, i32 delta, TValueSet&& values) {
                if (child) {
                    child->Parent = this;
                }
                Right = std::move(child);
                RightDelta += delta;
                RightValues += std::move(values);
                RecomputeMaxRightCount();
            }

            std::unique_ptr<TNode> RemoveLeft() {
                if (Left) {
                    Left->Parent = nullptr;
                    Left->LeftDelta += LeftDelta;
                    Left->MaxLeftCount += LeftDelta;
                    Left->RightDelta += LeftDelta;
                    Left->MaxRightCount += LeftDelta;
                    if (LeftValues) {
                        Left->LeftValues += LeftValues;
                        Left->RightValues += std::move(LeftValues);
                    }
                }
                LeftDelta = 0;
                MaxLeftCount = 0;
                LeftValues.Clear();
                return std::move(Left);
            }

            std::unique_ptr<TNode> RemoveRight() {
                if (Right) {
                    Right->Parent = nullptr;
                    Right->LeftDelta += RightDelta;
                    Right->MaxLeftCount += RightDelta;
                    Right->RightDelta += RightDelta;
                    Right->MaxRightCount += RightDelta;
                    if (RightValues) {
                        Right->LeftValues += RightValues;
                        Right->RightValues += std::move(RightValues);
                    }
                }
                RightDelta = 0;
                MaxRightCount = 0;
                RightValues.Clear();
                return std::move(Right);
            }

            i32 GetMaxCount() const {
                return std::max(MaxLeftCount, MaxRightCount);
            }

            void RecomputeMaxLeftCount() {
                MaxLeftCount = LeftDelta + (Left ? Left->GetMaxCount() : i32(0));
            }

            void RecomputeMaxRightCount() {
                MaxRightCount = RightDelta + (Right ? Right->GetMaxCount() : i32(0));
            }
        };

    public:
        void Add(const TValue& value, const TKey& leftKey, const TKey& rightKey) {
            auto res = Values.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(value),
                std::forward_as_tuple(value, leftKey, rightKey));
            Y_ENSURE(res.second);
            TValueNode* valueNode = &res.first->second;

            TNode* left = FindOrInsert(valueNode->LeftKey, RandomNumber<ui32>());
            TNode* right = FindOrInsert(valueNode->RightKey, RandomNumber<ui32>());
            left->UseCount++;
            right->UseCount++;
            AddRangeDelta(left, right, +1, valueNode);
        }

        void Remove(const TValue& value) {
            auto it = Values.find(value);
            if (it == Values.end()) {
                return;
            }
            TValueNode* valueNode = &it->second;

            TNode* left = Find(valueNode->LeftKey);
            TNode* right = Find(valueNode->RightKey);
            Y_ENSURE(left && right);
            left->UseCount--;
            right->UseCount--;

            // Note: this will remove valueNode from all value sets
            Values.erase(it);

            AddRangeDelta(left, right, -1);
            if (left->UseCount == 0) {
                DoRemove(left);
            }
            if (right->UseCount == 0) {
                DoRemove(right);
            }
        }

        friend IOutputStream& operator<<(IOutputStream& out, const TIntersectionTree& tree) {
            if (tree.Root) {
                out << *tree.Root;
            } else {
                out << "{ }";
            }
            return out;
        }

        i32 GetMaxCount() const {
            if (!Root) {
                return 0;
            }

            return Root->GetMaxCount();
        }

        class TFoundRange {
            friend TIntersectionTree<TKey, TValue, TCompare>;

        public:
            TFoundRange() = default;

            TFoundRange(const TNode* leftBorder, const TNode* rightBorder, i32 leftCount, i32 rightCount, bool lastLeft)
                : LeftBorder(leftBorder)
                , RightBorder(rightBorder)
                , LeftCount(leftCount)
                , RightCount(rightCount)
                , LastLeft(lastLeft)
            {}

            explicit operator bool() const {
                return LeftBorder && RightBorder;
            }

            bool HasLeftKey() const {
                return LeftBorder != nullptr;
            }

            const TPartitionKey& GetLeftPartitionKey() const {
                Y_ENSURE(LeftBorder);
                return LeftBorder->Key;
            }

            const TKey& GetLeftKey() const {
                return GetLeftPartitionKey().Key;
            }

            bool IsLeftInclusive() const {
                return !GetLeftPartitionKey().IsLeftInclusive();
            }

            bool HasRightKey() const {
                return RightBorder != nullptr;
            }

            const TPartitionKey& GetRightPartitionKey() const {
                Y_ENSURE(RightBorder);
                return RightBorder->Key;
            }

            const TKey& GetRightKey() const {
                return GetRightPartitionKey().Key;
            }

            bool IsRightInclusive() const {
                return GetRightPartitionKey().IsRightInclusive();
            }

            i32 GetCount() const {
                return LastLeft ? (RightCount + RightBorder->LeftDelta) : (LeftCount + LeftBorder->RightDelta);
            }

            template<class TCallback>
            bool ForEachValue(const TCallback& callback) {
                const TNode* node = LastLeft ? RightBorder : LeftBorder;
                if (!node) {
                    return true;
                }
                bool leftChild = LastLeft;
                for (;;) {
                    const TValueSet& values = leftChild ? node->LeftValues : node->RightValues;
                    bool keepGoing = values.ForEachValue([&](const TValueNode* p) {
                        return callback(p->Value);
                    });
                    if (!keepGoing) {
                        return false;
                    }
                    const TNode* parent = node->Parent;
                    if (!parent) {
                        break;
                    }
                    leftChild = parent->Left.get() == node;
                    Y_DEBUG_ABORT_UNLESS(leftChild || parent->Right.get() == node);
                    node = parent;
                }
                return true;
            }

            TFoundRange Prev() const {
                if (Y_UNLIKELY(!LeftBorder)) {
                    return TFoundRange();
                }
                const TNode* newRight = LeftBorder;
                i32 newRightCount = RightCount;
                auto [newLeft, newLeftCount] = Predecessor(newRight, newRightCount);
                if (newRight->Left) {
                    return TFoundRange(newLeft, newRight, newLeftCount, newRightCount, false);
                } else {
                    return TFoundRange(newLeft, newRight, newLeftCount, newRightCount, true);
                }
            }

            TFoundRange Next() const {
                if (Y_UNLIKELY(!RightBorder)) {
                    return TFoundRange();
                }
                const TNode* newLeft = RightBorder;
                i32 newLeftCount = RightCount;
                auto [newRight, newRightCount] = Successor(newLeft, newLeftCount);
                if (newLeft->Right) {
                    return TFoundRange(newLeft, newRight, newLeftCount, newRightCount, true);
                } else {
                    return TFoundRange(newLeft, newRight, newLeftCount, newRightCount, false);
                }
            }

        private:
            static std::pair<const TNode*, i32> Predecessor(const TNode* node, i32 count) {
                if (node->Left) {
                    count += node->LeftDelta;
                    node = node->Left.get();
                    while (node->Right) {
                        count += node->RightDelta;
                        node = node->Right.get();
                    }
                    return {node, count};
                } else {
                    while (const TNode* parent = node->Parent) {
                        if (parent->Right.get() == node) {
                            count -= parent->RightDelta;
                            return {parent, count};
                        }
                        Y_DEBUG_ABORT_UNLESS(parent->Left.get() == node);
                        count -= parent->RightDelta;
                        node = parent;
                    }
                    return {nullptr, count};
                }
            }

            static std::pair<const TNode*, i32> Successor(const TNode* node, i32 count) {
                if (node->Right) {
                    count += node->RightDelta;
                    node = node->Right.get();
                    while (node->Left) {
                        count += node->LeftDelta;
                        node = node->Left.get();
                    }
                    return {node, count};
                } else {
                    while (const TNode* parent = node->Parent) {
                        if (parent->Left.get() == node) {
                            count -= parent->LeftDelta;
                            return {parent, count};
                        }
                        Y_DEBUG_ABORT_UNLESS(parent->Right.get() == node);
                        count -= parent->RightDelta;
                        node = parent;
                    }
                    return {nullptr, count};
                }
            }

        private:
            const TNode* LeftBorder = nullptr;
            const TNode* RightBorder = nullptr;
            i32 LeftCount = 0;
            i32 RightCount = 0;
            bool LastLeft = false;
        };

        TFoundRange FirstRange() const {
            TFoundRange range;
            const TNode* node = Root.get();
            i32 count = 0;
            while (node) {
                if (node->Left || range.LeftBorder) {
                    // We go left, truncating range on the right
                    range.RightBorder = node;
                    range.RightCount = count;
                    range.LastLeft = true;
                    count += node->LeftDelta;
                    node = node->Left.get();
                } else {
                    // We go right, truncating range on the left
                    range.LeftBorder = node;
                    range.LeftCount = count;
                    range.LastLeft = false;
                    count += node->RightDelta;
                    node = node->Right.get();
                }
            }
            return range;
        }

        TFoundRange FindRange(const TKey& key) const {
            TFoundRange range;
            const TNode* node = Root.get();
            i32 count = 0;
            while (node) {
                auto order = CompareKeys(*this, key, node->Key.Key);
                if (order < 0 || order == 0 && node->Key.EqualGoesLeft) {
                    // We go left, truncating range on the right
                    range.RightBorder = node;
                    range.RightCount = count;
                    range.LastLeft = true;
                    count += node->LeftDelta;
                    node = node->Left.get();
                } else {
                    // We go right, truncating range on the left
                    range.LeftBorder = node;
                    range.LeftCount = count;
                    range.LastLeft = false;
                    count += node->RightDelta;
                    node = node->Right.get();
                }
            }
            return range;
        }

        TFoundRange GetMaxRange() const {
            TFoundRange range;
            const TNode* node = Root.get();
            i32 count = 0;
            while (node) {
                if (node->MaxLeftCount >= node->MaxRightCount) {
                    // We go left, truncating range on the right
                    range.RightBorder = node;
                    range.RightCount = count;
                    range.LastLeft = true;
                    count += node->LeftDelta;
                    node = node->Left.get();
                } else {
                    // We go right, truncating range on the left
                    range.LeftBorder = node;
                    range.LeftCount = count;
                    range.LastLeft = false;
                    count += node->RightDelta;
                    node = node->Right.get();
                }
            }
            return range;
        }

        template<class TCallback>
        bool ForEachRange(const TCallback& callback) const {
            auto range = FirstRange();
            while (range) {
                if (!callback((const TFoundRange&)range)) {
                    return false;
                }
                range = range.Next();
            }
            return true;
        }

    private:
        void AddRangeDelta(TNode* left, TNode* right, i32 delta, TValueNode* valueNode = nullptr) {
            TNode* stop = CommonAncestor(left, right);
            AddRangeDeltaLeftSpine(left, stop, delta, valueNode);
            AddRangeDeltaRightSpine(right, stop, delta, valueNode);
            while (TNode* parent = stop->Parent) {
                if (stop == parent->Left.get()) {
                    parent->RecomputeMaxLeftCount();
                } else {
                    parent->RecomputeMaxRightCount();
                }
                stop = parent;
            }
        }

        void AddRangeDeltaLeftSpine(TNode* node, TNode* stop, i32 delta, TValueNode* valueNode = nullptr) {
            bool selected = true;
            while (node != stop) {
                TNode* parent = node->Parent;
                if (selected) {
                    node->RightDelta += delta;
                    node->MaxRightCount += delta;
                    if (valueNode) {
                        node->RightValues.Add(valueNode);
                    }
                }
                if (node == parent->Left.get()) {
                    parent->RecomputeMaxLeftCount();
                    selected = true;
                } else {
                    parent->RecomputeMaxRightCount();
                    selected = false;
                }
                node = parent;
            }
        }

        void AddRangeDeltaRightSpine(TNode* node, TNode* stop, i32 delta, TValueNode* valueNode = nullptr) {
            bool selected = true;
            while (node != stop) {
                TNode* parent = node->Parent;
                if (selected) {
                    node->LeftDelta += delta;
                    node->MaxLeftCount += delta;
                    if (valueNode) {
                        node->LeftValues.Add(valueNode);
                    }
                }
                if (node == parent->Right.get()) {
                    parent->RecomputeMaxRightCount();
                    selected = true;
                } else {
                    parent->RecomputeMaxLeftCount();
                    selected = false;
                }
                node = parent;
            }
        }

    private:
        TNode* CommonAncestor(TNode* a, TNode* b) {
            TNode* p = a;
            TNode* q = b;
            // There are 2 cases:
            // 1. a and b are the same depth, in which case by following parent
            //    pointers we will reach the common ancestor directly.
            // 2. a and b are different depths, in which case either p or q
            //    will reach the root first, and we indirectly compute the
            //    difference between their depths. We loop from the shallower
            //    to the deeper node, and ascend it by the difference. By the
            //    time we switch from the deeper to the shallower node both
            //    p and q will be the same depth, which turns it into case 1.
            //    Complexity is equivalent to computing each depth first and
            //    then ascending to the same depth, but makes case 1 more
            //    efficient and doesn't need as many comparisons.
            while (p != q) {
                p = p->Parent;
                q = q->Parent;
                Y_DEBUG_ABORT_UNLESS(p || q);
                if (!p) {
                    p = b;
                }
                if (!q) {
                    q = a;
                }
            }
            return p;
        }

    private:
        TNode* Find(const TPartitionKey& key) {
            TNode* t = Root.get();
            while (t) {
                auto order = CompareKeys(*this, key, t->Key);
                if (order == std::partial_ordering::equivalent) {
                    return t;
                }

                t = order < 0 ? t->Left.get() : t->Right.get();
            }
            return nullptr;
        }

        TNode* FindOrInsert(const TPartitionKey& key, ui32 prio) {
            std::unique_ptr<TNode> l, r;
            i32 lDelta = 0;
            i32 rDelta = 0;
            TValueSet lValues;
            TValueSet rValues;
            TNode* parent = nullptr;
            bool leftChild = false;
            std::unique_ptr<TNode>* tptr = &Root;
            TNode* t;
            while ((t = tptr->get()) != nullptr) {
                if (prio < t->Prio) {
                    // New node must be a parent of t, split it first
                    if (TNode* p = FindOrSplit(std::move(*tptr), l, lDelta, lValues, r, rDelta, rValues, key)) {
                        // We found an existing node for key
                        return p;
                    }

                    break;
                }

                auto order = CompareKeys(*this, key, t->Key);
                if (order == std::partial_ordering::equivalent) {
                    return t;
                }

                parent = t;
                leftChild = order < 0;
                tptr = leftChild ? &t->Left : &t->Right;
            }

            // Make a new subtree root with l and r as children
            // Note: parent's max count aggregate doesn't change
            tptr->reset((t = new TNode(key, prio)));
            t->Parent = parent;
            t->SetLeft(std::move(l), lDelta, std::move(lValues));
            t->SetRight(std::move(r), rDelta, std::move(rValues));
            return t;
        }

        /**
         * Split subtree t into subtrees l and r, such that l < key and key < r
         *
         * Does nothing and returns existing node if key already exist in subtree t.
         */
        TNode* FindOrSplit(
                std::unique_ptr<TNode>&& t,
                std::unique_ptr<TNode>& l, i32& lDelta, TValueSet& lValues,
                std::unique_ptr<TNode>& r, i32& rDelta, TValueSet& rValues,
                const TPartitionKey& key)
        {
            if (!t) {
                return nullptr;
            }

            auto order = CompareKeys(*this, key, t->Key);
            if (order == std::partial_ordering::equivalent) {
                // Key already exists in the subtree
                return t.get();
            }

            if (order < 0) {
                // We must split the left subtree
                lDelta += t->LeftDelta;
                lValues += t->LeftValues;
                if (t->Left) {
                    std::unique_ptr<TNode> tmp;
                    i32 tmpDelta = 0;
                    TValueSet tmpValues;
                    if (TNode* found = FindOrSplit(std::move(t->Left), l, lDelta, lValues, tmp, tmpDelta, tmpValues, key)) {
                        return found;
                    }
                    t->SetLeft(std::move(tmp), tmpDelta, std::move(tmpValues));
                }
                t->Parent = nullptr;
                r = std::move(t);
            } else {
                // We must split the right subtree
                rDelta += t->RightDelta;
                rValues += t->RightValues;
                if (t->Right) {
                    std::unique_ptr<TNode> tmp;
                    i32 tmpDelta = 0;
                    TValueSet tmpValues;
                    if (TNode* found = FindOrSplit(std::move(t->Right), tmp, tmpDelta, tmpValues, r, rDelta, rValues, key)) {
                        return found;
                    }
                    t->SetRight(std::move(tmp), tmpDelta, std::move(tmpValues));
                }
                t->Parent = nullptr;
                l = std::move(t);
            }

            return nullptr;
        }

    private:
        void DoRemove(TNode* t) {
            TNode* p = t->Parent;
            if (p) {
                if (p->Left.get() == t) {
                    DoRemove(&p->Left, true);
                } else {
                    DoRemove(&p->Right, false);
                }
            } else {
                DoRemove(&Root, true);
            }
        }

        void DoRemove(std::unique_ptr<TNode>* tptr, bool leftChild) {
            std::unique_ptr<TNode> d = std::move(*tptr);
            TNode* parent = std::exchange(d->Parent, nullptr);

            // Note: parent's max count aggregate doesn't change

            if (d->Left || d->Right) {
                *tptr = Merge(d->RemoveLeft(), d->RemoveRight());
                tptr->get()->Parent = parent;
            } else if (parent) {
                if (leftChild) {
                    parent->LeftDelta += d->LeftDelta;
                    parent->LeftValues += std::move(d->LeftValues);
                } else {
                    parent->RightDelta += d->RightDelta;
                    parent->RightValues += std::move(d->RightValues);
                }
            }
        }

        std::unique_ptr<TNode> Merge(std::unique_ptr<TNode> l, std::unique_ptr<TNode> r) {
            if (!l || !r) {
                if (l) {
                    return l;
                } else {
                    return r;
                }
            } else if (l->Prio <= r->Prio) {
                l->SetRight(Merge(l->RemoveRight(), std::move(r)), 0, {});
                return l;
            } else {
                r->SetLeft(Merge(std::move(l), r->RemoveLeft()), 0, {});
                return r;
            }
        }

    private:
        std::unique_ptr<TNode> Root;
        TValueMap Values;
    };

} // namespace NKikimr
