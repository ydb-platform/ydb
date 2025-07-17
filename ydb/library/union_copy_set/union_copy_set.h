#pragma once
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/ptr.h>
#include <util/generic/intrlist.h>
#include <util/string/builder.h>
#include <cstdint>

namespace NKikimr {

    /**
     * Union-Copy Disjoint Set data structure
     *
     * Has O(1) union and O(1) copy operations for sets
     */
    template<class TValue>
    class TUnionCopySet {
    public:
        /**
         * Base class for TValue
         */
        class TItem;

    private:
        /**
         * Pointer tag that specifies the target type
         */
        enum class ETag : uint8_t {
            // Pointer is to either set or item
            Item = 0,
            // Pointer is to the copy node/item
            Copy = 1 << 0,
            // Pointer is to the union node/item
            Union = 1 << 1,
        };

        static constexpr uintptr_t TagMask = uintptr_t(3);
        static constexpr uintptr_t PointerMask = ~TagMask;

        struct TCopyNodeItem;
        struct TCopyNode;
        struct TUnionNodeItem;
        struct TUnionNode;

        /**
         * A tagged pointer that is pointing up (towards sets)
         */
        class TUpPtr {
        public:
            explicit operator bool() const {
                return Value != 0;
            }

            ETag GetTag() const {
                return ETag(Value & TagMask);
            }

            bool IsItemPtr() const {
                return GetTag() == ETag::Item;
            }

            TUnionCopySet* GetItemPtr() const {
                Y_ABORT_UNLESS(IsItemPtr());
                return reinterpret_cast<TUnionCopySet*>(Value);
            }

            bool IsCopyNodePtr() const {
                return GetTag() == ETag::Copy;
            }

            TCopyNode* GetCopyNodePtr() const {
                Y_ABORT_UNLESS(IsCopyNodePtr());
                return reinterpret_cast<TCopyNode*>(Value & PointerMask);
            }

            bool IsUnionNodeItemPtr() const {
                return GetTag() == ETag::Union;
            }

            TUnionNodeItem* GetUnionNodeItemPtr() const {
                Y_ABORT_UNLESS(IsUnionNodeItemPtr());
                return reinterpret_cast<TUnionNodeItem*>(Value & PointerMask);
            }

            void SetNull() {
                Value = 0;
            }

            void Set(TUnionCopySet* p) {
                Value = reinterpret_cast<uintptr_t>(p);
            }

            void Set(TCopyNode* p) {
                Value = reinterpret_cast<uintptr_t>(p) | uintptr_t(ETag::Copy);
            }

            void Set(TUnionNodeItem* p) {
                Value = reinterpret_cast<uintptr_t>(p) | uintptr_t(ETag::Union);
            }

            friend bool operator==(const TUpPtr& a, TUpPtr& b) noexcept {
                return a.Value == b.Value;
            }

            friend bool operator==(const TUpPtr& a, TUnionCopySet* b) noexcept {
                return a.IsItemPtr() && a.GetItemPtr() == b;
            }

            friend bool operator==(const TUpPtr& a, TCopyNode* b) noexcept {
                return a.IsCopyNodePtr() && a.GetCopyNodePtr() == b;
            }

            friend bool operator==(const TUpPtr& a, TUnionNodeItem* b) noexcept {
                return a.IsUnionNodeItemPtr() && a.GetUnionNodeItemPtr() == b;
            }

        private:
            uintptr_t Value = 0;
        };

        /**
         * A tagged pointer that is pointing down (towards items)
         */
        class TDownPtr {
        public:
            explicit operator bool() const {
                return Value != 0;
            }

            ETag GetTag() const {
                return ETag(Value & TagMask);
            }

            bool IsItemPtr() const {
                return GetTag() == ETag::Item;
            }

            TItem* GetItemPtr() const {
                Y_ABORT_UNLESS(IsItemPtr());
                return reinterpret_cast<TItem*>(Value);
            }

            bool IsCopyNodeItemPtr() const {
                return GetTag() == ETag::Copy;
            }

            TCopyNodeItem* GetCopyNodeItemPtr() const {
                Y_ABORT_UNLESS(IsCopyNodeItemPtr());
                return reinterpret_cast<TCopyNodeItem*>(Value & PointerMask);
            }

            bool IsUnionNodePtr() const {
                return GetTag() == ETag::Union;
            }

            TUnionNode* GetUnionNodePtr() const {
                Y_ABORT_UNLESS(IsUnionNodePtr());
                return reinterpret_cast<TUnionNode*>(Value & PointerMask);
            }

            void SetNull() {
                Value = 0;
            }

            void Set(TItem* p) {
                Value = reinterpret_cast<uintptr_t>(p);
            }

            void Set(TCopyNodeItem* p) {
                Value = reinterpret_cast<uintptr_t>(p) | uintptr_t(ETag::Copy);
            }

            void Set(TUnionNode* p) {
                Value = reinterpret_cast<uintptr_t>(p) | uintptr_t(ETag::Union);
            }

            friend bool operator==(const TDownPtr& a, const TDownPtr& b) noexcept {
                return a.Value == b.Value;
            }

            friend bool operator==(const TDownPtr& a, TItem* p) noexcept {
                return a.IsItemPtr() && a.GetItemPtr() == p;
            }

            friend bool operator==(const TDownPtr& a, TCopyNodeItem* b) noexcept {
                return a.IsCopyNodeItemPtr() && a.GetCopyNodeItemPtr() == b;
            }

            friend bool operator==(const TDownPtr& a, TUnionNode* b) noexcept {
                return a.IsUnionNodePtr() && a.GetUnionNodePtr() == b;
            }

        private:
            uintptr_t Value = 0;
        };

        struct TCopyNodeItem : public TIntrusiveListItem<TCopyNodeItem> {
            TUpPtr Up;
            TCopyNode* Down = nullptr;
        };

        struct TCopyNode {
            TIntrusiveList<TCopyNodeItem> Items;
            size_t Count = 0;
            TDownPtr Down;

            void Add(TCopyNodeItem* p) {
                Items.PushBack(p);
                p->Down = this;
                ++Count;
            }

            void Remove(TCopyNodeItem* p) {
                p->Down = nullptr;
                Items.Remove(p);
                --Count;
            }
        };

        struct TUnionNodeItem : public TIntrusiveListItem<TUnionNodeItem> {
            TUnionNode* Up = nullptr;
            TDownPtr Down;
        };

        struct TUnionNode {
            TIntrusiveList<TUnionNodeItem> Items;
            size_t Count = 0;
            TUpPtr Up;

            void Add(TUnionNodeItem* p) {
                Items.PushBack(p);
                p->Up = this;
                ++Count;
            }

            void Remove(TUnionNodeItem* p) {
                p->Up = nullptr;
                Items.Remove(p);
                --Count;
            }
        };

    public:
        class TItem {
            friend TUnionCopySet<TValue>;

        public:
            TItem() noexcept = default;

            TItem(TItem&& rhs) noexcept
                : Up(rhs.Up)
            {
                if (Up) {
                    rhs.Up.SetNull();
                    RelinkUpToSelf();
                }
            }

            ~TItem() {
                if (Up) {
                    DestroyUp(Up);
                }
            }

            TItem(const TItem&) = delete;
            TItem& operator=(const TItem&) = delete;

        private:
            void RelinkUpToSelf() noexcept {
                switch (Up.GetTag()) {
                    case ETag::Item: {
                        Up.GetItemPtr()->Down.Set(this);
                        break;
                    }
                    case ETag::Copy: {
                        Up.GetCopyNodePtr()->Down.Set(this);
                        break;
                    }
                    case ETag::Union: {
                        Up.GetUnionNodeItemPtr()->Down.Set(this);
                        break;
                    }
                }
            }

        private:
            TUpPtr Up;
        };

    private:
        static void DestroyUp(TUpPtr up) {
            // Note: we cannot actually have a deep recursion in an optimized tree,
            // because copy and union nodes alternate, and when we encounter a
            // union node it has at least 2 items. When a single item is left
            // it is optimized away without recursion. However, we might want
            // to skip expensive optimization passes in the future, and might
            // have a potentially deep chain of copy nodes, so avoid recursion.
            TStackVec<TUpPtr> queue;
            for (;;) {
                switch (up.GetTag()) {
                    case ETag::Item: {
                        TUnionCopySet* set = up.GetItemPtr();
                        set->Down.SetNull();
                        break;
                    }
                    case ETag::Copy: {
                        TCopyNode* node = up.GetCopyNodePtr();
                        if (node->Items.Empty()) {
                            // This node is empty and can finally be deleted
                            std::unique_ptr<TCopyNode> d(node);
                            break;
                        }
                        // Mark this node as "delete in progress" so when a set
                        // is not disjoint this node is not optimized away and
                        // deleted unexpectedly (since we still hold a pointer
                        // to it and need it to remain valid)
                        node->Count = 0;
                        node->Down.SetNull();
                        // We want to visit this node again until it's empty
                        queue.push_back(up);
                        // Remove one item at a time
                        std::unique_ptr<TCopyNodeItem> item(node->Items.PopFront());
                        Y_DEBUG_ABORT_UNLESS(item->Up, "Unexpected null link up during destruction");
                        if (item->Up) [[likely]] {
                            up = item->Up;
                            continue;
                        }
                        break;
                    }
                    case ETag::Union: {
                        // We remove current item from the union node
                        std::unique_ptr<TUnionNodeItem> item(up.GetUnionNodeItemPtr());
                        TUnionNode* node = item->Up;
                        Y_DEBUG_ABORT_UNLESS(node->Count > 0);
                        node->Remove(item.get());
                        item.reset();
                        switch (node->Count) {
                            case 0: [[unlikely]] {
                                // Note: this cannot normally happen, since nodes
                                // are removed from the graph when their count
                                // reaches 1. However allocation errors during
                                // insertions could leave count 1 in place.
                                std::unique_ptr<TUnionNode> d(node);
                                Y_DEBUG_ABORT_UNLESS(node->Up, "Unexpected null link up during destruction");
                                if (node->Up) [[likely]] {
                                    up = node->Up;
                                    continue;
                                }
                                break;
                            }
                            case 1: {
                                OptimizeAway(node);
                                break;
                            }
                        }
                        break;
                    }
                }
                if (queue.empty()) {
                    break;
                }
                up = queue.back();
                queue.pop_back();
            }
        }

        static void DestroyDown(TDownPtr down) {
            // Note: we cannot actually have a deep recursion in an optimized tree,
            // because copy and union nodes alternate, and when we encounter a
            // union node it has at least 2 items. When a single item is left
            // it is optimized away without recursion. However, we might want
            // to skip expensive optimization passes in the future, and might
            // have a potentially deep chain of copy nodes, so avoid recursion.
            TStackVec<TDownPtr> queue;
            for (;;) {
                switch (down.GetTag()) {
                    case ETag::Item: {
                        TItem* item = down.GetItemPtr();
                        item->Up.SetNull();
                        break;
                    }
                    case ETag::Copy: {
                        // We remove current item from the copy node
                        std::unique_ptr<TCopyNodeItem> item(down.GetCopyNodeItemPtr());
                        TCopyNode* node = item->Down;
                        Y_DEBUG_ABORT_UNLESS(node->Count > 0);
                        node->Remove(item.get());
                        item.reset();
                        switch (node->Count) {
                            case 0: [[unlikely]] {
                                // Note: this cannot normally happen, since nodes
                                // are removed from the graph when their count
                                // reaches 1. However allocation errors during
                                // insertions could leave count 1 in place.
                                std::unique_ptr<TCopyNode> d(node);
                                Y_DEBUG_ABORT_UNLESS(node->Down, "Unexpected null link down during destruction");
                                if (node->Down) [[likely]] {
                                    down = node->Down;
                                    continue;
                                }
                                break;
                            }
                            case 1: {
                                OptimizeAway(node);
                                break;
                            }
                        }
                        break;
                    }
                    case ETag::Union: {
                        TUnionNode* node = down.GetUnionNodePtr();
                        if (node->Items.Empty()) {
                            // This node is empty and can finally be deleted
                            std::unique_ptr<TUnionNode> d(node);
                            break;
                        }
                        // Mark this node as "delete in progress" so when a set
                        // is not disjoint this node is not optimized away and
                        // deleted unexpectedly (since we still hold a pointer
                        // to it and need it to remain valid)
                        node->Count = 0;
                        node->Up.SetNull();
                        // We want to visit this node again until it's empty
                        queue.push_back(down);
                        // Remove one item at a time
                        std::unique_ptr<TUnionNodeItem> item(node->Items.PopFront());
                        Y_DEBUG_ABORT_UNLESS(item->Down, "Unexpected null link down during destruction");
                        if (item->Down) [[likely]] {
                            down = item->Down;
                            continue;
                        }
                        break;
                    }
                }
                if (queue.empty()) {
                    break;
                }
                down = queue.back();
                queue.pop_back();
            }
        }

        template<class TParent>
        static void RelinkPair(TParent* parent, TDownPtr child) {
            parent->Down = child;
            switch (child.GetTag()) {
                case ETag::Item: {
                    TItem* item = child.GetItemPtr();
                    item->Up.Set(parent);
                    break;
                }
                case ETag::Copy: {
                    TCopyNodeItem* item = child.GetCopyNodeItemPtr();
                    item->Up.Set(parent);
                    break;
                }
                case ETag::Union: {
                    TUnionNode* node = child.GetUnionNodePtr();
                    node->Up.Set(parent);
                    break;
                }
            }
        }

        template<class TChild>
        static void RelinkPair(TUpPtr parent, TChild* child) {
            child->Up = parent;
            switch (parent.GetTag()) {
                case ETag::Item: {
                    TUnionCopySet* set = parent.GetItemPtr();
                    set->Down.Set(child);
                    break;
                }
                case ETag::Copy: {
                    TCopyNode* node = parent.GetCopyNodePtr();
                    node->Down.Set(child);
                    break;
                }
                case ETag::Union: {
                    TUnionNodeItem* item = parent.GetUnionNodeItemPtr();
                    item->Down.Set(child);
                    break;
                }
            }
        }

        template<class TParent, class TChild>
        static void OptimizePair(TParent* parent, TChild* child) {
            parent->Down.Set(child);
            child->Up.Set(parent);
        }

        static void OptimizePair(TCopyNode* parent, TCopyNodeItem* child) {
            parent->Down.Set(child);
            child->Up.Set(parent);
            TCopyNode* bottom = child->Down;
            if (parent->Count == 0 || bottom->Count == 0) {
                // Either parent or child are going to be deleted soon
                return;
            }
            // We currently have: parent items <-> parent <-> child <-> bottom
            // And we need to remove the redundant `child` item
            bottom->Remove(child);
            std::unique_ptr<TCopyNodeItem> dChild(child);
            // The smaller number of items are folded into the larger node
            if (parent->Count <= bottom->Count) {
                // Move all parent items to bottom
                parent->Count = 0;
                std::unique_ptr<TCopyNode> dParent(parent);
                while (!parent->Items.Empty()) {
                    TCopyNodeItem* item = parent->Items.PopFront();
                    bottom->Add(item);
                }
            } else {
                // Move all bottom items to parent
                bottom->Count = 0;
                RelinkPair(parent, bottom->Down);
                std::unique_ptr<TCopyNode> dBottom(bottom);
                while (!bottom->Items.Empty()) {
                    TCopyNodeItem* item = bottom->Items.PopFront();
                    parent->Add(item);
                }
            }
        }

        static void OptimizePair(TUnionNodeItem* parent, TUnionNode* child) {
            parent->Down.Set(child);
            child->Up.Set(parent);
            TUnionNode* top = parent->Up;
            if (top->Count == 0 || child->Count == 0) {
                // Either parent or child are going to be deleted soon
                return;
            }
            // We currently have top <-> parent <-> child -> child items
            // And we need to remove the redundant `parent` item
            top->Remove(parent);
            std::unique_ptr<TUnionNodeItem> dParent(parent);
            // The smaller number of items are folded into the larger node
            if (child->Count <= top->Count) {
                // Move all child items to top
                child->Count = 0;
                std::unique_ptr<TUnionNode> dChild(child);
                while (!child->Items.Empty()) {
                    TUnionNodeItem* item = child->Items.PopFront();
                    top->Add(item);
                }
            } else {
                // Move all top items to child
                top->Count = 0;
                RelinkPair(top->Up, child);
                std::unique_ptr<TUnionNode> dTop(top);
                while (!top->Items.Empty()) {
                    TUnionNodeItem* item = top->Items.PopFront();
                    child->Add(item);
                }
            }
        }

        template<class TParent>
        static void OptimizePair(TParent* parent, TDownPtr down) {
            Y_ABORT_UNLESS(down);
            switch (down.GetTag()) {
                case ETag::Item: {
                    TItem* child = down.GetItemPtr();
                    OptimizePair(parent, child);
                    break;
                }
                case ETag::Copy: {
                    TCopyNodeItem* child = down.GetCopyNodeItemPtr();
                    OptimizePair(parent, child);
                    break;
                }
                case ETag::Union: {
                    TUnionNode* child = down.GetUnionNodePtr();
                    OptimizePair(parent, child);
                    break;
                }
            }
        }

        static void OptimizePair(TUpPtr up, TDownPtr down) {
            Y_ABORT_UNLESS(up);
            switch (up.GetTag()) {
                case ETag::Item: {
                    TUnionCopySet* parent = up.GetItemPtr();
                    OptimizePair(parent, down);
                    break;
                }
                case ETag::Copy: {
                    TCopyNode* parent = up.GetCopyNodePtr();
                    OptimizePair(parent, down);
                    break;
                }
                case ETag::Union: {
                    TUnionNodeItem* parent = up.GetUnionNodeItemPtr();
                    OptimizePair(parent, down);
                    break;
                }
            }
        }

        static void OptimizeAway(TCopyNode* node) {
            Y_ABORT_UNLESS(node->Count == 1);
            Y_ABORT_UNLESS(!node->Items.Empty());
            std::unique_ptr<TCopyNode> d(node);
            std::unique_ptr<TCopyNodeItem> nodeItem(node->Items.PopFront());
            Y_ABORT_UNLESS(node->Items.Empty());
            node->Count = 0;
            OptimizePair(nodeItem->Up, node->Down);
        }

        static void OptimizeAway(TUnionNode* node) {
            Y_ABORT_UNLESS(node->Count == 1);
            Y_ABORT_UNLESS(!node->Items.Empty());
            std::unique_ptr<TUnionNode> d(node);
            std::unique_ptr<TUnionNodeItem> nodeItem(node->Items.PopFront());
            Y_ABORT_UNLESS(node->Items.Empty());
            node->Count = 0;
            OptimizePair(node->Up, nodeItem->Down);
        }

    private:
        template<class TParent, class TChild>
        static TCopyNode* InsertCopyNode(TParent* parent, TChild* child) {
            std::unique_ptr<TCopyNodeItem> nodeItem(new TCopyNodeItem);
            std::unique_ptr<TCopyNode> node(new TCopyNode);
            // parent <-> nodeItem <-> node <-> child
            node->Add(nodeItem.get());
            nodeItem->Up.Set(parent);
            node->Down.Set(child);
            parent->Down.Set(nodeItem.release());
            child->Up.Set(node.get());
            return node.release();
        }

        template<class TParent, class TChild>
        static TUnionNode* InsertUnionNode(TParent* parent, TChild* child) {
            std::unique_ptr<TUnionNodeItem> nodeItem(new TUnionNodeItem);
            std::unique_ptr<TUnionNode> node(new TUnionNode);
            // parent <-> node <-> nodeItem <-> child
            node->Add(nodeItem.get());
            nodeItem->Down.Set(child);
            node->Up.Set(parent);
            child->Up.Set(nodeItem.release());
            parent->Down.Set(node.get());
            return node.release();
        }

        static TCopyNode* EnsureCopyNode(TUnionCopySet* set) {
            switch (set->Down.GetTag()) {
                case ETag::Item: {
                    return InsertCopyNode(set, set->Down.GetItemPtr());
                }
                case ETag::Copy: {
                    return set->Down.GetCopyNodeItemPtr()->Down;
                }
                case ETag::Union: {
                    return InsertCopyNode(set, set->Down.GetUnionNodePtr());
                }
            }
        }

        static TUnionNode* EnsureUnionNode(TUnionCopySet* set) {
            switch (set->Down.GetTag()) {
                case ETag::Item: {
                    return InsertUnionNode(set, set->Down.GetItemPtr());
                }
                case ETag::Copy: {
                    return InsertUnionNode(set, set->Down.GetCopyNodeItemPtr());
                }
                case ETag::Union: {
                    return set->Down.GetUnionNodePtr();
                }
            }
        }

        static TCopyNode* EnsureCopyNode(TItem* item) {
            switch (item->Up.GetTag()) {
                case ETag::Item: {
                    return InsertCopyNode(item->Up.GetItemPtr(), item);
                }
                case ETag::Copy: {
                    return item->Up.GetCopyNodePtr();
                }
                case ETag::Union: {
                    return InsertCopyNode(item->Up.GetUnionNodeItemPtr(), item);
                }
            }
        }

    private:
        void RelinkDownToSelf() {
            switch (Down.GetTag()) {
                case ETag::Item: {
                    Down.GetItemPtr()->Up.Set(this);
                    break;
                }
                case ETag::Copy: {
                    Down.GetCopyNodeItemPtr()->Up.Set(this);
                    break;
                }
                case ETag::Union: {
                    Down.GetUnionNodePtr()->Up.Set(this);
                    break;
                }
            }
        }

    public:
        TUnionCopySet() noexcept = default;

        ~TUnionCopySet() {
            Clear();
        }

        void Clear() noexcept {
            if (Down) {
                DestroyDown(Down);
                Down.SetNull();
            }
        }

        TUnionCopySet(TUnionCopySet&& rhs) noexcept {
            *this = std::move(rhs);
        }

        TUnionCopySet& operator=(TUnionCopySet&& rhs) noexcept {
            if (this != &rhs) {
                Clear();
                Down = rhs.Down;
                if (Down) {
                    rhs.Down.SetNull();
                    RelinkDownToSelf();
                }
            }
            return *this;
        }

        TUnionCopySet(const TUnionCopySet& rhs) {
            *this = rhs;
        }

        TUnionCopySet& operator=(const TUnionCopySet& rhs) {
            if (this != &rhs) {
                Clear();
                if (rhs.Down) {
                    std::unique_ptr<TCopyNodeItem> nodeItem(new TCopyNodeItem);
                    TCopyNode* node = EnsureCopyNode(const_cast<TUnionCopySet*>(&rhs));
                    node->Add(nodeItem.get());
                    nodeItem->Up.Set(this);
                    Down.Set(nodeItem.release());
                }
            }
            return *this;
        }

        bool Empty() const {
            return !Down;
        }

        explicit operator bool() const {
            return !Empty();
        }

        TUnionCopySet& Add(TValue* value) {
            // Must be implicitly castable
            TItem* item = value;
            if (!Down && !item->Up) {
                // First time set/item connection
                item->Up.Set(this);
                Down.Set(item);
            } else if (!item->Up) {
                // First time item is connected anywhere
                std::unique_ptr<TUnionNodeItem> nodeItem(new TUnionNodeItem);
                TUnionNode* node = EnsureUnionNode(this);
                // node <-> nodeItem <-> item
                node->Add(nodeItem.get());
                nodeItem->Down.Set(item);
                item->Up.Set(nodeItem.release());
            } else if (!Down) {
                // First time set is connected anywhere
                std::unique_ptr<TCopyNodeItem> nodeItem(new TCopyNodeItem);
                TCopyNode* node = EnsureCopyNode(item);
                // set <-> nodeItem <-> node
                node->Add(nodeItem.get());
                nodeItem->Up.Set(this);
                Down.Set(nodeItem.release());
            } else {
                // Complex case where union to copy connection is needed
                std::unique_ptr<TUnionNodeItem> unionItem(new TUnionNodeItem);
                std::unique_ptr<TCopyNodeItem> copyItem(new TCopyNodeItem);
                TUnionNode* unionNode = EnsureUnionNode(this);
                TCopyNode* copyNode = EnsureCopyNode(value);
                // unionNode <-> unionItem <-> copyItem <-> copyNode
                unionItem->Down.Set(copyItem.get());
                copyItem->Up.Set(unionItem.get());
                unionNode->Add(unionItem.release());
                copyNode->Add(copyItem.release());
            }
            return *this;
        }

        TUnionCopySet& Add(const TUnionCopySet& rhs) {
            if (this != &rhs) {
                if (!rhs.Down) {
                    // Nothing to add
                } else if (!Down) {
                    // Adding to an empty set, make a copy
                    std::unique_ptr<TCopyNodeItem> nodeItem(new TCopyNodeItem);
                    TCopyNode* node = EnsureCopyNode(const_cast<TUnionCopySet*>(&rhs));
                    // this <-> nodeItem <-> node
                    node->Add(nodeItem.get());
                    nodeItem->Up.Set(this);
                    Down.Set(nodeItem.release());
                } else {
                    // Make a copy and add it to a union
                    std::unique_ptr<TUnionNodeItem> unionItem(new TUnionNodeItem);
                    std::unique_ptr<TCopyNodeItem> copyItem(new TCopyNodeItem);
                    TUnionNode* unionNode = EnsureUnionNode(this);
                    TCopyNode* copyNode = EnsureCopyNode(const_cast<TUnionCopySet*>(&rhs));
                    // unionNode <-> unionItem <-> copyItem <-> copyNode
                    unionItem->Down.Set(copyItem.get());
                    copyItem->Up.Set(unionItem.get());
                    unionNode->Add(unionItem.release());
                    copyNode->Add(copyItem.release());
                }
            }
            return *this;
        }

        TUnionCopySet& operator+=(const TUnionCopySet& rhs) {
            return Add(rhs);
        }

        TUnionCopySet& Add(TUnionCopySet&& rhs) {
            if (this != &rhs) {
                if (!rhs.Down) {
                    // Nothing to add/move
                } else if (!Down) {
                    // Add/moving to an empty set, this is a move
                    *this = std::move(rhs);
                } else {
                    // Move rhs's target to a union, unless it's already a union
                    std::unique_ptr<TUnionNodeItem> unionItem(new TUnionNodeItem);
                    // Special case add/moving for unions (avoid generating extra unions)
                    if (rhs.Down.IsUnionNodePtr()) {
                        TUnionNode* unionNode = rhs.Down.GetUnionNodePtr();
                        if (Down.IsUnionNodePtr()) {
                            // Add rhs union to our union, but then optimize them
                            TUnionNode* top = Down.GetUnionNodePtr();
                            top->Add(unionItem.get());
                            rhs.Down.SetNull();
                            // This will link and optimize nodes
                            OptimizePair(unionItem.release(), unionNode);
                        } else {
                            // Move current item to rhs union and then move that union here
                            RelinkPair(unionItem.get(), Down);
                            Down.SetNull();
                            unionNode->Add(unionItem.release());
                            *this = std::move(rhs);
                        }
                    } else {
                        // Ensure this is a union and add rhs's item there
                        TUnionNode* unionNode = EnsureUnionNode(this);
                        RelinkPair(unionItem.get(), rhs.Down);
                        rhs.Down.SetNull();
                        unionNode->Add(unionItem.release());
                    }
                }
            }
            return *this;
        }

        TUnionCopySet& operator+=(TUnionCopySet&& rhs) {
            return Add(std::move(rhs));
        }

    public:
        template<class TCallback>
        bool ForEachValue(const TCallback& callback) const {
            return ForEachValueImpl(Down, callback);
        }

    private:
        struct TForEachValueState {
            TDownPtr Ptr;
            const TUnionNodeItem* Item = nullptr;
        };

        template<class TCallback>
        static bool ForEachValueImpl(TDownPtr down, const TCallback& callback) {
            if (!down) {
                return true;
            }
            TForEachValueState state = { down };
            TStackVec<TForEachValueState> queue;
            for (;;) {
                switch (state.Ptr.GetTag()) {
                    case ETag::Item: {
                        const TItem* item = state.Ptr.GetItemPtr();
                        if (!callback(static_cast<const TValue*>(item))) {
                            return false;
                        }
                        break;
                    }
                    case ETag::Copy: {
                        const TCopyNodeItem* item = state.Ptr.GetCopyNodeItemPtr();
                        const TCopyNode* node = item->Down;
                        state = { node->Down };
                        continue;
                    }
                    case ETag::Union: {
                        const TUnionNode* node = state.Ptr.GetUnionNodePtr();
                        const TUnionNodeItem* item = state.Item; // next item
                        if (!item) {
                            if (node->Items.Empty()) {
                                // Shouldn't happen
                                break;
                            }
                            item = node->Items.Front();
                        }
                        typename TIntrusiveList<TUnionNodeItem>::const_iterator it(item);
                        if (++it != node->Items.End()) {
                            queue.push_back({ state.Ptr, &*it });
                        }
                        state = { item->Down };
                        continue;
                    }
                }
                if (queue.empty()) {
                    break;
                }
                state = queue.back();
                queue.pop_back();
            }
            return true;
        }

    public:
        TString DebugString(bool withAddresses = false) const {
            TStringBuilder sb;
            DebugPrint(sb.Out, withAddresses);
            return sb;
        }

        void DebugPrint(IOutputStream& out, bool withAddresses = false) const {
            if (withAddresses) {
                out << "Set(" << (const void*)this << ") -> ";
            }
            DebugPrintImpl(Down, out, withAddresses);
        }

    private:
        static void DebugPrintImpl(TDownPtr down, IOutputStream& out, bool withAddresses) {
            if (!down) {
                out << "empty";
                return;
            }

            TForEachValueState state = { down };
            TStackVec<TForEachValueState> queue;
            for (;;) {
                switch (state.Ptr.GetTag()) {
                    case ETag::Item: {
                        const TItem* item = state.Ptr.GetItemPtr();
                        const TValue* value = static_cast<const TValue*>(item);
                        if (withAddresses) {
                            out << "Value(" << (const void*)value << ") = ";
                        }
                        out << *value;
                        break;
                    }
                    case ETag::Copy: {
                        const TCopyNodeItem* item = state.Ptr.GetCopyNodeItemPtr();
                        const TCopyNode* node = item->Down;
                        if (withAddresses) {
                            out << "CopyNodeItem(" << (const void*)item << ") -> ";
                            out << "CopyNode(" << (const void*)node << ") -> ";
                        } else {
                            out << "Copy -> ";
                        }
                        state = { node->Down };
                        continue;
                    }
                    case ETag::Union: {
                        const TUnionNode* node = state.Ptr.GetUnionNodePtr();
                        const TUnionNodeItem* item = state.Item; // last item
                        if (!item) {
                            if (withAddresses) {
                                out << "UnionNode(" << (const void*)node << "){ ";
                            } else {
                                out << "Union{ ";
                            }
                            if (node->Items.Empty()) {
                                // Shouldn't happen
                                out << "}";
                                break;
                            }
                            item = node->Items.Front();
                        } else {
                            typename TIntrusiveList<TUnionNodeItem>::const_iterator it(item);
                            if (++it == node->Items.End()) {
                                out << " }";
                                break;
                            }
                            out << ", ";
                            item = &*it;
                        }
                        if (withAddresses) {
                            out << "UnionNodeItem(" << (const void*)&item << ") -> ";
                        }
                        queue.push_back({ state.Ptr, item });
                        state = { item->Down };
                        continue;
                    }
                }
                if (queue.empty()) {
                    break;
                }
                state = queue.back();
                queue.pop_back();
            }
        }

    private:
        TDownPtr Down;
    };

} // namespace NKikimr
