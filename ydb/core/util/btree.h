#pragma once

#include "btree_common.h"
#include "hazard.h"

#include <util/generic/algorithm.h>
#include <util/generic/typetraits.h>
#include <util/generic/ylimits.h>
#include <util/memory/pool.h>
#include <util/system/sanitizers.h>
#include <util/system/yassert.h>

#include <atomic>
#include <cstddef>
#include <vector>

namespace NKikimr {

    /**
     * Lock-free single-writer multiple-readers B+Tree
     *
     * WARNING: only one thread is allowed to call TBTree methods at any given time.
     *
     * All other threads must use TSafeAccess and TSafeIterator to guarantee thread safety.
     */
    template<
        class TKey,
        class TValue,
        class TCompare = TLess<TKey>,
        class TAllocator = TMemoryPool,
        size_t PageSize = 512>
    class TBTree : private TCompare {
    private:
        struct TPage;
        struct TLeafPage;
        struct TInnerPage;

        class TTag {
        public:
            TTag()
                : Value(0)
            { }

            TTag(TLeafPage* page)
                : Value(uintptr_t(page) | uintptr_t(0))
            { }

            TTag(TInnerPage* page)
                : Value(uintptr_t(page) | uintptr_t(1))
            {
                Y_DEBUG_ABORT_UNLESS(page, "Inner page cannot be null");
            }

            explicit operator bool() const {
                return bool(Value);
            }

            bool operator==(const TTag& rhs) const {
                return Value == rhs.Value;
            }

            bool operator!=(const TTag& rhs) const {
                return Value != rhs.Value;
            }

            void* ToTaggedPointer() const {
                return (void*)(Value);
            }

            static TTag FromTaggedPointer(void* ptr) {
                return TTag(ptr);
            }

            bool IsLeafPage() const {
                return (Value & uintptr_t(0)) == 0;
            }

            bool IsInnerPage() const {
                return (Value & uintptr_t(1)) == 1;
            }

            TPage* ToPage() const {
                return (TPage*)(Value & ~uintptr_t(1));
            }

            TLeafPage* ToLeafPage() const {
                Y_DEBUG_ABORT_UNLESS(!Value || IsLeafPage());
                return (TLeafPage*)(Value & ~uintptr_t(1));
            }

            TInnerPage* ToInnerPage() const {
                Y_DEBUG_ABORT_UNLESS(!Value || IsInnerPage());
                return (TInnerPage*)(Value & ~uintptr_t(1));
            }

        private:
            explicit TTag(void* ptr)
                : Value(uintptr_t(ptr))
            { }

        private:
            uintptr_t Value;
        };

        struct TEdge {
            std::atomic<void*> TaggedPointer{ nullptr };

            TEdge() = default;

            explicit TEdge(TTag tag)
                : TaggedPointer{ tag.ToTaggedPointer() }
            { }

            TTag GetUnsafe() const {
                return TTag::FromTaggedPointer(TaggedPointer.load(std::memory_order_relaxed));
            }

            void SetUnsafe(TTag tag) {
                TaggedPointer.store(tag.ToTaggedPointer(), std::memory_order_relaxed);
            }

            TTag SwapUnsafe(TTag tag) {
                return TTag::FromTaggedPointer(TaggedPointer.exchange(tag.ToTaggedPointer(), std::memory_order_relaxed));
            }

            TTag Follow(TAutoHazardPointer& hazard) const {
                return TTag::FromTaggedPointer(hazard.Protect(TaggedPointer));
            }

            TTag Publish(TTag tag) {
                // N.B. store must be sequentially consistent
                return TTag::FromTaggedPointer(TaggedPointer.exchange(tag.ToTaggedPointer()));
            }
        };

        using TLayout = TBTreeLayout<PageSize>;

        template<class THeader>
        using TInnerLayout = typename TLayout::template TInnerLayout<THeader, TKey, TEdge>;

        template<class THeader>
        using TLeafLayout = typename TLayout::template TLeafLayout<THeader, TKey, TValue>;

        using TRefCount = ui32;
        using TKeyCount = ui32;

        /**
         * Pages start with the same basic header
         */
        struct TPage {
            TRefCount RefCount = 0; // page is retired when refcount reaches zero
        };

        /**
         * Free pages only exist in a single-linked list
         */
        struct TFreePage {
            // These uninitialized fields should match corresponding fields in
            // normal page layout. When TFreePage is constructed they will stay
            // poisoned, which helps memory sanitizer catch bugs due to page
            // pointers that have already been freed.
            TRefCount UninitializedRefCount;
            TKeyCount UninitializedCount;
            TFreePage* Next = nullptr;
        };

        /**
         * Leaf pages have Count keys and values, as well as Prev/Next links for iteration
         */
        struct TLeafPage : public TPage {
            std::atomic<TKeyCount> Count_{ 0 }; // number of currently present keys
            TEdge Prev, Next;

            TKeyCount SafeCount() const {
                return Count_.load(std::memory_order_acquire);
            }

            TKeyCount UnsafeCount() const {
                return Count_.load(std::memory_order_relaxed);
            }

            void PublishCount(TKeyCount count) {
                Count_.store(count, std::memory_order_release);
            }

            void SetCountUnsafe(TKeyCount count) {
                Count_.store(count, std::memory_order_relaxed);
            }

            TKey* Keys() {
                return reinterpret_cast<TKey*>(reinterpret_cast<char*>(this) + TLeafLayout<TLeafPage>::KeysOffset);
            }

            TValue* Values() {
                return reinterpret_cast<TValue*>(reinterpret_cast<char*>(this) + TLeafLayout<TLeafPage>::ValuesOffset);
            }

            const TKey* Keys() const {
                return reinterpret_cast<const TKey*>(reinterpret_cast<const char*>(this) + TLeafLayout<TLeafPage>::KeysOffset);
            }

            const TValue* Values() const {
                return reinterpret_cast<const TValue*>(reinterpret_cast<const char*>(this) + TLeafLayout<TLeafPage>::ValuesOffset);
            }
        };

        /**
         * Inner pages have Count keys and Count+1 edges
         */
        struct TInnerPage : public TPage {
            TKeyCount Count; // number of currently present keys

            TKey* Keys() {
                return reinterpret_cast<TKey*>(reinterpret_cast<char*>(this) + TInnerLayout<TInnerPage>::KeysOffset);
            }

            TEdge* Edges() {
                return reinterpret_cast<TEdge*>(reinterpret_cast<char*>(this) + TInnerLayout<TInnerPage>::EdgesOffset);
            }

            const TKey* Keys() const {
                return reinterpret_cast<const TKey*>(reinterpret_cast<const char*>(this) + TInnerLayout<TInnerPage>::KeysOffset);
            }

            const TEdge* Edges() const {
                return reinterpret_cast<const TEdge*>(reinterpret_cast<const char*>(this) + TInnerLayout<TInnerPage>::EdgesOffset);
            }
        };

    public:
        enum : size_t {
            LeafPageCapacity = TLeafLayout<TLeafPage>::Capacity,
            InnerPageCapacity = TInnerLayout<TInnerPage>::Capacity,
        };

        static_assert(sizeof(TPage) <= PageSize, "PageSize is  too small");
        static_assert(sizeof(TFreePage) <= PageSize, "PageSize is too small");
        static_assert(sizeof(TLeafPage) <= PageSize && LeafPageCapacity >= 2, "PageSize is too small");
        static_assert(sizeof(TInnerPage) <= PageSize && InnerPageCapacity >= 2, "PageSize is too small");

    private:
        void FreeRawPage(void* base) {
            TFreePage* page = new(base) TFreePage();
            page->Next = FreePages;
            FreePages = page;
            ++FreePagesCount;
        }

        void* AllocateRawPage() {
            if (!FreePages) {
                void* raw = Allocator.Allocate(PageSize);
                FreeRawPage(raw);
                ++TotalPagesCount;
                Y_DEBUG_ABORT_UNLESS(FreePages);
            }

            Y_DEBUG_ABORT_UNLESS(FreePagesCount > 0);
            --FreePagesCount;
            TFreePage* page = FreePages;
            FreePages = page->Next;
            page->~TFreePage();
            NSan::Poison(page, PageSize);
            return page;
        }

        TLeafPage* AllocateLeafPage() {
            return new(AllocateRawPage()) TLeafPage();
        }

        void FreeLeafPage(TLeafPage* leaf) noexcept {
            Y_DEBUG_ABORT_UNLESS(leaf->RefCount == 0);
            auto count = leaf->UnsafeCount();
            if (!TTypeTraits<TKey>::IsPod) {
                auto* keys = leaf->Keys();
                for (size_t index = 0; index < count; ++index) {
                    (keys++)->~TKey();
                }
            }
            if (!TTypeTraits<TValue>::IsPod) {
                auto* values = leaf->Values();
                for (size_t index = 0; index < count; ++index) {
                    (values++)->~TValue();
                }
            }
            UnRefTag(leaf->Prev.GetUnsafe());
            UnRefTag(leaf->Next.GetUnsafe());
            leaf->~TLeafPage();
            NSan::Poison(leaf, PageSize);
            FreeRawPage(leaf);
        }

        TInnerPage* AllocateInnerPage() {
            return new(AllocateRawPage()) TInnerPage();
        }

        void FreeInnerPage(TInnerPage* inner) noexcept {
            Y_DEBUG_ABORT_UNLESS(inner->RefCount == 0);
            auto count = inner->Count;
            if (!TTypeTraits<TKey>::IsPod) {
                auto* keys = inner->Keys();
                for (size_t index = 0; index < count; ++index) {
                    (keys++)->~TKey();
                }
            }
            auto* edges = inner->Edges();
            for (size_t index = 0; index <= count; ++index) {
                UnRefTag(edges->GetUnsafe());
                (edges++)->~TEdge();
            }
            inner->~TInnerPage();
            NSan::Poison(inner, PageSize);
            FreeRawPage(inner);
        }

        TTag RefTag(TTag tag) {
            if (tag) {
                auto* page = tag.ToPage();
                ++page->RefCount;
            }
            return tag;
        }

        void UnRefTag(TTag tag) {
            if (tag) {
                auto* page = tag.ToPage();
                Y_DEBUG_ABORT_UNLESS(page->RefCount > 0);
                if (--page->RefCount == 0) {
                    RetireList.push_back(tag);
                    MoreGarbage = true;
                }
            }
        }

        size_t MaxGarbage() const {
            size_t count = Domain->MaxPointers();
            return count + (count >> 2); // 25% over the max pointers
        }

        void CollectGarbage(bool force = false) {
            if (force) {
                MoreGarbage = true;
            }

            while (MoreGarbage) {
                MoreGarbage = false;

                const bool collect = force || RetireList.size() > MaxGarbage();
                if (!collect) {
                    break; // not enough garbage
                }

                Hazards.clear();
                if (!force) {
                    // We only collect pages that are not hazards at each step
                    Domain->CollectHazards([this](void* p) { Hazards.push_back(p); });
                    if (!Hazards.empty()) {
                        SortUnique(Hazards);
                    }
                }

                // N.B. the RetireList may grow during the loop below
                size_t readPos = 0;
                size_t writePos = 0;
                size_t endPos = RetireList.size();
                while (readPos != endPos) {
                    TTag tag = RetireList[readPos];
                    if (Hazards.empty() || !std::binary_search(Hazards.begin(), Hazards.end(), tag.ToTaggedPointer())) {
                        if (tag.IsInnerPage()) {
                            FreeInnerPage(tag.ToInnerPage());
                        } else {
                            FreeLeafPage(tag.ToLeafPage());
                        }
                    } else if (writePos != readPos) {
                        RetireList[writePos++] = tag;
                    } else {
                        ++writePos;
                    }
                    ++readPos;
                }

                // N.B. by the MaxGarbage invariant we should have freed at least 25% of the retire list
                if (MoreGarbage) {
                    Y_DEBUG_ABORT_UNLESS(RetireList.size() > endPos);
                    if (Y_LIKELY(writePos != readPos)) {
                        while (readPos != RetireList.size()) {
                            RetireList[writePos++] = RetireList[readPos++];
                        }
                    }
                } else {
                    Y_DEBUG_ABORT_UNLESS(RetireList.size() == endPos);
                }

                RetireList.resize(writePos);
            }
        }

    public:
        TBTree(const TBTree&) = delete;
        TBTree& operator=(const TBTree&) = delete;

        TBTree(TAllocator& allocator, TCompare compare = TCompare())
            : TCompare(std::move(compare))
            , Allocator(allocator)
            , Domain(new TTreeDomain())
        {
            Init();
        }

        ~TBTree() noexcept {
            Destroy();
        }

        /**
         * WARNING: may only be called when no other threads are iterating over this tree
         */
        void Clear() {
            Destroy();
            Allocator.ClearKeepFirstChunk();
            Init();
        }

        const TCompare& key_comp() const {
            return static_cast<const TCompare&>(*this);
        }

        size_t size() const { return Size_; }
        size_t Size() const { return Size_; }
        size_t GetFreePages() const { return FreePagesCount; }
        size_t GetTotalPages() const { return TotalPagesCount; }
        size_t GetLiveReaders() const { return Domain->RefCount() - 1; }

    private:
        class TTreeDomain
            : public TThrRefBase
            , public THazardDomain
        {
        };

        using TTreeDomainRef = TIntrusivePtr<TTreeDomain>;

    private:
        class TTreeRef {
            friend class TBTree;

        public:
            TTreeRef() noexcept
                : Tree_(nullptr)
                , Domain_(nullptr)
            { }

        private:
            explicit TTreeRef(const TBTree* tree) noexcept
                : Tree_(tree)
            {
                if (tree) {
                    Domain_ = tree->Domain;
                }
            }

        public:
            explicit operator bool() const noexcept {
                return bool(Domain_);
            }

            const TBTree& operator*() const noexcept {
                return *Tree_;
            }

            const TBTree* operator->() const noexcept {
                return Tree_;
            }

            THazardDomain& Domain() const noexcept {
                return *Domain_;
            }

        private:
            const TBTree* Tree_;
            TTreeDomainRef Domain_;
        };

    public:
        class TSafeAccess;

        /**
         * TSafeIterator may be used to iterate over tree from reader threads
         *
         * WARNING: TSafeIterator class itself is not thread-safe, only one
         * thread is permitted to work with it at any given time.
         */
        class TSafeIterator {
            friend class TSafeAccess;

        private:
            explicit TSafeIterator(TTreeRef tree) noexcept
                : Tree(std::move(tree))
                , CurrentPointer(Tree.Domain())
                , CurrentPage(nullptr)
                , CurrentIndex(-1)
                , CurrentCount(-1)
            {
            }

        public:
            TSafeIterator() noexcept
                : Tree()
                , CurrentPointer()
                , CurrentPage(nullptr)
                , CurrentIndex(-1)
                , CurrentCount(-1)
            { }

            ~TSafeIterator() noexcept {
                Invalidate();
            }

            TSafeIterator(TSafeIterator&& rhs) noexcept
                : Tree(std::move(rhs.Tree))
                , CurrentPointer(std::move(rhs.CurrentPointer))
                , CurrentPage(rhs.CurrentPage)
                , CurrentIndex(rhs.CurrentIndex)
                , CurrentCount(rhs.CurrentCount)
            {
                rhs.CurrentPage = nullptr;
            }

            TSafeIterator& operator=(TSafeIterator&& rhs) noexcept {
                if (Y_LIKELY(this != &rhs)) {
                    Invalidate();
                    Tree = std::move(rhs.Tree);
                    CurrentPointer = std::move(rhs.CurrentPointer);
                    CurrentPage = rhs.CurrentPage;
                    CurrentIndex = rhs.CurrentIndex;
                    CurrentCount = rhs.CurrentCount;
                    rhs.CurrentPage = nullptr;
                }
                return *this;
            }

        public:
            void Invalidate() noexcept {
                CurrentPage = nullptr;
                CurrentPointer.Clear();
            }

            bool SeekFirst() {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                auto* page = Tree->First.Follow(CurrentPointer).ToLeafPage();
                Y_DEBUG_ABORT_UNLESS(page, "Tree is missing the first page");
                auto count = page->SafeCount();
                if (count == 0) {
                    // The tree is empty
                    CurrentPage = nullptr;
                    CurrentPointer.Clear();
                    return false;
                } else {
                    CurrentPage = page;
                    CurrentIndex = 0;
                    CurrentCount = count;
                    return true;
                }
            }

            bool SeekLast() {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                auto* page = Tree->Last.Follow(CurrentPointer).ToLeafPage();
                Y_DEBUG_ABORT_UNLESS(page, "Tree is missing the last page");
                auto count = page->SafeCount();
                if (count == 0) {
                    // The tree is empty
                    CurrentPage = nullptr;
                    CurrentPointer.Clear();
                    return false;
                } else {
                    CurrentPage = page;
                    CurrentIndex = count - 1;
                    CurrentCount = count;
                    return true;
                }
            }

            /**
             * Seeks to the first key that is >= search key
             *
             * If backwards is true seeks to the last key that is < search key (inverse upper bound)
             */
            template<class TKeyArg>
            bool SeekLowerBound(TKeyArg&& key, bool backwards = false) {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                auto current = Tree->Root.Follow(CurrentPointer);
                while (current.IsInnerPage()) {
                    auto* page = current.ToInnerPage();
                    size_t count = page->Count;
                    auto* keysBegin = page->Keys();
                    auto* keysEnd = keysBegin + count;
                    size_t index = std::lower_bound(keysBegin, keysEnd, key, Tree->key_comp()) - keysBegin;
                    // Follow the left edge, which includes keys <= our search key
                    TAutoHazardPointer edgePointer(Tree.Domain());
                    current = page->Edges()[index].Follow(edgePointer);
                    CurrentPointer = std::move(edgePointer);
                }

                auto* page = current.ToLeafPage();
                size_t count = page->SafeCount();
                if (count == 0) {
                    // Empty tree
                    CurrentPage = nullptr;
                    CurrentPointer.Clear();
                    return false;
                }
                auto* keysBegin = page->Keys();
                auto* keysEnd = keysBegin + count;
                size_t index = std::lower_bound(keysBegin, keysEnd, key, Tree->key_comp()) - keysBegin;
                if (backwards) {
                    return InitBackwards(page, index, count);
                } else {
                    return InitForwards(page, index, count);
                }
            }

            /**
             * Seeks to the first key that is > search key
             *
             * If backwards is true seeks to the last key that is <= search key (inverse lower bound)
             */
            template<class TKeyArg>
            bool SeekUpperBound(TKeyArg&& key, bool backwards = false) {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                auto current = Tree->Root.Follow(CurrentPointer);
                while (current.IsInnerPage()) {
                    auto* page = current.ToInnerPage();
                    size_t count = page->Count;
                    auto* keysBegin = page->Keys();
                    auto* keysEnd = keysBegin + count;
                    size_t index = std::upper_bound(keysBegin, keysEnd, key, Tree->key_comp()) - keysBegin;
                    // Follow the left edge, which includes keys >= our search key
                    TAutoHazardPointer edgePointer(Tree.Domain());
                    current = page->Edges()[index].Follow(edgePointer);
                    CurrentPointer = std::move(edgePointer);
                }

                auto* page = current.ToLeafPage();
                size_t count = page->SafeCount();
                if (count == 0) {
                    // Empty tree
                    CurrentPage = nullptr;
                    CurrentPointer.Clear();
                    return false;
                }
                auto* keysBegin = page->Keys();
                auto* keysEnd = keysBegin + count;
                size_t index = std::upper_bound(keysBegin, keysEnd, key, Tree->key_comp()) - keysBegin;
                if (backwards) {
                    return InitBackwards(page, index, count);
                } else {
                    return InitForwards(page, index, count);
                }
            }

            /**
             * Seeks to key equivalent to search key
             *
             * If tree has duplicates will seek to the last duplicate key,
             * which is more efficient when tree keys are unique.
             */
            template<class TKeyArg>
            bool SeekExact(TKeyArg&& key) {
                if (SeekUpperBound(key, true)) {
                    // We know current key <= search key
                    if (!Tree->key_comp()(GetKey(), key)) {
                        return true;
                    }
                    // Current key did not match
                    CurrentPage = nullptr;
                    CurrentPointer.Clear();
                }
                return false;
            }

            /**
             * Seeks to key equivalent to search key
             *
             * If tree has duplicates will seek to the first duplicate key,
             * which may be less efficient when tree keys are unique.
             */
            template<class TKeyArg>
            bool SeekExactFirst(TKeyArg&& key) {
                if (SeekLowerBound(key)) {
                    // We know search key <= current key
                    if (!Tree->key_comp()(key, GetKey())) {
                        return true;
                    }
                    // Current key did not match
                    CurrentPage = nullptr;
                    CurrentPointer.Clear();
                }
                return false;
            }

            bool Prev() {
                Y_DEBUG_ABORT_UNLESS(IsValid());

                if (CurrentIndex > 0) {
                    // We have more keys on the current page
                    --CurrentIndex;
                    return true;
                }

                return InitBackwards(CurrentPage, 0, CurrentCount);
            }

            bool Next() {
                Y_DEBUG_ABORT_UNLESS(IsValid());

                if (++CurrentIndex < CurrentCount) {
                    // We have more keys on the current page
                    return true;
                }

                return InitForwards(CurrentPage, CurrentCount, CurrentCount);
            }

            bool IsValid() const {
                return bool(CurrentPage);
            }

            const TKey& GetKey() const {
                Y_DEBUG_ABORT_UNLESS(IsValid());

                return CurrentPage->Keys()[CurrentIndex];
            }

            const TValue& GetValue() const {
                Y_DEBUG_ABORT_UNLESS(IsValid());

                return CurrentPage->Values()[CurrentIndex];
            }

        private:
            bool InitBackwards(TLeafPage* page, size_t index, size_t count) {
                if (index == 0) {
                    // We need to follow to the previous page
                    TAutoHazardPointer prevPointer(Tree.Domain());
                    page = page->Prev.Follow(prevPointer).ToLeafPage();
                    if (!page) {
                        // This page was the first page
                        CurrentPage = nullptr;
                        CurrentPointer.Clear();
                        return false;
                    }
                    index = count = page->SafeCount();
                    Y_DEBUG_ABORT_UNLESS(count > 0, "Unexpected empty prev page");
                    CurrentPointer = std::move(prevPointer);
                }
                CurrentPage = page;
                CurrentIndex = index - 1;
                CurrentCount = count;
                return true;
            }

            bool InitForwards(TLeafPage* page, size_t index, size_t count) {
                if (index == count) {
                    // We need to follow to the next page
                    TAutoHazardPointer nextPointer(Tree.Domain());
                    page = page->Next.Follow(nextPointer).ToLeafPage();
                    if (!page) {
                        // This page was the last page
                        CurrentPage = nullptr;
                        CurrentPointer.Clear();
                        return false;
                    }
                    index = 0;
                    count = page->SafeCount();
                    Y_DEBUG_ABORT_UNLESS(count > 0, "Unexpected empty next page");
                    CurrentPointer = std::move(nextPointer);
                }
                CurrentPage = page;
                CurrentIndex = index;
                CurrentCount = count;
                return true;
            }

        private:
            TTreeRef Tree;
            TAutoHazardPointer CurrentPointer;
            TLeafPage* CurrentPage;
            size_t CurrentIndex;
            size_t CurrentCount;
        };

        /**
         * TSafeAccess may be used for thread-safe access to underlying tree
         */
        class TSafeAccess {
            friend class TBTree;

        private:
            explicit TSafeAccess(TTreeRef tree)
                : Tree(std::move(tree))
            { }

        public:
            TSafeAccess() noexcept
                : Tree()
            { }

            TSafeIterator Iterator() const noexcept {
                Y_ABORT_UNLESS(Tree, "Use of an uninitialized object");

                return TSafeIterator(Tree);
            }

        private:
            TTreeRef Tree;
        };

        /**
         * WARNING: as a reminder this method is not safe to call concurrently
         * with other threads that may write to the tree. Tree may optimize
         * writes when there are no live TSafeAccess/TSafeIterator instances,
         * and those changes may not be synchronized properly.
         */
        TSafeAccess SafeAccess() const {
            return TSafeAccess(TTreeRef(this));
        }

    public:
        /**
         * Searches for the specified key and returns a pointer to the last
         * corresponding value if it exists. As a side effect it prepares the
         * next EmplaceUnsafe for inserting a new key/value pair, which would
         * be after the last value if key already exists.
         */
        template<class TKeyArg>
        TValue* FindPtr(TKeyArg&& key) {
            InsertPath.clear();
            auto current = Root.GetUnsafe();
            Y_DEBUG_ABORT_UNLESS(current);
            while (current.IsInnerPage()) {
                auto* inner = current.ToInnerPage();
                auto count = inner->Count;
                Y_DEBUG_ABORT_UNLESS(count > 0);
                auto* keysBegin = inner->Keys();
                auto* keysEnd = keysBegin + count;
                size_t index = std::upper_bound(keysBegin, keysEnd, key, key_comp()) - keysBegin;
                // We follow the left edge, towards the first entry > search key
                InsertPath.push_back(TInsertPath{ current, index });
                current = inner->Edges()[index].GetUnsafe();
            }

            auto* leaf = current.ToLeafPage();
            auto count = leaf->UnsafeCount();
            size_t index;
            if (count == 0) {
                // This tree is currently empty
                Y_ABORT_UNLESS(InsertPath.empty());
                index = 0;
            } else {
                // Insert position is before the upper bound
                auto* keysBegin = leaf->Keys();
                auto* keysEnd = keysBegin + count;
                index = std::upper_bound(keysBegin, keysEnd, key, key_comp()) - keysBegin;
            }
            InsertPath.push_back(TInsertPath{ current, index });

            if (index > 0) {
                // We want to inspect the previous key
                --index;
            } else {
                // The upper bound for the first key on the page
                leaf = leaf->Prev.GetUnsafe().ToLeafPage();
                if (!leaf) {
                    return nullptr;
                }
                // N.B.: this code path is currently never triggered, since we
                // never delete any keys and always use the first leaf key as
                // the inner page key. Thus if we encountered some max(key)
                // that is <= search key on some inner page, then we must have
                // followed the right link next to it and are guaranteed to
                // find it as the first key on the final leaf page. Since we
                // have searched for upper bound on the leaf page the only way
                // for us to get index == 0 is if key <= search key does not
                // exist in the tree. If delete operation is ever implemented
                // in the future without adjusting inner keys accordingly, then
                // this code path should produce correct results.
                count = leaf->UnsafeCount();
                Y_DEBUG_ABORT_UNLESS(count > 0);
                index = count - 1;
            }

            // We know that keys[index] <= key is true
            // If the following comparision is true then keys[index] != key
            if (key_comp()(leaf->Keys()[index], key)) {
                return nullptr;
            }

            return leaf->Values() + index;
        }

        template<class TKeyArg, class TValueArg>
        void EmplaceUnsafe(TKeyArg&& key, TValueArg&& value) {
            // TODO: exception safety?
            const bool threadSafe = GetLiveReaders() > 0;

            Y_ABORT_UNLESS(!InsertPath.empty() && InsertPath.back().Tag.IsLeafPage());
            TLeafPage* const leaf = InsertPath.back().Tag.ToLeafPage();
            const size_t leafIndex = InsertPath.back().Index;
            InsertPath.pop_back();

            const auto newLeaf = PerformLeafInsert(
                leaf, leafIndex,
                std::forward<TKeyArg>(key),
                std::forward<TValueArg>(value),
                threadSafe);

            ++Size_;

            if (!newLeaf.Left) {
                // Insert has been performed inplace, there is nothing else to do
                InsertPath.clear();
                return;
            }

            TTag currentLeft = newLeaf.Left;
            TTag currentRight = newLeaf.Right;
            const TKey* currentMiddle = newLeaf.Middle;
            while (currentMiddle) {
                if (InsertPath.empty()) {
                    // We are splitting the root page
                    auto* newRoot = AllocateInnerPage();
                    new (newRoot->Keys()) TKey(*currentMiddle);
                    new (newRoot->Edges() + 0) TEdge(RefTag(currentLeft));
                    new (newRoot->Edges() + 1) TEdge(RefTag(currentRight));
                    newRoot->Count = 1;

                    currentLeft = newRoot;
                    currentRight = newRoot;
                    currentMiddle = nullptr;
                } else {
                    Y_ABORT_UNLESS(InsertPath.back().Tag.IsInnerPage());
                    TInnerPage* const inner = InsertPath.back().Tag.ToInnerPage();
                    const size_t innerIndex = InsertPath.back().Index;
                    InsertPath.pop_back();

                    const auto newInner = PerformInnerInsert(
                        inner, innerIndex,
                        currentLeft, currentMiddle, currentRight,
                        threadSafe);

                    if (!newInner.Left) {
                        // Insert has been performed inplace, that means we have no readers
                        // Publish new leaf nodes, which is safe to do out of order
                        Y_DEBUG_ABORT_UNLESS(!threadSafe);
                        ReplaceLeafNode(leaf, newLeaf.Left, newLeaf.Right);
                        InsertPath.clear();
                        CollectGarbage();
                        return;
                    }

                    currentLeft = newInner.Left;
                    currentRight = newInner.Right;
                    currentMiddle = newInner.Middle;
                }
            }

            Y_DEBUG_ABORT_UNLESS(currentLeft == currentRight);

            // Publish new leaf nodes first, so it becomes available in sequential iteration
            ReplaceLeafNode(leaf, newLeaf.Left, newLeaf.Right);

            // Publish new edge, will make new leaf nodes available for searching
            TTag oldEdge;
            if (InsertPath.empty()) {
                oldEdge = Root.Publish(RefTag(currentLeft));
            } else {
                auto* const inner = InsertPath.back().Tag.ToInnerPage();
                const size_t innerIndex = InsertPath.back().Index;
                oldEdge = inner->Edges()[innerIndex].Publish(RefTag(currentLeft));
            }
            UnRefTag(oldEdge);

            // Finally collect garbage when needed
            InsertPath.clear();
            CollectGarbage();
        }

        template<class TValueArg>
        bool Emplace(const TKey& key, TValueArg&& value) {
            if (!FindPtr(key)) {
                EmplaceUnsafe(key, std::forward<TValueArg>(value));
                return true;
            } else {
                return false;
            }
        }

        template<class TValueArg>
        bool Emplace(TKey&& key, TValueArg&& value) {
            if (!FindPtr(key)) {
                EmplaceUnsafe(std::move(key), std::forward<TValueArg>(value));
                return true;
            } else {
                return false;
            }
        }

        size_t GetHeight() const {
            size_t height = 0;
            auto current = Root.GetUnsafe();
            while (current.IsInnerPage()) {
                auto* page = current.ToInnerPage();
                Y_DEBUG_ABORT_UNLESS(page->Count > 0);
                current = page->Edges()[0].GetUnsafe();
                ++height;
            }
            return ++height;
        }

    private:
        struct TLeafInsert {
            TLeafPage* const Left;
            TLeafPage* const Right;
            const TKey* const Middle;
        };

        template<class TKeyArg, class TValueArg>
        TLeafInsert PerformLeafInsert(
                TLeafPage* leaf, size_t index,
                TKeyArg&& key, TValueArg&& value,
                bool threadSafe)
        {
            const size_t count = leaf->UnsafeCount();
            Y_DEBUG_ABORT_UNLESS(index <= count);
            const size_t newCount = count + 1;

            // We don't expect any split
            if (newCount <= LeafPageCapacity) {
                if (index == count) {
                    // Case #1: atomically emplace the new last element, nothing else needed
                    new (leaf->Keys() + index) TKey(std::forward<TKeyArg>(key));
                    new (leaf->Values() + index) TValue(std::forward<TValueArg>(value));
                    if (threadSafe) {
                        leaf->PublishCount(newCount);
                    } else {
                        leaf->SetCountUnsafe(newCount);
                    }
                    return { nullptr, nullptr, nullptr };
                }

                if (!threadSafe) {
                    // Case #2a: insert new key without bothering with thread safety
                    auto* newKey = leaf->Keys() + index;
                    auto* nextKey = leaf->Keys() + count;
                    while (nextKey != newKey) {
                        auto* prevKey = nextKey - 1;
                        new (nextKey) TKey(std::move(*prevKey));
                        prevKey->~TKey();
                        nextKey = prevKey;
                    }
                    new (newKey) TKey(std::forward<TKeyArg>(key));

                    auto* newValue = leaf->Values() + index;
                    auto* nextValue = leaf->Values() + count;
                    while (nextValue != newValue) {
                        auto* prevValue = nextValue - 1;
                        new (nextValue) TValue(std::move(*prevValue));
                        prevValue->~TValue();
                        nextValue = prevValue;
                    }
                    new (newValue) TValue(std::forward<TValueArg>(value));

                    leaf->SetCountUnsafe(newCount);
                    return { nullptr, nullptr, nullptr };
                }

                // Case #2b: create a new leaf page that has new key inserted
                auto* oldKeys = leaf->Keys();
                auto* oldValues = leaf->Values();

                auto* newLeaf = AllocateLeafPage();
                auto* newKeys = newLeaf->Keys();
                auto* newValues = newLeaf->Values();
                for (size_t pos = 0; pos < index; ++pos) {
                    new (newKeys++) TKey(*oldKeys++);
                    new (newValues++) TValue(*oldValues++);
                }
                new (newKeys++) TKey(std::forward<TKeyArg>(key));
                new (newValues++) TValue(std::forward<TValueArg>(value));
                for (size_t pos = index; pos < count; ++pos) {
                    new (newKeys++) TKey(*oldKeys++);
                    new (newValues++) TValue(*oldValues++);
                }
                newLeaf->SetCountUnsafe(newCount);
                return { newLeaf, newLeaf, nullptr };
            }

            // Case #3: we must perform a split
            auto* oldKeys = leaf->Keys();
            auto* oldValues = leaf->Values();

            const size_t rightCount = newCount / 2; // we want less on the right leaf
            const size_t leftCount = newCount - rightCount; // we want more on the left leaf
            Y_DEBUG_ABORT_UNLESS(leftCount > 0 && rightCount > 0);

            auto* newLeft = AllocateLeafPage();
            auto* newRight = AllocateLeafPage();

            size_t copied = 0;
            auto* newKeys = newLeft->Keys();
            auto* newValues = newLeft->Values();

            for (size_t pos = 0; pos < index; ++pos) {
                new (newKeys++) TKey(*oldKeys++);
                new (newValues++) TValue(*oldValues++);
                if (++copied == leftCount) {
                    newKeys = newRight->Keys();
                    newValues = newRight->Values();
                }
            }
            new (newKeys++) TKey(std::forward<TKeyArg>(key));
            new (newValues++) TValue(std::forward<TValueArg>(value));
            if (++copied == leftCount) {
                newKeys = newRight->Keys();
                newValues = newRight->Values();
            }
            for (size_t pos = index; pos < count; ++pos) {
                new (newKeys++) TKey(*oldKeys++);
                new (newValues++) TValue(*oldValues++);
                if (++copied == leftCount) {
                    newKeys = newRight->Keys();
                    newValues = newRight->Values();
                }
            }
            newLeft->SetCountUnsafe(leftCount);
            newRight->SetCountUnsafe(rightCount);
            newLeft->Next.SetUnsafe(RefTag(newRight));
            newRight->Prev.SetUnsafe(RefTag(newLeft));

            return { newLeft, newRight, newRight->Keys() };
        }

        void ReplaceLeafNode(TLeafPage* leaf, TLeafPage* newLeft, TLeafPage* newRight) {
            // We expect chain from newLeft to newRight to already be wired internally
            auto* currentLeft = leaf->Prev.GetUnsafe().ToLeafPage();
            auto* currentRight = leaf->Next.GetUnsafe().ToLeafPage();
            newLeft->Prev.SetUnsafe(RefTag(currentLeft));
            newRight->Next.SetUnsafe(RefTag(currentRight));

            // Safely publish new leaf chain
            auto oldRightLink = (currentLeft ? &currentLeft->Next : &First)->Publish(RefTag(newLeft));
            auto oldLeftLink = (currentRight ? &currentRight->Prev : &Last)->Publish(RefTag(newRight));
            UnRefTag(oldRightLink);
            UnRefTag(oldLeftLink);
        }

        struct TInnerInsert {
            TInnerPage* const Left;
            TInnerPage* const Right;
            const TKey* const Middle;
        };

        TInnerInsert PerformInnerInsert(
                TInnerPage* inner, size_t index,
                TTag left, const TKey* middle, TTag right,
                bool threadSafe)
        {
            class TKeysReader {
            public:
                TKeysReader(const TKey* source, size_t index, size_t count, const TKey* middle)
                    : Current(source)
                    , Target(source + index)
                    , End(source + count)
                    , Middle(middle)
                    , MiddleUsed(false)
                { }

                const TKey* Read() {
                    if (Current == Target) {
                        if (!MiddleUsed) {
                            MiddleUsed = true;
                            return Middle;
                        }
                    }
                    Y_DEBUG_ABORT_UNLESS(Current != End);
                    return Current++;
                }

            private:
                const TKey* Current;
                const TKey* Target;
                const TKey* End;
                const TKey* Middle;
                bool MiddleUsed;
            };

            class TEdgesReader {
            public:
                TEdgesReader(TEdge* source, size_t index, size_t count, TTag left, TTag right)
                    : Current(source)
                    , Skipped(source + index)
                    , End(source + count)
                    , Replacements{ left, right }
                    , NextReplacement(0)
                {
                    Y_DEBUG_ABORT_UNLESS(Skipped != End);
                }

                TTag Read() {
                    if (Current == Skipped) {
                        if (NextReplacement < 2) {
                            return Replacements[NextReplacement++];
                        }
                        ++Current;
                    }
                    Y_DEBUG_ABORT_UNLESS(Current != End);
                    return (Current++)->GetUnsafe();
                }

            private:
                TEdge* Current;
                TEdge* Skipped;
                TEdge* End;
                TTag Replacements[2];
                size_t NextReplacement;
            };

            const size_t count = inner->Count;
            Y_DEBUG_ABORT_UNLESS(index <= count);
            const size_t newCount = count + 1;

            if (newCount <= InnerPageCapacity && !threadSafe) {
                // We have enough space and may update inner page inplace
                auto* newKey = inner->Keys() + index;
                auto* nextKey = inner->Keys() + count;
                while (nextKey != newKey) {
                    auto* prevKey = nextKey - 1;
                    new (nextKey) TKey(std::move(*prevKey));
                    prevKey->~TKey();
                    nextKey = prevKey;
                }
                new (newKey) TKey(*middle);

                auto* newEdge1 = inner->Edges() + index;
                auto* newEdge2 = newEdge1 + 1;
                auto* nextEdge = inner->Edges() + count + 1;
                while (nextEdge != newEdge2) {
                    auto* prevEdge = nextEdge - 1;
                    new (nextEdge) TEdge(prevEdge->GetUnsafe());
                    prevEdge->~TEdge();
                    nextEdge = prevEdge;
                }
                UnRefTag(newEdge1->GetUnsafe());
                newEdge1->~TEdge();
                new (newEdge1) TEdge(RefTag(left));
                new (newEdge2) TEdge(RefTag(right));

                inner->Count = newCount;
                return { nullptr, nullptr, nullptr };
            }

            TKeysReader keysReader(inner->Keys(), index, count, middle);
            TEdgesReader edgesReader(inner->Edges(), index, count + 1, left, right);

            if (newCount <= InnerPageCapacity) {
                // We have enough space to succeed without split
                auto* newInner = AllocateInnerPage();
                auto* newKeys = newInner->Keys();
                auto* newEdges = newInner->Edges();

                // Copy over keys
                for (size_t pos = 0; pos < newCount; ++pos) {
                    new (newKeys++) TKey(*keysReader.Read());
                }

                // Copy over edges with an increased refcount
                for (size_t pos = 0; pos <= newCount; ++pos) {
                    new (newEdges++) TEdge(RefTag(edgesReader.Read()));
                }

                newInner->Count = newCount;

                return { newInner, newInner, nullptr };
            }

            // We need to split with some new middle key bubbling up
            const size_t rightCount = (newCount - 1) / 2; // less keys on the right side
            const size_t leftCount = (newCount - 1) - rightCount; // more keys on the left side

            auto* newLeft = AllocateInnerPage();
            auto* newRight = AllocateInnerPage();
            const TKey* newMiddle;

            // Copy over keys, one key will be used as the new middle key
            for (size_t pos = 0; pos < leftCount; ++pos) {
                new (newLeft->Keys() + pos) TKey(*keysReader.Read());
            }
            newMiddle = keysReader.Read();
            for (size_t pos = 0; pos < rightCount; ++pos) {
                new (newRight->Keys() + pos) TKey(*keysReader.Read());
            }

            // Copy over edges with an increased refcount
            for (size_t pos = 0; pos <= leftCount; ++pos) {
                new (newLeft->Edges() + pos) TEdge(RefTag(edgesReader.Read()));
            }
            for (size_t pos = 0; pos <= rightCount; ++pos) {
                new (newRight->Edges() + pos) TEdge(RefTag(edgesReader.Read()));
            }

            newLeft->Count = leftCount;
            newRight->Count = rightCount;

            return { newLeft, newRight, newMiddle };
        }

    private:
        void Init() {
            Size_ = 0;
            FreePagesCount = 0;
            TotalPagesCount = 0;
            FreePages = nullptr;

            auto* initial = AllocateLeafPage();
            Root.Publish(RefTag(initial));
            First.Publish(RefTag(initial));
            Last.Publish(RefTag(initial));
            InsertPath.clear();

            Hazards.clear();
            RetireList.clear();
            MoreGarbage = false;
        }

        void Destroy() noexcept {
            UnRefTag(Root.SwapUnsafe({ }));
            CollectGarbage(true);

            auto current = First.SwapUnsafe({ });
            while (current) {
                auto* leaf = current.ToLeafPage();
                UnRefTag(leaf->Prev.SwapUnsafe({ }));
                auto next = leaf->Next.SwapUnsafe({ });
                UnRefTag(current);
                CollectGarbage(true);
                current = next;
            }
            UnRefTag(Last.SwapUnsafe({ }));
            CollectGarbage(true);

            Y_DEBUG_ABORT_UNLESS(FreePagesCount == TotalPagesCount,
                "BTree has %" PRISZT " out of %" PRISZT " pages leaked",
                TotalPagesCount - FreePagesCount, TotalPagesCount);
        }

    private:
        struct TInsertPath {
            TTag Tag;
            size_t Index;
        };

    private:
        TAllocator& Allocator;

        size_t Size_;
        size_t FreePagesCount;
        size_t TotalPagesCount;
        TFreePage* FreePages;

        TEdge Root, First, Last;
        std::vector<TInsertPath> InsertPath; // for insertions

        const TTreeDomainRef Domain;
        std::vector<void*> Hazards;
        std::vector<TTag> RetireList;
        bool MoreGarbage;
    };

}
