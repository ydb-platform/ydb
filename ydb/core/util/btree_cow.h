#pragma once

#include "btree_common.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/algorithm.h>
#include <util/generic/typetraits.h>
#include <util/generic/ptr.h>
#include <util/memory/alloc.h>
#include <util/system/sanitizers.h>
#include <util/system/yassert.h>

#include <atomic>
#include <vector>

namespace NKikimr {

    /**
     * Copy-on-write B+Tree with support for thread-safe snapshots
     *
     * WARNING: only one thread is allowed to call methods at any given time.
     *
     * All other threads must use TIterator to guarantee thread safety.
     */
    template<
        class TKey,
        class TValue,
        class TCompare = TLess<TKey>,
        class TAllocator = std::allocator<TValue>,
        size_t PageSize_ = 512>
    class TCowBTree : private TCompare {
        class TPage;

        using TEdge = TPage*;

        using TLayout = TBTreeLayout<PageSize_>;

        template<class THeader>
        using TLeafLayoutTemplate = typename TLayout::template TLeafLayout<THeader, TKey, TValue>;

        template<class THeader>
        using TInnerLayoutTemplate = typename TLayout::template TInnerLayout<THeader, TKey, TEdge>;

        enum class ETag {
            Leaf = 0,
            Inner = 1,
        };

        enum : int {
            PageCountBits = 8, // up to 255 elements per page
            PageEpochBits = 55, // up to 36 * 10^15 snapshots
        };

        static_assert(PageCountBits + PageEpochBits + 1 == 64, "Unexpected bit proportions");

        enum : ui64 {
            PageCountMax = (1ULL << PageCountBits) - 1,
            PageEpochMax = (1ULL << PageEpochBits) - 1,
        };

        /**
         * Pages start with the same basic header
         */
        class TPage {
        protected:
            TPage(ui64 epoch, ETag tag)
                : NextDroppedPage(nullptr)
                , Count(0)
                , Epoch(epoch)
                , Tag_(static_cast<int>(tag))
            { }

        public:
            ETag GetTag() const {
                return static_cast<ETag>(Tag_);
            }

        public:
            TPage* NextDroppedPage;
            ui64 Count : PageCountBits;
            ui64 Epoch : PageEpochBits;
            const ui64 Tag_ : 1;
        };

        static_assert(sizeof(TPage) == 16, "Unexpected TPage header size");

        using TLeafHeader = TPage;
        using TLeafLayout = TLeafLayoutTemplate<TLeafHeader>;

        class alignas(std::max({alignof(TLeafHeader), alignof(TKey), alignof(TValue)})) TLeafPage : public TLeafHeader {
        public:
            explicit TLeafPage(ui64 epoch)
                : TLeafHeader(epoch, ETag::Leaf)
            { }

            TKey* Keys() {
                return reinterpret_cast<TKey*>(reinterpret_cast<char*>(this) + TLeafLayout::KeysOffset);
            }

            TValue* Values() {
                return reinterpret_cast<TValue*>(reinterpret_cast<char*>(this) + TLeafLayout::ValuesOffset);
            }

            const TKey* Keys() const {
                return reinterpret_cast<const TKey*>(reinterpret_cast<const char*>(this) + TLeafLayout::KeysOffset);
            }

            const TValue* Values() const {
                return reinterpret_cast<const TValue*>(reinterpret_cast<const char*>(this) + TLeafLayout::ValuesOffset);
            }

            static TLeafPage* From(TPage* page) {
                Y_DEBUG_ABORT_UNLESS(page && page->GetTag() == ETag::Leaf);
                return static_cast<TLeafPage*>(page);
            }

            static const TLeafPage* From(const TPage* page) {
                Y_DEBUG_ABORT_UNLESS(page && page->GetTag() == ETag::Leaf);
                return static_cast<const TLeafPage*>(page);
            }

        private:
            char Data[PageSize_ - sizeof(TLeafHeader)];
        };

        static_assert(sizeof(TLeafPage) == PageSize_, "Unexpected leaf page size");

        using TInnerHeader = TPage;
        using TInnerLayout = TInnerLayoutTemplate<TInnerHeader>;

        class alignas(std::max({alignof(TInnerHeader), alignof(TKey), alignof(TEdge)})) TInnerPage : public TInnerHeader {
        public:
            explicit TInnerPage(ui64 epoch)
                : TInnerHeader(epoch, ETag::Inner)
            { }

            TKey* Keys() {
                return reinterpret_cast<TKey*>(reinterpret_cast<char*>(this) + TInnerLayout::KeysOffset);
            }

            TEdge* Edges() {
                return reinterpret_cast<TEdge*>(reinterpret_cast<char*>(this) + TInnerLayout::EdgesOffset);
            }

            const TKey* Keys() const {
                return reinterpret_cast<const TKey*>(reinterpret_cast<const char*>(this) + TInnerLayout::KeysOffset);
            }

            const TEdge* Edges() const {
                return reinterpret_cast<const TEdge*>(reinterpret_cast<const char*>(this) + TInnerLayout::EdgesOffset);
            }

            static TInnerPage* From(TPage* page) {
                Y_DEBUG_ABORT_UNLESS(page && page->GetTag() == ETag::Inner);
                return static_cast<TInnerPage*>(page);
            }

            static const TInnerPage* From(const TPage* page) {
                Y_DEBUG_ABORT_UNLESS(page && page->GetTag() == ETag::Inner);
                return static_cast<const TInnerPage*>(page);
            }

        private:
            char Data[PageSize_ - sizeof(TInnerHeader)];
        };

        static_assert(sizeof(TInnerPage) == PageSize_, "Unexpected inner page size");

        using TLeafAllocator = TReboundAllocator<TAllocator, TLeafPage>;
        using TInnerAllocator = TReboundAllocator<TAllocator, TInnerPage>;

        using TLeafAllocTraits = std::allocator_traits<TLeafAllocator>;
        using TInnerAllocTraits = std::allocator_traits<TInnerAllocator>;

    public:
        enum : size_t {
            PageSize = PageSize_,
            LeafPageAlignment = alignof(TLeafPage),
            InnerPageAlignment = alignof(TInnerPage),
            LeafPageCapacity = TLeafLayout::Capacity,
            InnerPageCapacity = TInnerLayout::Capacity,
        };

        static_assert(LeafPageCapacity >= 2, "PageSize is too small");
        static_assert(InnerPageCapacity >= 2, "PageSize is too small");
        static_assert(LeafPageCapacity <= PageCountMax, "PageSize is too large");
        static_assert(InnerPageCapacity <= PageCountMax, "PageSize is too large");

    private:
        struct TSnapshotContext {
            // Snapshot is garbage collected when RefCount reaches zero
            std::atomic<size_t> RefCount_{ 0 };

            // Epoch which created the snapshot
            const ui64 Epoch;

            // Linked list of snapshots that are no longer reachable
            // Points to the next snapshot in TSnapshotGCList
            TSnapshotContext* NextDroppedSnapshot = nullptr;

            // Linked list of active snapshots
            // Not thread safe, only accessed by a single writer thread
            TSnapshotContext* Prev = nullptr;
            TSnapshotContext* Next = nullptr;

            // Linked list of dropped pages neede for this snapshot
            // Not thread safe, only accessed by a single writer thread
            TPage* LastDroppedPage = nullptr;

            // True when it is possible to rollback to this snapshot
            bool RollbackSupported = true;

            // Disable all copy constructors
            TSnapshotContext(const TSnapshotContext&) = delete;
            TSnapshotContext& operator=(const TSnapshotContext&) = delete;

            explicit TSnapshotContext(ui64 epoch)
                : Epoch(epoch)
            { }

            size_t Ref() {
                return RefCount_.fetch_add(1, std::memory_order_relaxed) + 1;
            }

            size_t UnRef() {
                return RefCount_.fetch_sub(1, std::memory_order_release) - 1;
            }

            size_t RefCount() const {
                return RefCount_.load(std::memory_order_relaxed);
            }

            void LinkAfter(TSnapshotContext* last) {
                Y_DEBUG_ABORT_UNLESS(this != last);
                Y_DEBUG_ABORT_UNLESS(Prev == nullptr && Next == nullptr);
                if (last) {
                    Y_DEBUG_ABORT_UNLESS(last->Next == nullptr);
                    Prev = last;
                    last->Next = this;
                }
            }

            void Unlink() {
                auto* prev = Prev;
                auto* next = Next;
                if (prev) {
                    prev->Next = next;
                }
                if (next) {
                    next->Prev = prev;
                }
            }

            void AddDroppedPage(TPage* page) {
                Y_DEBUG_ABORT_UNLESS(page->NextDroppedPage == nullptr);
                page->NextDroppedPage = LastDroppedPage;
                LastDroppedPage = page;
            }
        };

    private:
        class TSnapshotGCList : public TThrRefBase {
        public:
            ~TSnapshotGCList() {
                auto* head = Head.exchange(nullptr, std::memory_order_acquire);
                while (head) {
                    THolder<TSnapshotContext> current(head);
                    head = std::exchange(current->NextDroppedSnapshot, nullptr);
                }
            }

            void AddDroppedSnapshot(TSnapshotContext* context) {
                TSnapshotContext* head = Head.load(std::memory_order_acquire);
                do {
                    context->NextDroppedSnapshot = head;
                } while (!Head.compare_exchange_weak(head, context, std::memory_order_acq_rel));
            }

            TSnapshotContext* CollectDroppedSnapshots() {
                if (Head.load(std::memory_order_relaxed)) {
                    return Head.exchange(nullptr, std::memory_order_acquire);
                } else {
                    return nullptr;
                }
            }

        private:
            std::atomic<TSnapshotContext*> Head;
        };

    private:
        class TTreeHandle {
            friend class TCowBTree;

        private:
            TTreeHandle(const TCowBTree* tree, TSnapshotContext* context, TIntrusivePtr<TSnapshotGCList> gcList) noexcept
                : Tree(tree)
                , Context(context)
                , GCList(std::move(gcList))
            {
                Y_DEBUG_ABORT_UNLESS(Tree && Context && GCList, "Explicit constructor got unexpected values");
                const size_t count = Context->Ref();
                Y_DEBUG_ABORT_UNLESS(count == 1, "Explicit constructor must be the first reference");
            }

            explicit TTreeHandle(const TCowBTree* tree) noexcept
                : Tree(tree)
                , Context(nullptr)
                , GCList()
            { }

        public:
            TTreeHandle() noexcept
                : Tree(nullptr)
                , Context(nullptr)
                , GCList()
            { }

            ~TTreeHandle() {
                Reset();
            }

            TTreeHandle(const TTreeHandle& rhs)
                : Tree(rhs.Tree)
                , Context(rhs.Context)
                , GCList(rhs.GCList)
            {
                if (Tree && Context) {
                    Y_DEBUG_ABORT_UNLESS(GCList, "Handle fields are out of sync");
                    const size_t count = Context->Ref();
                    Y_DEBUG_ABORT_UNLESS(count > 1, "Unexpected refcount in copy constructor");
                }
            }

            TTreeHandle(TTreeHandle&& rhs)
                : Tree(rhs.Tree)
                , Context(rhs.Context)
                , GCList(std::move(rhs.GCList))
            {
                rhs.Tree = nullptr;
                rhs.Context = nullptr;
            }

            void Reset() noexcept {
                if (Tree) {
                    if (Context) {
                        Y_DEBUG_ABORT_UNLESS(GCList, "Handle fields are out of sync");
                        TSnapshotContext* context = std::exchange(Context, nullptr);
                        TIntrusivePtr<TSnapshotGCList> gcList = std::move(GCList);
                        if (context->UnRef() == 0) {
                            gcList->AddDroppedSnapshot(context);
                        }
                    }
                    Tree = nullptr;
                }
            }

            TTreeHandle& operator=(const TTreeHandle& rhs) {
                if (Y_LIKELY(this != &rhs)) {
                    Reset();
                    Tree = rhs.Tree;
                    Context = rhs.Context;
                    GCList = rhs.GCList;
                    if (Tree && Context) {
                        Y_DEBUG_ABORT_UNLESS(GCList, "Handle fields are out of sync");
                        const size_t count = Context->Ref();
                        Y_DEBUG_ABORT_UNLESS(count > 1, "Unexpected refcount in copy assignment");
                    }
                }

                return *this;
            }

            TTreeHandle& operator=(TTreeHandle&& rhs) {
                if (Y_LIKELY(this != &rhs)) {
                    Reset();
                    Tree = rhs.Tree;
                    Context = rhs.Context;
                    GCList = std::move(rhs.GCList);
                    rhs.Tree = nullptr;
                    rhs.Context = nullptr;
                }

                return *this;
            }

            explicit operator bool() const {
                return bool(Tree);
            }

            const TCowBTree* operator->() const {
                return Tree;
            }

        private:
            const TCowBTree* Tree;
            TSnapshotContext* Context;
            TIntrusivePtr<TSnapshotGCList> GCList;
        };

    public:
        class TSnapshot;

        class TIterator {
            friend class TSnapshot;
            friend class TCowBTree;

        private:
            TIterator(TTreeHandle tree, const TPage* root, size_t size, size_t innerHeight)
                : Tree(std::move(tree))
                , Root(root)
                , Size_(size)
                , InnerHeight_(innerHeight)
            { }

        public:
            TIterator() = default;

        public:
            size_t Size() const {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");
                return Size_;
            }

            size_t Height() const {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");
                return Root ? InnerHeight_ + 1 : 0;
            }

            void Invalidate() {
                CurrentPage = nullptr;
            }

            bool SeekFirst() {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                if (!Root) {
                    CurrentPage = nullptr;
                    return false;
                }

                Path.clear();
                Path.reserve(InnerHeight_);

                const auto* page = FollowLeftMost(Root);
                const size_t count = page->Count;
                Y_DEBUG_ABORT_UNLESS(count > 0);

                CurrentPage = page;
                CurrentIndex = 0;
                CurrentCount = count;
                return true;
            }

            bool SeekLast() {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                if (!Root) {
                    CurrentPage = nullptr;
                    return false;
                }

                Path.clear();
                Path.reserve(InnerHeight_);

                const auto* page = FollowRightMost(Root);
                const size_t count = page->Count;
                Y_DEBUG_ABORT_UNLESS(count > 0);

                CurrentPage = page;
                CurrentIndex = count - 1;
                CurrentCount = count;
                return true;
            }

            /**
             * Seeks to the first key that is >= search key
             *
             * If backwards is true seeks to the last key that is < search key (inverse upper bound)
             */
            template<class TKeyArg>
            bool SeekLowerBound(TKeyArg&& key, bool backwards = false) {
                Y_ABORT_UNLESS(Tree, "Uninitialized iterator");

                if (!Root) {
                    CurrentPage = nullptr;
                    return false;
                }

                Path.clear();
                Path.reserve(InnerHeight_);

                const TPage* current = Root;
                while (current->GetTag() == ETag::Inner) {
                    const auto* inner = TInnerPage::From(current);
                    const size_t count = inner->Count;
                    Y_DEBUG_ABORT_UNLESS(count > 0);
                    auto* keysBegin = inner->Keys();
                    auto* keysEnd = keysBegin + count;
                    size_t index = std::lower_bound(keysBegin, keysEnd, key, Tree->key_comp()) - keysBegin;
                    // Follow the left edge, which includes keys <= our search key
                    Path.push_back(TEntry{ inner, index });
                    current = inner->Edges()[index];
                    Y_DEBUG_ABORT_UNLESS(current);
                }

                const auto* page = TLeafPage::From(current);
                const size_t count = page->Count;
                Y_DEBUG_ABORT_UNLESS(count > 0);

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

                if (!Root) {
                    CurrentPage = nullptr;
                    return false;
                }

                Path.clear();
                Path.reserve(InnerHeight_);

                const TPage* current = Root;
                while (current->GetTag() == ETag::Inner) {
                    const auto* inner = TInnerPage::From(current);
                    const size_t count = inner->Count;
                    Y_DEBUG_ABORT_UNLESS(count > 0);
                    auto* keysBegin = inner->Keys();
                    auto* keysEnd = keysBegin + count;
                    size_t index = std::upper_bound(keysBegin, keysEnd, key, Tree->key_comp()) - keysBegin;
                    // Follow the left edge, which includes keys >= our search key
                    Path.push_back(TEntry{ inner, index });
                    current = inner->Edges()[index];
                    Y_DEBUG_ABORT_UNLESS(current);
                }

                const auto* page = TLeafPage::From(current);
                const size_t count = page->Count;
                Y_DEBUG_ABORT_UNLESS(count > 0);

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

            template<class TKeyArg>
            const TValue* Find(TKeyArg&& key) {
                if (SeekExact(std::forward<TKeyArg>(key))) {
                    return &GetValue();
                } else {
                    return nullptr;
                }
            }

        private:
            const TLeafPage* FollowLeftMost(const TPage* current) {
                Y_DEBUG_ABORT_UNLESS(current);
                while (current->GetTag() == ETag::Inner) {
                    const auto* inner = TInnerPage::From(current);
                    const size_t count = inner->Count;
                    Y_DEBUG_ABORT_UNLESS(count > 0);
                    Path.push_back(TEntry{ inner, 0 });
                    current = inner->Edges()[0];
                    Y_DEBUG_ABORT_UNLESS(current);
                }

                return TLeafPage::From(current);
            }

            const TLeafPage* FollowRightMost(const TPage* current) {
                Y_DEBUG_ABORT_UNLESS(current);
                while (current->GetTag() == ETag::Inner) {
                    const auto* inner = TInnerPage::From(current);
                    const size_t count = inner->Count;
                    Y_DEBUG_ABORT_UNLESS(count > 0);
                    Path.push_back(TEntry{ inner, count });
                    current = inner->Edges()[count];
                    Y_DEBUG_ABORT_UNLESS(current);
                }

                return TLeafPage::From(current);
            }

        private:
            const TLeafPage* PrevLeaf() {
                while (!Path.empty()) {
                    auto& entry = Path.back();
                    if (entry.Index > 0) {
                        size_t index = --entry.Index;
                        const auto* current = entry.Page->Edges()[index];
                        return FollowRightMost(current);
                    }
                    Path.pop_back();
                }

                return nullptr;
            }

            const TLeafPage* NextLeaf() {
                while (!Path.empty()) {
                    auto& entry = Path.back();
                    if (entry.Index < entry.Page->Count) {
                        size_t index = ++entry.Index;
                        const auto* current = entry.Page->Edges()[index];
                        return FollowLeftMost(current);
                    }
                    Path.pop_back();
                }

                return nullptr;
            }

        private:
            bool InitBackwards(const TLeafPage* page, size_t index, size_t count) {
                if (index == 0) {
                    // We need to follow to the previous page
                    page = PrevLeaf();
                    if (!page) {
                        // This page was the first page
                        CurrentPage = nullptr;
                        return false;
                    }
                    index = count = page->Count;
                    Y_DEBUG_ABORT_UNLESS(count > 0, "Unexpected empty prev page");
                }
                CurrentPage = page;
                CurrentIndex = index - 1;
                CurrentCount = count;
                return true;
            }

            bool InitForwards(const TLeafPage* page, size_t index, size_t count) {
                if (index == count) {
                    // We need to follow to the next page
                    page = NextLeaf();
                    if (!page) {
                        // This page was the last page
                        CurrentPage = nullptr;
                        return false;
                    }
                    index = 0;
                    count = page->Count;
                    Y_DEBUG_ABORT_UNLESS(count > 0, "Unexpected empty next page");
                }
                CurrentPage = page;
                CurrentIndex = index;
                CurrentCount = count;
                return true;
            }

        private:
            struct TEntry {
                const TInnerPage* Page;
                size_t Index;
            };

        private:
            TTreeHandle Tree;

            const TPage* Root = nullptr;
            size_t Size_ = 0;
            size_t InnerHeight_ = 0;

            // Has enough space for 16 elements
            // When branch factor is 8 it's enough for 4 billion elements,
            // assuming each inner page is half empty (random insertion)
            TSmallVec<TEntry> Path;

            const TLeafPage* CurrentPage = nullptr;
            size_t CurrentIndex = 0;
            size_t CurrentCount = 0;
        };

        class TSnapshot {
            friend class TCowBTree;

        private:
            TSnapshot(TTreeHandle tree, const TPage* root, size_t size, size_t innerHeight)
                : Tree(std::move(tree))
                , Root(root)
                , Size_(size)
                , InnerHeight_(innerHeight)
            { }

        public:
            TSnapshot() = default;

            size_t Size() const {
                Y_ABORT_UNLESS(Tree, "Uninitialized snapshot");
                return Size_;
            }

            size_t Height() const {
                Y_ABORT_UNLESS(Tree, "Uninitialized snapshot");
                return Root ? InnerHeight_ + 1 : 0;
            }

            TIterator Iterator() const & {
                Y_ABORT_UNLESS(Tree, "Uninitialized snapshot");
                return TIterator(Tree, Root, Size_, InnerHeight_);
            }

            TIterator Iterator() && {
                Y_ABORT_UNLESS(Tree, "Uninitialized snapshot");
                return TIterator(std::move(Tree), Root, Size_, InnerHeight_);
            }

        private:
            TTreeHandle Tree;

            const TPage* Root = nullptr;
            size_t Size_ = 0;
            size_t InnerHeight_ = 0;
        };

    public:
        TCowBTree(const TCowBTree&) = delete;
        TCowBTree& operator=(const TCowBTree&) = delete;

        TCowBTree()
            : TCowBTree(TCompare(), TAllocator())
        { }

        explicit TCowBTree(const TCompare& compare)
            : TCowBTree(compare, TAllocator())
        { }

        explicit TCowBTree(const TAllocator& allocator)
            : TCowBTree(TCompare(), allocator)
        { }

        TCowBTree(const TCompare& compare, const TAllocator& allocator)
            : TCompare(compare)
            , Allocator(allocator)
            , LeafAllocator(Allocator)
            , InnerAllocator(Allocator)
            , GCList(MakeIntrusive<TSnapshotGCList>())
        { }

        ~TCowBTree() {
            while (LastSnapshot) {
                auto* current = LastSnapshot;
                LastSnapshot = current->Prev;
                DropSnapshotPages(current, /* gc = */ false);
                current->Prev = nullptr;
                current->Next = nullptr;
            }

            if (Root) {
                FreePageRecursive(Root);
                Root = nullptr;
            }

            Y_DEBUG_ABORT_UNLESS(DroppedPages_ == 0, "Unexpected dropped pages at destructor");
            Y_DEBUG_ABORT_UNLESS(AllocatedPages_ == 0, "Unexpected allocated pages at destructor");
        }

        const TCompare& key_comp() const {
            return static_cast<const TCompare&>(*this);
        }

        size_t size() const { return Size(); }
        size_t height() const { return Height(); }
        bool empty() const { return Empty(); }

        size_t Size() const { return Size_; }
        size_t Height() const { return Root ? InnerHeight_ + 1 : 0; }
        bool Empty() const { return !Root; }

        size_t AllocatedPages() const { return AllocatedPages_; }
        size_t DroppedPages() const { return DroppedPages_; }

    public:
        /**
         * Collect memory used by snapshots that are no longer in use
         */
        void CollectGarbage() {
            if (auto* garbage = GCList->CollectDroppedSnapshots()) {
                do {
                    Y_DEBUG_ABORT_UNLESS(garbage->RefCount() == 0);
                    THolder<TSnapshotContext> context(garbage);
                    garbage = std::exchange(context->NextDroppedSnapshot, nullptr);
                    DropSnapshot(context.Get());
                } while (garbage);
            }
        }

        /**
         * Creates a snapshot of current tree state. It is safe to copy and use
         * it in other threads without any additional synchronization. However
         * iterators may not be used beyond the lifetime of tree object itself.
         *
         * WARNING: as a reminder this method is not thread-safe, only a single
         *          writer thread may modify the tree and create snapshots for
         *          other threads.
         */
        TSnapshot Snapshot() {
            CollectGarbage();

            Y_ABORT_UNLESS(CurrentEpoch < PageEpochMax, "Epoch overflow: too many snapshots");

            TSnapshotContext* context = new TSnapshotContext(CurrentEpoch++);
            TTreeHandle handle(this, context, GCList);

            context->LinkAfter(LastSnapshot);
            LastSnapshot = context;
            LastSnapshotEpoch = context->Epoch;

            return TSnapshot(std::move(handle), Root, Size_, InnerHeight_);
        }

        /**
         * Creates a snapshot of current tree state. But unlike a safe snapshot
         * it cannot be used after the tree is modified.
         */
        TSnapshot UnsafeSnapshot() const {
            return TSnapshot(TTreeHandle(this), Root, Size_, InnerHeight_);
        }

        /**
         * Rollback to a previously created snapshot
         *
         * Note that newer snapshots will continue to be valid, but
         * rolling forward is undefined.
         */
        void RollbackTo(const TSnapshot& snapshot) {
            CollectGarbage();

            TSnapshotContext* context = snapshot.Tree.Context;
            Y_ABORT_UNLESS(context, "Cannot rollback to an invalid or an empty snapshot");
            Y_ABORT_UNLESS(context->RollbackSupported, "Cannot rollback to a newer snapshot");

            Y_ABORT_UNLESS(snapshot.Tree.Tree == this, "Cannot rollback to snapshot from a different tree");

            TPage* prevRoot = Root;
            ui64 epoch = context->Epoch;

            // Restore tree state to the one stored in snapshot
            Root = const_cast<TPage*>(snapshot.Root);
            Size_ = snapshot.Size_;
            InnerHeight_ = snapshot.InnerHeight_;
            InsertPath.clear();

            // Restore all dropped pages not newer than snapshot epoch
            for (;;) {
                TPage** nextDropped = &context->LastDroppedPage;
                while (TPage* page = *nextDropped) {
                    if (page->Epoch <= epoch) {
                        // Remove it from the list of dropped pages
                        Y_DEBUG_ABORT_UNLESS(DroppedPages_ > 0);
                        *nextDropped = page->NextDroppedPage;
                        page->NextDroppedPage = nullptr;
                        --DroppedPages_;
                    } else {
                        // Skip and inspect the next page
                        nextDropped = &page->NextDroppedPage;
                    }
                }
                context = context->Next;
                if (!context) {
                    break;
                }
                // All newer snapshots cannot be used for rollbacks, since
                // some invariants of dropped page lists become invalid at
                // future updates.
                context->RollbackSupported = false;
            }

            // Drop newer pages, they will be added to the latest snapshot
            if (prevRoot) {
                DropNewerPageRecursive(prevRoot, epoch);
            }
        }

    public:
        void clear() {
            Clear();
        }

        void Clear() {
            CollectGarbage();

            if (auto* root = Root) {
                Root = nullptr;
                Size_ = 0;
                InnerHeight_ = 0;
                InsertPath.clear();

                DropPageRecursive(root);
            }
        }

    public:
        /**
         * Searches for the specified key and returns a pointer to the last
         * corresponding value if it exists. As a side effect it prepares the
         * next EmplaceUnsafe for inserting a new key/value pair, which would
         * be after the last value if key already exists.
         */
        template<class TKeyArg>
        const TValue* Find(TKeyArg&& key) const {
            InsertPath.clear();

            auto* current = Root;
            if (!current) {
                // Empty tree
                return nullptr;
            }

            while (current->GetTag() == ETag::Inner) {
                auto* inner = TInnerPage::From(current);
                size_t count = inner->Count;
                Y_DEBUG_ABORT_UNLESS(count > 0);
                auto* keysBegin = inner->Keys();
                auto* keysEnd = keysBegin + count;
                size_t index = std::upper_bound(keysBegin, keysEnd, key, key_comp()) - keysBegin;
                // We follow the left edge, towards the first entry > search key
                InsertPath.push_back(TInsertPath{ current, index });
                current = inner->Edges()[index];
                Y_DEBUG_ABORT_UNLESS(current);
            }

            auto* leaf = TLeafPage::From(current);
            size_t count = leaf->Count;
            Y_DEBUG_ABORT_UNLESS(count > 0);
            // Insert position is before the upper bound
            auto* keysBegin = leaf->Keys();
            auto* keysEnd = keysBegin + count;
            size_t index = std::upper_bound(keysBegin, keysEnd, key, key_comp()) - keysBegin;
            InsertPath.push_back(TInsertPath{ current, index });

            if (index == 0) {
                // We never delete keys and always use first leaf key as inner
                // page key. Thus if we encountered some key <= search key on
                // some inner page, then we must have followed the right link
                // next to it and are guaranteed to find it as the first key
                // on the final leaf page. The only case where we would get
                // index == 0 on a leaf page, is if search key is less than
                // all already existing keys.
                for (const auto& path : InsertPath) {
                    Y_DEBUG_ABORT_UNLESS(path.Index == 0);
                }
                return nullptr;
            }

            // We want to inspect the previous key
            --index;

            // We know that keys[index] <= key is true
            // If the following comparision is true then keys[index] != key
            if (key_comp()(leaf->Keys()[index], key)) {
                return nullptr;
            }

            return leaf->Values() + index;
        }

        /**
         * Update an existing value, immediately after a successful Find call
         */
        TValue* UpdateUnsafe() {
            CollectGarbage();

            Y_DEBUG_ABORT_UNLESS(PagesToDrop.empty());
            Y_DEBUG_ABORT_UNLESS(KeysToDrop.empty());

            Y_ABORT_UNLESS(!InsertPath.empty() && InsertPath.back().Page->GetTag() == ETag::Leaf);
            TLeafPage* const leaf = TLeafPage::From(InsertPath.back().Page);
            Y_ABORT_UNLESS(InsertPath.back().Index > 0);
            const size_t leafIndex = InsertPath.back().Index - 1;
            InsertPath.pop_back();

            if (LastSnapshotEpoch < leaf->Epoch) {
                // Update may be performed inplace
                InsertPath.clear();
                return leaf->Values() + leafIndex;
            }

            auto* newLeaf = CloneLeafPage(leaf);
            PagesToDrop.push_back(leaf);

            PropagateReplace(newLeaf);
            PropagateEpoch();
            PropagateDrop();

            return newLeaf->Values() + leafIndex;
        }

        /**
         * Emplace a new key/value pair, immediately after a Find call
         */
        template<class TKeyArg, class... TValueArgs>
        TValue* EmplaceUnsafe(TKeyArg&& key, TValueArgs&&... valueArgs) {
            CollectGarbage();

            Y_DEBUG_ABORT_UNLESS(PagesToDrop.empty());
            Y_DEBUG_ABORT_UNLESS(KeysToDrop.empty());

            if (!Root) {
                // First value for an empty tree, create a new leaf page
                Y_DEBUG_ABORT_UNLESS(InsertPath.empty());
                TLeafPage* const newRoot = NewLeafPage();
                new (newRoot->Keys()) TKey(std::forward<TKeyArg>(key));
                new (newRoot->Values()) TValue(std::forward<TValueArgs>(valueArgs)...);
                newRoot->Count = 1;
                Root = newRoot;
                ++Size_;
                return newRoot->Values();
            }

            Y_ABORT_UNLESS(!InsertPath.empty() && InsertPath.back().Page->GetTag() == ETag::Leaf);
            TLeafPage* const leaf = TLeafPage::From(InsertPath.back().Page);
            const size_t leafIndex = InsertPath.back().Index;
            InsertPath.pop_back();

            const auto newLeaf = PerformLeafInsert(
                leaf, leafIndex,
                std::forward<TKeyArg>(key),
                std::forward<TValueArgs>(valueArgs)...);

            if (!newLeaf.Left) {
                // Insert has been performed inplace
                // Since leaf epoch is not updated we are done
                ++Size_;
                InsertPath.clear();
                return newLeaf.Value;
            }

            TPage* currentLeft = newLeaf.Left;
            TPage* currentRight = newLeaf.Right;
            const TKey* currentMiddle = newLeaf.Middle;
            while (currentMiddle) {
                if (InsertPath.empty()) {
                    // We are splitting the root page
                    auto* newRoot = NewInnerPage();
                    new (newRoot->Keys()) TKey(*currentMiddle);
                    new (newRoot->Edges() + 0) TEdge(currentLeft);
                    new (newRoot->Edges() + 1) TEdge(currentRight);
                    newRoot->Count = 1;

                    currentLeft = newRoot;
                    currentRight = newRoot;
                    currentMiddle = nullptr;
                    ++InnerHeight_;
                    break;
                }

                TInnerPage* const inner = TInnerPage::From(InsertPath.back().Page);
                const size_t innerIndex = InsertPath.back().Index;
                InsertPath.pop_back();

                const auto newInner = PerformInnerInsert(
                    inner, innerIndex,
                    currentLeft, currentMiddle, currentRight);

                currentLeft = newInner.Left;
                currentRight = newInner.Right;
                currentMiddle = newInner.Middle;
            }

            Y_DEBUG_ABORT_UNLESS(currentLeft == currentRight);

            if (currentLeft) {
                PropagateReplace(currentLeft);
            }
            PropagateEpoch();
            PropagateDrop();

            Y_DEBUG_ABORT_UNLESS(InsertPath.empty());

            ++Size_;
            return newLeaf.Value;
        }

        template<class TKeyArg, class... TValueArgs>
        bool Emplace(TKeyArg&& key, TValueArgs&&... valueArgs) {
            if (!Find(key)) {
                EmplaceUnsafe(std::forward<TKeyArg>(key), std::forward<TValueArgs>(valueArgs)...);
                return true;
            } else {
                return false;
            }
        }

        template<class TKeyArg>
        TValue* FindForUpdate(TKeyArg&& key) {
            if (Find(std::forward<TKeyArg>(key))) {
                return UpdateUnsafe();
            }

            return nullptr;
        }

        template<class TKeyArg>
        TValue& InsertOrUpdate(TKeyArg&& key) {
            if (Find(key)) {
                return *UpdateUnsafe();
            } else {
                return *EmplaceUnsafe(std::forward<TKeyArg>(key));
            }
        }

        template<class TKeyArg>
        TValue& operator[](TKeyArg&& key) {
            return InsertOrUpdate(std::forward<TKeyArg>(key));
        }

    private:
        void PropagateReplace(TPage* current) {
            Y_DEBUG_ABORT_UNLESS(current);
            while (!InsertPath.empty()) {
                TInnerPage* const inner = TInnerPage::From(InsertPath.back().Page);
                const size_t innerIndex = InsertPath.back().Index;
                InsertPath.pop_back();

                if (LastSnapshotEpoch < inner->Epoch) {
                    // It is possible to update this page inplace
                    inner->Edges()[innerIndex] = current;
                    inner->Epoch = CurrentEpoch;
                    return;
                }

                // Clone existing page and update a copy
                auto* newInner = CloneInnerPage(inner);
                newInner->Edges()[innerIndex] = current;
                PagesToDrop.push_back(inner);
                current = newInner;
            }

            Root = current;
        }

        void PropagateEpoch() {
            while (!InsertPath.empty()) {
                TInnerPage* const inner = TInnerPage::From(InsertPath.back().Page);
                InsertPath.pop_back();
                Y_DEBUG_ABORT_UNLESS(LastSnapshotEpoch < inner->Epoch);
                inner->Epoch = CurrentEpoch;
            }
        }

        void PropagateDrop() {
            while (!KeysToDrop.empty()) {
                TKey* key = KeysToDrop.back();
                KeysToDrop.pop_back();
                key->~TKey();
            }
            while (!PagesToDrop.empty()) {
                TPage* page = PagesToDrop.back();
                PagesToDrop.pop_back();
                DropPage(page);
            }
        }

    private:
        TLeafPage* CloneLeafPage(const TLeafPage* leaf) {
            const size_t count = leaf->Count;
            const auto* oldKeys = leaf->Keys();
            const auto* oldValues = leaf->Values();

            auto* newLeaf = NewLeafPage();
            auto* newKeys = newLeaf->Keys();
            auto* newValues = newLeaf->Values();
            for (size_t pos = 0; pos < count; ++pos) {
                new (newKeys++) TKey(*oldKeys++);
            }
            for (size_t pos = 0; pos < count; ++pos) {
                new (newValues++) TValue(*oldValues++);
            }
            newLeaf->Count = count;
            return newLeaf;
        }

        TInnerPage* CloneInnerPage(const TInnerPage* inner) {
            const size_t count = inner->Count;
            const auto* oldKeys = inner->Keys();
            const auto* oldEdges = inner->Edges();

            auto* newInner = NewInnerPage();
            auto* newKeys = newInner->Keys();
            auto* newEdges = newInner->Edges();
            for (size_t pos = 0; pos < count; ++pos) {
                new (newKeys++) TKey(*oldKeys++);
            }
            for (size_t pos = 0; pos <= count; ++pos) {
                new (newEdges++) TEdge(*oldEdges++);
            }
            newInner->Count = count;
            return newInner;
        }

    private:
        struct TLeafInsert {
            TLeafPage* const Left;
            TLeafPage* const Right;
            const TKey* Middle;
            TValue* Value;
        };

        template<class TKeyArg, class... TValueArgs>
        TLeafInsert PerformLeafInsertWithCopy(
                TLeafPage* leaf, size_t index,
                TKeyArg&& key, TValueArgs&&... valueArgs)
        {
            const size_t count = leaf->Count;
            Y_DEBUG_ABORT_UNLESS(index <= count);
            const size_t newCount = count + 1;

            // Case #1: create a new leaf page that has new item inserted
            if (newCount <= LeafPageCapacity) {
                auto* newLeaf = NewLeafPage();

                auto* oldKeys = leaf->Keys();
                auto* oldValues = leaf->Values();

                auto* newKeys = newLeaf->Keys();
                auto* newValues = newLeaf->Values();

                for (size_t pos = 0; pos < index; ++pos) {
                    new (newKeys++) TKey(*oldKeys++);
                    new (newValues++) TValue(*oldValues++);
                }

                new (newKeys++) TKey(std::forward<TKeyArg>(key));
                new (newValues++) TValue(std::forward<TValueArgs>(valueArgs)...);

                for (size_t pos = index; pos < count; ++pos) {
                    new (newKeys++) TKey(*oldKeys++);
                    new (newValues++) TValue(*oldValues++);
                }

                newLeaf->Count = newCount;
                PagesToDrop.push_back(leaf);

                return { newLeaf, newLeaf, nullptr, newLeaf->Values() + index };
            }

            // Case #2: we must perform a split
            const size_t rightCount = newCount / 2; // we want less on the right leaf
            const size_t leftCount = newCount - rightCount; // we want more on the left leaf
            Y_DEBUG_ABORT_UNLESS(leftCount > 0 && rightCount > 0);

            auto* newLeft = NewLeafPage();
            auto* newRight = NewLeafPage();

            auto* oldKeys = leaf->Keys();
            auto* oldValues = leaf->Values();

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

            auto* newValue = newValues;
            new (newKeys++) TKey(std::forward<TKeyArg>(key));
            new (newValues++) TValue(std::forward<TValueArgs>(valueArgs)...);
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

            newLeft->Count = leftCount;
            newRight->Count = rightCount;
            PagesToDrop.push_back(leaf);

            return { newLeft, newRight, newRight->Keys(), newValue };
        }

        void EnsureVacancy(TLeafPage* leaf, size_t index, size_t count) {
            // Move elements to make a vacancy at index
            if (index < count) {
                auto* newKey = leaf->Keys() + index;
                auto* nextKey = leaf->Keys() + count;
                while (nextKey != newKey) {
                    auto* prevKey = nextKey - 1;
                    new (nextKey) TKey(std::move(*prevKey));
                    prevKey->~TKey();
                    nextKey = prevKey;
                }

                auto* newValue = leaf->Values() + index;
                auto* nextValue = leaf->Values() + count;
                while (nextValue != newValue) {
                    auto* prevValue = nextValue - 1;
                    new (nextValue) TValue(std::move(*prevValue));
                    prevValue->~TValue();
                    nextValue = prevValue;
                }
            }
        }

        template<class TKeyArg, class... TValueArgs>
        TLeafInsert PerformLeafInsert(
                TLeafPage* leaf, size_t index,
                TKeyArg&& key, TValueArgs&&... valueArgs)
        {
            if (leaf->Epoch <= LastSnapshotEpoch) {
                return PerformLeafInsertWithCopy(
                        leaf, index,
                        std::forward<TKeyArg>(key),
                        std::forward<TValueArgs>(valueArgs)...);
            }

            const size_t count = leaf->Count;
            Y_DEBUG_ABORT_UNLESS(index <= count);
            const size_t newCount = count + 1;

            // Case #1: reuse current page and insert inplace
            if (newCount <= LeafPageCapacity) {
                EnsureVacancy(leaf, index, count);

                new (leaf->Keys() + index) TKey(std::forward<TKeyArg>(key));
                new (leaf->Values() + index) TValue(std::forward<TValueArgs>(valueArgs)...);

                // N.B. epoch is not updated for efficiency
                leaf->Count = newCount;

                return { nullptr, nullptr, nullptr, leaf->Values() + index };
            }

            // Case #2: we must perform a split
            const size_t rightCount = newCount / 2; // we want less on the right leaf
            const size_t leftCount = newCount - rightCount; // we want more on the left leaf
            Y_DEBUG_ABORT_UNLESS(leftCount > 0 && rightCount > 0);

            auto* newRight = NewLeafPage();
            TValue* newValue;

            if (index < leftCount) {
                // Case #2a: reuse current page with new item on the left
                auto* oldKeys = leaf->Keys() + leftCount - 1;
                auto* oldValues = leaf->Values() + leftCount - 1;

                auto* newKeys = newRight->Keys();
                auto* newValues = newRight->Values();
                for (size_t pos = 0; pos < rightCount; ++pos) {
                    new (newKeys++) TKey(std::move(*oldKeys));
                    new (newValues++) TValue(std::move(*oldValues));
                    (oldKeys++)->~TKey();
                    (oldValues++)->~TValue();
                }

                EnsureVacancy(leaf, index, leftCount - 1);

                new (leaf->Keys() + index) TKey(std::forward<TKeyArg>(key));
                new (leaf->Values() + index) TValue(std::forward<TValueArgs>(valueArgs)...);

                newValue = leaf->Values() + index;
            } else {
                // Case #2b: reuse current page with new item on the right
                auto* oldKeys = leaf->Keys() + leftCount;
                auto* oldValues = leaf->Values() + leftCount;

                auto* newKeys = newRight->Keys();
                auto* newValues = newRight->Values();
                size_t rightIndex = index - leftCount;

                Y_DEBUG_ABORT_UNLESS(rightIndex < rightCount);
                for (size_t pos = 0; pos < rightCount; ++pos) {
                    if (pos == rightIndex) {
                        new (newKeys++) TKey(std::forward<TKeyArg>(key));
                        new (newValues++) TValue(std::forward<TValueArgs>(valueArgs)...);
                    } else {
                        new (newKeys++) TKey(std::move(*oldKeys));
                        new (newValues++) TValue(std::move(*oldValues));
                        (oldKeys++)->~TKey();
                        (oldValues++)->~TValue();
                    }
                }

                newValue = newRight->Values() + rightIndex;
            }

            leaf->Count = leftCount;
            leaf->Epoch = CurrentEpoch;
            newRight->Count = rightCount;

            return { leaf, newRight, newRight->Keys(), newValue };
        }

    private:
        struct TInnerInsert {
            TInnerPage* const Left;
            TInnerPage* const Right;
            const TKey* const Middle;
        };

        TInnerInsert PerformInnerInsertWithCopy(
                TInnerPage* inner, size_t index,
                TPage* left, const TKey* middle, TPage* right)
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
                TEdgesReader(TEdge* source, size_t index, size_t count, TPage* left, TPage* right)
                    : Current(source)
                    , Skipped(source + index)
                    , End(source + count)
                    , Replacements{ left, right }
                    , NextReplacement(0)
                {
                    Y_DEBUG_ABORT_UNLESS(Skipped != End);
                }

                TEdge Read() {
                    if (Current == Skipped) {
                        if (NextReplacement < 2) {
                            return Replacements[NextReplacement++];
                        }
                        ++Current;
                    }
                    Y_DEBUG_ABORT_UNLESS(Current != End);
                    return *(Current++);
                }

            private:
                TEdge* Current;
                TEdge* Skipped;
                TEdge* End;
                TEdge Replacements[2];
                size_t NextReplacement;
            };

            const size_t count = inner->Count;
            Y_DEBUG_ABORT_UNLESS(index <= count);
            const size_t newCount = count + 1;

            TKeysReader keysReader(inner->Keys(), index, count, middle);
            TEdgesReader edgesReader(inner->Edges(), index, count + 1, left, right);

            // Case #1: create a new inner page that has new key inserted
            if (newCount <= InnerPageCapacity) {
                auto* newInner = NewInnerPage();
                auto* newKeys = newInner->Keys();
                auto* newEdges = newInner->Edges();

                // Copy over keys
                for (size_t pos = 0; pos < newCount; ++pos) {
                    new (newKeys++) TKey(*keysReader.Read());
                }

                // Copy over edges
                for (size_t pos = 0; pos <= newCount; ++pos) {
                    new (newEdges++) TEdge(edgesReader.Read());
                }

                newInner->Count = newCount;
                PagesToDrop.push_back(inner);

                return { newInner, newInner, nullptr };
            }

            // Case #2: create two new pages and copy items there
            const size_t rightCount = (newCount - 1) / 2; // less keys on the right side
            const size_t leftCount = (newCount - 1) - rightCount; // more keys on the left side
            Y_DEBUG_ABORT_UNLESS(leftCount > 0 && rightCount > 0);

            auto* newLeft = NewInnerPage();
            auto* newRight = NewInnerPage();
            const TKey* newMiddle;

            // Copy over keys, one key will be used as the new middle key
            for (size_t pos = 0; pos < leftCount; ++pos) {
                new (newLeft->Keys() + pos) TKey(*keysReader.Read());
            }
            newMiddle = keysReader.Read();
            for (size_t pos = 0; pos < rightCount; ++pos) {
                new (newRight->Keys() + pos) TKey(*keysReader.Read());
            }

            // Copy over edges, new pages will own them from now on
            for (size_t pos = 0; pos <= leftCount; ++pos) {
                new (newLeft->Edges() + pos) TEdge(edgesReader.Read());
            }
            for (size_t pos = 0; pos <= rightCount; ++pos) {
                new (newRight->Edges() + pos) TEdge(edgesReader.Read());
            }

            newLeft->Count = leftCount;
            newRight->Count = rightCount;
            PagesToDrop.push_back(inner);

            return { newLeft, newRight, newMiddle };
        }

        void EnsureVacancy(TInnerPage* inner, size_t index, size_t count) {
            // Move elements to make a vacancy at index
            if (index < count) {
                auto* newKey = inner->Keys() + index;
                auto* nextKey = inner->Keys() + count;
                while (nextKey != newKey) {
                    auto* prevKey = nextKey - 1;
                    new (nextKey) TKey(std::move(*prevKey));
                    prevKey->~TKey();
                    nextKey = prevKey;
                }

                auto* newEdge = inner->Edges() + index + 1;
                auto* nextEdge = inner->Edges() + count + 1;
                while (nextEdge != newEdge) {
                    auto* prevEdge = nextEdge - 1;
                    new (nextEdge) TEdge(*prevEdge);
                    prevEdge->~TEdge();
                    nextEdge = prevEdge;
                }
            }
        }

        TInnerInsert PerformInnerInsert(
                TInnerPage* inner, size_t index,
                TPage* left, const TKey* middle, TPage* right)
        {
            if (inner->Epoch <= LastSnapshotEpoch) {
                return PerformInnerInsertWithCopy(inner, index, left, middle, right);
            }

            const size_t count = inner->Count;
            Y_DEBUG_ABORT_UNLESS(index <= count);
            const size_t newCount = count + 1;

            // Case #1: reuse current page and insert new key inplace
            if (newCount <= InnerPageCapacity) {
                (inner->Edges() + index)->~TEdge();

                EnsureVacancy(inner, index, count);

                new (inner->Keys() + index) TKey(*middle);
                new (inner->Edges() + index + 0) TEdge(left);
                new (inner->Edges() + index + 1) TEdge(right);

                inner->Count = newCount;
                inner->Epoch = CurrentEpoch;

                return { nullptr, nullptr, nullptr };
            }

            // Case #2: we need to split with some new middle key bubbling up
            const size_t rightCount = (newCount - 1) / 2; // less keys on the right side
            const size_t leftCount = (newCount - 1) - rightCount; // more keys on the left side
            Y_DEBUG_ABORT_UNLESS(leftCount > 0 && rightCount > 0);
            Y_DEBUG_ABORT_UNLESS(leftCount < InnerPageCapacity);

            auto* newRight = NewInnerPage();
            const TKey* newMiddle;

            if (index < leftCount) {
                // Case #2a: reuse current page, with new key on the left
                // N.B. key at index == leftCount - 1 will be the new middle key
                auto* oldKeys = inner->Keys() + leftCount;
                auto* oldEdges = inner->Edges() + leftCount;

                auto* newKeys = newRight->Keys();
                auto* newEdges = newRight->Edges();

                new (newEdges++) TEdge(*oldEdges++);
                for (size_t pos = 0; pos < rightCount; ++pos) {
                    new (newKeys++) TKey(std::move(*oldKeys));
                    new (newEdges++) TEdge(*oldEdges);
                    (oldKeys++)->~TKey();
                    (oldEdges++)->~TEdge();
                }

                (inner->Edges() + index)->~TEdge();

                EnsureVacancy(inner, index, leftCount);

                (inner->Edges() + leftCount + 1)->~TEdge();

                new (inner->Keys() + index) TKey(*middle);
                new (inner->Edges() + index + 0) TEdge(left);
                new (inner->Edges() + index + 1) TEdge(right);

                // New middle key is left on the old page
                KeysToDrop.push_back(inner->Keys() + leftCount);

                newMiddle = inner->Keys() + leftCount;
            } else if (index > leftCount) {
                // Case #2b: reuse current page, with new key on the right
                // N.B. key at index == leftCount will be the new middle key
                auto* oldKeys = inner->Keys() + leftCount + 1;
                auto* oldEdges = inner->Edges() + leftCount + 1;

                auto* newKeys = newRight->Keys();
                auto* newEdges = newRight->Edges();
                size_t rightIndex = index - leftCount - 1;

                Y_DEBUG_ABORT_UNLESS(rightIndex < rightCount);
                for (size_t pos = 0; pos < rightCount; ++pos) {
                    if (pos == rightIndex) {
                        new (newKeys++) TKey(*middle);
                        new (newEdges++) TEdge(left);
                        new (newEdges++) TEdge(right);
                    } else {
                        new (newKeys++) TKey(std::move(*oldKeys));
                        new (newEdges++) TEdge(*oldEdges);
                        (oldKeys++)->~TKey();
                    }
                    (oldEdges++)->~TEdge();
                }

                // New middle key is left on the old page
                KeysToDrop.push_back(inner->Keys() + leftCount);

                newMiddle = inner->Keys() + leftCount;
            } else {
                // Case #2c: reuse current page, with new key bubbling up
                auto* oldKeys = inner->Keys() + leftCount;
                auto* oldEdges = inner->Edges() + leftCount;
                (*oldEdges++) = left;

                auto* newKeys = newRight->Keys();
                auto* newEdges = newRight->Edges();

                new (newEdges++) TEdge(right);
                for (size_t pos = 0; pos < rightCount; ++pos) {
                    new (newKeys++) TKey(std::move(*oldKeys));
                    new (newEdges++) TEdge(*oldEdges);
                    (oldKeys++)->~TKey();
                    (oldEdges++)->~TEdge();
                }

                newMiddle = middle;
            }

            inner->Count = leftCount;
            inner->Epoch = CurrentEpoch;
            newRight->Count = rightCount;

            return { inner, newRight, newMiddle };
        }

    private:
        void DropSnapshotPages(TSnapshotContext* context, bool gc) {
            TSnapshotContext* prev = context->Prev;
            ui64 prevEpoch = prev ? prev->Epoch : 0;

            TPage* lastPage = std::exchange(context->LastDroppedPage, nullptr);
            while (lastPage) {
                TPage* page = lastPage;
                lastPage = std::exchange(page->NextDroppedPage, nullptr);
                if (gc && page->Epoch <= prevEpoch) {
                    prev->AddDroppedPage(page);
                } else {
                    --DroppedPages_;
                    FreePage(page);
                }
            }
        }

        void DropSnapshot(TSnapshotContext* context) {
            DropSnapshotPages(context, /* gc = */ true);

            if (!context->Next) {
                // This is the last snapshot in our linked list
                Y_DEBUG_ABORT_UNLESS(LastSnapshot == context);
                LastSnapshot = context->Prev;
                LastSnapshotEpoch = LastSnapshot ? LastSnapshot->Epoch : 0;
            } else {
                Y_DEBUG_ABORT_UNLESS(LastSnapshot != context);
            }

            context->Unlink();
        }

    private:
        void DropNewerPageRecursive(TPage* page, ui64 epoch) {
            if (page->Epoch <= epoch) {
                return;
            }

            if (page->GetTag() == ETag::Inner) {
                TInnerPage* inner = TInnerPage::From(page);
                size_t count = inner->Count;
                TEdge* edges = inner->Edges();
                for (size_t index = 0; index <= count; ++index) {
                    DropNewerPageRecursive(*edges++, epoch);
                }
            }

            DropPage(page);
        }

        void DropPageRecursive(TPage* page) {
            if (page->GetTag() == ETag::Inner) {
                TInnerPage* inner = TInnerPage::From(page);
                size_t count = inner->Count;
                TEdge* edges = inner->Edges();
                for (size_t index = 0; index <= count; ++index) {
                    DropPageRecursive(*edges++);
                }
            }

            DropPage(page);
        }

        void DropPage(TPage* page) {
            if (page->Epoch <= LastSnapshotEpoch) {
                // This page cannot be freed right now, postpone
                LastSnapshot->AddDroppedPage(page);
                ++DroppedPages_;
            } else {
                FreePage(page);
            }
        }

        void FreePageRecursive(TPage* page) {
            if (page->GetTag() == ETag::Inner) {
                TInnerPage* inner = TInnerPage::From(page);
                size_t count = inner->Count;
                TEdge* edges = inner->Edges();
                for (size_t index = 0; index <= count; ++index) {
                    FreePageRecursive(*edges++);
                }
            }

            FreePage(page);
        }

        void FreePage(TPage* page) {
            switch (page->GetTag()) {
                case ETag::Leaf: {
                    TLeafPage* leaf = TLeafPage::From(page);
                    FreeLeafPage(leaf);
                    break;
                }
                case ETag::Inner: {
                    TInnerPage* inner = TInnerPage::From(page);
                    FreeInnerPage(inner);
                    break;
                }
            }
        }

        void FreeLeafPage(TLeafPage* page) {
            size_t count = page->Count;
            TKey* keys = page->Keys();
            for (size_t index = 0; index < count; ++index) {
                (keys++)->~TKey();
            }
            TValue* values = page->Values();
            for (size_t index = 0; index < count; ++index) {
                (values++)->~TValue();
            }
            TLeafAllocTraits::destroy(LeafAllocator, page);
            TLeafAllocTraits::deallocate(LeafAllocator, page, 1);
            --AllocatedPages_;
        }

        void FreeInnerPage(TInnerPage* page) {
            size_t count = page->Count;
            TKey* keys = page->Keys();
            for (size_t index = 0; index < count; ++index) {
                (keys++)->~TKey();
            }
            TEdge* edges = page->Edges();
            for (size_t index = 0; index <= count; ++index) {
                (edges++)->~TEdge();
            }
            TInnerAllocTraits::destroy(InnerAllocator, page);
            TInnerAllocTraits::deallocate(InnerAllocator, page, 1);
            --AllocatedPages_;
        }

    private:
        TLeafPage* NewLeafPage() {
            auto* page = TLeafAllocTraits::allocate(LeafAllocator, 1);
            TLeafAllocTraits::construct(LeafAllocator, page, CurrentEpoch);
            ++AllocatedPages_;
            return page;
        }

        TInnerPage* NewInnerPage() {
            auto* page = TInnerAllocTraits::allocate(InnerAllocator, 1);
            TInnerAllocTraits::construct(InnerAllocator, page, CurrentEpoch);
            ++AllocatedPages_;
            return page;
        }

    private:
        struct TInsertPath {
            TPage* Page;
            size_t Index;
        };

    private:
        TAllocator Allocator;
        TLeafAllocator LeafAllocator;
        TInnerAllocator InnerAllocator;

        const TIntrusivePtr<TSnapshotGCList> GCList;
        TSnapshotContext* LastSnapshot = nullptr;
        ui64 LastSnapshotEpoch = 0;
        ui64 CurrentEpoch = 1;

        TPage* Root = nullptr;
        size_t Size_ = 0;
        size_t InnerHeight_ = 0;

        mutable std::vector<TInsertPath> InsertPath;
        mutable std::vector<TPage*> PagesToDrop;
        mutable std::vector<TKey*> KeysToDrop;

        size_t AllocatedPages_ = 0;
        size_t DroppedPages_ = 0;
    };

}   // namespace NKikimr
