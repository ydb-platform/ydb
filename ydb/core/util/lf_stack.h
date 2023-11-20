#pragma once

#include <util/system/yassert.h>

#include <atomic>

namespace NKikimr {

    template<class T>
    class TLockFreeIntrusiveStackWithRefCount;

    template<class T>
    class TLockFreeIntrusiveStackItemWithRefCount {
        friend class TLockFreeIntrusiveStackWithRefCount<T>;

    public:
        TLockFreeIntrusiveStackItemWithRefCount() = default;

    private:
        std::atomic<uintptr_t> RefCount{ 0 };
        std::atomic<TLockFreeIntrusiveStackItemWithRefCount*> Next{ (TLockFreeIntrusiveStackItemWithRefCount*)-1 };
    };

    /**
     * Based on ideas from http://moodycamel.com/blog/2014/solving-the-aba-problem-for-lock-free-free-lists
     */
    template<class T>
    class TLockFreeIntrusiveStackWithRefCount {
        using TItem = TLockFreeIntrusiveStackItemWithRefCount<T>;

    public:
        TLockFreeIntrusiveStackWithRefCount() = default;

        void Push(T* x) {
            TItem* item = static_cast<TItem*>(x);

            // First we mark item as possibly shared on the list
            TItem* marker = item->Next.exchange(nullptr, std::memory_order_relaxed);
            Y_ABORT_UNLESS(marker == (TItem*)-1, "Pushing item that is not marked exclusive");

            // The item we push is guaranteed to have flag not set
            // N.B. release here synchronizes with fast path acquire during refcount decrement
            auto count = item->RefCount.fetch_add(FlagAddingToStack, std::memory_order_release);
            Y_DEBUG_ABORT_UNLESS((count & FlagAddingToStack) == 0, "Flag invariant failure during push");

            if (count == 0) {
                DoPush(item);
            }
        }

        T* Pop() {
            return static_cast<T*>(DoPop());
        }

    private:
        void DoPush(TItem* item) {
            auto* head = Head.load(std::memory_order_relaxed);
            while (true) {
                // No other thread may read or write Next right now
                item->Next.store(head, std::memory_order_relaxed);

                // We are guaranteed to have RefCount == FlagAddingToStack
                // No other thread may change RefCount until this store succeeds
                // N.B. release here synchronizes the store to Next above with refcount increment
                auto count = item->RefCount.exchange(RefCountInc, std::memory_order_release);
                Y_DEBUG_ABORT_UNLESS(count == FlagAddingToStack, "Flag invariant failure during push");

                // N.B. release here synchronizes item state with acquire on successful Pop
                if (Head.compare_exchange_strong(head, item, std::memory_order_release)) {
                    // New item is successfully published
                    return;
                }

                // Attempt to restart push, taking back RefCount we added above
                // N.B. release here synchronizes with fast path acquire during refcount decrement
                count = item->RefCount.fetch_add(FlagAddingToStack - RefCountInc, std::memory_order_release);
                Y_DEBUG_ABORT_UNLESS(count >= RefCountInc, "RefCount underflow");
                Y_DEBUG_ABORT_UNLESS((count & FlagAddingToStack) == 0, "Flag invariant failure during push");

                if (count != RefCountInc) {
                    // Some other thread is currently holding a reference
                    return;
                }
            }
        }

        TItem* DoPop() {
            auto* head = Head.load(std::memory_order_acquire);
            while (head) {
                auto* item = head;

                // N.B. acquire in refcount increment synchronizes the Next field
                auto count = item->RefCount.load(std::memory_order_relaxed);
                if ((count >> RefCountShift) == 0 ||
                    !item->RefCount.compare_exchange_strong(count, count + RefCountInc, std::memory_order_acquire, std::memory_order_relaxed))
                {
                    // Either RefCount is currently zero, which means it is
                    // not on the stack, or we failed to increment RefCount
                    // and need to try again loading a possibly new head.
                    // N.B. loading using cas to avoid seeing stale values
                    // for long periods of time (e.g. with relacy verifier)
                    Head.compare_exchange_weak(head, head, std::memory_order_acquire);
                    continue;
                }

                // N.B. success acquire in exchange synchronizes item state (successful pop)
                // N.B. failure acquire in exchange synchronizes the new head state
                auto* next = item->Next.load(std::memory_order_relaxed);
                if (Head.compare_exchange_strong(head, next, std::memory_order_acquire)) {
                    // We have successfully removed item, now we need to
                    // remove 2 previously added references, one from us
                    // and one from the stack itself.
                    count = item->RefCount.fetch_add(-RefCountInc2, std::memory_order_relaxed);
                    Y_DEBUG_ABORT_UNLESS(count >= RefCountInc2, "RefCount underflow");
                    Y_DEBUG_ABORT_UNLESS((count & FlagAddingToStack) == 0, "Flag invariant failure during pop");
                    // We are the owner, should be safe to change the Next pointer however we want
                    TItem* marker = item->Next.exchange((TItem*)-1, std::memory_order_relaxed);
                    Y_ABORT_UNLESS(marker != (TItem*)-1, "Popped item that is already marked exclusive");
                    return item;
                }

                // We failed to remove item and now we may have a new head,
                // however we need to remove our extra reference first,
                // since item may have already been taken by another thread
                // N.B. acquire in refcount decrement synchronizes with flag addition during push
                count = item->RefCount.fetch_add(-RefCountInc, std::memory_order_acquire);
                if (count == RefCountInc + FlagAddingToStack) {
                    // RefCount has dropped to zero and responsibility to
                    // add item back on the stack is now on us. However, as
                    // we want to remove some item we may as well take it
                    // for ourselves right now. We know that other threads
                    // may not change neither RefCount nor Next until
                    // RefCount becomes above zero again.
                    count = item->RefCount.exchange(0, std::memory_order_relaxed);
                    Y_DEBUG_ABORT_UNLESS(count == FlagAddingToStack, "Flag invariant failure during pop");
                    // We are the owner, should be safe to change the Next pointer however we want
                    TItem* marker = item->Next.exchange((TItem*)-1, std::memory_order_relaxed);
                    Y_ABORT_UNLESS(marker != (TItem*)-1, "Popped item that is already marked exclusive");
                    return item;
                }
            }

            return nullptr;
        }

    private:
        enum : int {
            RefCountShift = 1,
        };

        enum : uintptr_t {
            FlagAddingToStack = uintptr_t(1),
            RefCountInc = uintptr_t(1) << RefCountShift,
            RefCountInc2 = RefCountInc * 2,
        };

    private:
        std::atomic<TItem*> Head{ nullptr };
    };

    namespace NDetail {
        struct TLockFreePointerTag {
            void* Pointer;
            uintptr_t Tag;
        };
    }

    template<class T>
    class TLockFreeIntrusiveStackWithDCAS;

    template<class T>
    class TLockFreeIntrusiveStackItemWithDCAS {
        friend class TLockFreeIntrusiveStackWithDCAS<T>;

    public:
        TLockFreeIntrusiveStackItemWithDCAS() = default;

    private:
        std::atomic<TLockFreeIntrusiveStackItemWithDCAS*> Next{ nullptr };
    };

    template<class T>
    class TLockFreeIntrusiveStackWithDCAS {
        using TItem = TLockFreeIntrusiveStackItemWithDCAS<T>;
        using TLink = NDetail::TLockFreePointerTag;

    public:
        TLockFreeIntrusiveStackWithDCAS() = default;

        void Push(T* x) {
            DoPush(static_cast<TItem*>(x));
        }

        T* Pop() {
            return static_cast<T*>(DoPop());
        }

    private:
        void DoPush(TItem* item) {
            TLink head = Head.load(std::memory_order_relaxed);
            while (true) {
                item->Next.store(reinterpret_cast<TItem*>(head.Pointer), std::memory_order_relaxed);
                TLink newHead = { item, head.Tag + 1 };
                if (Head.compare_exchange_weak(head, newHead, std::memory_order_release)) {
                    break;
                }
            }
        }

        TItem* DoPop() {
            TLink head = Head.load(std::memory_order_acquire);
            while (head.Pointer) {
                auto* item = reinterpret_cast<TItem*>(head.Pointer);
                TLink newHead = { item->Next.load(std::memory_order_relaxed), head.Tag + 1 };
                if (Head.compare_exchange_weak(head, newHead, std::memory_order_acquire)) {
                    return item;
                }
            }

            return nullptr;
        }

    private:
        std::atomic<TLink> Head{ { nullptr, 0 } };
    };

    namespace NDetail {
        /**
         * Automatically selects DCAS when it is truly lock-free.
         */
        template<bool UseDCAS = std::atomic<TLockFreePointerTag>::is_always_lock_free>
        struct TSelectLockFreeIntrusiveStack {
            template<class T>
            using TItem = TLockFreeIntrusiveStackItemWithDCAS<T>;

            template<class T>
            using TStack = TLockFreeIntrusiveStackWithDCAS<T>;
        };

        template<>
        struct TSelectLockFreeIntrusiveStack<false> {
            template<class T>
            using TItem = TLockFreeIntrusiveStackItemWithRefCount<T>;

            template<class T>
            using TStack = TLockFreeIntrusiveStackWithRefCount<T>;
        };
    }

    template<class T>
    using TLockFreeIntrusiveStackItem = typename NDetail::TSelectLockFreeIntrusiveStack<>::TItem<T>;

    template<class T>
    using TLockFreeIntrusiveStack = typename NDetail::TSelectLockFreeIntrusiveStack<>::TStack<T>;

}
