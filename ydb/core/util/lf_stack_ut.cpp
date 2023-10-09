#include "lf_stack.h"

#include "ut_common.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TLockFreeIntrusiveStackTest) {

    template<template<class> class TItemBase, template<class> class TStack>
    void DoConcurrent(const size_t itemsCount, const size_t workersCount) {
        static constexpr size_t Iterations = 100000;

        struct TMyItem : public TItemBase<TMyItem> {
            size_t Count = 0; // this count is not lock-free
        };

        TVector<TMyItem> rawItems(itemsCount);
        TStack<TMyItem> stack;
        for (auto& item : rawItems) {
            stack.Push(&item);
        }
        std::atomic<size_t> totalCount{ 0 };

        TVector<THolder<TWorkerThread>> workers(workersCount);
        for (size_t i = 0; i < workersCount; ++i) {
            workers[i] = TWorkerThread::Spawn([&]() {
                THPTimer timer;
                while (timer.Passed() < 5.0) {
                    size_t added = 0;
                    for (size_t i = 0; i < Iterations; ++i) {
                        if (TMyItem* item = stack.Pop()) {
                            item->Count++;
                            stack.Push(item);
                            ++added;
                        } else {
                            Y_ABORT_UNLESS(itemsCount < workersCount, "Unexpected nullptr from stack.Pop()");
                        }
                    }
                    totalCount.fetch_add(added, std::memory_order_relaxed);
                }
            });
        }

        for (size_t i = 0; i < workersCount; ++i) {
            workers[i]->Join();
        }

        size_t checkCount = 0;
        for (auto& item : rawItems) {
            checkCount += item.Count;
        }

        UNIT_ASSERT_VALUES_EQUAL(checkCount, totalCount.load());
    }

    Y_UNIT_TEST(ConcurrentRefCountNeverEmpty) {
        DoConcurrent<TLockFreeIntrusiveStackItemWithRefCount, TLockFreeIntrusiveStackWithRefCount>(4, 4);
    }

    Y_UNIT_TEST(ConcurrentRefCountHeavyContention) {
        DoConcurrent<TLockFreeIntrusiveStackItemWithRefCount, TLockFreeIntrusiveStackWithRefCount>(4, 8);
    }

#if 0
    // Currently causes link errors with arcadia clang
    Y_UNIT_TEST(ConcurrentDCASNeverEmpty) {
        DoConcurrent<TLockFreeIntrusiveStackItemWithDCAS, TLockFreeIntrusiveStackWithDCAS>(4, 4);
    }

    // Currently causes link errors with arcadia clang
    Y_UNIT_TEST(ConcurrentDCASHeavyContention) {
        DoConcurrent<TLockFreeIntrusiveStackItemWithDCAS, TLockFreeIntrusiveStackWithDCAS>(4, 8);
    }
#endif

    Y_UNIT_TEST(ConcurrentAutoNeverEmpty) {
        DoConcurrent<TLockFreeIntrusiveStackItem, TLockFreeIntrusiveStack>(4, 4);
    }

    Y_UNIT_TEST(ConcurrentAutoHeavyContention) {
        DoConcurrent<TLockFreeIntrusiveStackItem, TLockFreeIntrusiveStack>(4, 8);
    }
}

}
