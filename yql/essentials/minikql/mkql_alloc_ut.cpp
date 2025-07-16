#include "mkql_alloc.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLAllocTest) {
    Y_UNIT_TEST(TestPagedArena) {
        TAlignedPagePool pagePool(__LOCATION__);

        {
            TPagedArena arena(&pagePool);
            auto p1 = arena.Alloc(10);
            auto p2 = arena.Alloc(20);
            auto p3 = arena.Alloc(100000);
            auto p4 = arena.Alloc(30);
            arena.Clear();
            auto p5 = arena.Alloc(40);
            Y_UNUSED(p1);
            Y_UNUSED(p2);
            Y_UNUSED(p3);
            Y_UNUSED(p4);
            Y_UNUSED(p5);

            TPagedArena arena2 = std::move(arena);
            auto p6 = arena2.Alloc(50);
            Y_UNUSED(p6);
        }
    }

    Y_UNIT_TEST(TestDeallocated) {
        TScopedAlloc alloc(__LOCATION__);
#if defined(_asan_enabled_)
        constexpr size_t EXTRA_ALLOCATION_SPACE = NYql::NUdf::SANITIZER_EXTRA_ALLOCATION_SPACE;
#else  // defined(_asan_enabled_)
        constexpr size_t EXTRA_ALLOCATION_SPACE = 0;
#endif // defined(_asan_enabled_)

        void* p1 = TWithDefaultMiniKQLAlloc::AllocWithSize(10);
        void* p2 = TWithDefaultMiniKQLAlloc::AllocWithSize(20);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 0);
        TWithDefaultMiniKQLAlloc::FreeWithSize(p1, 10);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 10 + EXTRA_ALLOCATION_SPACE);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 0);
        TWithDefaultMiniKQLAlloc::FreeWithSize(p2, 20);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 1);
        p1 = TWithDefaultMiniKQLAlloc::AllocWithSize(10);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 0);
        TWithDefaultMiniKQLAlloc::FreeWithSize(p1, 10);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 1);
    }

    Y_UNIT_TEST(FreeInWrongAllocator) {
        if (true) {
            return;
        }
        TScopedAlloc alloc1(__LOCATION__);
        void* p1 = TWithDefaultMiniKQLAlloc::AllocWithSize(10);
        void* p2 = TWithDefaultMiniKQLAlloc::AllocWithSize(10);
        {
            TScopedAlloc alloc2(__LOCATION__);
            TWithDefaultMiniKQLAlloc::FreeWithSize(p1, 10);
        }
        TWithDefaultMiniKQLAlloc::FreeWithSize(p2, 10);
    }
    Y_UNIT_TEST(InitiallyAcquired) {
        {
            TScopedAlloc alloc(__LOCATION__);
            UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsAttached());
            {
                auto guard = Guard(alloc);
                UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsAttached());
            }
            UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsAttached());
        }
        {
            TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), false, false);
            UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsAttached());
            {
                auto guard = Guard(alloc);
                UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsAttached());
            }
            UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsAttached());
        }
    }
#if !defined(_asan_enabled_)
    Y_UNIT_TEST(ArrowAllocateZeroSize) {
        // Choose small enough pieces to hit arena (using some internal knowledge)
        const auto pieceSize = AlignUp<size_t>(1, ArrowAlignment);
        UNIT_ASSERT_EQUAL(0, (TAllocState::POOL_PAGE_SIZE - sizeof(TMkqlArrowHeader)) % pieceSize);
        const auto pieceCount = (TAllocState::POOL_PAGE_SIZE - sizeof(TMkqlArrowHeader)) / pieceSize;
        void** ptrs = new void*[pieceCount];

        // Populate the current page on arena to maximum offset
        TScopedAlloc alloc(__LOCATION__);
        for (auto i = 0ul; i < pieceCount; ++i) {
            ptrs[i] = MKQLArrowAllocate(pieceSize);
        }

        // Check all pieces are on the same page
        void* pageStart = TAllocState::GetPageStart(ptrs[0]);
        for (auto i = 1ul; i < pieceCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(pageStart, TAllocState::GetPageStart(ptrs[i]));
        }

        // Allocate zero-sized piece twice and check it's the same address
        void* ptrZero1 = MKQLArrowAllocate(0);
        void* ptrZero2 = MKQLArrowAllocate(0);
        UNIT_ASSERT_VALUES_EQUAL(ptrZero1, ptrZero2);

        // Allocate one more small piece and check that it's on another page - different from zero-sized piece
        void* ptrOne = MKQLArrowAllocate(1);
        UNIT_ASSERT_VALUES_UNEQUAL(pageStart, TAllocState::GetPageStart(ptrOne));
        UNIT_ASSERT_VALUES_UNEQUAL(TAllocState::GetPageStart(ptrZero1), TAllocState::GetPageStart(ptrOne));

        // Untrack zero-sized piece
        MKQLArrowUntrack(ptrZero1);

        // Deallocate all the stuff
        for (auto i = 0ul; i < pieceCount; ++i) {
            MKQLArrowFree(ptrs[i], pieceSize);
        }
        MKQLArrowFree(ptrZero1, 0);
        MKQLArrowFree(ptrZero2, 0);
        MKQLArrowFree(ptrOne, 1);

        delete[] ptrs;
    }
#endif
}

} // namespace NKikimr::NMiniKQL
