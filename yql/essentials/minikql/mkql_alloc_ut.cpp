#include "mkql_alloc.h"

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/memory_pool.h>

#include <util/generic/size_literals.h>

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
        TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), /*supportsSizedAllocators=*/false, /*initiallyAcquired=*/false);
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
    for (auto i = 0UL; i < pieceCount; ++i) {
        ptrs[i] = MKQLArrowAllocate(pieceSize);
    }

    // Check all pieces are on the same page
    void* pageStart = TAllocState::GetPageStart(ptrs[0]);
    for (auto i = 1UL; i < pieceCount; ++i) {
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
    for (auto i = 0UL; i < pieceCount; ++i) {
        MKQLArrowFree(ptrs[i], pieceSize);
    }
    MKQLArrowFree(ptrZero1, 0);
    MKQLArrowFree(ptrZero2, 0);
    MKQLArrowFree(ptrOne, 1);

    delete[] ptrs;
}
#endif

Y_UNIT_TEST(ArrowAllocateWithDefaultArrowAllocator) {
    TScopedAlloc alloc(__LOCATION__);
    UseDefaultArrowAllocator();

    constexpr ui64 size = 1_KB;
    auto* ptr = MKQLArrowAllocate(size);
    UNIT_ASSERT(ptr);

    MKQLArrowFree(ptr, size);
}

Y_UNIT_TEST(ArrowAllocateWithStateArrowMemoryPool) {
    class TCountingPool: public arrow::MemoryPool {
    public:
        arrow::Status Allocate(int64_t size, uint8_t** out) override {
            Allocations++;
            Bytes += size;
            return arrow::default_memory_pool()->Allocate(size, out);
        }

        arrow::Status Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) override {
            Bytes += newSize - oldSize;
            return arrow::default_memory_pool()->Reallocate(oldSize, newSize, ptr);
        }

        void Free(uint8_t* buffer, int64_t size) override {
            Frees++;
            Bytes -= size;
            arrow::default_memory_pool()->Free(buffer, size);
        }

        int64_t bytes_allocated() const override {
            return Bytes;
        }

        std::string backend_name() const override {
            return "counting";
        }

        int64_t Allocations = 0;
        int64_t Frees = 0;
        int64_t Bytes = 0;
    };

    UseDefaultArrowAllocator();
    TCountingPool pool;

    constexpr ui64 size = 1_KB;
    void* ptr = nullptr;
    {
        TScopedAlloc alloc(__LOCATION__);
        alloc.Ref().ArrowMemoryPool = &pool;
        // as the compute actors do: the buffer outlives the state, which must not free it
        alloc.Ref().EnableArrowTracking = false;
        ptr = MKQLArrowAllocate(size);
        UNIT_ASSERT(ptr);
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocations, 1);
        UNIT_ASSERT(pool.Bytes >= static_cast<int64_t>(size));

        // the state without a pool uses the default one
        TScopedAlloc other(__LOCATION__);
        auto* otherPtr = MKQLArrowAllocate(size);
        UNIT_ASSERT_VALUES_EQUAL(pool.Allocations, 1);
        MKQLArrowFree(otherPtr, size);
        UNIT_ASSERT_VALUES_EQUAL(pool.Frees, 0);
    }

    // freed under another state: back to the pool that allocated it
    TScopedAlloc alloc(__LOCATION__);
    MKQLArrowFree(ptr, size);
    UNIT_ASSERT_VALUES_EQUAL(pool.Frees, 1);
    UNIT_ASSERT_VALUES_EQUAL(pool.Bytes, 0);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLAllocTest)

} // namespace NKikimr::NMiniKQL
