#include "mkql_alloc.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <array>
#include <thread>

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
    const auto freePages = alloc.Ref().GetFreePageCount();
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
    TWithDefaultMiniKQLAlloc::FreeWithSize(p1, 10);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 10 + EXTRA_ALLOCATION_SPACE);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), freePages);
    TWithDefaultMiniKQLAlloc::FreeWithSize(p2, 20);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), freePages + 1);
    p1 = TWithDefaultMiniKQLAlloc::AllocWithSize(10);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), freePages);
    TWithDefaultMiniKQLAlloc::FreeWithSize(p1, 10);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0);
    UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), freePages + 1);
}

Y_UNIT_TEST(LargeAllocationsRespectMemoryLimit) {
    constexpr std::array<size_t, 3> sizes = {MaxPageUserData + 1, 1_MB, 1_MB + 1};
    for (const size_t size : sizes) {
        TScopedAlloc alloc(__LOCATION__);
        const size_t allocatedSize = NUdf::GetSizeToAlloc(size) + sizeof(TAllocState::TListEntry);
        alloc.SetLimit(allocatedSize);

        void* memory = TWithDefaultMiniKQLAlloc::AllocWithSize(size);
        UNIT_ASSERT(memory);
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetUsed(), allocatedSize);
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), allocatedSize);

        TWithDefaultMiniKQLAlloc::FreeWithSize(memory, size);
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetUsed(), 0);
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), 0);
    }
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
        TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), /*initiallyAcquired=*/false);
        UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsAttached());
        {
            auto guard = Guard(alloc);
            UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsAttached());
        }
        UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsAttached());
    }
}

Y_UNIT_TEST(ZeroSizeAllocationAtPageEnd) {
    TScopedAlloc alloc(__LOCATION__);
    for (const auto memoryPool : {EMemorySubPool::Default, EMemorySubPool::Temporary}) {
        void* first = MKQLAllocFastWithSize(1, &alloc.Ref(), memoryPool);
        auto* page = static_cast<TAllocPageHeader*>(TAllocState::GetPageStart(first));
        const size_t fillerSize = page->Capacity - page->Offset - NYql::NUdf::SANITIZER_EXTRA_ALLOCATION_SPACE - 1;
        void* filler = MKQLAllocFastWithSize(fillerSize, &alloc.Ref(), memoryPool);
        UNIT_ASSERT_VALUES_EQUAL(page->Offset, page->Capacity);

        void* empty = MKQLAllocFastWithSize(0, &alloc.Ref(), memoryPool);
        UNIT_ASSERT_VALUES_EQUAL(TAllocState::GetPageStart(empty), alloc.Ref().CurrentPages[static_cast<TMemorySubPoolIdx>(memoryPool)]);
        MKQLFreeFastWithSize(empty, 0, &alloc.Ref(), memoryPool);
        MKQLFreeFastWithSize(filler, fillerSize, &alloc.Ref(), memoryPool);
        MKQLFreeFastWithSize(first, 1, &alloc.Ref(), memoryPool);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0);
    }
}

Y_UNIT_TEST(ZeroSizeAllocationOnEmptyPool) {
    TScopedAlloc alloc(__LOCATION__);
    for (const auto memoryPool : {EMemorySubPool::Default, EMemorySubPool::Temporary}) {
        void* empty = MKQLAllocFastWithSize(0, &alloc.Ref(), memoryPool);
        MKQLFreeFastWithSize(empty, 0, &alloc.Ref(), memoryPool);
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0);
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

Y_UNIT_TEST(ArrowFreeWithWrongSizeIsDetected) {
    TScopedAlloc alloc(__LOCATION__);

    constexpr ui64 smallSize = 1_KB;
    void* onArena = MKQLArrowAllocate(smallSize);

    UNIT_ASSERT_EXCEPTION(MKQLArrowFree(onArena, 32_KB), yexception);

    MKQLArrowFree(onArena, smallSize);
}

Y_UNIT_TEST(ArrowArenaPageOutlivesAllocator) {
    if (TAllocState::IsDefaultArrowAllocatorUsed()) {
        return;
    }

    constexpr ui64 smallSize = 1_KB;
    constexpr ui64 largeSize = 32_KB; // > ArrowSizeForArena

    void* escaped = nullptr;
    {
        TScopedAlloc alloc(__LOCATION__);
        alloc.DisableStrictAllocationCheck(); // the page held by the escaped buffer is reported as lost memory
        escaped = MKQLArrowAllocate(smallSize);
    }

    const void* escapedPage = TAllocState::GetPageStart(escaped);

    {
        TScopedAlloc alloc(__LOCATION__);
        void* large = MKQLArrowAllocate(largeSize);
        UNIT_ASSERT_VALUES_UNEQUAL(escapedPage, TAllocState::GetPageStart(large));

        void* small = MKQLArrowAllocate(smallSize);
        UNIT_ASSERT_VALUES_UNEQUAL(escapedPage, TAllocState::GetPageStart(small));

        MKQLArrowFree(large, largeSize);
        MKQLArrowFree(small, smallSize);
    }

    MKQLArrowFree(escaped, smallSize);
}

Y_UNIT_TEST(ArrowArenaPageIsReleasedOnce) {
    if (TAllocState::IsDefaultArrowAllocatorUsed()) {
        return;
    }

    constexpr ui64 size = 1_KB;
    constexpr ui64 pageSize = TAllocState::POOL_PAGE_SIZE;

    void* escaped = nullptr;
    ui64 arenaPool = 0;
    i64 allPools = 0;
    {
        TScopedAlloc alloc(__LOCATION__);
        alloc.DisableStrictAllocationCheck(); // the page held by the escaped buffer is reported as lost memory
        escaped = MKQLArrowAllocate(size);
        arenaPool = TAlignedPagePool::GetGlobalPagePoolSize();
        allPools = GetTotalFreeListBytes();
    }

    UNIT_ASSERT_VALUES_EQUAL(TAlignedPagePool::GetGlobalPagePoolSize(), arenaPool);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalFreeListBytes(), allPools);

    MKQLArrowFree(escaped, size);

    UNIT_ASSERT_VALUES_EQUAL(TAlignedPagePool::GetGlobalPagePoolSize(), arenaPool + pageSize);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalFreeListBytes(), allPools + static_cast<i64>(pageSize));
}

Y_UNIT_TEST(ArrowArenaBuffersAreFreedByForeignThread) {
    if (TAllocState::IsDefaultArrowAllocatorUsed()) {
        return;
    }

    constexpr ui64 size = 64;
    constexpr size_t count = 100;

    TScopedAlloc alloc(__LOCATION__);

    std::array<void*, count> escaped;
    for (auto& ptr : escaped) {
        ptr = MKQLArrowAllocate(size);
    }
    const void* page = TAllocState::GetPageStart(escaped.front());
    MKQLArrowUntrack(escaped.front());

    // The owner keeps allocating on the same page while its buffers are being freed.
    std::thread foreign([&escaped] {
        for (void* ptr : escaped) {
            MKQLArrowFree(ptr, size);
        }
    });

    std::array<void*, count> own;
    for (auto& ptr : own) {
        ptr = MKQLArrowAllocate(size);
    }
    foreign.join();

    for (void* ptr : escaped) {
        UNIT_ASSERT_VALUES_EQUAL(page, TAllocState::GetPageStart(ptr));
    }
    for (void* ptr : own) {
        UNIT_ASSERT_VALUES_EQUAL(page, TAllocState::GetPageStart(ptr));
        MKQLArrowFree(ptr, size);
    }
}

Y_UNIT_TEST(ArrowArenaPageIsReleasedByForeignThread) {
    if (TAllocState::IsDefaultArrowAllocatorUsed()) {
        return;
    }

    constexpr ui64 size = 1_KB;

    TScopedAlloc alloc(__LOCATION__);

    void* escaped = MKQLArrowAllocate(size);
    const void* page = TAllocState::GetPageStart(escaped);
    MKQLArrowUntrack(escaped);

    // Exhaust the page, so the escaped buffer holds the last reference to it.
    TVector<void*> own;
    void* next = MKQLArrowAllocate(size);
    while (TAllocState::GetPageStart(next) == page) {
        own.push_back(next);
        next = MKQLArrowAllocate(size);
    }
    for (void* ptr : own) {
        MKQLArrowFree(ptr, size);
    }

    const auto arenaPool = TAlignedPagePool::GetGlobalPagePoolSize();

    std::thread foreign([escaped] {
        MKQLArrowFree(escaped, size);
    });
    foreign.join();

    UNIT_ASSERT_VALUES_EQUAL(TAlignedPagePool::GetGlobalPagePoolSize(), arenaPool + TAllocState::POOL_PAGE_SIZE);

    MKQLArrowFree(next, size);
}

Y_UNIT_TEST(ArrowAllocateWithDefaultArrowAllocator) {
    TScopedAlloc alloc(__LOCATION__);
    UseDefaultArrowAllocator();

    constexpr ui64 size = 1_KB;
    auto* ptr = MKQLArrowAllocate(size);
    UNIT_ASSERT(ptr);

    MKQLArrowFree(ptr, size);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLAllocTest)

} // namespace NKikimr::NMiniKQL
