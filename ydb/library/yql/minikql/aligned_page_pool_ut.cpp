#include "aligned_page_pool.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/info.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TAlignedPagePoolTest) {

Y_UNIT_TEST(AlignedMmapPageSize) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeAlignedMmap> alloc(__LOCATION__);

    int munmaps = 0;
    TFakeAlignedMmap::OnMunmap = [&](void* addr, size_t s) {
        Y_UNUSED(addr);
        Y_UNUSED(s);
        munmaps ++;
    };

    auto size = TAlignedPagePool::POOL_PAGE_SIZE;
    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    TFakeAlignedMmap::OnMunmap = {};
    UNIT_ASSERT_EQUAL(0, munmaps);

    UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(block.get()), TAlignedPagePool::POOL_PAGE_SIZE);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount()
        , TAlignedPagePool::ALLOC_AHEAD_PAGES);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated()
        , TAlignedPagePool::POOL_PAGE_SIZE + TAlignedPagePool::ALLOC_AHEAD_PAGES*TAlignedPagePool::POOL_PAGE_SIZE
    );
}

Y_UNIT_TEST(UnalignedMmapPageSize) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeUnalignedMmap> alloc(__LOCATION__);

    int munmaps = 0;
    TFakeUnalignedMmap::OnMunmap = [&](void* addr, size_t s) {
        Y_UNUSED(addr);
        if (munmaps == 0) {
            UNIT_ASSERT_VALUES_EQUAL(s, TAlignedPagePool::POOL_PAGE_SIZE - 1);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(s, 1);
        }
        munmaps ++;
    };

    auto size = TAlignedPagePool::POOL_PAGE_SIZE;
    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    TFakeUnalignedMmap::OnMunmap = {};
    UNIT_ASSERT_EQUAL(2, munmaps);

    UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(block.get()), 2 * TAlignedPagePool::POOL_PAGE_SIZE);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount()
        , TAlignedPagePool::ALLOC_AHEAD_PAGES - 1);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated()
        , TAlignedPagePool::POOL_PAGE_SIZE + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 1) * TAlignedPagePool::POOL_PAGE_SIZE
    );
}

Y_UNIT_TEST(AlignedMmapUnalignedSize) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeAlignedMmap> alloc(__LOCATION__);
    auto smallSize = NSystemInfo::GetPageSize();
    auto size = smallSize + 1024 * TAlignedPagePool::POOL_PAGE_SIZE;

    int munmaps = 0;
    TFakeAlignedMmap::OnMunmap = [&](void* addr, size_t s) {
        if (munmaps == 0) {
            UNIT_ASSERT_VALUES_EQUAL(s, TAlignedPagePool::POOL_PAGE_SIZE - smallSize);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(addr), TAlignedPagePool::POOL_PAGE_SIZE + size);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(s, smallSize);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(addr), TAlignedPagePool::POOL_PAGE_SIZE + TAlignedPagePool::ALLOC_AHEAD_PAGES * TAlignedPagePool::POOL_PAGE_SIZE + size - smallSize);
        }

        munmaps ++;
    };

    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    TFakeAlignedMmap::OnMunmap = {};

    UNIT_ASSERT_EQUAL(2, munmaps);

    UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(block.get()), TAlignedPagePool::POOL_PAGE_SIZE);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount()
        , TAlignedPagePool::ALLOC_AHEAD_PAGES - 1);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated()
        , size + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 1) * TAlignedPagePool::POOL_PAGE_SIZE
    );
}

Y_UNIT_TEST(UnalignedMmapUnalignedSize) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeUnalignedMmap> alloc(__LOCATION__);
    auto smallSize = NSystemInfo::GetPageSize();
    auto size = smallSize + 1024 * TAlignedPagePool::POOL_PAGE_SIZE;
    int munmaps = 0;
    TFakeUnalignedMmap::OnMunmap = [&](void* addr, size_t s) {
        Y_UNUSED(addr);
        if (munmaps == 0) {
            UNIT_ASSERT_VALUES_EQUAL(s, TAlignedPagePool::POOL_PAGE_SIZE - 1);
        } else if (munmaps == 1) {
            UNIT_ASSERT_VALUES_EQUAL(s, TAlignedPagePool::POOL_PAGE_SIZE - smallSize);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(s, smallSize + 1);
        }
        munmaps ++;
    };

    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    TFakeUnalignedMmap::OnMunmap = {};
    UNIT_ASSERT_EQUAL(3, munmaps);

    UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(block.get()), 2 * TAlignedPagePool::POOL_PAGE_SIZE);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount()
        , TAlignedPagePool::ALLOC_AHEAD_PAGES - 2);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated()
        , size + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 2) * TAlignedPagePool::POOL_PAGE_SIZE
    );
}

Y_UNIT_TEST(YellowZoneSwitchesCorrectly) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl alloc(__LOCATION__);
    auto FreeWithAlloc = [&](size_t size) {
        alloc.OffloadFree(size);
        // Yellow zone is only updated during allocs
        alloc.OffloadAlloc(0);
    };

    alloc.SetLimit(100);

    UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());

    // Total allocated: 80
    alloc.OffloadAlloc(80);
    UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsMemoryYellowZoneEnabled());

    // Total allocated: 70
    FreeWithAlloc(10);
    UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsMemoryYellowZoneEnabled());

    // Total allocated: 50
    FreeWithAlloc(20);
    UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());

    FreeWithAlloc(50);
}

Y_UNIT_TEST(YellowZoneZeroDivision) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl alloc(__LOCATION__);

    alloc.SetLimit(0);

    UNIT_ASSERT_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());
}

} // Y_UNIT_TEST_SUITE(TAlignedPagePoolTest)

} // namespace NMiniKQL
} // namespace NKikimr
