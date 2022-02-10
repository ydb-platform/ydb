#include "mkql_alloc.h" 
 
#include <library/cpp/testing/unittest/registar.h>
 
namespace NKikimr { 
namespace NMiniKQL { 
 
Y_UNIT_TEST_SUITE(TMiniKQLAllocTest) {
    Y_UNIT_TEST(TestPagedArena) {
        TAlignedPagePool pagePool;
 
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
        TScopedAlloc alloc; 
        void* p1 = MKQLAllocWithSize(10); 
        void* p2 = MKQLAllocWithSize(20); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 0); 
        MKQLFreeWithSize(p1, 10); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 10); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 0); 
        MKQLFreeWithSize(p2, 20); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 1); 
        p1 = MKQLAllocWithSize(10); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), TAlignedPagePool::POOL_PAGE_SIZE); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 0); 
        MKQLFreeWithSize(p1, 10); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetUsed(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetDeallocatedInPages(), 0); 
        UNIT_ASSERT_VALUES_EQUAL(alloc.Ref().GetFreePageCount(), 1); 
    } 
} 
 
} 
} 
