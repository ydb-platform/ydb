#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/slab_allocator.h>

#include <library/cpp/yt/malloc/malloc.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSlabAllocatorTest, Reallocate)
{
    constexpr int objSize = 8;

    TSlabAllocator allocator;
    EXPECT_FALSE(allocator.IsReallocationNeeded());

    void* ptr = allocator.Allocate(objSize);
    EXPECT_FALSE(allocator.IsReallocationNeeded());

    TSlabAllocator::Free(ptr);
    EXPECT_TRUE(allocator.IsReallocationNeeded());
    ptr = allocator.Allocate(objSize);

    void* ptr0 = allocator.Allocate(objSize);
    EXPECT_FALSE(allocator.IsReallocationNeeded());

    int objectsInSegment = TSlabAllocator::SegmentSize / nallocx(objSize + sizeof(void*), 0);

    std::vector<void*> ptrs;
    // Fill segment.
    for (int i = 0; i < objectsInSegment - 2; ++i) {
        ptrs.push_back(allocator.Allocate(objSize));
    }

    EXPECT_FALSE(allocator.IsReallocationNeeded());
    void* ptr1 = allocator.Allocate(objSize);

    TSlabAllocator::Free(ptr);
    TSlabAllocator::Free(ptr0);
    TSlabAllocator::Free(ptr1);

    EXPECT_TRUE(allocator.IsReallocationNeeded());

    allocator.ReallocateArenasIfNeeded();

    for (auto& ptr : ptrs) {
        TSlabAllocator::Free(ptr);
        ptr = allocator.Allocate(objSize);
    }

    EXPECT_FALSE(allocator.IsReallocationNeeded());

    for (auto* ptr : ptrs) {
        TSlabAllocator::Free(ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
