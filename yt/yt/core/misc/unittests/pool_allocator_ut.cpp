#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/pool_allocator.h>

namespace NYT {
namespace {

using ::testing::TProbe;
using ::testing::TProbeState;

////////////////////////////////////////////////////////////////////////////////

TEST(TPoolAllocator, Alignment16)
{
    struct TTag
    { };
    TPoolAllocator allocator(48, 16, 64_KB, GetRefCountedTypeCookie<TTag>());

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(0ULL,  reinterpret_cast<uintptr_t>(allocator.Allocate()) % 16);
    }
}

TEST(TPoolAllocator, Reuse)
{
    struct TTag
    { };
    TPoolAllocator allocator(256, 1, 64_KB, GetRefCountedTypeCookie<TTag>());

    THashSet<void*> ptrs;

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(ptrs.insert(allocator.Allocate()).second);
    }

    for (auto* ptr : ptrs) {
        TPoolAllocator::Free(ptr);
    }

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(ptrs.contains(allocator.Allocate()));
    }

    EXPECT_FALSE(ptrs.contains(allocator.Allocate()));
}

TEST(TPoolAllocator, Object)
{
    TProbeState state;

    class TObject
        : public TPoolAllocator::TObjectBase
        , public TProbe
    {
    public:
        explicit TObject(TProbeState* state)
            : TProbe(state)
        { }
    };

    auto obj = TPoolAllocator::New<TObject>(&state);
    EXPECT_EQ(1, state.Constructors);

    obj.reset();
    EXPECT_EQ(1, state.Destructors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
