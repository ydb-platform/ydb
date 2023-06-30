#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/maybe_inf.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMaybeInf, IncreaseBy)
{
    using TLimit = TMaybeInf<ui32>;
    TLimit a(1);
    EXPECT_FALSE(a.CanBeIncreased(TLimit::Infinity()));
}

TEST(TMaybeInf, DecreaseBy)
{
    using TLimit = TMaybeInf<ui32>;
    TLimit a(1);
    auto b = TLimit::Infinity();
    EXPECT_FALSE(a.CanBeDecreased(b));
    a = TLimit::Infinity();
    EXPECT_FALSE(a.CanBeDecreased(b));
    b = TLimit();
    EXPECT_TRUE(a.CanBeDecreased(b));
    b = TLimit(1);
    EXPECT_FALSE(a.CanBeDecreased(b));
    a = TLimit(4);
    EXPECT_TRUE(a.CanBeDecreased(b));
    b = TLimit(4);
    EXPECT_TRUE(a.CanBeDecreased(b));
    a.DecreaseBy(b);
    EXPECT_EQ(a.ToUnderlying(), 0u);
    a = TLimit(4);
    b = TLimit(5);
    EXPECT_FALSE(a.CanBeDecreased(b));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
