#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/maybe_inf.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMaybeInfTest, Increase)
{
    using TLimit = TMaybeInf<ui32>;
    TLimit a(1);
    EXPECT_FALSE(a.CanIncrease(TLimit::Infinity()));
    EXPECT_TRUE(a.CanIncrease(TLimit(1)));
    a.Increase(TLimit(1).ToUnderlying());
    EXPECT_EQ(a.ToUnderlying(), ui32(2));
    EXPECT_TRUE(a.IsFinite());
}

TEST(TMaybeInfTest, DecreaseBy)
{
    using TLimit = TMaybeInf<ui32>;
    TLimit a(1);
    auto b = TLimit::Infinity();
    EXPECT_FALSE(a.CanDecrease(b));
    a = TLimit::Infinity();
    EXPECT_FALSE(a.CanDecrease(b));
    b = TLimit();
    EXPECT_TRUE(a.CanDecrease(b));
    b = TLimit(1);
    EXPECT_FALSE(a.CanDecrease(b));
    a = TLimit(4);
    EXPECT_TRUE(a.CanDecrease(b));
    b = TLimit(4);
    EXPECT_TRUE(a.CanDecrease(b));
    a.Decrease(b.ToUnderlying());
    EXPECT_EQ(a.ToUnderlying(), 0u);
    a = TLimit(4);
    b = TLimit(5);
    EXPECT_FALSE(a.CanDecrease(b));
}

TEST(TMaybeInfTest, ChangeInfinityByZero)
{
    auto check = [] <class T> () {
        auto limit = TMaybeInf<T>::Infinity();
        EXPECT_TRUE(limit.CanIncrease(TMaybeInf<T>(0)));
        limit.Increase(0);
        EXPECT_TRUE(limit.IsInfinite());

        EXPECT_TRUE(limit.CanDecrease(TMaybeInf<T>(0)));
        limit.Decrease(0);
        EXPECT_TRUE(limit.IsInfinite());

        EXPECT_FALSE(limit.CanIncrease(TMaybeInf<T>(1)));
        EXPECT_FALSE(limit.CanDecrease(TMaybeInf<T>(1)));
        EXPECT_FALSE(limit.CanIncrease(TMaybeInf<T>::Infinity()));
    };
    check.operator()<ui32>();
    check.operator()<ui64>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
