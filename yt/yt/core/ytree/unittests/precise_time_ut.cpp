#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/precise_time.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NYTree {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TPreciseTime, Arithmetic)
{
    TPreciseInstant now = TInstant::Seconds(1771588960);
    TPreciseInstant other = now + TDuration::Seconds(10);
    EXPECT_EQ(other.Seconds(), 1771588970ull);
    other += TDuration::Seconds(15);
    EXPECT_EQ(other.Seconds(), 1771588985ull);
    TPreciseDuration delta = other - now;
    EXPECT_EQ(delta, TDuration::Seconds(25));

    other -= delta;
    EXPECT_EQ(now, other);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
