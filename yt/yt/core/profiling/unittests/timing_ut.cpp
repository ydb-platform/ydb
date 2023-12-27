#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/profiling/tscp.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T>
i64 DiffMS(T a, T b)
{
    return a >= b
        ? static_cast<i64>(a.MilliSeconds()) - static_cast<i64>(b.MilliSeconds())
        : DiffMS(b, a);
}

TEST(TTimingTest, GetInstant)
{
    EXPECT_LE(DiffMS(GetInstant(), TInstant::Now()), 10);
}

TEST(TTimingTest, InstantVSCpuInstant)
{
    auto instant1 = TInstant::Now();
    auto cpuInstant = InstantToCpuInstant(instant1);
    auto instant2 = CpuInstantToInstant(cpuInstant);
    EXPECT_LE(DiffMS(instant1, instant2), 10);
}

TEST(TTimingTest, DurationVSCpuDuration)
{
    auto cpuInstant1 = GetCpuInstant();
    constexpr auto duration1 = TDuration::MilliSeconds(100);
    Sleep(duration1);
    auto cpuInstant2 = GetCpuInstant();
    auto duration2 = CpuDurationToDuration(cpuInstant2 - cpuInstant1);
    EXPECT_LE(DiffMS(duration1, duration2), 10);
}

TEST(TTimingTest, TTscp_Get)
{
    auto cpuInstant = GetCpuInstant();
    auto tscp = TTscp::Get();
    EXPECT_GE(tscp.Instant, cpuInstant);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
