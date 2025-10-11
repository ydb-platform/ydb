#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicYielderTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_W(TPeriodicYielderTest, NoYieldWhenPeriodNotReached)
{
    auto yielder = CreatePeriodicYielder(TDuration::Seconds(10));

    // Do minimal work, not enough to exceed period.
    volatile i64 sum = 0;
    for (int i = 0; i < 100; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    EXPECT_FALSE(yielder.NeedYield());
    EXPECT_FALSE(yielder.TryYield());
}

TEST_W(TPeriodicYielderTest, YieldWhenPeriodExceeded)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(100));

    // Simulate some CPU work to exceed the period.
    volatile i64 sum = 0;
    for (int i = 0; i < 10'000'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    EXPECT_TRUE(yielder.NeedYield());
    EXPECT_TRUE(yielder.TryYield());

    // After yield, timer should have reset.
    EXPECT_FALSE(yielder.NeedYield());
}

TEST_W(TPeriodicYielderTest, NoPeriodNeverYields)
{
    auto yielder = CreatePeriodicYielder(std::nullopt);

    volatile i64 sum = 0;
    for (int i = 0; i < 10'000'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    EXPECT_FALSE(yielder.NeedYield());
    EXPECT_FALSE(yielder.TryYield());
}

TEST_W(TPeriodicYielderTest, ZeroPeriodAlwaysYields)
{
    auto yielder = CreatePeriodicYielder(TDuration::Zero());

    EXPECT_TRUE(yielder.NeedYield());
    EXPECT_TRUE(yielder.TryYield());
}

TEST_W(TPeriodicYielderTest, MultipleYieldsInLoop)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(100));

    int yieldCount = 0;
    for (int i = 0; i < 100; ++i) {
        // Do some work
        volatile i64 sum = 0;
        for (int j = 0; j < 100'000; ++j) {
            sum += j;
            DoNotOptimizeAway(sum);
        }

        if (yielder.TryYield()) {
            ++yieldCount;
        }
    }

    EXPECT_GT(yieldCount, 1);
}

TEST_W(TPeriodicYielderTest, DifferentPeriods)
{
    auto shortPeriod = CreatePeriodicYielder(TDuration::MicroSeconds(1));
    auto longPeriod = CreatePeriodicYielder(TDuration::Seconds(10));

    volatile i64 sum = 0;
    for (int i = 0; i < 1'000'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    // Short period should need yield, long period should not.
    EXPECT_TRUE(shortPeriod.NeedYield());
    EXPECT_FALSE(longPeriod.NeedYield());
}

TEST_W(TPeriodicYielderTest, ElapsedTimeTracking)
{
    auto yielder = CreatePeriodicYielder(TDuration::Seconds(1));

    auto startTime = yielder.GetElapsedTime();

    volatile i64 sum = 0;
    for (int i = 0; i < 100'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    auto endTime = yielder.GetElapsedTime();

    EXPECT_GT(endTime, startTime);
}

TEST_W(TPeriodicYielderTest, CpuTimeTracking)
{
    auto yielder = CreatePeriodicYielder(TDuration::MilliSeconds(10));

    auto startCpu = yielder.GetElapsedCpuTime();

    volatile i64 sum = 0;
    for (int i = 0; i < 100'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    auto endCpu = yielder.GetElapsedCpuTime();

    EXPECT_GT(endCpu, startCpu);
}

TEST_W(TPeriodicYielderTest, NestedYielders)
{
    auto outerYielder = CreatePeriodicYielder(TDuration::MicroSeconds(500));

    for (int i = 0; i < 5; ++i) {
        auto innerYielder = CreatePeriodicYielder(TDuration::MicroSeconds(100));

        volatile i64 sum = 0;
        for (int j = 0; j < 1'000'000; ++j) {
            sum += j;
            DoNotOptimizeAway(sum);
        }

        // Inner yielder should need to yield.
        EXPECT_TRUE(innerYielder.NeedYield());
    }

    // Outer yielder should also need to yield after all iterations.
    EXPECT_TRUE(outerYielder.NeedYield());
}

TEST_W(TPeriodicYielderTest, StressTestManyIterations)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(10));

    int iterations = 0;
    int yields = 0;

    auto startTime = TInstant::Now();
    while (TInstant::Now() - startTime < TDuration::MilliSeconds(100)) {
        ++iterations;

        // Minimal work per iteration.
        volatile i64 x = iterations * 2;
        DoNotOptimizeAway(x);

        if (yielder.TryYield()) {
            ++yields;
        }
    }

    // Should have done many iterations and some yields.
    EXPECT_GT(iterations, 100);
    EXPECT_GT(yields, 0);
}

TEST_W(TPeriodicYielderTest, ConsistentNeedYieldResults)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(100));

    // Do work to exceed period.
    volatile i64 sum = 0;
    for (int i = 0; i < 10'000'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    // Multiple calls to NeedYield should return the same result.
    EXPECT_TRUE(yielder.NeedYield());
    EXPECT_TRUE(yielder.NeedYield());
    EXPECT_TRUE(yielder.NeedYield());

    // After yield, should consistently return false.
    yielder.TryYield();
    EXPECT_FALSE(yielder.NeedYield());
    EXPECT_FALSE(yielder.NeedYield());
}

TEST_W(TPeriodicYielderTest, MultipleYieldCycles)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(10));

    for (int cycle = 0; cycle < 5; ++cycle) {
        // Do work to exceed period.
        volatile i64 sum = 0;
        for (int i = 0; i < 1'000'000; ++i) {
            sum += i;
            DoNotOptimizeAway(sum);
        }

        EXPECT_TRUE(yielder.NeedYield());
        EXPECT_TRUE(yielder.TryYield());
        EXPECT_FALSE(yielder.NeedYield());
    }
}

TEST_W(TPeriodicYielderTest, ContextSwitchResetsTimer)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(100));

    // Do work to exceed period.
    volatile i64 sum = 0;
    for (int i = 0; i < 10'000'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    EXPECT_TRUE(yielder.NeedYield());

    // Manually trigger context switch.
    Yield();

    EXPECT_FALSE(yielder.NeedYield());
    EXPECT_FALSE(yielder.TryYield());
}

TEST_W(TPeriodicYielderTest, NoYieldWhenContextSwitchForbidden)
{
    auto yielder = CreatePeriodicYielder(TDuration::MicroSeconds(100));

    // Simulate some CPU work to exceed the period.
    volatile i64 sum = 0;
    for (int i = 0; i < 10'000'000; ++i) {
        sum += i;
        DoNotOptimizeAway(sum);
    }

    TForbidContextSwitchGuard guard;

    EXPECT_TRUE(yielder.NeedYield());
    EXPECT_FALSE(yielder.TryYield());
    EXPECT_TRUE(yielder.NeedYield());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
