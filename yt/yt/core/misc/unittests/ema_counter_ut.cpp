#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/ema_counter.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TEmaCounterTest, Simple)
{
    const auto min = TDuration::Minutes(1);

    TEmaCounter<i64> counter({min});

    EXPECT_EQ(std::nullopt, counter.LastTimestamp);
    EXPECT_EQ(std::nullopt, counter.StartTimestamp);
    EXPECT_EQ(0, counter.Count);
    EXPECT_EQ(0.0, counter.ImmediateRate);
    EXPECT_EQ(0.0, counter.WindowRates[0]);

    counter.Update(10, TInstant::Zero());

    EXPECT_EQ(TInstant::Zero(), counter.LastTimestamp);
    EXPECT_EQ(TInstant::Zero(), counter.StartTimestamp);
    EXPECT_EQ(10, counter.Count);
    // Still no information about rates.
    EXPECT_EQ(0.0, counter.ImmediateRate);
    EXPECT_EQ(0.0, counter.WindowRates[0]);

    counter.Update(20, TInstant::Zero() + min);

    EXPECT_EQ(TInstant::Zero() + min, counter.LastTimestamp);
    EXPECT_EQ(TInstant::Zero(), counter.StartTimestamp);
    EXPECT_EQ(20, counter.Count);
    EXPECT_DOUBLE_EQ(10.0 / 60.0, counter.ImmediateRate);
    // New rate should be considered with weight 1 - e^{-2}, new one with 1/e^{-2}.
    EXPECT_DOUBLE_EQ(10.0 / 60.0 * (1 - std::exp(-2)) + 0.0 * std::exp(-2), counter.WindowRates[0]);
}

TEST(TEmaCounterTest, MockTime)
{
    const auto sec = TDuration::Seconds(1), min = TDuration::Minutes(1);

    TEmaCounter<i64> counter({min});

    int obsoleteRate = 1;
    int actualRate = 10;

    // Set up some history.

    i64 currentCount = 0;
    TInstant currentTimestamp = TInstant::Zero();

    for (int index = 0; index < 300; ++index, currentTimestamp += sec) {
        currentCount += obsoleteRate;
        counter.Update(currentCount, currentTimestamp);

        if (index < 60) {
            EXPECT_FALSE(counter.GetRate(0, TInstant::Zero() + index * sec));
        }
    }

    EXPECT_DOUBLE_EQ(1.0, counter.ImmediateRate);
    // Result should be almost 1 (recall that the initial rate value of 0
    // is remembered by EMA for some time).
    EXPECT_NEAR(1.0, counter.WindowRates[0], 1e-3);
    ASSERT_TRUE(counter.GetRate(0, currentTimestamp));
    EXPECT_NEAR(1.0, *counter.GetRate(0, currentTimestamp - sec), 1e-3);
    EXPECT_NEAR(1e-3, *counter.GetRate(0, currentTimestamp + 5 * min), 1e-3); // log2(1e-3) ~ -10. Window is doubled half-decay period.

    for (int index = 300; index < 360; ++index, currentTimestamp += sec) {
        currentCount += actualRate;
        counter.Update(currentCount, currentTimestamp);
    }

    EXPECT_DOUBLE_EQ(10.0, counter.ImmediateRate);
    // Actual value would be 8.78, which is quite close to 10.0.
    EXPECT_NEAR(10.0, counter.WindowRates[0], 2.0);
    EXPECT_TRUE(counter.GetRate(0, currentTimestamp));

    for (int index = 360; index < 420; ++index, currentTimestamp += sec) {
        currentCount += actualRate;
        counter.Update(currentCount, currentTimestamp);
    }

    EXPECT_DOUBLE_EQ(10.0, counter.ImmediateRate);
    // Actual value would be 9.83, which is notably close to 10.0.
    EXPECT_NEAR(10.0, counter.WindowRates[0], 0.2);
    EXPECT_TRUE(counter.GetRate(0, currentTimestamp));
}

TEST(TEmaCounterTest, RealTime)
{
    const auto quant = TDuration::MilliSeconds(10), sec = TDuration::Seconds(1);

    TEmaCounter<i64> counter({sec});

    const int valueCount = 200;
    std::mt19937 generator(/*seed*/ 42);
    const int maxValue = 200'000;
    std::uniform_int_distribution<int> valueDistribution(0, maxValue);
    std::vector<i64> values;
    values.reserve(valueCount);
    for (int index = 0; index < valueCount; ++index) {
        values.push_back(valueDistribution(generator));
    }
    std::sort(values.begin(), values.end());

    auto start = TInstant::Now();

    for (int index = 0; index < valueCount; ++index) {
        counter.Update(values[index]);
        Sleep(quant);
        if (TInstant::Now() - start < sec * 0.9) {
            EXPECT_FALSE(counter.GetRate(0));
        }
    }

    auto end = TInstant::Now();

    auto testDuration = end - start;
    auto expectedRate = counter.Count / (testDuration).SecondsFloat();

    const double relativeTolerance = 0.2;

    Cerr << "Test duration = " << testDuration << " sec" << Endl;
    Cerr << "Expected rate = " << expectedRate << Endl;
    Cerr << "Window rate = " << counter.WindowRates[0] << Endl;
    Cerr << "Relative error = " << counter.WindowRates[0] / expectedRate - 1.0 << Endl;

    EXPECT_NEAR(1, counter.WindowRates[0] / expectedRate, relativeTolerance);
    EXPECT_TRUE(counter.GetRate(0));
}

TEST(TEmaCounterTest, Merge)
{
    const auto min = TDuration::Minutes(1);
    TEmaCounter<i64> base({min});
    TEmaCounter<i64> delta({min});

    auto startTimestamp = TInstant::Now();

    delta.Count = 1;
    delta.ImmediateRate = 1.0;
    delta.WindowRates[0] = 1.0;
    delta.StartTimestamp = startTimestamp;
    delta.LastTimestamp = startTimestamp;

    base.Merge(delta, startTimestamp + 0.5 * min);

    EXPECT_EQ(1, base.Count);
    EXPECT_EQ(0.0, base.ImmediateRate);
    EXPECT_EQ(base.LastTimestamp, startTimestamp + 0.5 * min);
    EXPECT_EQ(base.StartTimestamp, startTimestamp + 0.5 * min);
    EXPECT_NEAR(0.368, base.WindowRates[0], 1e-3);

    delta.Count = 2;
    delta.WindowRates[0] = 2.0;

    base.Merge(delta);

    EXPECT_EQ(3, base.Count);
    EXPECT_EQ(0.0, base.ImmediateRate);
    EXPECT_EQ(base.LastTimestamp, startTimestamp + 0.5 * min);
    EXPECT_EQ(base.StartTimestamp, startTimestamp + 0.5 * min);
    EXPECT_NEAR(0.968, base.WindowRates[0], 1e-3);

    delta.LastTimestamp = startTimestamp + 1.5 * min;
    base.Merge(delta);

    EXPECT_EQ(3, base.Count);
    EXPECT_EQ(0.0, base.ImmediateRate);
    EXPECT_EQ(base.StartTimestamp, startTimestamp + 0.5 * min);
    EXPECT_EQ(base.LastTimestamp, startTimestamp + 0.5 * min);
    EXPECT_NEAR(0.968, base.WindowRates[0], 1e-3);
}

TEST(TEmaCounterTest, SameTimestampUpdatesMatchOneBatch)
{
    TEmaCounter<i64> incremental({TDuration::Seconds(30), TDuration::Minutes(5)});
    auto batched = incremental;
    auto now = TInstant::Seconds(1000);
    incremental.Update(0, now);
    batched.Update(0, now);
    i64 count = 0;
    for (int batch = 1; batch <= 100; ++batch) {
        now += TDuration::Seconds(batch % 7 + 1);
        for (int row = 0; row < batch; ++row) {
            incremental.Update(++count, now);
        }
        batched.Update(count, now);
        EXPECT_EQ(incremental.Count, batched.Count);
        EXPECT_NEAR(incremental.ImmediateRate, batched.ImmediateRate, 1e-10);
        for (int window = 0; window < std::ssize(incremental.WindowDurations); ++window) {
            EXPECT_NEAR(incremental.WindowRates[window], batched.WindowRates[window], 1e-10);
        }
    }
    // The same increments must not leak into the next empty interval.
    now += TDuration::Seconds(30);
    incremental.Update(count, now);
    batched.Update(count, now);
    EXPECT_EQ(incremental.ImmediateRate, 0);
    ASSERT_TRUE(incremental.GetRate(0, now));
    ASSERT_TRUE(batched.GetRate(0, now));
    EXPECT_NEAR(*incremental.GetRate(0, now), *batched.GetRate(0, now), 1e-10);
}

TEST(TEmaCounterTest, SameTimestampAtInitializationAndObsoleteUpdates)
{
    auto start = TInstant::Seconds(1000);
    TEmaCounter<i64> counter({TDuration::Seconds(30)});
    counter.Update(10, start);
    counter.Update(20, start);
    EXPECT_EQ(counter.Count, 20);
    EXPECT_EQ(counter.ImmediateRate, 0);
    EXPECT_EQ(counter.WindowRates[0], 0);

    counter.Update(50, start + TDuration::Seconds(30));
    EXPECT_EQ(counter.ImmediateRate, 1);
    auto rate = counter.WindowRates[0];

    counter.Update(500, start);
    counter.Update(50, start + TDuration::Seconds(30));
    EXPECT_EQ(counter.Count, 50);
    EXPECT_EQ(counter.WindowRates[0], rate);
}

TEST(TEmaCounterTest, SameTimestampFloatingPointAndCounterReset)
{
    auto start = TInstant::Seconds(1000);
    TEmaCounter<double> counter({TDuration::Seconds(30)});
    counter.Update(10, start);
    counter.Update(0, start + TDuration::Seconds(30));
    EXPECT_EQ(counter.ImmediateRate, 0);
    counter.Update(0.5, start + TDuration::Seconds(30));
    EXPECT_NEAR(counter.ImmediateRate, 0.5 / 30, 1e-10);
    counter.Update(1.5, start + TDuration::Seconds(60));
    EXPECT_NEAR(counter.ImmediateRate, 1.0 / 30, 1e-10);
}

TEST(TEmaCounterTest, SameTimestampAfterMerging)
{
    for (bool useMerge : {false, true}) {
        SCOPED_TRACE(useMerge);
        auto start = TInstant::Seconds(1000);
        TEmaCounter<i64> counter({TDuration::Seconds(30), TDuration::Minutes(5)});
        auto other = counter;
        counter.Update(0, start);
        other.Update(0, start);
        counter.Update(30, start + TDuration::Seconds(30));
        other.Update(15, start + TDuration::Seconds(15));

        if (useMerge) {
            counter.Merge(other, start + TDuration::Seconds(30));
        } else {
            counter += other;
        }
        auto immediateRate = counter.ImmediateRate;
        auto windowRates = counter.WindowRates;

        counter.Update(55, start + TDuration::Seconds(30));
        EXPECT_EQ(counter.Count, 55);
        EXPECT_EQ(counter.ImmediateRate, immediateRate);
        EXPECT_EQ(counter.WindowRates, windowRates);

        counter.Update(75, start + TDuration::Seconds(60));
        EXPECT_EQ(counter.ImmediateRate, 20.0 / 30);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
