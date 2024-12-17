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
    EXPECT_TRUE(counter.GetRate(0, currentTimestamp));

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
