#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>

#include <util/generic/ymath.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TAdjustedExponentialMovingAverageTest
    : public testing::Test
{
public:
    void InitializeEma(TDuration halflife)
    {
        AdjustedEma_ = TAdjustedExponentialMovingAverage(halflife);
    }

protected:
    TAdjustedExponentialMovingAverage AdjustedEma_;
};

TEST_F(TAdjustedExponentialMovingAverageTest, ZeroHalflife)
{
    InitializeEma(TDuration::Zero());

    TInstant now = TInstant::Zero();
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 0.0);

    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    AdjustedEma_.UpdateAt(now, 2.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 2.0);

    now += TDuration::Days(365);
    AdjustedEma_.UpdateAt(now, 3.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 3.0);
}

TEST_F(TAdjustedExponentialMovingAverageTest, ConstantDataSet)
{
    InitializeEma(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 0.0);

    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    // Spread should be irrelevant.
    now += TDuration::Days(10000000);
    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);
}

TEST_F(TAdjustedExponentialMovingAverageTest, NonConstantSet)
{
    InitializeEma(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 0.0);

    AdjustedEma_.UpdateAt(now, 1.0);
    double expectedUsageNumerator = 1.0;
    double expectedUsageDenominator = 1.0;
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), expectedUsageNumerator / expectedUsageDenominator);

    auto testUsage = [&] (TDuration delay, double value) {
        now += delay;
        AdjustedEma_.UpdateAt(now, value);
        double multiplier = Exp2(-delay.SecondsFloat() / 10.0);
        expectedUsageDenominator = 1.0 + expectedUsageDenominator * multiplier;
        expectedUsageNumerator = value + expectedUsageNumerator * multiplier;
        ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), expectedUsageNumerator / expectedUsageDenominator);
    };

    testUsage(TDuration::Seconds(1), 5.0);
    testUsage(TDuration::Seconds(1), 1.0);
    testUsage(TDuration::Days(10000000), 42.0);
}

TEST_F(TAdjustedExponentialMovingAverageTest, SetHalflife)
{
    InitializeEma(TDuration::Seconds(10));
    auto oldHalflife = TDuration::Seconds(10);
    auto newHalflife = TDuration::Seconds(1);

    auto now = TInstant::Seconds(1);

    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 0.0);
    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    AdjustedEma_.SetHalflife(oldHalflife);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    AdjustedEma_.SetHalflife(newHalflife);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 0.0);

    now += TDuration::Seconds(1);
    AdjustedEma_.UpdateAt(now, 1.0);
    auto expectedUsage = 1.0;
    now += TDuration::Seconds(0.1);
    AdjustedEma_.UpdateAt(now, 10);
    expectedUsage = (Exp2(-0.1) * expectedUsage + 10.0) / (Exp2(-0.1) + 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), expectedUsage);

    AdjustedEma_.SetHalflife(newHalflife, /*resetOnNewHalflife*/ false);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), expectedUsage);
}

TEST_F(TAdjustedExponentialMovingAverageTest, EstimateAverageWithNewValue)
{
    InitializeEma(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    AdjustedEma_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);

    auto expectedUsage = (1.0 * Exp2(-0.1 * 15.0) + 42.0) / (Exp2(-0.1 * 15.0) + 1.0);
    auto simulatedUsage = AdjustedEma_.EstimateAverageWithNewValue(now + TDuration::Seconds(15), 42.0);
    ASSERT_DOUBLE_EQ(simulatedUsage, expectedUsage);
    ASSERT_DOUBLE_EQ(AdjustedEma_.GetAverage(), 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
