#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/historic_usage_aggregator.h>

#include <util/generic/ymath.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class THistoricUsageAggregatorTest
    : public testing::Test
{
public:
    void InitializeNoneMode()
    {
        auto params = THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::None
        );
        HistoricUsageAggregator_ = THistoricUsageAggregator(params);
    }

    void InitializeEmaMode(double emaAlpha, bool persistent = false)
    {
        auto params = THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::ExponentialMovingAverage,
            emaAlpha,
            !persistent);
        HistoricUsageAggregator_ = THistoricUsageAggregator(params);
    }

protected:
    THistoricUsageAggregator HistoricUsageAggregator_;
};

TEST_F(THistoricUsageAggregatorTest, NoneModeJustWorks)
{
    InitializeNoneMode();

    TInstant now = TInstant::Zero();
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 2.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 2.0);
}

TEST_F(THistoricUsageAggregatorTest, EmaModeZeroAlpha)
{
    InitializeEmaMode(0.0);

    TInstant now = TInstant::Zero();
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 2.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 2.0);
}

TEST_F(THistoricUsageAggregatorTest, EmaModeConstantDataSet)
{
    InitializeEmaMode(0.1);

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    // Spread should be irrelevant.
    now += TDuration::Days(10000000);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);
}

TEST_F(THistoricUsageAggregatorTest, EmaModeNonConstantSet)
{
    InitializeEmaMode(0.1);

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    double expectedUsage = 1.0;
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), expectedUsage);

    auto testUsage = [&] (TDuration delay, double value) {
        now += delay;
        HistoricUsageAggregator_.UpdateAt(now, value);
        expectedUsage = expectedUsage * Exp2(-0.1 * delay.SecondsFloat()) + (1 - Exp2(-0.1 * delay.SecondsFloat())) * value;
        ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), expectedUsage);
    };

    testUsage(TDuration::Seconds(1), 5.0);
    testUsage(TDuration::Seconds(1), 1.0);
    testUsage(TDuration::Days(10000000), 42.0);
}

TEST_F(THistoricUsageAggregatorTest, EmaModeUpdateParameters)
{
    InitializeEmaMode(0.1);
    auto oldParameters = THistoricUsageAggregationParameters(
        EHistoricUsageAggregationMode::ExponentialMovingAverage,
        0.1);
    auto newParameters = THistoricUsageAggregationParameters(
        EHistoricUsageAggregationMode::ExponentialMovingAverage,
        15);
    auto finalParameters = THistoricUsageAggregationParameters(
        EHistoricUsageAggregationMode::ExponentialMovingAverage,
        15,
        false);

    auto now = TInstant::Seconds(1);

    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    HistoricUsageAggregator_.UpdateParameters(oldParameters);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    HistoricUsageAggregator_.UpdateParameters(newParameters);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    auto expectedUsage = 1.0;
    now += TDuration::Seconds(0.1);
    HistoricUsageAggregator_.UpdateAt(now, 10);
    expectedUsage = Exp2(-1.5) * expectedUsage + (1 - Exp2(-1.5)) * 10;
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), expectedUsage);

    HistoricUsageAggregator_.UpdateParameters(finalParameters);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), expectedUsage);

    HistoricUsageAggregator_.UpdateParameters(newParameters);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);
}

TEST_F(THistoricUsageAggregatorTest, NodeModeSimulateUpdate)
{
    InitializeNoneMode();

    auto now = TInstant::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    auto expectedUsage = 42.0;
    auto simulatedUsage = HistoricUsageAggregator_.SimulateUpdate(now + TDuration::Seconds(15), 42.0);
    ASSERT_DOUBLE_EQ(simulatedUsage, expectedUsage);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);
}

TEST_F(THistoricUsageAggregatorTest, EmaModeSimulateUpdate)
{
    InitializeEmaMode(0.1);

    auto now = TInstant::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);

    auto expectedUsage = 1.0 * Exp2(-0.1 * 15) + (1 - Exp2(-0.1 * 15)) * 42.0;
    auto simulatedUsage = HistoricUsageAggregator_.SimulateUpdate(now + TDuration::Seconds(15), 42.0);
    ASSERT_DOUBLE_EQ(simulatedUsage, expectedUsage);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
