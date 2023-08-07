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
        HistoricUsageAggregator_.Reset();
        HistoricUsageAggregator_.UpdateParameters(THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::None
        ));
    }

    void InitializeEmaMode(double emaAlpha)
    {
        HistoricUsageAggregator_.Reset();
        HistoricUsageAggregator_.UpdateParameters(THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::ExponentialMovingAverage,
            emaAlpha
        ));
    }

protected:
    THistoricUsageAggregator HistoricUsageAggregator_;
};

TEST_F(THistoricUsageAggregatorTest, TestNoneMode)
{
    InitializeNoneMode();

    TInstant now = TInstant::Zero();
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);
}

TEST_F(THistoricUsageAggregatorTest, TestExponentialMovingAverageModeUpdateAt)
{
    InitializeEmaMode(0.1);

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), 0.0);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    auto currentHistoricUsage = 1.0 - Exp2(-0.1);
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), currentHistoricUsage);

    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    currentHistoricUsage = Exp2(-0.1) * currentHistoricUsage + (1.0 - Exp2(-0.1));
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), currentHistoricUsage);
}

TEST_F(THistoricUsageAggregatorTest, TestExponentialMovingAverageModeUpdateParametersAndResetIfNecessary)
{
    InitializeEmaMode(0.1);

    // Aggregate some usage.
    auto now = TInstant::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    now += TDuration::Seconds(1);
    HistoricUsageAggregator_.UpdateAt(now, 1.0);
    auto currentHistoricUsage = 1.0 - Exp2(-0.1);

    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), currentHistoricUsage);

    // No reset if parameters haven't changed.
    HistoricUsageAggregator_.UpdateParameters(THistoricUsageAggregationParameters(
        EHistoricUsageAggregationMode::ExponentialMovingAverage,
        0.1
    ));
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), currentHistoricUsage);

    // Reset if parameters have changed.
    HistoricUsageAggregator_.UpdateParameters(THistoricUsageAggregationParameters(
        EHistoricUsageAggregationMode::ExponentialMovingAverage,
        0.05
    ));
    currentHistoricUsage = 0.0;
    ASSERT_DOUBLE_EQ(HistoricUsageAggregator_.GetHistoricUsage(), currentHistoricUsage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
