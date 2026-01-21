#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/duration_moving_average.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TDurationMovingAverageTest
    : public testing::Test
{
public:
    void InitializeMovingAverage(TDuration window)
    {
        MovingAverage_ = TDurationMovingAverage(window);
    }

protected:
    TDurationMovingAverage MovingAverage_;
};

TEST_F(TDurationMovingAverageTest, ConstantDataSet)
{
    InitializeMovingAverage(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 0.0);

    MovingAverage_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);

    // All values within window should be averaged.
    now += TDuration::Seconds(5);
    MovingAverage_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);
}

TEST_F(TDurationMovingAverageTest, NonConstantSet)
{
    InitializeMovingAverage(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 0.0);

    MovingAverage_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 5.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 3.0); // (1.0 + 5.0) / 2

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 3.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 3.0); // (1.0 + 5.0 + 3.0) / 3
}

TEST_F(TDurationMovingAverageTest, OldValuesExpire)
{
    InitializeMovingAverage(TDuration::Seconds(5));

    auto now = TInstant::Seconds(1);
    MovingAverage_.UpdateAt(now, 10.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 10.0);

    now += TDuration::Seconds(2);
    MovingAverage_.UpdateAt(now, 20.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0); // (10.0 + 20.0) / 2

    // Move past the window - first value should expire
    now += TDuration::Seconds(4);
    MovingAverage_.UpdateAt(now, 30.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 25.0); // (20.0 + 30.0) / 2

    // Move past the window again - second value should expire
    now += TDuration::Seconds(3);
    MovingAverage_.UpdateAt(now, 40.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 35.0); // (30.0 + 40.0) / 2
}

TEST_F(TDurationMovingAverageTest, AllValuesExpire)
{
    InitializeMovingAverage(TDuration::Seconds(5));

    auto now = TInstant::Seconds(1);
    MovingAverage_.UpdateAt(now, 10.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 10.0);

    now += TDuration::Seconds(2);
    MovingAverage_.UpdateAt(now, 20.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0);

    // Move far past the window - all values should expire
    now += TDuration::Seconds(100);
    MovingAverage_.UpdateAt(now, 30.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 30.0);
}

TEST_F(TDurationMovingAverageTest, SetWindow)
{
    InitializeMovingAverage(TDuration::Seconds(10));
    auto oldWindow = TDuration::Seconds(10);
    auto newWindow = TDuration::Seconds(5);

    auto now = TInstant::Seconds(1);

    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 0.0);
    MovingAverage_.UpdateAt(now, 1.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);

    MovingAverage_.SetWindow(oldWindow);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 1.0);

    MovingAverage_.SetWindow(newWindow);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 0.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 2.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 2.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 4.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 3.0); // (2.0 + 4.0) / 2

    MovingAverage_.SetWindow(newWindow, /*resetOnNewWindow*/ false);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 3.0);
}

TEST_F(TDurationMovingAverageTest, EstimateAverageWithNewValue)
{
    InitializeMovingAverage(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    MovingAverage_.UpdateAt(now, 10.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 10.0);

    now += TDuration::Seconds(2);
    MovingAverage_.UpdateAt(now, 20.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0);

    // Estimate adding a new value without actually updating
    auto simulatedAverage = MovingAverage_.EstimateAverageWithNewValue(now + TDuration::Seconds(1), 30.0);
    ASSERT_DOUBLE_EQ(simulatedAverage, 20.0); // (10.0 + 20.0 + 30.0) / 3

    // Original average should remain unchanged
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0);
}

TEST_F(TDurationMovingAverageTest, EstimateWithExpiredValues)
{
    InitializeMovingAverage(TDuration::Seconds(5));

    auto now = TInstant::Seconds(1);
    MovingAverage_.UpdateAt(now, 10.0);

    now += TDuration::Seconds(2);
    MovingAverage_.UpdateAt(now, 20.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0);

    // Estimate with a time that would expire the first value
    auto simulatedAverage = MovingAverage_.EstimateAverageWithNewValue(now + TDuration::Seconds(4), 30.0);
    ASSERT_DOUBLE_EQ(simulatedAverage, 25.0); // (20.0 + 30.0) / 2, first value expired

    // Original average should remain unchanged
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0);
}

TEST_F(TDurationMovingAverageTest, Reset)
{
    InitializeMovingAverage(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    MovingAverage_.UpdateAt(now, 10.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 20.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 15.0);

    MovingAverage_.Reset();
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 0.0);

    now += TDuration::Seconds(1);
    MovingAverage_.UpdateAt(now, 30.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 30.0);
}

TEST_F(TDurationMovingAverageTest, EmptyEstimate)
{
    InitializeMovingAverage(TDuration::Seconds(10));

    auto now = TInstant::Seconds(1);
    auto simulatedAverage = MovingAverage_.EstimateAverageWithNewValue(now, 42.0);
    ASSERT_DOUBLE_EQ(simulatedAverage, 42.0);
    ASSERT_DOUBLE_EQ(MovingAverage_.GetAverage(), 0.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
