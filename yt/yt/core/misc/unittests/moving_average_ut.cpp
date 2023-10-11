#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/moving_average.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMovingAverageTest, Empty)
{
    TMovingAverage<int> movingAverage;
    EXPECT_FALSE(movingAverage.GetAverage());

    movingAverage.AddValue(0);
    EXPECT_FALSE(movingAverage.GetAverage());
}

TEST(TMovingAverageTest, Reset)
{
    TMovingAverage<int> movingAverage(3);
    EXPECT_FALSE(movingAverage.GetAverage());

    movingAverage.AddValue(0);
    movingAverage.AddValue(1);
    movingAverage.AddValue(2);
    EXPECT_EQ(1, movingAverage.GetAverage());

    movingAverage.Reset();
    EXPECT_FALSE(movingAverage.GetAverage());
}

TEST(TMovingAverageTest, SimpleInt)
{
    TMovingAverage<int> movingAverage(3);
    EXPECT_FALSE(movingAverage.GetAverage());

    movingAverage.AddValue(0);
    movingAverage.AddValue(1);
    for (int i = 1; i < 10; ++i) {
        movingAverage.AddValue(i + 1);
        EXPECT_EQ(i, movingAverage.GetAverage());
    }

    movingAverage.SetWindowSize(1);
    EXPECT_EQ(10, movingAverage.GetAverage());

    movingAverage.SetWindowSize(2);
    EXPECT_EQ(10, movingAverage.GetAverage());

    movingAverage.AddValue(20);
    movingAverage.AddValue(30);
    EXPECT_EQ(25, movingAverage.GetAverage());
}

TEST(TMovingAverageTest, SimpleDouble)
{
    TMovingAverage<double> movingAverage(100);
    EXPECT_FALSE(movingAverage.GetAverage());

    for (int i = 0; i < 1000; ++i) {
        movingAverage.AddValue(i);
    }
    EXPECT_EQ(949.5, movingAverage.GetAverage());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
