#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFinallyTest, Common)
{
    bool executed = false;
    {
        auto f = Finally(
            [&] {
                executed = true;
            });
        EXPECT_FALSE(executed);
    }
    EXPECT_TRUE(executed);
}

TEST(TFinallyTest, Exception)
{
    bool executed = false;
    try {
        auto f = Finally(
            [&] {
                executed = true;
            });
        EXPECT_FALSE(executed);
        throw 1;
    } catch (...) {
        EXPECT_TRUE(executed);
    }
}

TEST(TFinallyTest, Move)
{
    int executedCounter = 0;
    {
        auto f = Finally(
            [&] {
                ++executedCounter;
            });
        {
            auto f2 = std::move(f);
        }
        EXPECT_EQ(1, executedCounter);
    }
    EXPECT_EQ(1, executedCounter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
