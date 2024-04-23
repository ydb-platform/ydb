#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/threading/count_down_latch.h>

#include <thread>

namespace NYT::NConcurrency {
namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

void WaitForLatch(const TCountDownLatch& latch)
{
    latch.Wait();
    EXPECT_EQ(0, latch.GetCount());
}

TEST(TCountDownLatch, TwoThreads)
{
    TCountDownLatch latch(2);

    std::thread t1(std::bind(&WaitForLatch, std::cref(latch)));
    std::thread t2(std::bind(&WaitForLatch, std::cref(latch)));

    EXPECT_EQ(2, latch.GetCount());
    latch.CountDown();
    EXPECT_EQ(1, latch.GetCount());
    latch.CountDown();
    EXPECT_EQ(0, latch.GetCount());

    t1.join();
    t2.join();
}

TEST(TCountDownLatch, TwoThreadsPredecremented)
{
    TCountDownLatch latch(2);

    EXPECT_EQ(2, latch.GetCount());
    latch.CountDown();
    EXPECT_EQ(1, latch.GetCount());
    latch.CountDown();
    EXPECT_EQ(0, latch.GetCount());

    std::thread t1(std::bind(&WaitForLatch, std::cref(latch)));
    std::thread t2(std::bind(&WaitForLatch, std::cref(latch)));

    t1.join();
    t2.join();
}

TEST(TCountDownLatch, TwoThreadsTwoLatches)
{
    TCountDownLatch first(1);
    TCountDownLatch second(1);

    std::thread t1([&] {
        first.Wait();
        second.CountDown();
        EXPECT_EQ(0, first.GetCount());
        EXPECT_EQ(0, second.GetCount());
    });

    std::thread t2([&] {
        first.CountDown();
        second.Wait();
        EXPECT_EQ(0, first.GetCount());
        EXPECT_EQ(0, second.GetCount());
    });

    t1.join();
    t2.join();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency

