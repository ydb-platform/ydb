#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/async_barrier.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncBarrierTest, Empty)
{
    TAsyncBarrier barrier;
    auto future = barrier.GetBarrierFuture();
    EXPECT_TRUE(future.IsSet());
}

TEST(TAsyncBarrierTest, Simple)
{
    TAsyncBarrier barrier;
    auto cookie = barrier.Insert();
    auto future = barrier.GetBarrierFuture();
    EXPECT_FALSE(future.IsSet());
    barrier.Remove(cookie);
    EXPECT_TRUE(future.IsSet());
}

TEST(TAsyncBarrierTest, ReuseFuture)
{
    TAsyncBarrier barrier;
    auto cookie = barrier.Insert();
    auto future1 = barrier.GetBarrierFuture();
    auto future2 = barrier.GetBarrierFuture();
    EXPECT_FALSE(future1.IsSet());
    EXPECT_EQ(future1, future2);
    barrier.Remove(cookie);
    EXPECT_TRUE(future1.IsSet());
}

TEST(TAsyncBarrierTest, Overlapped)
{
    TAsyncBarrier barrier;

    auto cookie1 = barrier.Insert();

    auto future1 = barrier.GetBarrierFuture();
    EXPECT_FALSE(future1.IsSet());

    auto cookie2 = barrier.Insert();

    auto future2 = barrier.GetBarrierFuture();
    EXPECT_FALSE(future1.IsSet());

    barrier.Remove(cookie1);
    EXPECT_TRUE(future1.IsSet());
    EXPECT_FALSE(future2.IsSet());

    barrier.Remove(cookie2);
    EXPECT_TRUE(future2.IsSet());
}

TEST(TAsyncBarrierTest, Nested)
{
    TAsyncBarrier barrier;

    auto cookie1 = barrier.Insert();

    auto future1 = barrier.GetBarrierFuture();
    EXPECT_FALSE(future1.IsSet());

    auto cookie2 = barrier.Insert();

    auto future2 = barrier.GetBarrierFuture();
    EXPECT_FALSE(future1.IsSet());

    barrier.Remove(cookie2);
    EXPECT_FALSE(future2.IsSet());
    EXPECT_FALSE(future1.IsSet());

    barrier.Remove(cookie1);
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future2.IsSet());
}

TEST(TAsyncBarrierTest, Clear)
{
    TAsyncBarrier barrier;

    Y_UNUSED(barrier.Insert());

    auto future = barrier.GetBarrierFuture();
    EXPECT_FALSE(future.IsSet());

    barrier.Clear(TError("oops"));
    EXPECT_TRUE(future.IsSet());
    EXPECT_FALSE(future.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT:::NConcurrency
