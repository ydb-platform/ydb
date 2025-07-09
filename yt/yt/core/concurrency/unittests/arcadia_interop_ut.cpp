#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/arcadia_interop.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFutureInteropTest, FromArcadiaFutureWithValue1)
{
    auto future = FromArcadiaFuture(::NThreading::MakeFuture<int>(1));
    ASSERT_TRUE(future.IsSet());
    EXPECT_EQ(1, future.Get().ValueOrThrow());
}

TEST(TFutureInteropTest, FromArcadiaFutureWithValue2)
{
    ::testing::TProbeState state;
    auto promise = ::NThreading::NewPromise<::testing::TProbe>();
    auto future = FromArcadiaFuture(promise.GetFuture());
    EXPECT_TRUE(!future.IsSet());
    promise.SetValue(::testing::TProbe(&state));
    ASSERT_TRUE(future.IsSet());
    EXPECT_TRUE(future.Get().ValueOrThrow().IsValid());
    EXPECT_THAT(state, ::testing::HasCopyMoveCounts(0, 3));
}

TEST(TFutureInteropTest, FromArcadiaFutureWithError1)
{
    auto promise = ::NThreading::NewPromise<int>();
    promise.SetException("error");
    auto future = FromArcadiaFuture(promise.GetFuture());
    ASSERT_TRUE(future.IsSet());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(future.Get().ThrowOnError(), std::exception, "error");
}

TEST(TFutureInteropTest, FromArcadiaFutureWithError2)
{
    auto promise = ::NThreading::NewPromise<int>();
    auto future = FromArcadiaFuture(promise.GetFuture());
    EXPECT_TRUE(!future.IsSet());
    promise.SetException("error");
    ASSERT_TRUE(future.IsSet());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(future.Get().ThrowOnError(), std::exception, "error");
}

TEST(TFutureInteropTest, FromArcadiaFutureVoid1)
{
    auto promise = ::NThreading::NewPromise<void>();
    auto future = FromArcadiaFuture(promise.GetFuture());
    EXPECT_TRUE(!future.IsSet());
    promise.SetValue();
    ASSERT_TRUE(future.IsSet());
    EXPECT_NO_THROW(future.Get().ThrowOnError());
}

TEST(TFutureInteropTest, FromArcadiaFutureVoid2)
{
    auto promise = ::NThreading::NewPromise<void>();
    auto future = FromArcadiaFuture(promise.GetFuture());
    EXPECT_TRUE(!future.IsSet());
    promise.SetException("error");
    ASSERT_TRUE(future.IsSet());
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(future.Get().ThrowOnError(), std::exception, "error");
}

TEST(TFutureInteropTest, FromArcadiaFutureCancel)
{
    auto promise = ::NThreading::NewPromise<void>();
    auto future = FromArcadiaFuture(promise.GetFuture());
    EXPECT_TRUE(!future.IsSet());
    future.Cancel(TError("canceled"));
    promise.SetValue();
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(future.Get().ThrowOnError(), std::exception, "canceled");
}

TEST(TFutureInteropTest, ToArcadiaFutureWithValue)
{
    ::testing::TProbeState state;
    auto promise = NewPromise<::testing::TProbe>();
    auto future = ToArcadiaFuture(promise.ToFuture());
    EXPECT_FALSE(future.HasValue());
    promise.Set(::testing::TProbe(&state));
    ASSERT_TRUE(future.HasValue());
    EXPECT_TRUE(future.GetValue().IsValid());
    EXPECT_THAT(state, ::testing::HasCopyMoveCounts(1, 2));
}

TEST(TFutureInteropTest, ToArcadiaFutureWithError1)
{
    ::testing::TProbeState state;
    auto promise = NewPromise<::testing::TProbe>();
    auto future = ToArcadiaFuture(promise.ToFuture());
    EXPECT_FALSE(future.HasValue());
    promise.Set(TError("error"));
    ASSERT_TRUE(future.HasException());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(future.TryRethrow(), std::exception, "error");
    EXPECT_THAT(state, ::testing::HasCopyMoveCounts(0, 0));
}

TEST(TFutureInteropTest, ToArcadiaFutureCanceled)
{
    ::testing::TProbeState state;
    auto promise = NewPromise<::testing::TProbe>();
    auto future = ToArcadiaFuture(promise.ToFuture());
    EXPECT_FALSE(future.HasValue());

    promise.ToFuture().Cancel(TError("canceled"));
    ASSERT_TRUE(future.HasException());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(future.TryRethrow(), std::exception, "canceled");
    EXPECT_THAT(state, ::testing::HasCopyMoveCounts(0, 0));
}

TEST(TFutureInteropTest, ToArcadiaFutureVoid1)
{
    auto promise = NewPromise<void>();
    auto future = ToArcadiaFuture(promise.ToFuture());
    EXPECT_FALSE(future.HasValue());
    promise.Set();
    EXPECT_TRUE(future.HasValue());
}

TEST(TFutureInteropTest, ToArcadiaFutureVoid2)
{
    auto promise = NewPromise<void>();
    auto future = ToArcadiaFuture(promise.ToFuture());
    EXPECT_FALSE(future.HasValue());
    promise.Set(TError("error"));
    EXPECT_TRUE(future.HasException());
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(future.TryRethrow(), std::exception, "error");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT:::NConcurrency
