#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/logging/log.h>

#include <exception>
#include <atomic>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPeriodicTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_W(TPeriodicTest, Simple)
{
    std::atomic<int> count = {0};

    auto callback = BIND([&] {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(100));

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(600));
    WaitFor(executor->Stop())
        .ThrowOnError();
    EXPECT_EQ(2, count.load());

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(600));
    WaitFor(executor->Stop())
        .ThrowOnError();
    EXPECT_EQ(4, count.load());

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(250));
    WaitFor(executor->GetExecutedEvent())
        .ThrowOnError();
    EXPECT_EQ(6, count.load());
    WaitFor(executor->Stop())
        .ThrowOnError();
}

TEST_W(TPeriodicTest, SimpleScheduleOutOfBand)
{
    std::atomic<int> count = {0};

    auto callback = BIND([&] {
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(300));

    executor->Start();
    // Wait for first invocation.
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(20));

    auto future = executor->GetExecutedEvent();
    const auto& now = TInstant::Now();
    executor->ScheduleOutOfBand();
    WaitFor(future)
        .ThrowOnError();
    auto executionDuration = TInstant::Now() - now;
    EXPECT_LT(executionDuration, TDuration::MilliSeconds(20));
    EXPECT_EQ(2, count.load());
}

TEST_W(TPeriodicTest, ParallelStop)
{
    std::atomic<int> count = {0};

    auto callback = BIND([&] {
        ++count;
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    {
        auto future1 = executor->Stop();
        auto future2 = executor->Stop();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
    }
    EXPECT_EQ(1, count.load());

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    {
        auto future1 = executor->Stop();
        auto future2 = executor->Stop();
        auto future3 = executor->Stop();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2, future3})))
            .ThrowOnError();
    }
    EXPECT_EQ(2, count.load());
}

TEST_W(TPeriodicTest, ParallelOnExecuted1)
{
    std::atomic<int> count = 0;

    auto callback = BIND([&] {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(10));

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
    }
    EXPECT_EQ(2, count.load());

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(450));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        auto future3 = executor->GetExecutedEvent();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2, future3})))
            .ThrowOnError();
    }
    EXPECT_EQ(4, count.load());
}

TEST_W(TPeriodicTest, ParallelOnExecuted2)
{
    std::atomic<int> count = 0;

    auto callback = BIND([&] {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(400));

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
    }
    EXPECT_EQ(2, count.load());

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        auto future3 = executor->GetExecutedEvent();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2, future3})))
            .ThrowOnError();
    }
    EXPECT_EQ(3, count.load());
}

TEST_W(TPeriodicTest, OnExecutedEventCanceled)
{
    std::atomic<int> count = 0;

    auto callback = BIND([&] {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(400));

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();

        // Cancellation of the executed event future must not propagate to the underlying event.
        auto future3 = executor->GetExecutedEvent();
        future3.Cancel(TError(NYT::EErrorCode::Canceled, "Canceled"));

        EXPECT_NO_THROW(WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError());
    }
    EXPECT_EQ(2, count.load());
}

TEST_W(TPeriodicTest, Stop)
{
    auto neverSetPromise = NewPromise<void>();
    auto immediatelyCancelableFuture = neverSetPromise.ToFuture().ToImmediatelyCancelable();

    auto callback = BIND([&] {
        WaitUntilSet(immediatelyCancelableFuture);
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(100));

    executor->Start();
    // Wait for the callback to enter WaitFor.
    Sleep(TDuration::MilliSeconds(100));
    WaitFor(executor->Stop())
        .ThrowOnError();

    EXPECT_TRUE(immediatelyCancelableFuture.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, immediatelyCancelableFuture.Get().GetCode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
