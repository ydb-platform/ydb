#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduled_executor.h>

#include <atomic>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TScheduledExecutorTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

void CheckTimeSlotCorrectness(const TDuration& interval)
{
    const auto& now = TInstant::Now();
    auto lastTick = TInstant::FromValue((now.GetValue() / interval.GetValue()) * interval.GetValue());
    YT_VERIFY(now - lastTick <= TDuration::MilliSeconds(10));
}

TEST_W(TScheduledExecutorTest, Simple)
{
    const auto& interval = TDuration::MilliSeconds(200);
    std::atomic<int> count = {0};

    auto callback = BIND([&] () {
        CheckTimeSlotCorrectness(interval);
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        interval);

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    WaitFor(executor->Stop())
        .ThrowOnError();
    EXPECT_LE(1, count.load());
    EXPECT_GE(2, count.load());
}

TEST_W(TScheduledExecutorTest, SimpleScheduleOutOfBand)
{
    std::atomic<int> count = {0};

    auto callback = BIND([&] () {
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(300));

    executor->Start();
    TDuration executionDuration;
    {
        auto future1 = executor->GetExecutedEvent();
        auto future2 = executor->GetExecutedEvent();
        const auto& now = TInstant::Now();
        executor->ScheduleOutOfBand();
        WaitFor(AllSucceeded(std::vector<TFuture<void>>({future1, future2})))
            .ThrowOnError();
        executionDuration = TInstant::Now() - now;
    }
    EXPECT_EQ(1, count.load());
    EXPECT_GT(TDuration::MilliSeconds(20), executionDuration);
}

TEST_W(TScheduledExecutorTest, ParallelStop)
{
    const auto& interval = TDuration::MilliSeconds(10);
    std::atomic<int> count = {0};

    auto callback = BIND([&] () {
        CheckTimeSlotCorrectness(interval);
        ++count;
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        interval);

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

TEST_W(TScheduledExecutorTest, ParallelOnExecuted1)
{
    const auto& interval = TDuration::MilliSeconds(10);
    std::atomic<int> count = 0;

    auto callback = BIND([&] () {
        CheckTimeSlotCorrectness(interval);
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        interval);

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

TEST_W(TScheduledExecutorTest, ParallelOnExecuted2)
{
    const auto& interval = TDuration::MilliSeconds(400);
    std::atomic<int> count = 0;

    auto callback = BIND([&] () {
        CheckTimeSlotCorrectness(interval);
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        interval);

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(400));
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

TEST_W(TScheduledExecutorTest, OnExecutedEventCanceled)
{
    const auto& interval = TDuration::MilliSeconds(50);
    std::atomic<int> count = 0;

    auto callback = BIND([&] () {
        CheckTimeSlotCorrectness(interval);
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        interval);

    executor->Start();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
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

TEST_W(TScheduledExecutorTest, Stop)
{
    const auto& interval = TDuration::MilliSeconds(20);
    auto neverSetPromise = NewPromise<void>();
    auto immediatelyCancelableFuture = neverSetPromise.ToFuture().ToImmediatelyCancelable();

    auto callback = BIND([&] {
        CheckTimeSlotCorrectness(interval);
        WaitUntilSet(immediatelyCancelableFuture);
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TScheduledExecutor>(
        actionQueue->GetInvoker(),
        callback,
        interval);

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
