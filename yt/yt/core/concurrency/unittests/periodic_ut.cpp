#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/logging/log.h>

#include <exception>
#include <atomic>

namespace NYT::NConcurrency {
namespace {

using ::testing::Each;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Property;

////////////////////////////////////////////////////////////////////////////////

class TPeriodicTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_W(TPeriodicTest, Simple)
{
    std::atomic<int> count = 0;

    auto callback = BIND([&] {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        ++count;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(100));

    auto firstExecution = executor->StartAndGetFirstExecutedEvent();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(600));
    EXPECT_TRUE(firstExecution.IsSet());
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
    EXPECT_EQ(6, count.load());

    WaitFor(executor->StartAndGetFirstExecutedEvent())
        .ThrowOnError();
    EXPECT_EQ(7, count.load());
    WaitFor(executor->Stop())
        .ThrowOnError();
}

TEST_W(TPeriodicTest, SimpleScheduleOutOfBand)
{
    std::atomic<int> count = 0;

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

TEST_W(TPeriodicTest, ParallelStart)
{
    static constexpr int ThreadCount = 4;

    TPromise<void> threadStartBarrier = NewPromise<void>();
    TPromise<void> callbackStartBarrier = NewPromise<void>();
    TPromise<void> callbackEndBarrier = NewPromise<void>();
    std::atomic<int> countStarted = 0;
    std::atomic<int> countFinished = 0;
    std::atomic<int> countWaiting = 0;

    auto callback = BIND([&] {
        ++countStarted;
        threadStartBarrier.ToFuture().Get();
        callbackStartBarrier.Set();
        callbackEndBarrier.ToFuture().Get();
        ++countFinished;
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(200));

    auto startCallback = BIND([&] {
        auto result = executor->StartAndGetFirstExecutedEvent();
        if (++countWaiting == ThreadCount) {
            threadStartBarrier.Set();
        }
        return result;
    });

    auto threadPool = CreateThreadPool(ThreadCount, "test");

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < ThreadCount; ++i) {
        futures.push_back(startCallback.AsyncVia(threadPool->GetInvoker()).Run());
    }

    // Check that start futures are set correctly in all threads
    // after the first execution, but before the second one.

    callbackStartBarrier.ToFuture().Get();
    EXPECT_THAT(
        futures,
        Each(
            Property(&TFuture<void>::IsSet, IsFalse())));
    callbackEndBarrier.Set();
    threadStartBarrier = NewPromise<void>();
    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
    EXPECT_EQ(1, countStarted.load());
    EXPECT_EQ(1, countFinished.load());
    threadStartBarrier.Set();

    WaitFor(executor->Stop())
        .ThrowOnError();
}

TEST_W(TPeriodicTest, ParallelStop)
{
    std::atomic<int> count = 0;

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

TEST_W(TPeriodicTest, OnStartCancelled)
{
    auto callbackStarted = NewPromise<void>();

    auto callback = BIND([&] {
        callbackStarted.Set();
    });

    auto actionQueue = New<TActionQueue>();
    auto executor = New<TPeriodicExecutor>(
        actionQueue->GetInvoker(),
        callback,
        TDuration::MilliSeconds(200));

    auto startFuture1 = executor->StartAndGetFirstExecutedEvent();
    auto startFuture2 = executor->StartAndGetFirstExecutedEvent();
    startFuture1.Cancel(TError(NYT::EErrorCode::Canceled, "Canceled"));

    // NB(pavook): cancellation of a start future shouldn't cause an executor stop
    // and should not propagate to the underlying promise (and other futures).
    EXPECT_TRUE(callbackStarted.ToFuture().Get().IsOK());
    EXPECT_TRUE(executor->IsStarted());
    EXPECT_TRUE(startFuture2.IsSet());
    EXPECT_TRUE(startFuture2.Get().IsOK());
    WaitFor(executor->Stop())
        .ThrowOnError();
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

    auto startFuture = executor->StartAndGetFirstExecutedEvent();
    // Wait for the callback to enter WaitFor.
    Sleep(TDuration::MilliSeconds(100));
    WaitFor(executor->Stop())
        .ThrowOnError();

    EXPECT_TRUE(immediatelyCancelableFuture.IsSet());
    EXPECT_EQ(NYT::EErrorCode::Canceled, immediatelyCancelableFuture.Get().GetCode());
    EXPECT_FALSE(startFuture.Get().IsOK());
    EXPECT_EQ(NYT::EErrorCode::Canceled, startFuture.Get().GetCode());

    startFuture = executor->StartAndGetFirstExecutedEvent();
    Sleep(TDuration::MilliSeconds(200));
    WaitFor(executor->Stop())
        .ThrowOnError();
    // startFuture should be set after the first execution.
    EXPECT_TRUE(startFuture.IsSet());
    EXPECT_TRUE(startFuture.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
