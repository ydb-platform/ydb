#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/suspendable_action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/utilex/random.h>

#include <random>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSuspendableActionQueueTest
    : public ::testing::Test
{
protected:
    ISuspendableActionQueuePtr Queue_ = CreateSuspendableActionQueue("TestQueue");
    IInvokerPtr Invoker_ = Queue_->GetInvoker();

    void RandomSleep()
    {
        if (RandomNumber<ui64>(5) == 0u) {
            return;
        }

        TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::MilliSeconds(15)));
    }
};

TEST_F(TSuspendableActionQueueTest, Simple)
{
    std::atomic<i64> x = 0;
    BIND([&x] { ++x; })
        .AsyncVia(Invoker_)
        .Run()
        .Get()
        .ThrowOnError();

    EXPECT_EQ(x, 1);
}

TEST_F(TSuspendableActionQueueTest, SuspendResume)
{
    std::atomic<i64> x = 0;
    auto future = BIND([&x] {
        while (true) {
            ++x;
            Yield();
        }
    })
        .AsyncVia(Invoker_)
        .Run();

    TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::MilliSeconds(15)));

    Queue_->Suspend(/*immediate*/ true)
        .Get()
        .ThrowOnError();

    i64 x1 = x;
    EXPECT_GT(x1, 0);

    TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::MilliSeconds(15)));

    i64 x2 = x;
    EXPECT_EQ(x2, x1);

    Queue_->Resume();

    TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::MilliSeconds(15)));

    i64 x3 = x;
    EXPECT_GT(x3, x2);

    future.Cancel(TError("Test ended"));
}

TEST_F(TSuspendableActionQueueTest, SuspendEmptyQueue)
{
    Queue_->Suspend(/*immedidate*/ true)
        .Get()
        .ThrowOnError();
    Queue_->Resume();

    Queue_->Suspend(/*immedidate*/ false)
        .Get()
        .ThrowOnError();
    Queue_->Resume();

    int x = 0;
    BIND([&x] {++x; })
        .AsyncVia(Invoker_)
        .Run()
        .Get()
        .ThrowOnError();

    EXPECT_EQ(x, 1);
}

TEST_F(TSuspendableActionQueueTest, NotImmediateSuspend)
{
    std::atomic<i64> x = 0;
    Invoker_->Invoke(BIND([&x] {
        for (int iteration = 0; iteration < 50; ++iteration) {
            ++x;

            Sleep(TDuration::MilliSeconds(10));

            Yield();
        }
    }));

    auto future = Queue_->Suspend(/*immedidate*/ false);

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));

    EXPECT_FALSE(future.IsSet());

    future
        .Get()
        .ThrowOnError();

    EXPECT_EQ(x, 50);

    Queue_->Resume();
}

TEST_F(TSuspendableActionQueueTest, PromoteSuspendToImmediate)
{
    i64 x = 0;
    auto future = BIND([&x] {
        while (true) {
            ++x;
            Yield();
        }
    })
        .AsyncVia(Invoker_)
        .Run();

    auto suspendFuture = Queue_->Suspend(/*immedidate*/ false);

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));

    EXPECT_FALSE(suspendFuture.IsSet());

    Queue_->Suspend(/*immediately*/ true)
        .Get()
        .ThrowOnError();

    EXPECT_TRUE(suspendFuture.IsSet());
    EXPECT_GT(x, 0);

    Queue_->Resume();

    future.Cancel(TError("Test ended"));
}

TEST_F(TSuspendableActionQueueTest, StressTest1)
{
    std::atomic<i64> x = 0;

    std::vector<TFuture<void>> futures;
    for (int index = 0; index < 100; ++index) {
        auto future = BIND([&x] {
            while (true) {
                ++x;
                Yield();
            }
        })
            .AsyncVia(Invoker_)
            .Run();
        futures.push_back(future);
    }

    i64 lastX = 0;
    for (int iteration = 0; iteration < 100; ++iteration) {
        RandomSleep();

        Queue_->Suspend(/*immedidate*/ true)
            .Get()
            .ThrowOnError();

        i64 currentX = x;
        EXPECT_GE(currentX, lastX);
        lastX = currentX;

        RandomSleep();

        currentX = x;
        EXPECT_EQ(currentX, lastX);

        Queue_->Resume();
    }

    for (auto& future : futures) {
        future.Cancel(TError("Test ended"));
    }
}

TEST_F(TSuspendableActionQueueTest, StressTest2)
{
    std::atomic<i64> x = 0;

    std::vector<TFuture<void>> futures;
    for (int index = 0; index < 100; ++index) {
        auto future = BIND([&x] {
            for (int iteration = 0; iteration < 1000; ++iteration) {
                ++x;
                Yield();
            }
        })
            .AsyncVia(Invoker_)
            .Run();
        futures.push_back(future);
    }

    i64 lastX = 0;
    for (int iteration = 0; iteration < 100; ++iteration) {
        RandomSleep();

        Queue_->Suspend(/*immedidate*/ true)
            .Get()
            .ThrowOnError();

        i64 currentX = x;
        EXPECT_GE(currentX, lastX);
        lastX = currentX;

        RandomSleep();

        currentX = x;
        EXPECT_EQ(currentX, lastX);

        Queue_->Resume();
    }

    for (auto& future : futures) {
        future.Get();
    }

    EXPECT_EQ(x, 100'000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
