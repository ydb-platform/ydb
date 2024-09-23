#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TCancelableRunWithBoundedConcurrencyTest, Simple)
{
    int x = 0;

    auto future = CancelableRunWithBoundedConcurrency<void>(
        {
            BIND([&] {
                ++x;
                return VoidFuture;
            })
        },
        /*concurrencyLimit*/ 1);
    WaitFor(future)
        .ThrowOnError();

    EXPECT_EQ(x, 1);
}

TEST(TCancelableRunWithBoundedConcurrencyTest, ManyCallbacks)
{
    auto threadPool = CreateThreadPool(4, "ThreadPool");

    std::atomic<int> x = 0;

    const int callbackCount = 10000;
    std::vector<TCallback<TFuture<void>()>> callbacks;
    callbacks.reserve(callbackCount);
    for (int i = 0; i < callbackCount; ++i) {
        callbacks.push_back(BIND([&] {
            ++x;
        })
        .AsyncVia(threadPool->GetInvoker()));
    }

    auto future = CancelableRunWithBoundedConcurrency(
        std::move(callbacks),
        /*concurrencyLimit*/ 10);
    WaitFor(future)
        .ThrowOnError();

    EXPECT_EQ(x, callbackCount);
}

TEST(TCancelableRunWithBoundedConcurrencyTest, Cancelation)
{
    auto threadPool = CreateThreadPool(4, "ThreadPool");

    std::atomic<int> x = 0;
    std::atomic<int> canceledCount = 0;

    std::vector<TCallback<TFuture<void>()>> callbacks;
    for (int i = 0; i < 9; ++i) {
        callbacks.push_back(BIND([&] {
            if (x++ < 5) {
                return VoidFuture;
            }

            auto promise = NewPromise<void>();
            promise.OnCanceled(BIND([&, promise] (const TError& /*error*/) {
                ++canceledCount;
            }));

            return promise.ToFuture();
        }));
    }

    auto future = CancelableRunWithBoundedConcurrency<void>(
        std::move(callbacks),
        /*concurrencyLimit*/ 5);

    while (x < 9) {
        Sleep(TDuration::MilliSeconds(10));
    }

    future.Cancel(TError("Canceled"));

    EXPECT_EQ(x, 9);
    EXPECT_EQ(canceledCount, 4);
}

TEST(TAllSucceededBoundedConcurrencyTest, AllSucceededFail)
{
    using TCounter = std::atomic<int>;

    auto threadPool = CreateThreadPool(4, "ThreadPool");

    auto x = std::make_shared<TCounter>(0);
    auto startingSleepCount = std::make_shared<TCounter>(0);
    auto finishedSleepCount = std::make_shared<TCounter>(0);

    std::vector<TCallback<TFuture<void>()>> callbacks;
    for (int i = 0; i < 9; ++i) {
        callbacks.push_back(BIND([x, startingSleepCount, finishedSleepCount]() mutable {
            int curX = (*x)++;
            if (curX < 5) {
                return;
            } else if (curX == 5) {
                //Make sure other callbacks have a chance to start first
                Sleep(TDuration::MilliSeconds(5));
                THROW_ERROR_EXCEPTION("My Error");
            }

            (*startingSleepCount)++;
            Sleep(TDuration::MilliSeconds(50));
            (*finishedSleepCount)++;
        })
        .AsyncVia(threadPool->GetInvoker()));
    }

    auto future = RunWithAllSucceededBoundedConcurrency<void>(
        std::move(callbacks),
        /*concurrencyLimit*/ 5);

    auto result = WaitFor(future);
    EXPECT_EQ(result.IsOK(), false);
    EXPECT_EQ(result.GetCode(), NYT::EErrorCode::Generic);
    EXPECT_EQ(result.GetMessage(), "My Error");

    EXPECT_EQ(x->load(), 9);
    EXPECT_EQ(startingSleepCount->load(), 3);
    EXPECT_EQ(finishedSleepCount->load(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
