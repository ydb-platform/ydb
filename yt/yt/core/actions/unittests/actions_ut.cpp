#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>

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

TEST(TAllSucceededBoundedConcurrencyTest, CancelOthers)
{
    using TCounter = std::atomic<int>;

    auto pool = CreateThreadPool(5, "ThreadPool");

    auto numDone = std::make_shared<TCounter>(0);

    std::vector<TCallback<TFuture<void>()>> callbacks;

    for (int i = 0; i < 9; ++i) {
        callbacks.push_back(BIND([numDone] {
            if (numDone->fetch_add(1) == 3) {
                THROW_ERROR_EXCEPTION("Testing");
            }

            while (true) {
                Yield();
            }
        }).AsyncVia(pool->GetInvoker()));
    }

    auto error = WaitFor(RunWithAllSucceededBoundedConcurrency(std::move(callbacks), 4));

    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetMessage(), TString("Testing"));

    EXPECT_EQ(numDone->load(), 4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
