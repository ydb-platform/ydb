#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncSemaphoreTest, CancelReadyEvent)
{
    auto semaphore = New<TAsyncSemaphore>(1);
    semaphore->Acquire(); // Drain single slot.

    auto readyOne = semaphore->GetReadyEvent();
    auto readyTwo = semaphore->GetReadyEvent();

    readyOne.Cancel(TError("canceled"));
    semaphore->Release();
    EXPECT_TRUE(readyTwo.IsSet());
    EXPECT_TRUE(WaitFor(readyTwo).IsOK());
}

TEST(TAsyncSemaphoreTest, OverdraftSlots)
{
    constexpr static int ThreadCount = 4;
    constexpr static int RequestCount = 10;
    constexpr static int SemaphoreTotalSlots = 1;
    constexpr static int RequestWeight = 100;

    auto threadPool = CreateThreadPool(ThreadCount, "SemaphoreAcqusition");
    auto semaphore = New<TAsyncSemaphore>(SemaphoreTotalSlots, /*enableOverdraft*/ true);

    {
        std::vector<TFuture<void>> futures;
        auto barrierPromise = NewPromise<void>();
        for (int i = 0; i < RequestCount; ++i) {
            futures.push_back(BIND([
                &semaphore,
                barrierFuture = barrierPromise.ToFuture()
            ] {
                WaitForFast(barrierFuture)
                    .ThrowOnError();

                WaitFor(semaphore->AsyncAcquire(RequestWeight).AsVoid())
                    .ThrowOnError();
            })
                .AsyncVia(threadPool->GetInvoker())
                .Run());
        }

        barrierPromise.Set();

        WaitFor(AllSucceeded(std::move(futures)))
            .ThrowOnError();
    }

    {
        semaphore->SetTotal(RequestWeight);
        std::vector<TFuture<void>> futures;
        auto barrierPromise = NewPromise<void>();
        for (int i = 0; i < RequestCount; ++i) {
            futures.push_back(BIND([
                &semaphore,
                barrierFuture = barrierPromise.ToFuture()
            ] {
                WaitForFast(barrierFuture)
                    .ThrowOnError();

                WaitFor(semaphore->AsyncAcquire(RequestWeight).AsVoid())
                    .ThrowOnError();
            })
                .AsyncVia(threadPool->GetInvoker())
                .Run());
        }

        futures.push_back(BIND([
            &semaphore,
            barrierFuture = barrierPromise.ToFuture()
        ] {
            WaitForFast(barrierFuture)
                .ThrowOnError();

            semaphore->SetTotal(SemaphoreTotalSlots);
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());

        barrierPromise.Set();

        WaitFor(AllSucceeded(std::move(futures)))
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
