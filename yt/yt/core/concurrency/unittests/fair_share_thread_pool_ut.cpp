#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <util/random/random.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TFairShareThreadPoolTest, Configure)
{
    auto threadPool = CreateFairShareThreadPool(1, "Test");
    auto counter = std::make_shared<std::atomic<int>>();
    auto callback = BIND([=] { ++*counter; });
    std::vector<TFuture<void>> futures;

    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        auto invoker = threadPool->GetInvoker(ToString(RandomNumber<size_t>(10)));
        futures.push_back(callback.AsyncVia(invoker).Run());
        if (i % 100 == 0) {
            threadPool->SetThreadCount(RandomNumber<size_t>(10) + 1);
        }
    }

    WaitUntilSet(AllSucceeded(std::move(futures)));
    threadPool->Shutdown();
    EXPECT_EQ(N, counter->load());
}

////////////////////////////////////////////////////////////////////////////////

// Test correct draining on Shutdown.
// Reproducing bug from YTADMINREQ-56377.
TEST(TFairShareThreadPoolTest, ShutdownClearsHeap)
{
    // Probability of missing bug in Shutdown is about 20%. Make it almost zero by repeating.
    for (int iter = 0; iter < 100; ++iter) {
        auto threadPool = CreateFairShareThreadPool(2, "Test");
        auto invoker = threadPool->GetInvoker("tag");

        // Enqueue tasks — Shutdown() will drain them but (before fix) leave the bucket in the heap.
        for (int i = 0; i < 10; ++i) {
            invoker->Invoke(BIND([] {
                Sleep(TDuration::MilliSeconds(1)); // Blocking wait.
            }));
        }

        threadPool->Shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
