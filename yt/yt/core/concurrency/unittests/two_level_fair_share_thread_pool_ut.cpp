#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <util/random/random.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTwoLevelFairShareThreadPoolTest, Configure)
{
    auto threadPool = CreateTwoLevelFairShareThreadPool(1, "Test");
    auto counter = std::make_shared<std::atomic<int>>();
    auto callback = BIND([=] { ++*counter; });
    std::vector<TFuture<void>> futures;

    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        auto invoker = threadPool->GetInvoker(
            ToString(RandomNumber<size_t>(10)),
            ToString(RandomNumber<size_t>(10)));
        futures.push_back(callback.AsyncVia(invoker).Run());
        if (i % 100 == 0) {
            threadPool->SetThreadCount(RandomNumber<size_t>(10) + 1);
        }
    }

    WaitUntilSet(AllSucceeded(std::move(futures)));

    threadPool->Shutdown();
    EXPECT_EQ(N, counter->load());
}

TEST(TTwoLevelFairShareThreadPoolTest, CurrentInvoker)
{
    auto threadPool = CreateTwoLevelFairShareThreadPool(1, "Test");
    auto invoker = threadPool->GetInvoker("pool", "bucket");

    auto future = BIND([&] {
        EXPECT_EQ(invoker.Get(), GetCurrentInvoker());
        // The bucket must remain the current invoker across a context switch.
        Yield();
        EXPECT_EQ(invoker.Get(), GetCurrentInvoker());
    })
        .AsyncVia(invoker)
        .Run();

    WaitUntilSet(future);
    threadPool->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
