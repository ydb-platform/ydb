#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/actions/future.h>

#include <util/random/random.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TThreadPoolTest, Configure)
{
    auto threadPool = CreateThreadPool(1, "Test");
    auto counter = std::make_shared<std::atomic<int>>();
    auto callback = BIND([=] { ++*counter; });
    std::vector<TFuture<void>> futures;

    const int N = 10000;
    int threadCount = 0;
    for (int i = 0; i < N; ++i) {
        futures.push_back(callback.AsyncVia(threadPool->GetInvoker()).Run());
        if (i % 100 == 0) {
            threadCount = RandomNumber<size_t>(10) + 1;
            threadPool->Configure(threadCount);
            EXPECT_EQ(threadPool->GetThreadCount(), threadCount);
        }
    }

    AllSucceeded(std::move(futures))
        .Get();

    // Thread pool doesn't contain less than one thread whatever you configured.
    threadPool->Configure(0);
    EXPECT_EQ(threadPool->GetThreadCount(), 1);

    // Thread pool doesn't contain more than maximal threads count whatever you configured.
    threadPool->Configure(1e8);
    EXPECT_LE(threadPool->GetThreadCount(), 1e8);

    threadPool->Shutdown();
    EXPECT_EQ(N, counter->load());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
