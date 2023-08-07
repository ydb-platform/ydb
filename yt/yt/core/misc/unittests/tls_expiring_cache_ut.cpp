#include <thread>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/tls_expiring_cache.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TThreadLocalExpiringCacheTest, Simple)
{
    TThreadLocalExpiringCache<TString, int> cache(TDuration::Hours(1));

    EXPECT_EQ(std::nullopt, cache.Get("hello"));
    EXPECT_EQ(std::nullopt, cache.Get("hello"));

    cache.Set("hello", 42);
    EXPECT_EQ(42, cache.Get("hello"));
}

TEST(TThreadLocalExpiringCacheTest, ThreadLocal)
{
    TThreadLocalExpiringCache<TString, int> cache(TDuration::Hours(1));

    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            EXPECT_EQ(std::nullopt, cache.Get("hello"));
            EXPECT_EQ(std::nullopt, cache.Get("hello"));

            cache.Set("hello", 42);
            EXPECT_EQ(42, cache.Get("hello"));
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

TEST(TThreadLocalExpiringCacheTest, Expiration)
{
    auto duration = TDuration::MilliSeconds(100);
    auto cpuDuration = DurationToCpuDuration(duration);

    TThreadLocalExpiringCache<TString, int> cache(duration);
    cache.Set("hello", 42);
    EXPECT_EQ(42, cache.Get("hello"));
    auto now = GetCpuInstant();

    while (GetCpuInstant() < now + cpuDuration) {
        Sleep(TDuration::MilliSeconds(15));
    }
    EXPECT_EQ(std::nullopt, cache.Get("hello"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
