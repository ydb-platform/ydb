#include <thread>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/sync_expiring_cache.h>

namespace NYT {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TSyncExpiringCacheTest, MultipleGet_RaceCondition)
{
    auto cache = New<TSyncExpiringCache<int, int>>(
        TDuration::Seconds(1),
        GetSyncInvoker());

    auto get = [&] (int key) {
        return cache->GetOrPut(
            key,
            [&] {
                Sleep(TDuration::MilliSeconds(1));
                return key;
            });
    };

    std::thread thread1([&] { get(1); });
    std::thread thread2([&] { get(1); });

    thread1.join();
    thread2.join();
}

TEST(TSyncExpiringCacheTest, FindSetClear)
{
    auto cache = New<TSyncExpiringCache<int, int>>(
        TDuration::Seconds(1),
        GetSyncInvoker());

    EXPECT_EQ(std::nullopt, cache->Find(1));

    cache->Put(1, 2);

    EXPECT_EQ(2, cache->Find(1));

    cache->Clear();

    EXPECT_EQ(std::nullopt, cache->Find(1));
}

TEST(TSyncExpiringCacheTest, SetExpirationTimeout)
{
    auto cache = New<TSyncExpiringCache<int, int>>(
        TDuration::Seconds(1),
        GetSyncInvoker());

    cache->Put(1, 2);

    cache->SetExpirationTimeout(TDuration::MilliSeconds(100));

    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(std::nullopt, cache->Find(1));

    cache->Put(1, 3);

    cache->SetExpirationTimeout(std::nullopt);

    Sleep(TDuration::MilliSeconds(200));

    EXPECT_EQ(3, cache->Find(1));
}

TEST(TSyncExpiringCacheTest, GetMany)
{
    auto cache = New<TSyncExpiringCache<int, int>>(
        TDuration::Seconds(1),
        GetSyncInvoker());

    auto getMany = [&] (const std::vector<int>& keys) {
        return cache->GetOrPutMany(
            TRange(keys),
            [&] (int index) { return keys[index]; });
    };

    {
        std::vector<int> expected{1};
        EXPECT_EQ(expected, getMany({1}));
    }

    {
        std::vector<int> expected{1};
        EXPECT_EQ(expected, getMany({1}));
    }

    {
        std::vector<int> expected{0, 1, 2};
        EXPECT_EQ(expected, getMany({0, 1, 2}));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
