#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/library/profiling/testing.h>

#include <random>

namespace NYT {
namespace {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSimpleCachedValue)

class TSimpleCachedValue
    : public TAsyncCacheValueBase<int, TSimpleCachedValue>
{
public:
    explicit TSimpleCachedValue(int key, int value, int weight = 1)
        : TAsyncCacheValueBase(key)
        , Value(value)
        , Weight(weight)
    { }

    int Value;
    int Weight;
};

DEFINE_REFCOUNTED_TYPE(TSimpleCachedValue)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSimpleSlruCache)

class TSimpleSlruCache
    : public TAsyncSlruCacheBase<int, TSimpleCachedValue>
{
public:
    explicit TSimpleSlruCache(TSlruCacheConfigPtr config, TProfiler profiler = {})
        : TAsyncSlruCacheBase(std::move(config), std::move(profiler))
    { }

    struct TCountersState
    {
        i64 SyncHitWeight;
        i64 AsyncHitWeight;
        i64 MissedWeight;
        i64 SyncHit;
        i64 AsyncHit;
        i64 Missed;

        TCountersState operator -(const TCountersState& other) const
        {
            return TCountersState {
                .SyncHitWeight = SyncHitWeight - other.SyncHitWeight,
                .AsyncHitWeight = AsyncHitWeight - other.AsyncHitWeight,
                .MissedWeight = MissedWeight - other.MissedWeight,
                .SyncHit = SyncHit - other.SyncHit,
                .AsyncHit = AsyncHit - other.AsyncHit,
                .Missed = Missed - other.Missed
            };
        }
    };

    TCountersState ReadSmallGhostCounters() const
    {
        return ReadCounters(GetSmallGhostCounters());
    }

    TCountersState ReadLargeGhostCounters() const
    {
        return ReadCounters(GetLargeGhostCounters());
    }

protected:
    i64 GetWeight(const TSimpleCachedValuePtr& value) const override
    {
        return value->Weight;
    }

    static TCountersState ReadCounters(const TCounters& counters)
    {
        return TCountersState {
            .SyncHitWeight = TTesting::ReadCounter(counters.SyncHitWeightCounter),
            .AsyncHitWeight = TTesting::ReadCounter(counters.AsyncHitWeightCounter),
            .MissedWeight = TTesting::ReadCounter(counters.MissedWeightCounter),
            .SyncHit = TTesting::ReadCounter(counters.SyncHitCounter),
            .AsyncHit = TTesting::ReadCounter(counters.AsyncHitCounter),
            .Missed = TTesting::ReadCounter(counters.MissedCounter)
        };
    }
};

DEFINE_REFCOUNTED_TYPE(TSimpleSlruCache)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCountingSlruCache)

class TCountingSlruCache
    : public TAsyncSlruCacheBase<int, TSimpleCachedValue>
{
public:
    explicit TCountingSlruCache(TSlruCacheConfigPtr config, bool enableResurrection = true)
        : TAsyncSlruCacheBase(std::move(config)), EnableResurrection_(enableResurrection)
    { }

    DEFINE_BYVAL_RO_PROPERTY(int, ItemCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, TotalAdded, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, TotalRemoved, 0);

protected:
    i64 GetWeight(const TSimpleCachedValuePtr& value) const override
    {
        return value->Weight;
    }

    void OnAdded(const TSimpleCachedValuePtr& /*value*/) override
    {
        ++ItemCount_;
        ++TotalAdded_;
    }

    void OnRemoved(const TSimpleCachedValuePtr& /*value*/) override
    {
        --ItemCount_;
        ++TotalRemoved_;
        EXPECT_GE(ItemCount_, 0);
    }
    bool IsResurrectionSupported() const override
    {
        return EnableResurrection_;
    }

private:
    bool EnableResurrection_;
};

DEFINE_REFCOUNTED_TYPE(TCountingSlruCache)

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetAllKeys(const TSimpleSlruCachePtr& cache)
{
    std::vector<int> result;

    for (const auto& cachedValue : cache->GetAll()) {
        result.emplace_back(cachedValue->GetKey());
    }
    std::sort(result.begin(), result.end());

    return result;
}

std::vector<int> GetKeysFromRanges(std::vector<std::pair<int, int>> ranges)
{
    std::vector<int> result;

    for (const auto& [from, to] : ranges) {
        for (int i = from; i < to; ++i) {
            result.push_back(i);
        }
    }
    std::sort(result.begin(), result.end());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TSlruCacheConfigPtr CreateCacheConfig(i64 cacheSize)
{
    auto config = TSlruCacheConfig::CreateWithCapacity(cacheSize);
    config->ShardCount = 1;

    return config;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncSlruCacheTest, Simple)
{
    const int cacheSize = 10;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(config);

    for (int i = 0; i < 2 * cacheSize; ++i) {
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(i, i));
    }

    // Cache size is small, so on the second pass every element should be missing too.
    for (int i = 0; i < 2 * cacheSize; ++i) {
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(i, i * 2));
    }

    // Only last cacheSize items.
    EXPECT_EQ(GetAllKeys(cache), GetKeysFromRanges({{cacheSize, 2 * cacheSize}}));

    // Check that Find() works as expected.
    for (int i = 0; i < cacheSize; ++i) {
        auto cachedValue = cache->Find(i);
        EXPECT_EQ(cachedValue, nullptr);
    }
    for (int i = cacheSize; i < 2 * cacheSize; ++i) {
        auto cachedValue = cache->Find(i);
        ASSERT_NE(cachedValue, nullptr);
        EXPECT_EQ(cachedValue->GetKey(), i);
        EXPECT_EQ(cachedValue->Value, i * 2);
    }
}

TEST(TAsyncSlruCacheTest, Youngest)
{
    const int cacheSize = 10;
    const int oldestSize = 5;
    auto config = CreateCacheConfig(cacheSize);
    config->YoungerSizeFraction = 0.5;
    auto cache = New<TSimpleSlruCache>(config);

    for (int i = 0; i < oldestSize; ++i) {
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(i, i));
        // Move to oldest.
        cache->Find(i);
    }

    for (int i = cacheSize; i < 2 * cacheSize; ++i) {
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(i, i));
    }

    EXPECT_EQ(GetAllKeys(cache), GetKeysFromRanges({{0, oldestSize}, {cacheSize + oldestSize, 2 * cacheSize}}));
}

TEST(TAsyncSlruCacheTest, Resurrection)
{
    const int cacheSize = 10;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(config);

    std::vector<TSimpleCachedValuePtr> values;

    for (int i = 0; i < 2 * cacheSize; ++i) {
        auto value = New<TSimpleCachedValue>(i, i);
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(value);
        values.push_back(value);
    }

    EXPECT_EQ(cache->GetSize(), cacheSize);
    // GetAll() returns values which are in cache or can be resurrected.
    EXPECT_EQ(GetAllKeys(cache), GetKeysFromRanges({{0, 2 * cacheSize}}));

    for (int i = 0; i < 2 * cacheSize; ++i) {
        // It's expired because our cache is too small.
        EXPECT_EQ(cache->Find(i), nullptr);
        // But lookup can find and restore it (and make some other values expired)
        // because the value is alive in 'values' vector.
        EXPECT_EQ(cache->Lookup(i).Get().ValueOrThrow(), values[i]);
    }
}

TEST(TAsyncSlruCacheTest, LookupBetweenBeginAndEndInsert)
{
    const int cacheSize = 10;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(config);

    auto cookie = cache->BeginInsert(1);
    EXPECT_TRUE(cookie.IsActive());

    EXPECT_FALSE(cache->Find(1).operator bool ());

    auto future = cache->Lookup(1);
    EXPECT_TRUE(future.operator bool());
    EXPECT_FALSE(future.IsSet());

    auto value = New<TSimpleCachedValue>(1, 10);
    cookie.EndInsert(value);

    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(future.Get().IsOK());
    EXPECT_EQ(value, future.Get().Value());
}

TEST(TAsyncSlruCacheTest, UpdateWeight)
{
    const int cacheSize = 10;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(config);

    for (int i = 0; i < cacheSize; ++i) {
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(i, i));
    }

    // All values fit in cache.
    for (int i = 0; i < cacheSize; ++i) {
        auto value = cache->Find(i);
        EXPECT_NE(value, nullptr);
        EXPECT_EQ(value->GetKey(), i);
        EXPECT_EQ(value->Value, i);
    }

    {
        // When we search '0' again, it goes to the end of the queue to be deleted.
        auto value = cache->Find(0);
        value->Weight = cacheSize;
        cache->UpdateWeight(value);
        // It should not be deleted.
        EXPECT_EQ(cache->Find(0), value);
    }

    for (int i = 1; i < cacheSize; ++i) {
        EXPECT_EQ(cache->Find(i), nullptr);
    }

    {
        auto value = New<TSimpleCachedValue>(1, 1);
        auto cookie = cache->BeginInsert(1);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(value);

        // After first insert we can not find value '1' because '0' was in 'oldest' segment.
        EXPECT_EQ(cache->Find(1), nullptr);
        // But now '0' should be moved to 'youngest' after Trim() call.
        // Second insert should delete '0' and insert '1' because it's newer.
        cookie = cache->BeginInsert(1);
        // Cookie is not active because we still hold value and it can be resurrected.
        EXPECT_FALSE(cookie.IsActive());

        // '0' is deleted, because it is too big.
        EXPECT_EQ(cache->Find(0), nullptr);
        EXPECT_EQ(cache->Find(1), value);
    }
}

TEST(TAsyncSlruCacheTest, Touch)
{
    const int cacheSize = 2;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(config);

    std::vector<TSimpleCachedValuePtr> values;

    for (int i = 0; i < cacheSize; ++i) {
        values.push_back(New<TSimpleCachedValue>(i, i));
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(values.back());
    }

    // Move v0 to touch buffer.
    cache->Touch(values[0]);

    values.push_back(New<TSimpleCachedValue>(cacheSize, cacheSize));
    auto cookie = cache->BeginInsert(cacheSize);
    EXPECT_TRUE(cookie.IsActive());
    // Move v0 to older, evict v1 and insert v2.
    cookie.EndInsert(values.back());

    EXPECT_EQ(cache->Find(0), values[0]);
    EXPECT_EQ(cache->Find(1), nullptr);
    EXPECT_EQ(cache->Find(2), values[2]);
}

TEST(TAsyncSlruCacheTest, AddRemoveWithResurrection)
{
    constexpr int cacheSize = 2;
    constexpr int valueCount = 10;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TCountingSlruCache>(std::move(config));

    std::vector<TSimpleCachedValuePtr> values;
    for (int i = 0; i < valueCount; ++i) {
        values.push_back(New<TSimpleCachedValue>(i, i));
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(values.back());
        EXPECT_EQ(cache->GetItemCount(), std::min(2, i + 1));
        EXPECT_EQ(cache->GetItemCount(), cache->GetSize());
    }

    for (int iter = 0; iter < 5; ++iter) {
        for (int i = 0; i < valueCount; ++i) {
            auto value = cache->Lookup(i)
                .Get()
                .ValueOrThrow();
            EXPECT_EQ(value->Value, i);
            EXPECT_EQ(cache->GetItemCount(), 2);
            EXPECT_EQ(cache->GetItemCount(), cache->GetSize());
        }
        for (int i = 0; i < valueCount; ++i) {
            auto cookie = cache->BeginInsert(i);
            EXPECT_TRUE(!cookie.IsActive());
            auto value = cookie.GetValue()
                .Get()
                .ValueOrThrow();
            EXPECT_EQ(value->Value, i);
            EXPECT_EQ(cache->GetItemCount(), 2);
            EXPECT_EQ(cache->GetItemCount(), cache->GetSize());
        }
    }
}

TEST(TAsyncSlruCacheTest, AddThenImmediatelyRemove)
{
    constexpr int cacheSize = 1;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TCountingSlruCache>(std::move(config));

    auto persistentValue = New<TSimpleCachedValue>(
        /*key*/ 0,
        /*value*/ 42,
        /*weight*/ 100);

    {
        auto cookie = cache->BeginInsert(0);
        cookie.EndInsert(persistentValue);
        EXPECT_EQ(cache->GetItemCount(), 0);
        EXPECT_EQ(cache->GetTotalAdded(), 1);
        EXPECT_EQ(cache->GetTotalRemoved(), 1);
    }

    {
        auto cookie = cache->BeginInsert(1);
        auto temporaryValue = New<TSimpleCachedValue>(
            /*key*/ 1,
            /*value*/ 43,
            /*weight*/ 100);
        cookie.EndInsert(temporaryValue);
        temporaryValue.Reset();
        EXPECT_EQ(cache->GetItemCount(), 0);
        EXPECT_EQ(cache->GetTotalAdded(), 2);
        EXPECT_EQ(cache->GetTotalRemoved(), 2);
    }

    {
        auto value = cache->Lookup(0)
            .Get()
            .ValueOrThrow();
        EXPECT_EQ(cache->GetItemCount(), 0);
        EXPECT_EQ(value->Value, 42);
    }

    {
        auto value = cache->Lookup(1);
        EXPECT_EQ(cache->GetItemCount(), 0);
        ASSERT_FALSE(static_cast<bool>(value));
    }
}

TEST(TAsyncSlruCacheTest, TouchRemovedValue)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TCountingSlruCache>(std::move(config), /*enableResurrection*/ true);

    auto value = New<TSimpleCachedValue>(
        /*key*/ 1,
        /*value*/ 1,
        /*weight*/ 1);
    {
        auto insertCookie = cache->BeginInsert(value->GetKey());
        ASSERT_TRUE(insertCookie.IsActive());
        insertCookie.EndInsert(value);
    }
    cache->TryRemove(value->GetKey());

    cache->Touch(value);

    auto value2 = New<TSimpleCachedValue>(
        /*key*/ 2,
        /*value*/ 2,
        /*weight*/ 1);
    {
        auto insertCookie = cache->BeginInsert(value2->GetKey());
        ASSERT_TRUE(insertCookie.IsActive());
        insertCookie.EndInsert(value2);
    }
    cache->TryRemove(value2->GetKey(), /*forbidResurrection*/ true);

    cache->Touch(value2);

    // Start and cancel insertion to forcefully drain touch buffer. If touch buffer
    // contains already freed items due to bug, they will be put into the main linked
    // list, and the bug won't be hidden. See also YT-15976.
    {
        auto insertCookie = cache->BeginInsert(3);
        ASSERT_TRUE(insertCookie.IsActive());
        insertCookie.Cancel(TError("Cancelled"));
    }
}

TEST(TAsyncSlruCacheTest, TouchEvictedValue)
{
    constexpr int cacheSize = 1;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TCountingSlruCache>(std::move(config));

    auto value = New<TSimpleCachedValue>(
        /*key*/ 1,
        /*value*/ 1,
        /*weight*/ 1);
    {
        auto insertCookie = cache->BeginInsert(value->GetKey());
        ASSERT_TRUE(insertCookie.IsActive());
        insertCookie.EndInsert(value);
    }

    // Evict value.
    auto value2 = New<TSimpleCachedValue>(
        /*key*/ 2,
        /*value*/ 2,
        /*weight*/ 1);
    {
        auto insertCookie = cache->BeginInsert(value2->GetKey());
        ASSERT_TRUE(insertCookie.IsActive());
        insertCookie.EndInsert(value2);
    }

    cache->Touch(value);

    // Start and cancel insertion to forcefully drain touch buffer. If touch buffer
    // contains already freed items due to bug, they will be put into the main linked
    // list, and the bug won't be hidden. See also YT-15976.
    {
        auto insertCookie = cache->BeginInsert(3);
        ASSERT_TRUE(insertCookie.IsActive());
        insertCookie.Cancel(TError("Cancelled"));
    }
}

////////////////////////////////////////////////////////////////////////////////

// Profiling is not supported on Windows for now.
#ifdef _unix_

TEST(TAsyncSlruGhostCacheTest, InsertSmall)
{
    constexpr int cacheSize = 10;
    constexpr int numStages = 3;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    auto oldSmallCounters = cache->ReadSmallGhostCounters();
    auto oldLargeCounters = cache->ReadLargeGhostCounters();

    for (int stage = 0; stage < numStages; ++stage) {
        for (int index = 0; index < cacheSize / 2; ++index) {
            auto cookie = cache->BeginInsert(index);
            if (!cookie.IsActive()) {
                ASSERT_NE(stage, 0);
                continue;
            }
            ASSERT_EQ(stage, 0);
            cookie.EndInsert(New<TSimpleCachedValue>(
                /*key*/ index,
                /*value*/ 42,
                /*weight*/ 1));
        }
    }

    auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
    auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

    EXPECT_EQ(smallCount.SyncHit, cacheSize / 2 * (numStages - 1));
    EXPECT_EQ(smallCount.Missed, cacheSize / 2);
    EXPECT_EQ(largeCount.SyncHit, cacheSize / 2 * (numStages - 1));
    EXPECT_EQ(largeCount.Missed, cacheSize / 2);
}

TEST(TAsyncSlruGhostCacheTest, InsertLarge)
{
    constexpr int cacheSize = 10;
    constexpr int numStages = 3;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    auto oldSmallCounters = cache->ReadSmallGhostCounters();
    auto oldLargeCounters = cache->ReadLargeGhostCounters();

    for (int stage = 0; stage < numStages; ++stage) {
        for (int index = 0; index < 2 * cacheSize; ++index) {
            auto cookie = cache->BeginInsert(index);
            ASSERT_TRUE(cookie.IsActive());
            cookie.EndInsert(New<TSimpleCachedValue>(
                /*key*/ index,
                /*value*/ 42,
                /*weight*/ 1));
        }
    }

    auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
    auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

    EXPECT_EQ(smallCount.SyncHit, 0);
    EXPECT_EQ(smallCount.Missed, 2 * cacheSize * numStages);
    EXPECT_EQ(largeCount.SyncHit, 2 * cacheSize * (numStages - 1));
    EXPECT_EQ(largeCount.Missed, 2 * cacheSize);
}

TEST(TAsyncSlruGhostCacheTest, Weights)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    auto value = New<TSimpleCachedValue>(
        /*key*/ 1,
        /*value*/ 42,
        /*weight*/ 64);

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        auto firstCookie = cache->BeginInsert(1);
        ASSERT_TRUE(firstCookie.IsActive());

        auto secondCookie = cache->BeginInsert(1);
        ASSERT_TRUE(!secondCookie.IsActive());

        firstCookie.EndInsert(value);

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 0);
        EXPECT_EQ(smallCount.AsyncHit, 1);
        EXPECT_EQ(smallCount.AsyncHitWeight, 64);
        EXPECT_EQ(smallCount.Missed, 1);
        EXPECT_EQ(smallCount.MissedWeight, 64);

        EXPECT_EQ(largeCount.SyncHit, 0);
        EXPECT_EQ(largeCount.AsyncHit, 1);
        EXPECT_EQ(largeCount.AsyncHitWeight, 64);
        EXPECT_EQ(largeCount.Missed, 1);
        EXPECT_EQ(largeCount.MissedWeight, 64);
    }

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        value->Weight = 90;
        value->UpdateWeight();

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 0);
        EXPECT_EQ(smallCount.AsyncHit, 0);
        EXPECT_EQ(smallCount.AsyncHitWeight, 0);
        EXPECT_EQ(smallCount.Missed, 0);
        EXPECT_EQ(smallCount.MissedWeight, 0);

        EXPECT_EQ(largeCount.SyncHit, 0);
        EXPECT_EQ(largeCount.AsyncHit, 0);
        EXPECT_EQ(largeCount.AsyncHitWeight, 0);
        EXPECT_EQ(largeCount.Missed, 0);
        EXPECT_EQ(largeCount.MissedWeight, 26);
    }
}

TEST(TAsyncSlruGhostCacheTest, Lookups)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    for (int index = 0; index < 6; ++index) {
        auto cookie = cache->BeginInsert(index);
        ASSERT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ index,
            /*value*/ 42,
            /*weight*/ 50));
    }

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        for (int index = 0; index < 6; ++index) {
            YT_UNUSED_FUTURE(cache->Lookup(index));
        }

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 1);
        EXPECT_EQ(smallCount.SyncHitWeight, 50);
        EXPECT_EQ(smallCount.Missed, 5);

        EXPECT_EQ(largeCount.SyncHit, 4);
        EXPECT_EQ(largeCount.SyncHitWeight, 200);
        EXPECT_EQ(largeCount.Missed, 2);
    }
}

TEST(TAsyncSlruGhostCacheTest, MoveConstructCookie)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    for (int index = 0; index < 5; ++index) {
        auto originalCookie = cache->BeginInsert(index);
        ASSERT_TRUE(originalCookie.IsActive());

        auto newCookie = std::move(originalCookie);
        ASSERT_FALSE(originalCookie.IsActive());
        ASSERT_TRUE(newCookie.IsActive());

        newCookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ index,
            /*value*/ 42,
            /*weight*/ 1));
    }

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        for (int index = 0; index < 5; ++index) {
            YT_UNUSED_FUTURE(cache->Lookup(index));
        }

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 5);
        EXPECT_EQ(smallCount.AsyncHit, 0);
        EXPECT_EQ(smallCount.Missed, 0);

        EXPECT_EQ(largeCount.SyncHit, 5);
        EXPECT_EQ(largeCount.AsyncHit, 0);
        EXPECT_EQ(largeCount.Missed, 0);
    }
}

TEST(TAsyncSlruGhostCacheTest, MoveAssignCookie)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    // Ensure that all the necessary items are present in large ghost, but absent in main
    // cache and small ghost.
    for (int index = 0; index < 5; ++index) {
        auto cookie = cache->BeginInsert(index);
        ASSERT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ index,
            /*value*/ 42,
            /*weight*/ 1));
    }
    {
        auto cookie = cache->BeginInsert(43);
        ASSERT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ 43,
            /*value*/ 100500,
            /*weight*/ 101));
    }

    for (int index = 0; index < 5; ++index) {
        auto otherCookie = cache->BeginInsert(index);
        ASSERT_TRUE(otherCookie.IsActive());

        auto cookie = cache->BeginInsert(42);
        ASSERT_TRUE(cookie.IsActive());

        cookie = std::move(otherCookie);
        ASSERT_FALSE(otherCookie.IsActive());
        ASSERT_TRUE(cookie.IsActive());

        cookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ index,
            /*value*/ 42,
            /*weight*/ 1));
    }

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        for (int index = 0; index < 5; ++index) {
            YT_UNUSED_FUTURE(cache->Lookup(index));
        }

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 5);
        EXPECT_EQ(smallCount.AsyncHit, 0);
        EXPECT_EQ(smallCount.Missed, 0);

        EXPECT_EQ(largeCount.SyncHit, 5);
        EXPECT_EQ(largeCount.AsyncHit, 0);
        EXPECT_EQ(largeCount.Missed, 0);
    }
}

TEST(TAsyncSlruGhostCacheTest, Disable)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        auto cookie = cache->BeginInsert(1);
        ASSERT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ 1,
            /*value*/ 42,
            /*weight*/ 1));

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 0);
        EXPECT_EQ(smallCount.AsyncHit, 0);
        EXPECT_EQ(smallCount.Missed, 1);

        EXPECT_EQ(largeCount.SyncHit, 0);
        EXPECT_EQ(largeCount.AsyncHit, 0);
        EXPECT_EQ(largeCount.Missed, 1);
    }

    auto dynamicConfig = New<TSlruCacheDynamicConfig>();
    dynamicConfig->EnableGhostCaches = false;
    cache->Reconfigure(dynamicConfig);

    {
        auto oldSmallCounters = cache->ReadSmallGhostCounters();
        auto oldLargeCounters = cache->ReadLargeGhostCounters();

        auto cookie = cache->BeginInsert(2);
        ASSERT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(
            /*key*/ 2,
            /*value*/ 57,
            /*weight*/ 1));

        auto value1 = cache->Find(1);
        ASSERT_NE(value1, nullptr);
        ASSERT_EQ(value1->Value, 42);

        auto value2 = cache->Lookup(2);
        ASSERT_TRUE(value2.IsSet());
        ASSERT_TRUE(value2.Get().IsOK());
        ASSERT_EQ(value2.Get().Value()->Value, 57);

        auto smallCount = cache->ReadSmallGhostCounters() - oldSmallCounters;
        auto largeCount = cache->ReadLargeGhostCounters() - oldLargeCounters;

        EXPECT_EQ(smallCount.SyncHit, 0);
        EXPECT_EQ(smallCount.AsyncHit, 0);
        EXPECT_EQ(smallCount.Missed, 0);

        EXPECT_EQ(largeCount.SyncHit, 0);
        EXPECT_EQ(largeCount.AsyncHit, 0);
        EXPECT_EQ(largeCount.Missed, 0);
    }
}

TEST(TAsyncSlruGhostCacheTest, ReconfigureTrim)
{
    constexpr int cacheSize = 100;
    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TSimpleSlruCache>(std::move(config), TProfiler{"/cache"});

    for (int i = 0; i < cacheSize; ++i) {
        auto cookie = cache->BeginInsert(i);
        EXPECT_TRUE(cookie.IsActive());
        cookie.EndInsert(New<TSimpleCachedValue>(i, i));
    }

    EXPECT_EQ(cacheSize, cache->GetSize());
    EXPECT_EQ(cacheSize, cache->GetCapacity());

    const int newCacheSize = 30;
    auto dynamicConfig = New<TSlruCacheDynamicConfig>();
    dynamicConfig->Capacity = newCacheSize;
    cache->Reconfigure(dynamicConfig);

    EXPECT_EQ(newCacheSize, cache->GetSize());
    EXPECT_EQ(newCacheSize, cache->GetCapacity());
}

#endif

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStressOperation,
    ((Find) (0))
    ((Lookup) (1))
    ((Touch) (2))
    ((BeginInsert) (3))
    ((CancelInsert) (4))
    ((EndInsert) (5))
    ((TryRemove) (6))
    ((UpdateWeight) (7))
    ((ReleaseValue) (8))
    ((Reconfigure) (9))
);

class TAsyncSlruCacheStressTest
    : public ::testing::TestWithParam<bool>
{ };

TEST_P(TAsyncSlruCacheStressTest, Stress)
{
    constexpr int cacheSize = 100;
    constexpr int stepCount = 1'000'000;
    constexpr double forbidResurrectionProbability = 0.25;

    const bool enableResurrection = GetParam();

    auto config = CreateCacheConfig(cacheSize);
    auto cache = New<TCountingSlruCache>(std::move(config), enableResurrection);

    // Use a fixed-seed random generator for deterministic testing.
    std::mt19937 randomGenerator(142857);

    auto operationDomainValues = TEnumTraits<EStressOperation>::GetDomainValues();
    std::vector<EStressOperation> operations(operationDomainValues.begin(), operationDomainValues.end());
    if (!enableResurrection) {
        operations.erase(
            std::find(operations.begin(), operations.end(), EStressOperation::ReleaseValue));
    }

    std::uniform_int_distribution<int> weightDistribution(1, 10);
    std::uniform_int_distribution<int> keyDistribution(1, 20);
    std::uniform_int_distribution<int> capacityDistribution(50, 150);
    std::uniform_real_distribution<double> youngerSizeFractionDistribution(0.0, 1.0);

    std::vector<TCountingSlruCache::TInsertCookie> activeInsertCookies;

    // Pointers to all the values that are either present in cache now or were in cache
    // earlier. We hold weak pointers, allowing the values to be deleted to prevent their
    // resurrection.
    std::vector<TWeakPtr<TSimpleCachedValue>> cacheValues;

    // Holds references to some of the values. This is needed to allow resurrection. Used
    // only if enableResurrection is true.
    std::vector<TSimpleCachedValuePtr> heldValues;

    // For each key, stores the last inserted value with key. Can be null if we are sure
    // that there is no value with the given key in the cache.
    THashMap<int, TWeakPtr<TSimpleCachedValue>> lastInsertedValues;

    auto pickCacheValue = [&] () -> TSimpleCachedValuePtr {
        while (!cacheValues.empty()) {
            size_t cacheValueIndex = randomGenerator() % cacheValues.size();
            std::swap(cacheValues[cacheValueIndex], cacheValues.back());
            auto value = cacheValues.back().Lock();
            if (value) {
                return value;
            }
            cacheValues.pop_back();
        }
        return nullptr;
    };

    for (int step = 0; step < stepCount; ++step) {
        auto operation = operations[randomGenerator() % operations.size()];

        switch (operation) {
            case EStressOperation::Find: {
                auto value = cache->Find(keyDistribution(randomGenerator));
                if (value) {
                    ASSERT_EQ(lastInsertedValues[value->GetKey()].Lock(), value);
                }
                break;
            }
            case EStressOperation::Lookup: {
                auto key = keyDistribution(randomGenerator);
                auto valueFuture = cache->Lookup(key);
                if (!valueFuture) {
                    break;
                }
                if (valueFuture.IsSet()) {
                    ASSERT_TRUE(valueFuture.Get().IsOK());
                    const auto& value = valueFuture.Get().Value();
                    ASSERT_EQ(lastInsertedValues[key].Lock(), value);
                } else {
                    // The value insertion is in progress, so lastInsertedValues must contain nullptr
                    // for our key.
                    ASSERT_EQ(lastInsertedValues[key].Lock(), nullptr);
                }
                break;
            }
            case EStressOperation::Touch: {
                auto value = pickCacheValue();
                if (!value) {
                    break;
                }
                cache->Touch(value);
                break;
            }
            case EStressOperation::BeginInsert: {
                int key = keyDistribution(randomGenerator);
                auto cookie = cache->BeginInsert(key);
                if (cookie.IsActive()) {
                    activeInsertCookies.emplace_back(std::move(cookie));
                    lastInsertedValues[key] = nullptr;
                } else {
                    auto valueFuture = cookie.GetValue();
                    ASSERT_TRUE(static_cast<bool>(valueFuture));
                    if (valueFuture.IsSet()) {
                        ASSERT_TRUE(valueFuture.Get().IsOK());
                        const auto& value = valueFuture.Get().Value();
                        ASSERT_EQ(lastInsertedValues[value->GetKey()].Lock(), value);
                    } else {
                        // The value insertion is in progress, so lastInsertedValues must contain nullptr
                        // for our key.
                        ASSERT_EQ(lastInsertedValues[key].Lock(), nullptr);
                    }
                }
                break;
            }
            case EStressOperation::EndInsert: {
                if (activeInsertCookies.empty()) {
                    break;
                }
                size_t cookieIndex = randomGenerator() % activeInsertCookies.size();
                std::swap(activeInsertCookies[cookieIndex], activeInsertCookies.back());
                auto value = New<TSimpleCachedValue>(
                    /*key*/ activeInsertCookies.back().GetKey(),
                    /*value*/ step,
                    /*weight*/ weightDistribution(randomGenerator));
                cacheValues.emplace_back(value);
                if (enableResurrection) {
                    heldValues.push_back(value);
                }
                lastInsertedValues[value->GetKey()] = value;
                ASSERT_TRUE(activeInsertCookies.back().IsActive());
                activeInsertCookies.back().EndInsert(std::move(value));
                activeInsertCookies.pop_back();
                break;
            }
            case EStressOperation::CancelInsert: {
                if (activeInsertCookies.empty()) {
                    break;
                }
                size_t cookieIndex = randomGenerator() % activeInsertCookies.size();
                std::swap(activeInsertCookies[cookieIndex], activeInsertCookies.back());
                ASSERT_TRUE(activeInsertCookies.back().IsActive());
                activeInsertCookies.back().Cancel(TError("Cancelled"));
                activeInsertCookies.pop_back();
                break;
            }
            case EStressOperation::TryRemove: {
                std::bernoulli_distribution distribution(forbidResurrectionProbability);
                bool forbidResurrection = distribution(randomGenerator);
                auto key = keyDistribution(randomGenerator);
                cache->TryRemove(key, forbidResurrection);
                if (!enableResurrection || forbidResurrection) {
                    lastInsertedValues[key] = nullptr;
                }
                break;
            }
            case EStressOperation::UpdateWeight: {
                auto value = pickCacheValue();
                if (!value) {
                    break;
                }
                value->Weight = weightDistribution(randomGenerator);
                value->UpdateWeight();
                break;
            }
            case EStressOperation::ReleaseValue: {
                if (heldValues.empty()) {
                    break;
                }
                size_t valueIndex = randomGenerator() % heldValues.size();
                std::swap(heldValues[valueIndex], heldValues.back());
                heldValues.pop_back();
                break;
            }
            case EStressOperation::Reconfigure: {
                auto config = New<TSlruCacheDynamicConfig>();
                config->Capacity = capacityDistribution(randomGenerator);
                config->YoungerSizeFraction = youngerSizeFractionDistribution(randomGenerator);
                cache->Reconfigure(std::move(config));
                break;
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Stress, TAsyncSlruCacheStressTest, ::testing::Values(false, true));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
