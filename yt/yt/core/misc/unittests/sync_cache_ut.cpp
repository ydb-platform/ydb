#include <gtest/gtest.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/library/profiling/testing.h>

#include <util/random/fast.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTestValue)

struct TTestValue
    : TSyncCacheValueBase<std::string, TTestValue>
{
public:
    using TSyncCacheValueBase::TSyncCacheValueBase;

    i64 Weight = 1;
};

DEFINE_REFCOUNTED_TYPE(TTestValue)

class TTestCache
    : public TSyncSlruCacheBase<std::string, TTestValue>
{
public:
    using TSyncSlruCacheBase::TSyncSlruCacheBase;

    i64 GetRejectedOversizedCount() const
    {
        return NProfiling::TTesting::ReadCounter(GetRejectedOversizedCounter());
    }

    i64 GetRejectedOversizedWeight() const
    {
        return NProfiling::TTesting::ReadCounter(GetRejectedOversizedWeightCounter());
    }

    i64 GetEvictedCount() const
    {
        return NProfiling::TTesting::ReadCounter(GetEvictedCounter());
    }

    i64 GetEvictedWeight() const
    {
        return NProfiling::TTesting::ReadCounter(GetEvictedWeightCounter());
    }

    virtual i64 GetWeight(const TTestValuePtr& value) const
    {
        return value->Weight;
    }
};

TSharedRef CreateRandomReference(TFastRng64& rnd, i64 size)
{
    std::string s;
    s.resize(size, '*');

    for (i64 index = 0; index < size; ++index) {
        s[index] = (char)rnd.GenRand64();
    }

    auto output = TSharedRef::FromString(s);
    YT_ASSERT(std::ssize(output) == size);
    return output;
}

TEST(TSyncSlruCacheTest, DownsizeSegfault)
{
    auto config = New<TSlruCacheConfig>();
    config->Capacity = 100;

    auto cache = New<TTestCache>(config);
    for (int i = 0; i < 100; i++) {
        cache->TryInsert(New<TTestValue>(ToString(i)));
    }

    for (int i = 0; i < 10; i++) {
        cache->Find(ToString(i));
    }

    auto smallConfig = New<TSlruCacheDynamicConfig>();
    smallConfig->Capacity = 50;
    cache->Reconfigure(smallConfig);

    for (int i = 0; i < 100; i++) {
        cache->TryInsert(New<TTestValue>(ToString(i + 100)));
    }
}

TEST(TSyncSlruCacheTest, EntryWeightUpdate)
{
    TFastRng64 rng(27);
    auto config = New<TSlruCacheConfig>();
    config->Capacity = 1000;

    auto cache = New<TTestCache>(config);
    for (; cache->GetSize() < 990;) {
        cache->TryInsert(New<TTestValue>(std::string(CreateRandomReference(rng,  256).ToStringBuf())));
    }

    EXPECT_GE(990, cache->GetSize());

    for (auto& value : cache->GetAll()) {
        value->Weight *= 2;
        cache->UpdateWeight(value);
    }

    EXPECT_GE(500, cache->GetSize());

    for (auto& value : cache->GetAll()) {
        value->Weight *= 2;
        cache->UpdateWeight(value);
    }

    EXPECT_GE(250, cache->GetSize());
}

TEST(TSyncSlruCacheTest, HeterogeneousLookup)
{
    auto config = New<TSlruCacheConfig>();
    config->Capacity = 100;

    auto cache = New<TTestCache>(config);
    cache->TryInsert(New<TTestValue>(std::string("alpha")));
    cache->TryInsert(New<TTestValue>(std::string("beta")));

    // Lookup via TStringBuf must not materialize a std::string key.
    EXPECT_TRUE(cache->Find(TStringBuf("alpha")));
    EXPECT_TRUE(cache->Find(TStringBuf("beta")));
    EXPECT_FALSE(cache->Find(TStringBuf("gamma")));
}

TEST(TSyncSlruCacheTest, RejectsOversizedValue)
{
    constexpr int cacheCapacity = 10;
    constexpr int oversizedValueWeight = cacheCapacity + 1;

    auto config = TSlruCacheConfig::CreateWithCapacity(cacheCapacity, /*shardCount*/ 1);
    config->RejectOversizedItems = true;

    NProfiling::TProfiler profiler{"/sync_slru_cache_test"};
    auto cache = New<TTestCache>(config, profiler);
    auto oldRejectedOversized = cache->GetRejectedOversizedCount();
    auto oldRejectedOversizedWeight = cache->GetRejectedOversizedWeight();
    auto oldEvicted = cache->GetEvictedCount();
    auto oldEvictedWeight = cache->GetEvictedWeight();

    auto cachedValue = New<TTestValue>("cached");
    cachedValue->Weight = cacheCapacity;
    EXPECT_TRUE(cache->TryInsert(cachedValue));

    auto oversizedValue = New<TTestValue>("oversized");
    oversizedValue->Weight = oversizedValueWeight;
    EXPECT_TRUE(cache->TryInsert(oversizedValue));

    EXPECT_EQ(cache->GetSize(), 1);
    EXPECT_EQ(cache->Find(cachedValue->GetKey()), cachedValue);
    EXPECT_FALSE(cache->Find(oversizedValue->GetKey()));
    EXPECT_EQ(cache->GetRejectedOversizedCount() - oldRejectedOversized, 1);
    EXPECT_EQ(cache->GetRejectedOversizedWeight() - oldRejectedOversizedWeight, oversizedValueWeight);
    EXPECT_EQ(cache->GetEvictedCount() - oldEvicted, 0);
    EXPECT_EQ(cache->GetEvictedWeight() - oldEvictedWeight, 0);
}

TEST(TSyncSlruCacheTest, OversizedValueIsEvictedWhenRejectionIsDisabled)
{
    constexpr int cacheCapacity = 1;
    constexpr int oversizedValueWeight = cacheCapacity + 1;

    auto config = TSlruCacheConfig::CreateWithCapacity(cacheCapacity, /*shardCount*/ 1);

    NProfiling::TProfiler profiler{"/sync_slru_cache_disabled_rejection_test"};
    auto cache = New<TTestCache>(config, profiler);
    auto oldRejectedOversized = cache->GetRejectedOversizedCount();
    auto oldEvicted = cache->GetEvictedCount();
    auto oldEvictedWeight = cache->GetEvictedWeight();

    auto oversizedValue = New<TTestValue>("oversized");
    oversizedValue->Weight = oversizedValueWeight;
    EXPECT_TRUE(cache->TryInsert(oversizedValue));

    EXPECT_EQ(cache->GetSize(), 0);
    EXPECT_FALSE(cache->Find(oversizedValue->GetKey()));
    EXPECT_EQ(cache->GetRejectedOversizedCount() - oldRejectedOversized, 0);
    EXPECT_EQ(cache->GetEvictedCount() - oldEvicted, 1);
    EXPECT_EQ(cache->GetEvictedWeight() - oldEvictedWeight, oversizedValueWeight);
}

TEST(TSyncSlruCacheTest, TracksEvictedValues)
{
    auto config = TSlruCacheConfig::CreateWithCapacity(/*capacity*/ 2, /*shardCount*/ 1);

    NProfiling::TProfiler profiler{"/sync_slru_cache_eviction_test"};
    auto cache = New<TTestCache>(config, profiler);
    auto oldEvicted = cache->GetEvictedCount();
    auto oldEvictedWeight = cache->GetEvictedWeight();

    auto firstValue = New<TTestValue>("first");
    firstValue->Weight = 2;
    EXPECT_TRUE(cache->TryInsert(firstValue));

    auto secondValue = New<TTestValue>("second");
    EXPECT_TRUE(cache->TryInsert(secondValue));

    EXPECT_EQ(cache->GetSize(), 1);
    EXPECT_FALSE(cache->Find(firstValue->GetKey()));
    EXPECT_EQ(cache->Find(secondValue->GetKey()), secondValue);
    EXPECT_EQ(cache->GetEvictedCount() - oldEvicted, 1);
    EXPECT_EQ(cache->GetEvictedWeight() - oldEvictedWeight, firstValue->Weight);
}

TEST(TSyncSlruCacheTest, EnablesOversizedRejectionDynamically)
{
    auto config = TSlruCacheConfig::CreateWithCapacity(/*capacity*/ 1, /*shardCount*/ 1);

    NProfiling::TProfiler profiler{"/sync_slru_cache_dynamic_rejection_test"};
    auto cache = New<TTestCache>(config, profiler);
    auto oldRejectedOversized = cache->GetRejectedOversizedCount();

    auto dynamicConfig = New<TSlruCacheDynamicConfig>();
    dynamicConfig->RejectOversizedItems = true;
    cache->Reconfigure(dynamicConfig);

    auto oversizedValue = New<TTestValue>("oversized");
    oversizedValue->Weight = 2;
    EXPECT_TRUE(cache->TryInsert(oversizedValue));

    EXPECT_FALSE(cache->Find(oversizedValue->GetKey()));
    EXPECT_EQ(cache->GetRejectedOversizedCount() - oldRejectedOversized, 1);
}

TEST(TSyncSlruCacheTest, DisablesOversizedRejectionDynamically)
{
    constexpr int cacheCapacity = 1;
    constexpr int oversizedValueWeight = cacheCapacity + 1;

    auto config = TSlruCacheConfig::CreateWithCapacity(cacheCapacity, /*shardCount*/ 1);
    config->RejectOversizedItems = true;

    NProfiling::TProfiler profiler{"/sync_slru_cache_dynamic_disable_rejection_test"};
    auto cache = New<TTestCache>(config, profiler);

    auto rejectedValue = New<TTestValue>("rejected");
    rejectedValue->Weight = oversizedValueWeight;
    EXPECT_TRUE(cache->TryInsert(rejectedValue));
    EXPECT_FALSE(cache->Find(rejectedValue->GetKey()));

    auto rejectedOversized = cache->GetRejectedOversizedCount();
    auto evicted = cache->GetEvictedCount();

    auto dynamicConfig = New<TSlruCacheDynamicConfig>();
    dynamicConfig->RejectOversizedItems = false;
    cache->Reconfigure(dynamicConfig);

    auto evictedValue = New<TTestValue>("evicted");
    evictedValue->Weight = oversizedValueWeight;
    EXPECT_TRUE(cache->TryInsert(evictedValue));

    EXPECT_FALSE(cache->Find(evictedValue->GetKey()));
    EXPECT_EQ(cache->GetRejectedOversizedCount(), rejectedOversized);
    EXPECT_EQ(cache->GetEvictedCount() - evicted, 1);
}

TEST(TSyncSlruCacheTest, RejectsValuesByShardAndOlderOccupancy)
{
    constexpr int shardCount = 2;
    constexpr int shardCapacity = 4;
    constexpr int cacheCapacity = shardCount * shardCapacity;

    auto config = TSlruCacheConfig::CreateWithCapacity(cacheCapacity, shardCount);
    config->RejectOversizedItems = true;

    NProfiling::TProfiler profiler{"/sync_slru_cache_sharded_rejection_test"};
    auto cache = New<TTestCache>(config, profiler);
    auto oldRejectedOversized = cache->GetRejectedOversizedCount();

    auto makeKeyForShard = [=] (TStringBuf prefix, int shardIndex) {
        for (int index = 0; ; ++index) {
            auto key = Format("%v-%v", prefix, index);
            if (THash<std::string>()(key) % shardCount == shardIndex) {
                return key;
            }
        }
    };

    auto olderValue = New<TTestValue>(makeKeyForShard("older", /*shardIndex*/ 0));
    olderValue->Weight = shardCapacity - 1;
    EXPECT_TRUE(cache->TryInsert(olderValue));
    EXPECT_EQ(cache->Find(olderValue->GetKey()), olderValue);

    auto rejectedByOlderOccupancy = New<TTestValue>(
        makeKeyForShard("rejected-by-older", /*shardIndex*/ 0));
    rejectedByOlderOccupancy->Weight = 2;
    EXPECT_TRUE(cache->TryInsert(rejectedByOlderOccupancy));
    EXPECT_FALSE(cache->Find(rejectedByOlderOccupancy->GetKey()));

    auto admittedAtBoundary = New<TTestValue>(
        makeKeyForShard("admitted-at-boundary", /*shardIndex*/ 1));
    admittedAtBoundary->Weight = shardCapacity;
    EXPECT_TRUE(cache->TryInsert(admittedAtBoundary));
    EXPECT_EQ(cache->Find(admittedAtBoundary->GetKey()), admittedAtBoundary);

    auto rejectedAboveBoundary = New<TTestValue>(
        makeKeyForShard("rejected-above-boundary", /*shardIndex*/ 1));
    rejectedAboveBoundary->Weight = shardCapacity + 1;
    EXPECT_TRUE(cache->TryInsert(rejectedAboveBoundary));
    EXPECT_FALSE(cache->Find(rejectedAboveBoundary->GetKey()));

    EXPECT_EQ(cache->GetSize(), 2);
    EXPECT_EQ(cache->Find(olderValue->GetKey()), olderValue);
    EXPECT_EQ(cache->Find(admittedAtBoundary->GetKey()), admittedAtBoundary);
    EXPECT_EQ(cache->GetRejectedOversizedCount() - oldRejectedOversized, 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
