#include <gtest/gtest.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTestValue)

struct TTestValue
    : TSyncCacheValueBase<TString, TTestValue>
{
public:
    using TSyncCacheValueBase::TSyncCacheValueBase;

    i64 Weight = 1;
};

DEFINE_REFCOUNTED_TYPE(TTestValue)

class TTestCache
    : public TSyncSlruCacheBase<TString, TTestValue>
{
public:
    using TSyncSlruCacheBase::TSyncSlruCacheBase;

    virtual i64 GetWeight(const TTestValuePtr& value) const
    {
        return value->Weight;
    }
};

TEST(TSyncSlruCache, DownsizeSegfault)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
