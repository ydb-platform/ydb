#include <gtest/gtest.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <util/random/fast.h>

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

TSharedRef CreateRandomReference(TFastRng64& rnd, i64 size)
{
    TString s;
    s.resize(size, '*');

    for (i64 index = 0; index < size; ++index) {
        s[index] = (char)rnd.GenRand64();
    }

    auto output = TSharedRef::FromString(s);
    YT_ASSERT(static_cast<i64>(output.Size()) == size);
    return output;
}

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

TEST(TSyncSlruCache, EntryWeightUpdate)
{
    TFastRng64 rng(27);
    auto config = New<TSlruCacheConfig>();
    config->Capacity = 1000;

    auto cache = New<TTestCache>(config);
    for (; cache->GetSize() < 990;) {
        cache->TryInsert(New<TTestValue>(TString(CreateRandomReference(rng,  256).ToStringBuf())));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
