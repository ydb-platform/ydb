#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/bloom_filter.h>
#include <yt/yt_proto/yt/core/misc/proto/bloom_filter.pb.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <random>
#include <unordered_set>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 Random()
{
    static std::random_device random;
    static std::mt19937_64 generator(random());
    static std::uniform_int_distribution<ui64> uniform;
    return uniform(generator);
}

TBloomFilter FromBuilder(const TBloomFilterBuilder& bloomBuilder)
{
    NProto::TBloomFilter bloomProto;
    TBloomFilter bloomFilter;
    ToProto(&bloomProto, bloomBuilder);
    FromProto(&bloomFilter, bloomProto);
    return bloomFilter;
}

TEST(TBloomFilterTest, Null)
{
    auto bloom = FromBuilder(TBloomFilterBuilder(65636, 0.03));

    for (int index = 0; index < 1000; ++index){
        EXPECT_FALSE(bloom.Contains(Random()));
    }
}

TEST(TBloomFilterTest, Simple)
{
    TBloomFilterBuilder bloomBuilder(65636, 0.03);
    auto items = std::vector<ui64>{0,1,2};

    for (const auto item : items) {
        bloomBuilder.Insert(item);
    }

    auto bloom = FromBuilder(bloomBuilder);

    for (const auto item : items) {
        EXPECT_TRUE(bloom.Contains(item));
    }

    auto size = bloomBuilder.EstimateSize();
    EXPECT_EQ(size, 4);
    bloomBuilder.Shrink();
    EXPECT_EQ(bloomBuilder.Size(), size);

    bloom = FromBuilder(bloomBuilder);

    for (const auto item : items) {
        EXPECT_TRUE(bloom.Contains(item));
    }
}

TEST(TBloomFilterTest, FalsePositiveRate)
{
    TBloomFilterBuilder bloomBuilder(65636, 0.03);
    auto items = std::unordered_set<ui64>();

    for (int index = 0; index < 1000; ++index){
        auto item = Random();
        items.insert(item);
        bloomBuilder.Insert(item);
    }

    auto bloom = FromBuilder(bloomBuilder);

    for (const auto item : items) {
        EXPECT_TRUE(bloom.Contains(item));
    }

    bloom = FromBuilder(bloomBuilder);

    int falsePositiveCount = 0;
    int lookupCount = 10000;
    for (int index = 0; index < lookupCount; ++index) {
        auto item = Random();
        if (items.find(item) == items.end() && bloom.Contains(item)) {
            ++falsePositiveCount;
        }
    }

    EXPECT_LT(static_cast<double>(falsePositiveCount) / lookupCount, 0.05);

    bloomBuilder.Shrink();

    EXPECT_LE(bloomBuilder.Size(), 8192);

    bloom = FromBuilder(bloomBuilder);

    falsePositiveCount = 0;
    lookupCount = 10000;
    for (int index = 0; index < lookupCount; ++index) {
        auto item = Random();
        if (items.find(item) == items.end() && bloom.Contains(item)) {
            ++falsePositiveCount;
        }
    }

    EXPECT_LT(static_cast<double>(falsePositiveCount) / lookupCount, 0.05);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

