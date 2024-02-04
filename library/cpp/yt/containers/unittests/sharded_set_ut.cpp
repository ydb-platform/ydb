#include <library/cpp/yt/containers/sharded_set.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TIntToShard
{
    int operator()(int value) const
    {
        return value % 16;
    }
};

using TSet = TShardedSet<int, 16, TIntToShard>;

////////////////////////////////////////////////////////////////////////////////

TEST(TShardedSetTest, Insert)
{
    TSet set;

    for (int i = 0; i < 4; i++) {
        set.insert(i);
    }

    for (int i = 0; i < 4; i++) {
        set.insert(i);
    }

    EXPECT_EQ(4u, set.size());

    for (int i = 0; i < 4; i++)
    EXPECT_EQ(1u, set.count(i));

    EXPECT_EQ(0u, set.count(4));
}

TEST(TShardedSetTest, Erase)
{
    TSet set;

    for (int i = 0; i < 8; i++) {
        set.insert(i);
    }

    EXPECT_EQ(8u, set.size());

    // Remove elements one by one and check if all other elements are still there.
    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(1u, set.count(i));
        EXPECT_TRUE(set.erase(i));
        EXPECT_EQ(0u, set.count(i));
        EXPECT_EQ(8u - i - 1, set.size());
        for (int j = i + 1; j < 8; j++) {
            EXPECT_EQ(1u, set.count(j));
        }
    }

    EXPECT_EQ(0u, set.count(8));
}

TEST(TShardedSetTest, StressTest)
{
    TSet set;

    constexpr int Iterations = 1'000'000;
    constexpr int Values = 128;

    THashSet<int> values;

    auto checkEverything = [&] {
        EXPECT_EQ(values.size(), set.size());
        EXPECT_EQ(values.empty(), set.empty());
        EXPECT_EQ(values, THashSet<int>(set.begin(), set.end()));

        std::array<THashSet<int>, 16> shards;
        for (int value : values) {
            shards[value % 16].insert(value);
        }
        for (int shardIndex = 0; shardIndex < 16; ++shardIndex) {
            EXPECT_EQ(shards[shardIndex], set.Shard(shardIndex));
        }

        for (int value = 0; value < Values; ++value) {
            EXPECT_EQ(values.contains(value), set.contains(value));
            EXPECT_EQ(values.count(value), set.count(value));
        }
    };

    std::mt19937_64 rng(42);

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        if (rng() % 100 == 0) {
            set.clear();
            values.clear();
            checkEverything();
        }

        int value = rng() % Values;
        if (rng() % 2 == 0) {
            set.insert(value);
            values.insert(value);
        } else {
            set.erase(value);
            values.erase(value);
        }

        checkEverything();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
