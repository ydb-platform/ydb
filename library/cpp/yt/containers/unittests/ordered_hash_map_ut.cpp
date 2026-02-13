#include <library/cpp/yt/containers/ordered_hash_map.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TInt
{
    std::vector<int> Data;

    explicit TInt(int value = 0)
        : Data(1, value)
    { }

    operator int () const
    {
        return Data[0];
    }
};

TInt operator""_i(unsigned long long n)
{
    return TInt(n);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TOrderedHashMapTest, Trivial)
{
    TOrderedHashMap<TInt, TInt> map;

    {
        auto [it, success] =  map.emplace(1_i, 10_i);
        EXPECT_TRUE(success);
        EXPECT_EQ(1_i, it->first);
        EXPECT_EQ(10_i, it->second);
    }

    {
        auto [it, success] =  map.emplace(1_i, 20_i);
        EXPECT_FALSE(success);
        EXPECT_EQ(1_i, it->first);
        EXPECT_EQ(10_i, it->second);
    }

    {
        auto it = map.find(2_i);
        EXPECT_EQ(map.end(), it);
    }

    {
        auto it = map.find(1_i);
        EXPECT_EQ(map.begin(), it);
        EXPECT_EQ(1_i, it->first);
        EXPECT_EQ(10_i, it->second);
    }

    {
        auto result = map.erase(1_i);
        EXPECT_TRUE(result);
        EXPECT_EQ(map.end(), map.find(1_i));
    }

    {
        map.emplace(2_i, 20_i);
        auto it = map.find(2_i);
        EXPECT_EQ(20_i, it->second);
        map.erase(it);
        EXPECT_EQ(map.end(), map.find(2_i));
    }
}

TEST(TOrderedHashMapTest, Order)
{
    std::vector<std::pair<TInt, TInt>> elements;
    TOrderedHashMap<TInt, TInt> map;
    int n = 100;

    for (int i = 0; i < n; ++i) {
        elements.emplace_back(TInt(i * 13 % (n + 1)), TInt(i * 10));
        map.emplace(elements.back().first, elements.back().second);
    }

    auto check = [&] {
        ASSERT_EQ(elements.size(), map.size());

        auto vecIt = elements.begin();
        auto mapIt = map.begin();
        while (vecIt != elements.end() && mapIt != map.end()) {
            ASSERT_EQ(vecIt->first, mapIt->first);
            ASSERT_EQ(vecIt->second, mapIt->second);
            ++vecIt;
            ++mapIt;
        }
        ASSERT_EQ(vecIt, elements.end());
        ASSERT_EQ(mapIt, map.end());
    };

    check();

    // Now remove part of elements and repeat check.
    std::vector<std::pair<TInt, TInt>> keptElements;
    for (int i = 0; i < n; ++i) {
        if (i % 3 == 1) {
            keptElements.push_back(elements[i]);
            continue;
        }
        map.erase(elements[i].first);
    }
    elements = std::move(keptElements);

    check();
}


TEST(TOrderedHashMapTest, Misc)
{
    TOrderedHashMap<TInt, TInt> map;

    // operator[].
    map[1_i] = 10_i;
    EXPECT_EQ(map[1_i], 10_i);
    EXPECT_EQ(map.begin()->second, 10_i);
    map[1_i] = 20_i;
    EXPECT_EQ(map.begin()->second, 20_i);
    map.begin()->second = 30_i;
    EXPECT_EQ(map[1_i], 30_i);

    // size().
    EXPECT_EQ(map.size(), 1u);

    // contains().
    EXPECT_TRUE(map.contains(1_i));
    EXPECT_FALSE(map.contains(2_i));

    // clear().
    map.clear();
    EXPECT_EQ(map.size(), 0u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
