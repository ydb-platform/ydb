#include <library/cpp/yt/compact_containers/compact_map.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TMap = TCompactMap<std::string, std::string, 2>;

TMap CreateMap()
{
    std::vector<std::pair<std::string, std::string>> data = {
        {"I", "met"},
        {"a", "traveller"},
        {"from", "an"},
        {"antique", "land"},
    };
    return {data.begin(), data.end()};
}

////////////////////////////////////////////////////////////////////////////////
// Basic operations.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, DefaultEmpty)
{
    TMap m;
    EXPECT_TRUE(m.empty());
    EXPECT_EQ(m.begin(), m.end());
    EXPECT_EQ(m.size(), 0u);
}

TEST(TCompactMapTest, Size)
{
    auto m = CreateMap();

    EXPECT_EQ(m.size(), 4u);

    m.insert({"Who", "said"});

    EXPECT_EQ(m.size(), 5u);

    m.erase("antique");

    EXPECT_EQ(m.size(), 4u);
}

TEST(TCompactMapTest, ClearAndEmpty)
{
    auto m = CreateMap();

    EXPECT_FALSE(m.empty());
    EXPECT_NE(m.begin(), m.end());

    m.clear();

    EXPECT_TRUE(m.empty());
    EXPECT_EQ(m.begin(), m.end());

    m.insert({"Who", "said"});

    EXPECT_FALSE(m.empty());
    EXPECT_NE(m.begin(), m.end());
}

////////////////////////////////////////////////////////////////////////////////
// Small size (inline vector) tests.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, InsertSmall)
{
    TCompactMap<int, std::string, 4> m;

    auto [it1, inserted1] = m.insert({1, "one"});
    EXPECT_TRUE(inserted1);
    EXPECT_EQ(it1->first, 1);
    EXPECT_EQ(it1->second, "one");
    EXPECT_EQ(m.size(), 1u);

    auto [it2, inserted2] = m.insert({2, "two"});
    EXPECT_TRUE(inserted2);
    EXPECT_EQ(m.size(), 2u);

    auto [it3, inserted3] = m.insert({1, "uno"});
    EXPECT_FALSE(inserted3);
    // The value is not changed.
    EXPECT_EQ(it3->second, "one");
    EXPECT_EQ(m.size(), 2u);
}

TEST(TCompactMapTest, InsertMoveOnlyValue)
{
    TCompactMap<int, std::unique_ptr<int>, 1> m;

    auto [smallIt, smallInserted] = m.insert({1, std::make_unique<int>(10)});
    EXPECT_TRUE(smallInserted);
    EXPECT_EQ(*smallIt->second, 10);

    auto [largeIt, largeInserted] = m.insert({2, std::make_unique<int>(20)});
    EXPECT_TRUE(largeInserted);
    EXPECT_EQ(*largeIt->second, 20);
}

TEST(TCompactMapTest, FindSmall)
{
    TCompactMap<int, std::string, 4> m;
    m.insert({1, "one"});
    m.insert({2, "two"});
    m.insert({3, "three"});

    auto it = m.find(2);
    EXPECT_NE(it, m.end());
    EXPECT_EQ(it->first, 2);
    EXPECT_EQ(it->second, "two");

    auto it2 = m.find(99);
    EXPECT_EQ(it2, m.end());
}

TEST(TCompactMapTest, EraseSmall)
{
    TCompactMap<int, std::string, 4> m;
    m.insert({1, "one"});
    m.insert({2, "two"});
    m.insert({3, "three"});

    EXPECT_TRUE(m.erase(2));
    EXPECT_EQ(m.size(), 2u);
    EXPECT_EQ(m.find(2), m.end());
    EXPECT_NE(m.find(1), m.end());
    EXPECT_NE(m.find(3), m.end());

    EXPECT_FALSE(m.erase(99));
    EXPECT_EQ(m.size(), 2u);
}

TEST(TCompactMapTest, IteratorSmall)
{
    TCompactMap<int, std::string, 4> m;
    m.insert({3, "three"});
    m.insert({1, "one"});
    m.insert({2, "two"});

    std::vector<int> keys;
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, std::vector<int>({1, 2, 3}));
}

////////////////////////////////////////////////////////////////////////////////
// Large size (std::map) tests.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, GrowToLarge)
{
    TCompactMap<int, std::string, 4> m;

    for (int i = 0; i < 8; i++) {
        m.insert({i, std::to_string(i)});
    }

    EXPECT_EQ(m.size(), 8u);

    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(m.count(i), 1u);
        EXPECT_EQ(m.find(i)->second, std::to_string(i));
    }

    EXPECT_EQ(m.count(99), 0u);
}

TEST(TCompactMapTest, InsertLarge)
{
    TCompactMap<int, std::string, 2> m;

    for (int i = 0; i < 5; i++) {
        auto [it, inserted] = m.insert({i, std::to_string(i)});
        EXPECT_TRUE(inserted);
        EXPECT_EQ(it->first, i);
        EXPECT_EQ(it->second, std::to_string(i));
    }

    EXPECT_EQ(m.size(), 5u);

    auto [it, inserted] = m.insert({2, "two"});
    EXPECT_FALSE(inserted);
    // The value is not changed.
    EXPECT_EQ(it->second, "2");
    EXPECT_EQ(m.size(), 5u);
}

TEST(TCompactMapTest, FindLarge)
{
    TCompactMap<int, std::string, 2> m;

    for (int i = 0; i < 8; i++) {
        m.insert({i, std::to_string(i)});
    }

    for (int i = 0; i < 8; i++) {
        auto it = m.find(i);
        EXPECT_NE(it, m.end());
        EXPECT_EQ(it->first, i);
        EXPECT_EQ(it->second, std::to_string(i));
    }

    EXPECT_EQ(m.find(99), m.end());
}

TEST(TCompactMapTest, EraseLarge)
{
    TCompactMap<int, std::string, 2> m;

    for (int i = 0; i < 8; i++) {
        m.insert({i, std::to_string(i)});
    }

    for (int i = 0; i < 8; i++) {
        EXPECT_EQ(m.count(i), 1u);
        EXPECT_TRUE(m.erase(i));
        EXPECT_EQ(m.count(i), 0u);
        EXPECT_EQ(m.size(), 8u - i - 1);

        for (int j = i + 1; j < 8; j++) {
            EXPECT_EQ(m.count(j), 1u);
        }
    }

    EXPECT_EQ(m.count(99), 0u);
}

TEST(TCompactMapTest, IteratorLarge)
{
    TCompactMap<int, std::string, 2> m;

    for (int i = 0; i < 6; i++) {
        m.insert({i, std::to_string(i)});
    }

    std::vector<int> keys;
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    std::sort(keys.begin(), keys.end());
    std::vector<int> expected = {0, 1, 2, 3, 4, 5};
    EXPECT_EQ(keys, expected);
}

////////////////////////////////////////////////////////////////////////////////
// Transition from small to large.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, SmallToLargeTransition)
{
    TCompactMap<int, std::string, 4> m;

    // Fill small storage
    for (int i = 0; i < 4; i++) {
        m.insert({i, std::to_string(i)});
    }

    EXPECT_EQ(m.size(), 4u);

    // Add one more to trigger transition.
    m.insert({4, "4"});

    EXPECT_EQ(m.size(), 5u);

    // Check all elements are still there.
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(m.count(i), 1u);
        EXPECT_EQ(m.find(i)->second, std::to_string(i));
    }
}

TEST(TCompactMapTest, GrowShrink)
{
    TCompactMap<std::string, std::string, 2> m;
    m.insert({"Two", "vast"});
    m.insert({"and", "trunkless"});
    m.insert({"legs", "of"});
    m.insert({"stone", "Stand"});
    m.insert({"in", "the"});
    m.insert({"desert", "..."});

    EXPECT_EQ(m.size(), 6u);

    m.erase("legs");
    m.erase("stone");
    m.erase("in");
    m.erase("desert");

    EXPECT_EQ(m.size(), 2u);

    // All remaining elements should be accessible.
    EXPECT_NE(m.find("Two"), m.end());
    EXPECT_NE(m.find("and"), m.end());
}

////////////////////////////////////////////////////////////////////////////////
// Find operations (mutable and const).
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, FindMutable)
{
    auto m = CreateMap();
    {
        auto it = m.find("from");
        EXPECT_NE(it, m.end());
        EXPECT_EQ(it->second, "an");
        it->second = "the";
    }
    {
        auto it = m.find("from");
        EXPECT_NE(it, m.end());
        EXPECT_EQ(it->second, "the");
    }
    {
        auto it = m.find("Who");
        EXPECT_EQ(it, m.end());
    }
}

TEST(TCompactMapTest, FindConst)
{
    const auto& m = CreateMap();
    {
        auto it = m.find("from");
        EXPECT_NE(it, m.end());
        EXPECT_EQ(it->second, "an");
    }
    {
        auto it = m.find("Who");
        EXPECT_EQ(it, m.end());
    }
}

TEST(TCompactMapTest, Contains)
{
    auto m = CreateMap();

    EXPECT_TRUE(m.contains("I"));
    EXPECT_TRUE(m.contains("from"));
    EXPECT_FALSE(m.contains("Who"));

    m.erase("I");
    EXPECT_FALSE(m.contains("I"));
}

TEST(TCompactMapTest, HeterogeneousLookupSmallAndLarge)
{
    using namespace std::string_view_literals;

    TCompactMap<std::string, int, 2> small = {
        {"alpha", 1},
        {"beta", 2},
    };

    EXPECT_NE(small.find("alpha"sv), small.end());
    EXPECT_TRUE(small.contains("beta"sv));
    EXPECT_EQ(small.count("missing"sv), 0u);

    TCompactMap<std::string, int, 2> large = {
        {"alpha", 1},
        {"beta", 2},
        {"gamma", 3},
    };

    EXPECT_NE(large.find("alpha"sv), large.end());
    EXPECT_TRUE(large.contains("gamma"sv));
    EXPECT_EQ(large.count("missing"sv), 0u);
}

////////////////////////////////////////////////////////////////////////////////
// Insert operations.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, Insert)
{
    auto m = CreateMap();

    auto [it, inserted] = m.insert({"Who", "said"});
    EXPECT_TRUE(inserted);
    EXPECT_EQ(m.size(), 5u);
    EXPECT_NE(it, m.end());
    EXPECT_EQ(it, m.find("Who"));
    EXPECT_EQ(it->second, "said");

    auto [it2, inserted2] = m.insert({"Who", "told"});
    EXPECT_FALSE(inserted2);
    EXPECT_EQ(m.size(), 5u);
    EXPECT_EQ(it2, it);
    EXPECT_EQ(it->second, "said");

    std::vector<std::pair<std::string, std::string>> data = {
        {"Two", "vast"},
        {"and", "trunkless"},
        {"legs", "of"},
    };
    m.insert(data.begin(), data.end());
    EXPECT_EQ(m.size(), 8u);
    EXPECT_NE(m.find("and"), m.end());
    EXPECT_EQ(m.find("and")->second, "trunkless");
}

TEST(TCompactMapTest, Emplace)
{
    TCompactMap<int, std::string, 3> m;

    auto [it1, inserted1] = m.emplace(1, "one");
    EXPECT_TRUE(inserted1);
    EXPECT_EQ(it1->second, "one");

    auto [it2, inserted2] = m.emplace(1, "uno");
    EXPECT_FALSE(inserted2);
    EXPECT_EQ(it2->second, "one");

    auto [it3, inserted3] = m.emplace(0, "zero");
    EXPECT_TRUE(inserted3);
    EXPECT_EQ(it3->first, 0);
    EXPECT_EQ(m.size(), 2u);

    for (int i = 2; i < 6; ++i) {
        m.emplace(i, std::to_string(i));
    }

    EXPECT_EQ(m.size(), 6u);
    auto [tailIt, tailInserted] = m.emplace(10, "ten");
    EXPECT_TRUE(tailInserted);
    EXPECT_EQ(tailIt->first, 10);

    auto [dupIt, dupInserted] = m.emplace(3, "tres");
    EXPECT_FALSE(dupInserted);
    EXPECT_EQ(dupIt->second, "3");
}

TEST(TCompactMapTest, TryEmplace)
{
    TCompactMap<int, std::string, 2> m;

    auto [it1, inserted1] = m.try_emplace(1, "one");
    EXPECT_TRUE(inserted1);
    EXPECT_EQ(it1->second, "one");

    auto [it2, inserted2] = m.try_emplace(1, "uno");
    EXPECT_FALSE(inserted2);
    EXPECT_EQ(it2->second, "one");

    auto [it3, inserted3] = m.try_emplace(0, "zero");
    EXPECT_TRUE(inserted3);
    EXPECT_EQ(it3->first, 0);

    auto [it4, inserted4] = m.try_emplace(5, "five");
    EXPECT_TRUE(inserted4);

    // Grow to std::map backed mode.
    for (int i = 6; i < 9; ++i) {
        m.try_emplace(i, std::to_string(i));
    }

    auto [it5, inserted5] = m.try_emplace(9, "nine");
    EXPECT_TRUE(inserted5);

    auto [it6, inserted6] = m.try_emplace(5, "FIVE");
    EXPECT_FALSE(inserted6);
    EXPECT_EQ(it6->second, "five");
}

TEST(TCompactMapTest, InsertOrAssign)
{
    TCompactMap<int, std::string, 4> m;

    auto [it1, inserted1] = m.insert_or_assign(1, "one");
    EXPECT_TRUE(inserted1);
    EXPECT_EQ(it1->second, "one");
    EXPECT_EQ(m.size(), 1u);

    auto [it2, inserted2] = m.insert_or_assign(1, "uno");
    EXPECT_FALSE(inserted2);
    EXPECT_EQ(it2->second, "uno");
    EXPECT_EQ(m.size(), 1u);

    // Test after growing to large.
    for (int i = 2; i < 6; i++) {
        m.insert_or_assign(i, std::to_string(i));
    }

    auto [it3, inserted3] = m.insert_or_assign(3, "three");
    EXPECT_FALSE(inserted3);
    EXPECT_EQ(it3->second, "three");
}

TEST(TCompactMapTest, InitializerList)
{
    TCompactMap<int, std::string, 4> m = {
        {1, "one"},
        {2, "two"},
        {3, "three"},
    };

    EXPECT_EQ(m.size(), 3u);
    EXPECT_EQ(m.find(1)->second, "one");
    EXPECT_EQ(m.find(2)->second, "two");
    EXPECT_EQ(m.find(3)->second, "three");
}

////////////////////////////////////////////////////////////////////////////////
// Subscript operator
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, SubscriptSmall)
{
    TCompactMap<std::string, std::string, 4> m;

    m["key1"] = "value1";
    EXPECT_EQ(m["key1"], "value1");
    EXPECT_EQ(m.size(), 1u);

    m["key2"] = "value2";
    EXPECT_EQ(m.size(), 2u);

    EXPECT_EQ(m["key3"], "");
    EXPECT_EQ(m.size(), 3u);
}

TEST(TCompactMapTest, SubscriptLarge)
{
    TCompactMap<std::string, std::string, 2> m;

    for (int i = 0; i < 5; i++) {
        m["key" + std::to_string(i)] = "value" + std::to_string(i);
    }

    EXPECT_EQ(m.size(), 5u);
    EXPECT_EQ(m["key3"], "value3");

    m["key3"] = "new_value";
    EXPECT_EQ(m["key3"], "new_value");
}

TEST(TCompactMapTest, SubscriptRvalue)
{
    TCompactMap<std::string, std::string, 4> m;

    std::string key = "temp";
    m[std::move(key)] = "value";
    EXPECT_EQ(m["temp"], "value");
}

////////////////////////////////////////////////////////////////////////////////
// At operations.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, At)
{
    TCompactMap<int, std::string, 4> m;
    m[1] = "one";
    m[2] = "two";

    EXPECT_EQ(m.at(1), "one");
    EXPECT_EQ(m.at(2), "two");

    EXPECT_THROW(m.at(99), std::out_of_range);

    m.at(1) = "uno";
    EXPECT_EQ(m.at(1), "uno");
}

TEST(TCompactMapTest, AtConst)
{
    TCompactMap<int, std::string, 4> m;
    m[1] = "one";
    m[2] = "two";

    const auto& cm = m;

    EXPECT_EQ(cm.at(1), "one");
    EXPECT_EQ(cm.at(2), "two");

    EXPECT_THROW(cm.at(99), std::out_of_range);
}

TEST(TCompactMapTest, AtLarge)
{
    TCompactMap<int, std::string, 2> m;
    for (int i = 0; i < 6; i++) {
        m[i] = std::to_string(i);
    }

    EXPECT_EQ(m.at(3), "3");
    EXPECT_THROW(m.at(99), std::out_of_range);
}

////////////////////////////////////////////////////////////////////////////////
// Iterator tests.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, IteratorModification)
{
    TCompactMap<int, std::string, 4> m;
    m[1] = "one";
    m[2] = "two";
    m[3] = "three";

    for (auto it = m.begin(); it != m.end(); ++it) {
        it->second += "_modified";
    }

    EXPECT_EQ(m[1], "one_modified");
    EXPECT_EQ(m[2], "two_modified");
    EXPECT_EQ(m[3], "three_modified");
}

TEST(TCompactMapTest, IteratorString)
{
    TCompactMap<std::string, std::string, 2> m;

    m["str1"] = "val1";
    m["str2"] = "val2";

    std::vector<std::string> keys;
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(m.size(), 2u);
    EXPECT_EQ(keys[0], "str1");
    EXPECT_EQ(keys[1], "str2");

    // Add more to grow.
    m["str4"] = "val4";
    m["str0"] = "val0";

    keys.clear();
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(m.size(), 4u);
    EXPECT_EQ(keys[0], "str0");
    EXPECT_EQ(keys[1], "str1");
    EXPECT_EQ(keys[2], "str2");
    EXPECT_EQ(keys[3], "str4");
}

TEST(TCompactMapTest, IteratorConversion)
{
    TCompactMap<int, std::string, 4> m;
    m[1] = "one";
    m[2] = "two";

    auto it = m.begin();
    TCompactMap<int, std::string, 4>::const_iterator cit = it;

    EXPECT_EQ(*it, *cit);
    EXPECT_EQ(it, cit);
    EXPECT_EQ(cit, it);
    EXPECT_NE(it, m.cend());
    EXPECT_NE(m.cend(), it);
}

TEST(TCompactMapTest, IteratorTypeProperties)
{
    using TMap = TCompactMap<int, std::string, 4>;
    using TIter = TMap::iterator;
    using TCIter = TMap::const_iterator;

    static_assert(std::is_default_constructible_v<TIter>);
    static_assert(std::is_default_constructible_v<TCIter>);
    static_assert(std::bidirectional_iterator<TIter>);
    static_assert(std::bidirectional_iterator<TCIter>);

    // Key type cannot be modified via any iterator.
    static_assert(!std::is_assignable_v<decltype((std::declval<TIter>()->first)), std::string>);
    static_assert(!std::is_assignable_v<decltype((std::declval<TCIter>()->first)), std::string>);

    // Mapped type can only be modified via non-const iterator.
    static_assert(std::is_assignable_v<decltype((std::declval<TIter>()->second)), std::string>);
    static_assert(!std::is_assignable_v<decltype((std::declval<TCIter>()->second)), std::string>);
}

TEST(TCompactMapTest, ErasePostIncrementedIterator)
{
    TCompactMap<int, int, 1> map = {
        {1, 10},
        {2, 20},
    };

    auto it = map.begin();
    auto next = map.erase(it++);

    ASSERT_EQ(map.size(), 1u);
    EXPECT_EQ(next, map.begin());
    EXPECT_EQ(next->first, 2);
}

TEST(TCompactMapTest, IteratorDecrementSmallAndLarge)
{
    {
        TCompactMap<int, std::string, 4> m;
        m[1] = "one";
        m[2] = "two";
        m[3] = "three";

        auto it = m.end();
        --it;
        EXPECT_EQ(it->first, 3);
        EXPECT_EQ((it--)->first, 3);
        EXPECT_EQ(it->first, 2);
        --it;
        EXPECT_EQ(it->first, 1);
        EXPECT_EQ(it, m.begin());
    }

    {
        TCompactMap<int, std::string, 2> m;
        for (int i = 0; i < 5; ++i) {
            m[i] = std::to_string(i);
        }

        auto it = m.end();
        --it;
        EXPECT_EQ(it->first, 4);
        EXPECT_EQ((it--)->first, 4);
        EXPECT_EQ(it->first, 3);
        --it;
        EXPECT_EQ(it->first, 2);
        --it;
        EXPECT_EQ(it->first, 1);
        --it;
        EXPECT_EQ(it->first, 0);
        EXPECT_EQ(it, m.begin());
    }
}

TEST(TCompactMapTest, EraseByIteratorSmallAndLarge)
{
    {
        TCompactMap<int, std::string, 4> m;
        m[1] = "one";
        m[2] = "two";
        m[3] = "three";

        auto middle = m.find(2);
        ASSERT_NE(middle, m.end());
        auto next = m.erase(middle);
        EXPECT_EQ(next->first, 3);
        EXPECT_EQ(m.count(2), 0u);

        auto rangeBegin = m.begin();
        auto rangeEnd = m.end();
        // Removing the tail via iterator range should leave the container empty.
        auto afterRange = m.erase(rangeBegin, rangeEnd);
        EXPECT_EQ(afterRange, m.end());
        EXPECT_TRUE(m.empty());
    }

    {
        TCompactMap<int, std::string, 2> m;
        for (int i = 0; i < 5; ++i) {
            m[i] = std::to_string(i);
        }

        auto it = m.find(2);
        ASSERT_NE(it, m.end());
        auto next = m.erase(it);
        EXPECT_EQ(next->first, 3);
        EXPECT_EQ(m.count(2), 0u);

        auto first = m.find(0);
        auto last = m.find(3);
        ASSERT_NE(first, m.end());
        ASSERT_NE(last, m.end());
        auto afterRange = m.erase(first, last);
        EXPECT_EQ(afterRange->first, 3);
        std::vector<int> remaining;
        for (const auto& [k, v] : m) {
            remaining.push_back(k);
        }
        EXPECT_EQ(remaining, std::vector<int>({3, 4}));
    }
}

TEST(TCompactMapTest, EraseUpgradedMapToEndOneByOne)
{
    TCompactMap<int, int, 2> map = {
        {1, 1},
        {2, 2},
        {3, 3},
    };

    for (auto it = map.begin(); it != map.end();) {
        it = map.erase(it);
    }

    EXPECT_TRUE(map.empty());
    EXPECT_EQ(map.begin(), map.end());
}

TEST(TCompactMapTest, EraseEntireUpgradedMapByRange)
{
    TCompactMap<int, int, 2> map = {
        {1, 1},
        {2, 2},
        {3, 3},
    };

    auto result = map.erase(map.begin(), map.end());

    EXPECT_EQ(result, map.end());
    EXPECT_TRUE(map.empty());
}

TEST(TCompactMapTest, StructuredBindingModifySmallAndLarge)
{
    // Small-mode: everything stays in the inline storage.
    {
        TCompactMap<int, std::string, 4> m;
        m[1] = "one";
        m[2] = "two";
        m[3] = "three";

        for (auto&& [k, v] : m) {
            v += "_x";
        }

        EXPECT_EQ(m[1], "one_x");
        EXPECT_EQ(m[2], "two_x");
        EXPECT_EQ(m[3], "three_x");
    }

    // Large-mode: container has switched to std::map storage.
    {
        TCompactMap<int, std::string, 2> m;
        m[1] = "one";
        m[2] = "two";
        m[3] = "three";
        // Triggers growth to large mode.
        m[4] = "four";

        for (auto&& [k, v] : m) {
            v += "_y";
        }

        EXPECT_EQ(m[1], "one_y");
        EXPECT_EQ(m[2], "two_y");
        EXPECT_EQ(m[3], "three_y");
        EXPECT_EQ(m[4], "four_y");
    }
}

////////////////////////////////////////////////////////////////////////////////
// Edge cases
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, EmptyAfterClear)
{
    TCompactMap<int, std::string, 4> m;

    for (int i = 0; i < 8; i++) {
        m[i] = std::to_string(i);
    }

    m.clear();

    EXPECT_TRUE(m.empty());
    EXPECT_EQ(m.size(), 0u);
    EXPECT_EQ(m.begin(), m.end());
}

TEST(TCompactMapTest, CountZeroOrOne)
{
    TCompactMap<int, std::string, 4> m;

    EXPECT_EQ(m.count(1), 0u);

    m[1] = "one";
    EXPECT_EQ(m.count(1), 1u);

    m.erase(1);
    EXPECT_EQ(m.count(1), 0u);
}

TEST(TCompactMapTest, SortedOrder)
{
    TCompactMap<int, std::string, 4> m;
    m[5] = "five";
    m[1] = "one";
    m[3] = "three";
    m[2] = "two";

    std::vector<int> keys;
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    EXPECT_EQ(keys, std::vector<int>({1, 2, 3, 5}));
}

TEST(TCompactMapTest, CustomComparator)
{
    TCompactMap<int, std::string, 4, std::greater<int>> m;
    m[1] = "one";
    m[2] = "two";
    m[3] = "three";

    std::vector<int> keys;
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    EXPECT_EQ(keys, std::vector<int>({3, 2, 1}));
}

TEST(TCompactMapTest, CustomComparatorAfterUpgrade)
{
    TCompactMap<int, std::string, 2, std::greater<int>> m;
    for (int i = 0; i < 5; ++i) {
        m[i] = std::to_string(i);
    }

    std::vector<int> keys;
    for (const auto& [k, v] : m) {
        keys.push_back(k);
    }

    EXPECT_EQ(keys, std::vector<int>({4, 3, 2, 1, 0}));
}

TEST(TCompactMapTest, ZeroInlineCapacityBehavesLikeMap)
{
    TCompactMap<int, std::string, 0> m;
    EXPECT_TRUE(m.empty());

    m[7] = "seven";
    EXPECT_EQ(m.size(), 1u);
    EXPECT_EQ(m.at(7), "seven");

    auto [it, inserted] = m.insert_or_assign(7, "SEVEN");
    EXPECT_FALSE(inserted);
    EXPECT_EQ(it->second, "SEVEN");

    auto [it2, inserted2] = m.insert_or_assign(4, "four");
    EXPECT_TRUE(inserted2);
    EXPECT_EQ(m.size(), 2u);
    EXPECT_EQ(m.at(4), "four");

    EXPECT_EQ(m.erase(7), 1u);
    EXPECT_EQ(m.erase(7), 0u);
    EXPECT_FALSE(m.contains(7));
    EXPECT_EQ(m.begin()->first, 4);
}

////////////////////////////////////////////////////////////////////////////////
// Exception safety.
////////////////////////////////////////////////////////////////////////////////

struct TThrowingCopy
{
    static int Alive;
    static int CopiesBeforeThrow;

    int Value;

    explicit TThrowingCopy(int value)
        : Value(value)
    {
        ++Alive;
    }

    TThrowingCopy(const TThrowingCopy& other)
        : Value(other.Value)
    {
        if (CopiesBeforeThrow == 0) {
            throw std::runtime_error("copy failed");
        }
        if (CopiesBeforeThrow > 0) {
            --CopiesBeforeThrow;
        }
        ++Alive;
    }

    TThrowingCopy(TThrowingCopy&& other) noexcept
        : Value(other.Value)
    {
        ++Alive;
    }

    TThrowingCopy& operator=(const TThrowingCopy&) = default;
    TThrowingCopy& operator=(TThrowingCopy&&) = default;

    ~TThrowingCopy()
    {
        --Alive;
    }
};

int TThrowingCopy::Alive = 0;
int TThrowingCopy::CopiesBeforeThrow = -1;

TEST(TCompactMapTest, FailedInlineCopyDestroysPartialCopy)
{
    using TTestMap = TCompactMap<int, TThrowingCopy, 4>;

    EXPECT_EQ(TThrowingCopy::Alive, 0);
    {
        TTestMap source;
        source.try_emplace(1, 10);
        source.try_emplace(2, 20);
        source.try_emplace(3, 30);

        TThrowingCopy::CopiesBeforeThrow = 1;
        EXPECT_THROW(TTestMap copy(source), std::runtime_error);
        EXPECT_EQ(TThrowingCopy::Alive, 3);

        TTestMap target;
        target.try_emplace(4, 40);
        TThrowingCopy::CopiesBeforeThrow = 1;
        EXPECT_THROW(target = source, std::runtime_error);
        EXPECT_TRUE(target.empty());
        EXPECT_EQ(TThrowingCopy::Alive, 3);
    }
    EXPECT_EQ(TThrowingCopy::Alive, 0);

    TThrowingCopy::CopiesBeforeThrow = -1;
}

struct TThrowingMapped
{
    static int Alive;
    static int MovesBeforeThrow;
    static bool ThrowOnConstruction;

    int Value;

    explicit TThrowingMapped(int value)
        : Value(value)
    {
        if (ThrowOnConstruction) {
            throw std::runtime_error("construction failed");
        }
        ++Alive;
    }

    TThrowingMapped(const TThrowingMapped&) = delete;
    TThrowingMapped& operator=(const TThrowingMapped&) = delete;

    TThrowingMapped(TThrowingMapped&& other) // NOLINT
        : Value(other.Value)
    {
        if (MovesBeforeThrow == 0) {
            throw std::runtime_error("move failed");
        }
        if (MovesBeforeThrow > 0) {
            --MovesBeforeThrow;
        }
        ++Alive;
    }

    TThrowingMapped& operator=(TThrowingMapped&&) = delete;

    ~TThrowingMapped()
    {
        --Alive;
    }
};

int TThrowingMapped::Alive = 0;
int TThrowingMapped::MovesBeforeThrow = -1;
bool TThrowingMapped::ThrowOnConstruction = false;

TEST(TCompactMapTest, FailedInlineValueConstructionLeavesMapUnchanged)
{
    TCompactMap<int, TThrowingMapped, 4> map;
    map.try_emplace(1, 10);
    map.try_emplace(3, 30);

    TThrowingMapped::ThrowOnConstruction = true;
    EXPECT_THROW(map.try_emplace(2, 20), std::runtime_error);
    TThrowingMapped::ThrowOnConstruction = false;

    EXPECT_EQ(map.size(), 2u);
    EXPECT_EQ(map.at(1).Value, 10);
    EXPECT_EQ(map.at(3).Value, 30);
}

TEST(TCompactMapTest, FailedInlineRelocationLeavesValidMap)
{
    EXPECT_EQ(TThrowingMapped::Alive, 0);
    {
        TCompactMap<int, TThrowingMapped, 4> map;
        map.try_emplace(1, 10);
        map.try_emplace(2, 20);
        map.try_emplace(3, 30);

        TThrowingMapped::MovesBeforeThrow = 1;
        EXPECT_THROW(map.try_emplace(0, 0), std::runtime_error);
        TThrowingMapped::MovesBeforeThrow = -1;

        EXPECT_EQ(map.size(), 2u);
        EXPECT_EQ(map.at(1).Value, 10);
        EXPECT_EQ(map.at(2).Value, 20);

        map.try_emplace(4, 40);
        EXPECT_EQ(map.at(4).Value, 40);
    }
    EXPECT_EQ(TThrowingMapped::Alive, 0);
}

TEST(TCompactMapTest, FailedInlineMoveDestroysPartialMove)
{
    using TTestMap = TCompactMap<int, TThrowingMapped, 4>;

    EXPECT_EQ(TThrowingMapped::Alive, 0);
    {
        TTestMap source;
        source.try_emplace(1, 10);
        source.try_emplace(2, 20);
        source.try_emplace(3, 30);

        TThrowingMapped::MovesBeforeThrow = 1;
        EXPECT_THROW(TTestMap moved(std::move(source)), std::runtime_error);
        EXPECT_EQ(TThrowingMapped::Alive, 3);

        TTestMap target;
        target.try_emplace(4, 40);
        TThrowingMapped::MovesBeforeThrow = 1;
        EXPECT_THROW(target = std::move(source), std::runtime_error);
        EXPECT_TRUE(target.empty());
        EXPECT_EQ(TThrowingMapped::Alive, 3);
    }
    EXPECT_EQ(TThrowingMapped::Alive, 0);

    TThrowingMapped::MovesBeforeThrow = -1;
}

TEST(TCompactMapTest, FailedInlineEraseRelocationLeavesValidMap)
{
    EXPECT_EQ(TThrowingMapped::Alive, 0);
    {
        TCompactMap<int, TThrowingMapped, 5> map;
        map.try_emplace(1, 10);
        map.try_emplace(2, 20);
        map.try_emplace(3, 30);
        map.try_emplace(4, 40);

        TThrowingMapped::MovesBeforeThrow = 1;
        EXPECT_THROW(map.erase(map.begin()), std::runtime_error);
        TThrowingMapped::MovesBeforeThrow = -1;

        ASSERT_EQ(map.size(), 1u);
        EXPECT_EQ(map.begin()->first, 2);
        EXPECT_EQ(map.begin()->second.Value, 20);

        map.try_emplace(5, 50);
        EXPECT_EQ(map.at(5).Value, 50);
    }
    EXPECT_EQ(TThrowingMapped::Alive, 0);
}

////////////////////////////////////////////////////////////////////////////////
// Copy, move, and lifetime management.
////////////////////////////////////////////////////////////////////////////////

TEST(TCompactMapTest, CopyAndMoveConstructionSmallAndLarge)
{
    using TTestMap = TCompactMap<int, int, 2>;

    TTestMap small = {
        {1, 10},
        {2, 20},
    };
    TTestMap smallCopy(small);
    EXPECT_EQ(smallCopy.size(), 2u);
    EXPECT_EQ(smallCopy.at(1), 10);
    EXPECT_EQ(smallCopy.at(2), 20);

    TTestMap smallMove(std::move(smallCopy));
    EXPECT_EQ(smallMove.size(), 2u);
    EXPECT_TRUE(smallCopy.empty());
    smallCopy.emplace(3, 30);
    EXPECT_EQ(smallCopy.at(3), 30);

    TTestMap large = {
        {1, 10},
        {2, 20},
        {3, 30},
    };
    TTestMap largeCopy(large);
    EXPECT_EQ(largeCopy.size(), 3u);
    EXPECT_EQ(largeCopy.at(3), 30);

    TTestMap largeMove(std::move(largeCopy));
    EXPECT_EQ(largeMove.size(), 3u);
    EXPECT_TRUE(largeCopy.empty());
    largeCopy.emplace(4, 40);
    EXPECT_EQ(largeCopy.at(4), 40);
}

TEST(TCompactMapTest, CopyAndMoveAssignmentBetweenSmallAndLarge)
{
    using TTestMap = TCompactMap<int, int, 2>;

    TTestMap smallSource = {{1, 10}};
    TTestMap largeTarget = {
        {2, 20},
        {3, 30},
        {4, 40},
    };
    largeTarget = smallSource;
    EXPECT_EQ(largeTarget.size(), 1u);
    EXPECT_EQ(largeTarget.at(1), 10);

    TTestMap largeSource = {
        {5, 50},
        {6, 60},
        {7, 70},
    };
    TTestMap smallTarget = {{8, 80}};
    smallTarget = largeSource;
    EXPECT_EQ(smallTarget.size(), 3u);
    EXPECT_EQ(smallTarget.at(7), 70);

    TTestMap smallMoveSource = {{9, 90}};
    largeTarget = std::move(smallMoveSource);
    EXPECT_EQ(largeTarget.at(9), 90);
    EXPECT_TRUE(smallMoveSource.empty());
    smallMoveSource.emplace(10, 100);
    EXPECT_EQ(smallMoveSource.at(10), 100);

    TTestMap largeMoveSource = {
        {11, 110},
        {12, 120},
        {13, 130},
    };
    smallTarget = std::move(largeMoveSource);
    EXPECT_EQ(smallTarget.size(), 3u);
    EXPECT_EQ(smallTarget.at(13), 130);
    EXPECT_TRUE(largeMoveSource.empty());
    largeMoveSource.emplace(14, 140);
    EXPECT_EQ(largeMoveSource.at(14), 140);
}

TEST(TCompactMapTest, SelfAssignmentSmallAndLarge)
{
    auto copyAssign = [] (auto* lhs, const auto* rhs) {
        *lhs = *rhs;
    };
    auto moveAssign = [] (auto* lhs, auto* rhs) {
        *lhs = std::move(*rhs);
    };

    TCompactMap<int, int, 2> small = {{1, 10}};
    copyAssign(&small, &small);
    EXPECT_EQ(small.at(1), 10);
    moveAssign(&small, &small);
    EXPECT_EQ(small.at(1), 10);

    TCompactMap<int, int, 2> large = {
        {1, 10},
        {2, 20},
        {3, 30},
    };
    copyAssign(&large, &large);
    EXPECT_EQ(large.size(), 3u);
    EXPECT_EQ(large.at(3), 30);
    moveAssign(&large, &large);
    EXPECT_EQ(large.size(), 3u);
    EXPECT_EQ(large.at(3), 30);
}

struct TCounted
{
    static int Alive;

    int Value;

    explicit TCounted(int value)
        : Value(value)
    {
        ++Alive;
    }

    TCounted(const TCounted& other)
        : Value(other.Value)
    {
        ++Alive;
    }

    TCounted(TCounted&& other) noexcept
        : Value(other.Value)
    {
        ++Alive;
    }

    TCounted& operator=(const TCounted&) = default;
    TCounted& operator=(TCounted&&) = default;

    ~TCounted()
    {
        --Alive;
    }
};

int TCounted::Alive = 0;

TEST(TCompactMapTest, DestroysAllValues)
{
    EXPECT_EQ(TCounted::Alive, 0);
    {
        TCompactMap<int, TCounted, 2> map;
        map.emplace(1, 10);
        map.emplace(2, 20);
        map.emplace(3, 30);

        auto copy = map;
        auto moved = std::move(copy);
        EXPECT_TRUE(copy.empty());
        EXPECT_EQ(moved.size(), 3u);
    }
    EXPECT_EQ(TCounted::Alive, 0);
}

////////////////////////////////////////////////////////////////////////////////
// Move-only value support.
////////////////////////////////////////////////////////////////////////////////

struct TMoveOnly
{
    int Value;

    explicit TMoveOnly(int value)
        : Value(value)
    { }

    TMoveOnly(const TMoveOnly&) = delete;
    TMoveOnly& operator=(const TMoveOnly&) = delete;

    TMoveOnly(TMoveOnly&&) = default;
    TMoveOnly& operator=(TMoveOnly&&) = default;
};

TEST(TCompactMapTest, MoveOnlyValueSmallAndUpgrade)
{
    TCompactMap<int, TMoveOnly, 2> m;

    auto [it1, inserted1] = m.emplace(1, 10);
    ASSERT_TRUE(inserted1);
    EXPECT_EQ(it1->second.Value, 10);

    auto [it2, inserted2] = m.emplace(2, 20);
    ASSERT_TRUE(inserted2);
    EXPECT_EQ(it2->second.Value, 20);

    // try_emplace should move-construct without copying.
    auto [it3, inserted3] = m.try_emplace(3, 30);
    ASSERT_TRUE(inserted3);
    EXPECT_EQ(it3->second.Value, 30);

    // All elements survive the upgrade to std::map.
    EXPECT_EQ(m.size(), 3u);
    EXPECT_EQ(m.find(1)->second.Value, 10);
    EXPECT_EQ(m.find(2)->second.Value, 20);
    EXPECT_EQ(m.find(3)->second.Value, 30);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
