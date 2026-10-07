#include <library/cpp/yt/containers/default_map.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/hash_table.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDefaultMapTest, Common)
{
    TDefaultMap<THashMap<int, std::string>> defaultMap("Hello");
    EXPECT_EQ(defaultMap[1], "Hello");
    defaultMap[1].append(", World");
    EXPECT_EQ(defaultMap[1], "Hello, World");
    defaultMap.insert({2, "abc"});
    EXPECT_EQ(defaultMap[2], "abc");
    EXPECT_EQ(defaultMap.find(3), defaultMap.end());
    EXPECT_EQ(defaultMap.GetOrDefault(7), "Hello");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
