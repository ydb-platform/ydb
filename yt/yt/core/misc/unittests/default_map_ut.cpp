#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/default_map.h>

#include <util/generic/hash_table.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TDefaultMap, Common)
{
    TDefaultMap<THashMap<int, TString>> defaultMap("Hello");
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
