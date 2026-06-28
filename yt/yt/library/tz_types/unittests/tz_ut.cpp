#include <yt/yt/library/tz_types/tz_types.h>

#include <yt/yt/core/test_framework/framework.h>

#include <string>

namespace NYT::NTzTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui16 tzEuropeMoscow = 1;

TEST(TTzHelpersTest, Unsigned)
{
    ui32 timestamp = 42;
    auto presortedString = MakeTzString<ui32>(timestamp, tzEuropeMoscow);
    ASSERT_EQ(presortedString.substr(4), std::string({'\x00', '\x01'}));
    auto res = ParseTzValue<ui32>(presortedString);
    ASSERT_EQ(timestamp, res.first);
    ASSERT_EQ(res.second, tzEuropeMoscow);
}

TEST(TTzHelpersTest, Signed)
{
    i32 timestamp = -42;
    auto presortedString  = MakeTzString<i32>(timestamp, tzEuropeMoscow);
    ASSERT_EQ(presortedString.substr(4), std::string({'\x00', '\x01'}));
    auto res = ParseTzValue<i32>(presortedString);
    ASSERT_EQ(timestamp, res.first);
    ASSERT_EQ(res.second, tzEuropeMoscow);
}

TEST(TTzHelpersTest, TzName)
{
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(GetTzIndex(GetTzName(i)), i);
    }
}

TEST(TTzHelpersTest, CorrectSort)
{
    // Make sure that our string representations are sorted correctly.
    std::vector<i32> timestamps = {0, -1, -10, 4322, 12};
    std::vector<std::string> presortedStrings;
    for (auto timestamp :  timestamps) {
        presortedStrings.push_back(MakeTzString<i32>(timestamp, 0));
        presortedStrings.push_back(MakeTzString<i32>(timestamp, 2));
        presortedStrings.push_back(MakeTzString<i32>(timestamp, 1));
    }
    std::sort(timestamps.begin(), timestamps.end());
    std::sort(presortedStrings.begin(), presortedStrings.end());
    for (int i = 0; i < std::ssize(timestamps); i++) {
        for (int j = 0; j < 3; ++j) {
            auto [timestamp, tzId] = ParseTzValue<i32>(presortedStrings[3 * i + j]);
            ASSERT_EQ(timestamps[i], timestamp);
            ASSERT_EQ(j, tzId);
        }
    }
}

TEST(TTzHelpersTest, MakeTzStringWithTzName)
{
    ui32 timestamp = 42;
    auto presortedString = MakeTzString<ui32>(timestamp, "Europe/Moscow");
    ASSERT_EQ(presortedString.substr(4), std::string({'\x00', '\x01'}));
    auto res = ParseTzValue<ui32>(presortedString);
    ASSERT_EQ(timestamp, res.first);
    ASSERT_EQ(res.second, tzEuropeMoscow);
}

//////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTzTypes
