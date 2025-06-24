#include <yt/yt/library/tz_types/tz_types.h>

#include <yt/yt/core/test_framework/framework.h>

#include <string>

namespace NYT::NTzTypes {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTzHelpers, Unsigned)
{
    ui32 timestamp = 42;
    auto presortedString = MakeTzString<ui32>(timestamp, "Europe/Moscow");
    ASSERT_EQ(presortedString.substr(4), "Europe/Moscow");
    auto res = ParseTzValue<ui32>(presortedString);
    ASSERT_EQ(timestamp, res.first);
    ASSERT_EQ(res.second, "Europe/Moscow");
}

TEST(TTzHelpers, Signed)
{
    i32 timestamp = -42;
    auto presortedString  = MakeTzString<i32>(timestamp, "Europe/Moscow");
    ASSERT_EQ(presortedString.substr(4), "Europe/Moscow");
    auto res = ParseTzValue<i32>(presortedString);
    ASSERT_EQ(timestamp, res.first);
    ASSERT_EQ(res.second, "Europe/Moscow");
}

TEST(TTzHelpers, TzName)
{
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(GetTzIndex(GetTzName(i)), i);
    }
}

TEST(TTzHelpers, CorrectSort)
{
    // Make sure that our string representations are sorted correctly.
    std::vector<i32> timestamps = {0, -1, -10, 4322, 12};
    std::vector<std::string> presortedStrings;
    for (auto timestamp :  timestamps) {
        presortedStrings.push_back(MakeTzString<i32>(timestamp, "Europe/Moscow"));
    }
    std::sort(timestamps.begin(), timestamps.end());
    std::sort(presortedStrings.begin(), presortedStrings.end());
    for (int i = 0; i < std::ssize(timestamps); i++) {
        ASSERT_EQ(timestamps[i],  ParseTzValue<i32>(presortedStrings[i]).first);
    }
}

//////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTzTypes
