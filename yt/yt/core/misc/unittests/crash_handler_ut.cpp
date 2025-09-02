#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/default_map.h>

#include <util/generic/hash_table.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCrashHandler, Simple)
{
    EXPECT_DEATH(
        {
            YT_ABORT("MY_PROBLEM");
        },
        "YT_ABORT\\(\"MY_PROBLEM\"\\)");

    EXPECT_DEATH(
        {
            YT_VERIFY(false, "MY_PROBLEM");
        },
        "YT_VERIFY\\(false, \"MY_PROBLEM\"\\)");

    EXPECT_DEATH(
        {
            YT_VERIFY(false && "MY_PROBLEM");
        },
        "YT_VERIFY\\(false && \"MY_PROBLEM\"\\)");

    // Description that is not zero-terminated.
    EXPECT_DEATH(
        {
            YT_VERIFY(false, std::string_view("MY_PROBLEM").substr(0, 5));
        },
        "YT_VERIFY\\(false, \"MY_PR\"\\)");

    // Check escaping double quotes.
    EXPECT_DEATH(
        {
            YT_VERIFY(false, "MY_PROBLEM \"X\"");
        },
        R"_(YT_VERIFY\(false, "MY_PROBLEM \\"X\\""\))_");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
