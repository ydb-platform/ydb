#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/cgroup/process.h>

namespace NYT::NCGroups {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TParseProcessCGroupsTest, V1Typical)
{
    auto result = ParseProcessCGroups(
        "12:cpu,cpuacct:/user.slice/session-1.scope\n"
        "11:memory:/user.slice/session-1.scope\n"
        "10:pids:/user.slice/session-1.scope\n"
        "9:blkio:/user.slice/session-1.scope\n"
        "1:name=systemd:/user.slice/session-1.scope\n");

    EXPECT_EQ(result["cpu"], "/user.slice/session-1.scope");
    EXPECT_EQ(result["cpuacct"], "/user.slice/session-1.scope");
    EXPECT_EQ(result["memory"], "/user.slice/session-1.scope");
    EXPECT_EQ(result["pids"], "/user.slice/session-1.scope");
    EXPECT_EQ(result["blkio"], "/user.slice/session-1.scope");

    // name=systemd should be skipped.
    EXPECT_EQ(result.find("name=systemd"), result.end());
    EXPECT_EQ(result.find("systemd"), result.end());
}

TEST(TParseProcessCGroupsTest, V1Docker)
{
    auto result = ParseProcessCGroups(
        "3:cpu,cpuacct:/docker/abc123\n"
        "2:memory:/docker/abc123\n"
        "1:name=systemd:/docker/abc123\n");

    EXPECT_EQ(result["cpu"], "/docker/abc123");
    EXPECT_EQ(result["cpuacct"], "/docker/abc123");
    EXPECT_EQ(result["memory"], "/docker/abc123");
}

TEST(TParseProcessCGroupsTest, V1RootCgroup)
{
    auto result = ParseProcessCGroups(
        "3:cpu,cpuacct:/\n"
        "2:memory:/\n");

    EXPECT_EQ(result["cpu"], "/");
    EXPECT_EQ(result["memory"], "/");
}

TEST(TParseProcessCGroupsTest, EmptyInput)
{
    auto result = ParseProcessCGroups("");
    EXPECT_TRUE(result.empty());
}

TEST(TParseProcessCGroupsTest, TrailingNewline)
{
    auto result = ParseProcessCGroups("3:cpu:/user.slice\n");
    EXPECT_EQ(result["cpu"], "/user.slice");
}

TEST(TParseProcessCGroupsTest, V2Typical)
{
    auto result = ParseProcessCGroups("0::/user.slice/session-1.scope\n");

    ASSERT_TRUE(result.contains(""));
    EXPECT_EQ(result[""], "/user.slice/session-1.scope");
}

TEST(TParseProcessCGroupsTest, V2RootCgroup)
{
    auto result = ParseProcessCGroups("0::/\n");

    ASSERT_TRUE(result.contains(""));
    EXPECT_EQ(result[""], "/");
}

TEST(TParseProcessCGroupsTest, Hybrid)
{
    // Some controllers on v1, others on the v2 unified hierarchy.
    auto result = ParseProcessCGroups(
        "3:cpu,cpuacct:/user.slice\n"
        "2:memory:/user.slice\n"
        "0::/user.slice/session-1.scope\n");

    EXPECT_EQ(result["cpu"], "/user.slice");
    EXPECT_EQ(result["memory"], "/user.slice");
    EXPECT_EQ(result[""], "/user.slice/session-1.scope");
}

TEST(TGetProcessCGroupsTest, SelfReturnsNonEmpty)
{
    auto result = GetSelfProcessCGroups();
    EXPECT_FALSE(result.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCGroups
