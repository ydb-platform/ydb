#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/absolute_normalized_path.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAbsoluteNormalizedPathTest, Constructor)
{
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("//////");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/.");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/././././");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("///////.");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("///////.///");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/a");
        EXPECT_EQ(path.Path(), "/a");
    }
    {
        TAbsoluteNormalizedPath path("/a/");
        EXPECT_EQ(path.Path(), "/a");
    }
    {
        TAbsoluteNormalizedPath path("//////////a");
        EXPECT_EQ(path.Path(), "/a");
    }
    {
        TAbsoluteNormalizedPath path("//////////a/");
        EXPECT_EQ(path.Path(), "/a");
    }
    {
        TAbsoluteNormalizedPath path("//////////a////");
        EXPECT_EQ(path.Path(), "/a");
    }
    {
        TAbsoluteNormalizedPath path("/a/..");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/a/./..");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/1/2/3/../../..");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/1/../2/3/../..");
        EXPECT_EQ(path.Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/1/../2/3/..");
        EXPECT_EQ(path.Path(), "/2");
    }

    ASSERT_THROW(TAbsoluteNormalizedPath("."), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath(".."), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath("a"), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath("../"), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath("/.."), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath("//////////////.."), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath("/a////////../////////.."), std::exception);
    ASSERT_THROW(TAbsoluteNormalizedPath("/a/b/c/../../../.."), std::exception);

    {
        std::filesystem::path absolutePath("/absolute");
        TAbsoluteNormalizedPath path(absolutePath);
        EXPECT_EQ(path.Path(), "/absolute");
        EXPECT_EQ(absolutePath, "/absolute");
    }
    {
        std::filesystem::path absolutePath("/absolute");
        TAbsoluteNormalizedPath path(std::move(absolutePath));
        EXPECT_EQ(path.Path(), "/absolute");
        EXPECT_EQ(absolutePath, "");
    }
}

TEST(TAbsoluteNormalizedPathTest, AppendPath)
{
    {
        TAbsoluteNormalizedPath path;
        EXPECT_EQ((path / "/").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "/").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/a");
        EXPECT_EQ((path / "..").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "/.").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "/a/..").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "a/..").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "/a/b/c/").Path(), "/a/b/c");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "/a/../").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        EXPECT_EQ((path / "/").Path(), "/");
    }
    {
        TAbsoluteNormalizedPath path("/");
        ASSERT_THROW(path / "/..", std::exception);
    }
}

TEST(TAbsoluteNormalizedPathTest, Clear)
{
    TAbsoluteNormalizedPath path("/ads");
    path.Clear();
    EXPECT_EQ(path.Path(), "");
}

TEST(TAbsoluteNormalizedPathTest, Swap)
{
    TAbsoluteNormalizedPath first("/second");
    TAbsoluteNormalizedPath second("/first");
    first.Swap(second);
    EXPECT_EQ(first.Path(), "/first");
    EXPECT_EQ(second.Path(), "/second");
}

TEST(TAbsoluteNormalizedPathTest, Move)
{
    TAbsoluteNormalizedPath first("/abc/123");
    TAbsoluteNormalizedPath second(std::move(first));
    EXPECT_EQ(first.Path(), "");
    EXPECT_EQ(second.Path(), "/abc/123");

    first = std::move(second);
    EXPECT_EQ(first.Path(), "/abc/123");
    EXPECT_EQ(second.Path(), "");
}

TEST(TAbsoluteNormalizedPathTest, CopyAssignmentOperator)
{
    TAbsoluteNormalizedPath path;
    path = "/charstr";
    EXPECT_EQ(path.Path(), "/charstr");

    ASSERT_THROW(path = "charStrRelative", std::exception);
    EXPECT_EQ(path.Path(), "/charstr");

    std::filesystem::path newPath("/absolute");
    path = newPath;
    EXPECT_EQ(path.Path(), "/absolute");
}

TEST(TAbsoluteNormalizedPathTest, MoveAssignmentOperator)
{
    TAbsoluteNormalizedPath path;
    std::filesystem::path movePath("/path");
    path = std::move(movePath);
    EXPECT_EQ(path.Path(), "/path");
    EXPECT_EQ(movePath, "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
