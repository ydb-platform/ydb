#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/containers/non_empty.h>

#include <vector>
#include <string>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TNonEmptyTest, ConstructFromNonEmptyVector)
{
    std::vector<int> vec = {1, 2, 3};
    TNonEmpty<std::vector<int>> nonEmpty(vec);

    EXPECT_EQ(nonEmpty.size(), 3u);
    EXPECT_EQ(nonEmpty[0], 1);
    EXPECT_EQ(nonEmpty[1], 2);
    EXPECT_EQ(nonEmpty[2], 3);
}

TEST(TNonEmptyTest, ConstructFromEmptyVectorThrows)
{
#ifdef NDEBUG
    GTEST_SKIP() << "Assertions are disabled in release build";
#endif

    std::vector<int> vec;
    EXPECT_DEATH(
        { TNonEmpty<std::vector<int>> nonEmpty(vec); },
        ".*"
    );
}

TEST(TNonEmptyTest, FrontAndBack)
{
    TNonEmpty<std::vector<int>> nonEmpty = {10, 20, 30};

    EXPECT_EQ(nonEmpty.front(), 10);
    EXPECT_EQ(nonEmpty.back(), 30);

    nonEmpty.front() = 100;
    nonEmpty.back() = 300;

    EXPECT_EQ(nonEmpty[0], 100);
    EXPECT_EQ(nonEmpty[2], 300);
}

TEST(TNonEmptyTest, PushBackAndEmplaceBack)
{
    TNonEmpty<std::vector<std::string>> nonEmpty = {"hello"};

    nonEmpty.push_back("world");
    EXPECT_EQ(nonEmpty.size(), 2u);
    EXPECT_EQ(nonEmpty[1], "world");

    nonEmpty.emplace_back("test");
    EXPECT_EQ(nonEmpty.size(), 3u);
    EXPECT_EQ(nonEmpty[2], "test");
}

TEST(TNonEmptyTest, PopBackMaintainsNonEmpty)
{
    TNonEmpty<std::vector<int>> nonEmpty = {1, 2, 3};

    nonEmpty.pop_back();
    EXPECT_EQ(nonEmpty.size(), 2u);

    nonEmpty.pop_back();
    EXPECT_EQ(nonEmpty.size(), 1u);

#ifdef NDEBUG
    GTEST_SKIP() << "Assertions are disabled in release build";
#endif

    // Cannot pop the last element
    EXPECT_DEATH(
        { nonEmpty.pop_back(); },
        ".*"
    );
}

TEST(TNonEmptyTest, ResizeWithAssertion)
{
    TNonEmpty<std::vector<int>> nonEmpty = {1, 2, 3};

    nonEmpty.resize(5);
    EXPECT_EQ(nonEmpty.size(), 5u);

    nonEmpty.resize(2, 99);
    EXPECT_EQ(nonEmpty.size(), 2u);

#ifdef NDEBUG
    GTEST_SKIP() << "Assertions are disabled in release build";
#endif

    // Cannot resize to 0
    EXPECT_DEATH(
        { nonEmpty.resize(0); },
        ".*"
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
