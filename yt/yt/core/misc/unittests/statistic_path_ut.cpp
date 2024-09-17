#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/statistic_path.h>

namespace NYT {
namespace {

using namespace NStatisticPath;
using namespace NStatisticPathLiterals;

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Literal)
{
    EXPECT_EQ("ABC"_L.Literal(), "ABC");

    EXPECT_TRUE(CheckStatisticPathLiteral("$a/b.c").IsOK());
    EXPECT_FALSE(CheckStatisticPathLiteral(TString(Delimiter)).IsOK());
    EXPECT_FALSE(CheckStatisticPathLiteral("\0").IsOK());
    EXPECT_FALSE(CheckStatisticPathLiteral("").IsOK());

    EXPECT_THROW(TStatisticPathLiteral("\0"), TErrorException);
    EXPECT_THROW(TStatisticPathLiteral(TString(Delimiter)), TErrorException);
    EXPECT_THROW(TStatisticPathLiteral(""), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Append)
{
    EXPECT_EQ(("A"_L / "BB"_L / "CCC"_L).Path(),
        TString(Delimiter) + "A" + Delimiter + "BB" + Delimiter + "CCC");

    EXPECT_EQ(("A"_L / "B"_L) / "C"_L, "A"_L / ("B"_L / "C"_L));

    EXPECT_EQ("A"_L / TStatisticPath(), "A"_L);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Iterator)
{
    EXPECT_TRUE(std::bidirectional_iterator<TStatisticPath::const_iterator>);
    EXPECT_TRUE(std::bidirectional_iterator<TStatisticPath::const_reverse_iterator>);
    EXPECT_TRUE(std::ranges::range<TStatisticPath>);

    TStatisticPath path = "A"_L / "BB"_L / "CCC"_L;

    std::vector<TString> expected{"A", "BB", "CCC"};

    {
        std::vector<TString> actual(path.begin(), path.end());
        EXPECT_EQ(actual, expected);
    }

    {
        std::reverse(expected.begin(), expected.end());
        std::vector<TString> actual(path.rbegin(), path.rend());
        EXPECT_EQ(actual, expected);
    }

    {
        // Post-increment, post-decrement validity test.
        auto it = path.begin();
        EXPECT_EQ(*(it++), "A");
        EXPECT_EQ(*it, "BB");
        EXPECT_EQ(*(it--), "BB");
        EXPECT_EQ(*it, "A");
    }

    // Sanity check.
    auto it = path.begin();
    const auto& value = *it;
    ++it;
    EXPECT_NE(value, *it);

    TStatisticPath emptyPath;
    EXPECT_EQ(emptyPath.begin(), emptyPath.end());
    EXPECT_TRUE(emptyPath.Empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Compare)
{
    EXPECT_EQ("A"_L / "B"_L, "A"_L / "B"_L);

    EXPECT_LT("A"_L / "B"_L, "A"_L / "B"_L / "C"_L);

    EXPECT_LT("A"_L / "B"_L, "A"_L / "C"_L);
    EXPECT_LT("A"_L / "B"_L, "B"_L);
}

////////////////////////////////////////////////////////////////////////////////

// We want a statistic path and it's "path prefix" to be adjacent according to
// path comparison. See YT-22118 for motivation.
TEST(TStatisticPathTest, Adjacent)
{
    for (TChar c = std::numeric_limits<TChar>::min();; ++c) {
        auto literal = ParseStatisticPathLiteral(TString("A") + c + "B");
        if (literal.IsOK()) {
            EXPECT_LT("A"_L / "B"_L, TStatisticPath(literal.Value()));
        }
        if (c == std::numeric_limits<TChar>::max()) {
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Parse)
{
    EXPECT_EQ(ParseStatisticPath(TString(Delimiter) + "ABC" + Delimiter + "DEF").ValueOrThrow(), "ABC"_L / "DEF"_L);
    EXPECT_EQ(ParseStatisticPath("").ValueOrThrow(), TStatisticPath());

    EXPECT_FALSE(ParseStatisticPath(TString(Delimiter) + "A\0B"_sb).IsOK());
    EXPECT_FALSE(ParseStatisticPath(TString(Delimiter) + "AB" + Delimiter).IsOK());
    EXPECT_FALSE(ParseStatisticPath(TString(Delimiter) + "A" + Delimiter + Delimiter + "B").IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Constructor)
{
    EXPECT_EQ(TStatisticPath("abc"_L).Path(), TString(Delimiter) + "abc");

    TStatisticPath defaultConstructed;
    EXPECT_EQ(defaultConstructed.Path(), TString());
    EXPECT_TRUE(defaultConstructed.Empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, StartsWith)
{
    // Simple prefix.
    EXPECT_TRUE(("A"_L / "BB"_L / "CCC"_L).StartsWith("A"_L / "BB"_L));
    // Equal paths.
    EXPECT_TRUE(("A"_L / "BB"_L).StartsWith("A"_L / "BB"_L));
    // Empty prefix.
    EXPECT_TRUE(("A"_L / "B"_L).StartsWith({}));

    // Reverse prefix.
    EXPECT_FALSE(("A"_L / "BB"_L).StartsWith("A"_L / "BB"_L / "CCC"_L));
    // There are string prefixes that are not path prefixes.
    EXPECT_FALSE(("A"_L / "BB"_L / "CCC"_L).StartsWith("A"_L / "BB"_L / "C"_L));
    // Sanity check.
    EXPECT_FALSE(("A"_L / "B"_L).StartsWith("B"_L));
}

TEST(TStatisticPathTest, EndsWith)
{
    // Simple suffix.
    EXPECT_TRUE(("A"_L / "BB"_L / "CCC"_L).EndsWith("BB"_L / "CCC"_L));
    // Equal paths.
    EXPECT_TRUE(("A"_L / "BB"_L).EndsWith("A"_L / "BB"_L));
    // Empty prefix.
    EXPECT_TRUE(("A"_L / "B"_L).EndsWith({}));

    // Reverse suffix.
    EXPECT_FALSE(("A"_L / "BB"_L).EndsWith("CCC"_L / "A"_L / "BB"_L));
    // There are string suffixes that are not path suffixes.
    EXPECT_FALSE(("CCC"_L / "A"_L / "BB"_L).EndsWith("C"_L / "A"_L / "BB"_L));
    // Sanity check.
    EXPECT_FALSE(("A"_L / "B"_L).EndsWith("A"_L));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, FrontBack)
{
    EXPECT_EQ(("A"_L / "BB"_L / "CCC"_L).Front(), "A");
    EXPECT_EQ(("A"_L / "BB"_L / "CCC"_L).Back(), "CCC");
    EXPECT_EQ(TStatisticPath("A"_L).Front(), "A");
    EXPECT_EQ(TStatisticPath("A"_L).Back(), "A");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, PopBack)
{
    TStatisticPath path = "A"_L / "BB"_L / "CCC"_L;
    EXPECT_EQ(path.Back(), "CCC");
    path.PopBack();
    EXPECT_EQ(path.Back(), "BB");
    path.PopBack();
    EXPECT_EQ(path.Back(), "A");
    path.PopBack();
    EXPECT_TRUE(path.Empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatisticPathTest, Swap)
{
    TStatisticPath a = "A"_L / "BB"_L;
    TStatisticPath b = "B"_L / "AA"_L;
    TStatisticPath originalA = a;
    TStatisticPath originalB = b;

    a.Swap(b);
    EXPECT_EQ(a, originalB);
    EXPECT_EQ(b, originalA);

    a.Swap(b);
    EXPECT_EQ(a, originalA);
    EXPECT_EQ(b, originalB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
