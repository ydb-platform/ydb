#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/pattern_formatter.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatterTest
    : public ::testing::Test
{
protected:
    TPatternFormatter Formatter;
};

#define EXPECT_FORMAT(pattern, expected) \
    EXPECT_EQ((expected), Formatter.Format(pattern))

TEST_F(TPatternFormatterTest, EmptyPattern)
{
    Formatter.SetProperty("key", "value");
    EXPECT_FORMAT("", "");
}

TEST_F(TPatternFormatterTest, PatternWithoutProperties)
{
    Formatter.SetProperty("key", "value");
    EXPECT_FORMAT("some text", "some text");
}

TEST_F(TPatternFormatterTest, PropertyWithEmptyName1)
{
    Formatter.SetProperty("", "");
    EXPECT_FORMAT("<<-$()->>", "<<-->>");
}

TEST_F(TPatternFormatterTest, PropertyWithEmptyName2)
{
    Formatter.SetProperty("", "foobar");
    EXPECT_FORMAT("<<-$()->>", "<<-foobar->>");
}

TEST_F(TPatternFormatterTest, CommonCase)
{
    Formatter.SetProperty("a", "1");
    Formatter.SetProperty("b", "10");
    Formatter.SetProperty("c", "100");
    EXPECT_FORMAT("$(a)", "1");
    EXPECT_FORMAT(
        "Hello! You own me $(a)$(b) dollars; not $(c)!",
        "Hello! You own me 110 dollars; not 100!");

}

TEST_F(TPatternFormatterTest, MultiplePropertyDeclaration)
{
    Formatter.SetProperty("x", "1");
    EXPECT_FORMAT("<$(x)>", "<1>");
    Formatter.SetProperty("x", "2");
    EXPECT_FORMAT("<$(x)>", "<2>");
    Formatter.SetProperty("x", "3");
    EXPECT_FORMAT("<$(x)>", "<3>");
}

TEST_F(TPatternFormatterTest, MultiplePropertyUsage)
{
    Formatter.SetProperty("x", "2");
    EXPECT_FORMAT("$(x) = 2", "2 = 2");
    EXPECT_FORMAT("$(x) + $(x) = 4", "2 + 2 = 4");
    EXPECT_FORMAT("$(x) + $(x) + $(x) = 6", "2 + 2 + 2 = 6");
}

TEST_F(TPatternFormatterTest, UndefinedValue)
{
    Formatter.SetProperty("a", "b");
    Formatter.SetProperty("key", "value");
    EXPECT_FORMAT("$(a) $(kkeeyy)", "b $(kkeeyy)");
    EXPECT_FORMAT("$$(a)", "$b");
}

TEST_F(TPatternFormatterTest, NoRightParenthesis)
{
    Formatter.SetProperty("a", "b");
    EXPECT_FORMAT("$(a", "$(a");
}

#undef EXPECT_FORMAT

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

