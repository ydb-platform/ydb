#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/error.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TBooleanFormulaTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        std::vector<TString>,
        bool>>
{ };

TEST_P(TBooleanFormulaTest, Test)
{
    const auto& args = GetParam();
    const auto& formula = std::get<0>(args);
    const auto& values = std::get<1>(args);
    bool expected = std::get<2>(args);

    auto filter = MakeBooleanFormula(formula);

    EXPECT_EQ(expected, filter.IsSatisfiedBy(values))
        << "formula: " << formula << std::endl
        << "true variables: " << ::testing::PrintToString(values) << std::endl
        << "expected: " << expected;
}

INSTANTIATE_TEST_SUITE_P(
    TBooleanFormulaTest,
    TBooleanFormulaTest,
    ::testing::Values(
        std::tuple("", std::vector<TString>{}, true),
        std::tuple("", std::vector<TString>{"b"}, true),
        std::tuple("a", std::vector<TString>{"b"}, false),
        std::tuple("!a", std::vector<TString>{"b"}, true),
        std::tuple("b", std::vector<TString>{"b"}, true),
        std::tuple("a|b", std::vector<TString>{"b"}, true),
        std::tuple("a & b", std::vector<TString>{"b"}, false),
        std::tuple("(b)", std::vector<TString>{"b"}, true),
        std::tuple("a|(a|b)", std::vector<TString>{"b"}, true),
        std::tuple("(a|b)&(!a&b)", std::vector<TString>{"b"}, true),
        std::tuple("a&b", std::vector<TString>{"a", "b"}, true),
        std::tuple("(a|c)&(b|c)", std::vector<TString>{"a", "b"}, true),
        std::tuple("(a|b)&c", std::vector<TString>{"a", "b"}, false),
        std::tuple("a|b|c", std::vector<TString>{"b"}, true),
        std::tuple("!a & b & !c", std::vector<TString>{"b"}, true),
        std::tuple("var-1 | !var/2", std::vector<TString>{"var-1"}, true),
        std::tuple("var-1 | !var/2", std::vector<TString>{"var/2"}, false),
        std::tuple("var-1 | !var/2", std::vector<TString>{}, true),
        std::tuple("!in-", std::vector<TString>{}, true),
        std::tuple("in/|x", std::vector<TString>{"in/"}, true),
        std::tuple("%true", std::vector<TString>{""}, true),
        std::tuple("%false", std::vector<TString>{"false"}, false),
        std::tuple("%true|%false", std::vector<TString>{""}, true),
        std::tuple("a.b.c-d.e:1234", std::vector<TString>{"a.b.c-d.e:1234"}, true),
        std::tuple("!a.b.c-d.e:1234", std::vector<TString>{"a.b.c-d.e:1234"}, false)
));

////////////////////////////////////////////////////////////////////////////////

class TBooleanFormulaParseErrorTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TBooleanFormulaParseErrorTest, Test)
{
    const auto& formula = GetParam();

    EXPECT_THROW(MakeBooleanFormula(formula), std::exception)
        << "formula: " << formula;
}

INSTANTIATE_TEST_SUITE_P(
    TBooleanFormulaParseErrorTest,
    TBooleanFormulaParseErrorTest,
    ::testing::Values(
        "!",
        "&",
        "|",
        "(",
        ")",
        "()",
        "()|a",
        "a&()",
        "a&(",
        "a|)",
        "&a",
        "a&",
        "a!",
        "a!b",
        "a|c!",
        "a!|c",
        "a|(c!)",
        "a|(c&)",
        "a|(|c)",
        "1",
        "a||b",
        "a&&b",
        "a+b",
        "a^b",
        "a==b",
        "a!=b",
        "a>=b",
        "a<=b",
        "a>b",
        "a<b",
        "a*b",
        "a%b",
        "/a",
        "-",
        "a /b",
        "a & /b",
        "in",
        "% true",
        "%something",
        "%true.abc"
));

class TBooleanFormulaTest2
    : public ::testing::Test
{ };

TEST_F(TBooleanFormulaTest2, Equality)
{
    auto formulaA = MakeBooleanFormula("internal");
    auto formulaB = MakeBooleanFormula("internal");
    auto formulaC = MakeBooleanFormula("cloud");

    EXPECT_TRUE(formulaA == formulaB);
    EXPECT_FALSE(formulaA == formulaC);

    EXPECT_TRUE(formulaA.GetHash() == formulaB.GetHash());
    EXPECT_FALSE(formulaA.GetHash() == formulaC.GetHash());
}

TEST(TBooleanFormulaTest, ValidateVariable)
{
    ValidateBooleanFormulaVariable("var");
    ValidateBooleanFormulaVariable("var/2");
    ValidateBooleanFormulaVariable("var-var-var");
    ValidateBooleanFormulaVariable("tablet_common/news-queue");
    ValidateBooleanFormulaVariable("VAR123VAR");
    ValidateBooleanFormulaVariable("IN");
    ValidateBooleanFormulaVariable("2var");

    EXPECT_THROW(ValidateBooleanFormulaVariable("2"), TErrorException);
    EXPECT_THROW(ValidateBooleanFormulaVariable("foo bar"), TErrorException);
    EXPECT_THROW(ValidateBooleanFormulaVariable(""), TErrorException);
    EXPECT_THROW(ValidateBooleanFormulaVariable("a+b"), TErrorException);
    EXPECT_THROW(ValidateBooleanFormulaVariable("in"), TErrorException);
}

TEST(TBooleanFormulaTest, ExternalOperators)
{
    auto formulaA = MakeBooleanFormula("a");
    auto formulaB = MakeBooleanFormula("b");
    auto aAndB = formulaA & formulaB;
    auto aOrB = formulaA | formulaB;
    auto notA = !formulaA;

    for (auto vars : std::vector<std::vector<TString>>{{}, {"a"}, {"b"}, {"a", "b"}}) {
        bool resA = formulaA.IsSatisfiedBy(vars);
        bool resB = formulaB.IsSatisfiedBy(vars);

        EXPECT_EQ(resA & resB, aAndB.IsSatisfiedBy(vars));
        EXPECT_EQ(resA | resB, aOrB.IsSatisfiedBy(vars));
        EXPECT_EQ(!resA, notA.IsSatisfiedBy(vars));
    }

    EXPECT_FALSE((!MakeBooleanFormula("a | b"))
        .IsSatisfiedBy(std::vector<TString>{"b"}));

    EXPECT_EQ((formulaA & formulaB).GetFormula(), "(a) & (b)");
    EXPECT_EQ((formulaA | formulaB).GetFormula(), "(a) | (b)");
    EXPECT_EQ((!formulaA).GetFormula(), "!(a)");

    auto empty = MakeBooleanFormula("");
    EXPECT_TRUE(empty.IsSatisfiedBy(THashSet<TString>{}));
    EXPECT_FALSE((!empty).IsSatisfiedBy(THashSet<TString>{}));
    EXPECT_TRUE((empty | !empty).IsSatisfiedBy(THashSet<TString>{}));

    EXPECT_TRUE((empty | formulaA).IsSatisfiedBy(THashSet<TString>{}));
    EXPECT_TRUE((empty | formulaA).IsSatisfiedBy(THashSet<TString>{"a"}));
    EXPECT_TRUE((formulaA | empty).IsSatisfiedBy(THashSet<TString>{}));
    EXPECT_TRUE((formulaA | empty).IsSatisfiedBy(THashSet<TString>{"a"}));
    EXPECT_FALSE((empty & formulaA).IsSatisfiedBy(THashSet<TString>{}));
    EXPECT_TRUE((empty & formulaA).IsSatisfiedBy(THashSet<TString>{"a"}));
    EXPECT_FALSE((formulaA & empty).IsSatisfiedBy(THashSet<TString>{}));
    EXPECT_TRUE((formulaA & empty).IsSatisfiedBy(THashSet<TString>{"a"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
