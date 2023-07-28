#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/error.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTimeFormulaParseTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, bool>>
{ };

TEST_P(TTimeFormulaParseTest, Test)
{
    const auto& args = GetParam();
    const auto& formula = std::get<0>(args);
    bool wellformed = std::get<1>(args);

    if (wellformed) {
        MakeTimeFormula(formula);
    } else {
        EXPECT_THROW(MakeTimeFormula(formula), TErrorException);
    }
}


INSTANTIATE_TEST_SUITE_P(
    TTimeFormulaParseTest,
    TTimeFormulaParseTest,
    ::testing::Values(
        std::make_tuple("", true),
        std::make_tuple("hours == 0", true),
        std::make_tuple("minutes == 0", true),
        std::make_tuple("hours % 2 == 0 && minutes == 1", true),
        std::make_tuple("hours * 100 + minutes >= 1030", true),
        std::make_tuple("seconds < 10", false),
        std::make_tuple("HOURS > 10", false),
        std::make_tuple("hours ++ 1", false)
));

class TTimeFormulaCorrectnessTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, int>>
{ };

TEST_P(TTimeFormulaCorrectnessTest, Test)
{
    const auto& args = GetParam();
    const auto& formula = std::get<0>(args);
    int expectedCount = std::get<1>(args);

    auto timeFormula = MakeTimeFormula(formula);


    struct timeval tv;
    tv.tv_usec = 0;
    tv.tv_sec = 1527777898;
    TInstant timePoint(tv);

    int count = 0;
    for (int i  = 0; i < 24 * 60; ++i) {
        count += timeFormula.IsSatisfiedBy(timePoint);
        timePoint += TDuration::Minutes(1);
    }

    EXPECT_EQ(count, expectedCount);
}

INSTANTIATE_TEST_SUITE_P(
    TTimeFormulaCorrectnessTest,
    TTimeFormulaCorrectnessTest,
    ::testing::Values(
        std::make_tuple("1", 24 * 60),
        std::make_tuple("hours == 0", 60),
        std::make_tuple("minutes == 0", 24),
        std::make_tuple("hours % 2 == 0 && minutes == 1", 12),
        std::make_tuple("hours * 100 + minutes >= 1030", 24 * 60 - (10 * 60 + 30)),
        std::make_tuple("minutes % 5 == 3 || minutes % 5 == 0", 24 * 60 / 5 * 2)
));

////////////////////////////////////////////////////////////////////////////////

TEST(TTimeFormulaTest, Misc)
{
    auto formula1 = MakeTimeFormula("");
    auto formula2 = MakeTimeFormula("hours/0");
    EXPECT_THROW(formula1.IsSatisfiedBy(TInstant{}), TErrorException);
    EXPECT_THROW(formula2.IsSatisfiedBy(TInstant{}), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
