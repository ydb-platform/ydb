#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/error.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TArithmeticFormulaTestWithoutVariables
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, i64>>
{ };

TEST_P(TArithmeticFormulaTestWithoutVariables, Test)
{
    const auto& args = GetParam();
    const auto& formula = std::get<0>(args);
    const auto& expected = std::get<1>(args);

    auto expr = MakeArithmeticFormula(formula);

    EXPECT_EQ(expected, expr.Eval({}))
        << "formula: " << formula << std::endl
        << "expected value: " << expected;
}

INSTANTIATE_TEST_SUITE_P(
    TArithmeticFormulaTestWithoutVariables1,
    TArithmeticFormulaTestWithoutVariables,
    ::testing::Values(
        // +, -, few /%, parens
        std::tuple("(((35)-35)+(47)+(12))/((13+(31))-(24-(37)-23)+(30-30-33+24-39))", 1),
        std::tuple("((49)-34+(44)-35+(11-28)-(40)%46+20+22)+((19-50-40)-13+(17))", -58),
        std::tuple("17%25-(23)-24+(45)+34-32+23-34-(10)", -4),
        std::tuple("31%26-42+22+(19+48-(40+28)+(27))-(45-21)+(15)+10+26%43", 38),
        std::tuple("((11)+46)-10%14-(29+43)-33-47+27+48+50-((19-18-20)+(46)-(29))", 22),
        std::tuple("(10)-28-13%21+32+34+(40-48+(32)%41)+(32%44)+(35)%37+50-20+(31)", 187),
        std::tuple("(48-29)-(44-(14)-35+28-16-((48)/13-(43)/22)-((12-41+32/45)+32+33-50-39))", -39),

        // all except /%
        std::tuple("(33-(35))*13|31<=43*37||(17+38)==!(34*(16)!=28!=15)<=50<17", 1),
        std::tuple("(12&&49&(17)|27)==20!=35&&46==34>=(!!(16)-41<(10)==11||11*11*19!=((30)<=(40)<=48))", 0),
        std::tuple("!!(37)||(27)<39&28<=25^16|(!40+40)^(47<15<25)<47^45+16==23!=17&&22<=!38!=14", 1),
        std::tuple("!!27!=15==(35||23||!!(40)&&18<=((17>=34)-38==(47))&&22>13|40*31!=40+(!38^43)&(34)|(27))", 1),
        std::tuple("(!(28)-(25)||49^(23>(21)==(36<=17)!=23>(27<=33))!=!!(47)||46^22+19<11>=15)==46", 0),
        std::tuple("(!24-(12)+24&(45)|(43))<=!28==(50&&(46)<21<(17))<=28-(20)<=29&&(28)==!(24)!=24<=(20<=37)", 0),
        std::tuple("16<46&40*30", 0),
        std::tuple("38>=(48)|(13>=30)|!!31!=21|(49!=(31)>=(36)==(33))>=((!(30)==24)*25||48)||!47!=18", 1),
        std::tuple("!22<23^((49)*33)!=50==!(32)||(49)||!(36)+(35)-26>=18>(!(31)>=21-(10||39)||(11)>=25)", 1),

        // spaces
        std::tuple("( 14!=24  )<  27 <=44== 15*22", 0),
        std::tuple("  24<36 >=( 41*48)<= ( !23!=16  )<=( 49- 34)", 1),
        std::tuple(" (34 *13  )!=(48 >= 37  ) <=(  43!=( 19 ) )-  34* 45", 1),
        std::tuple("  13 * 50 +(14) <43 == 17  ==( !18 < 47  ) ", 0),
        std::tuple("  44!=18 >  43 -( 41 )>=(34 )!= 30", 1),
        std::tuple(" (( 14)*(15 ) )+43 -!(23 )>= 36 > 33 <=(24 )", 1),
        std::tuple(" !24 != 43<(!50-34>= 49 )", 0),
        std::tuple(" ! 11>=43  >  19< 12  <((32)>39 )", 0),
        std::tuple("!14 ==46 <=22 !=(15 )< 31", 0),
        std::tuple("(( 38)>= 16  >  20>=(26)  )<= 50 -33", 1),
        std::tuple("  36> 37 == 12< !(11 )> 32 > 25+14 ", 1),
        std::tuple(" (26)>=38  +( !16> 28  )", 0),
        std::tuple(" 30 >(25) * 23 +26-( 20 )", 0),
        std::tuple("!  26<= 11<!  13 + 10 >( 26==36 )", 1),
        std::tuple("!10* ! 33 <16  <= 11", 1),
        std::tuple("(34 )- 50  *  11- 33 !=(26 )<= 16", 1),
        std::tuple(" !( 27)> 30 >=36== 44 != !  18 <= 23  *35", 1),
        std::tuple("( 29 >= 12)<=( ! 35==17)", 0),
        std::tuple("!16 <= 23  ==! 47 <27  <=( (24)< 23 >= 36< 31 )", 1),
        std::tuple(" ! 12 >=17+ ( 25+ 10)<15 < 36", 1)
));

INSTANTIATE_TEST_SUITE_P(
    TArithmeticFormulaTestWithoutVariables2,
    TArithmeticFormulaTestWithoutVariables,
    ::testing::Values(
        // some manual stuff
        std::tuple("1+1", 2),
        std::tuple("1+1+1", 3),
        std::tuple("10-1", 9),
        std::tuple("10-5-3", 2),
        std::tuple( "10 -5 --2", 7),
        std::tuple("( 1) +  -2 ",-1),
        std::tuple("(((((5*5)))))", 25),
        std::tuple( "2 * 2 +  10 / 5 ", 6),
        std::tuple("7 % 4 * 4", 12),
        std::tuple("7 % (4 * 4)", 7),
        std::tuple("1&&2&&3||6", 1),
        std::tuple("1&2&3|6", 6),
        std::tuple("!!(1||(2))", 1),
        std::tuple("(1)-1", 0),
        std::tuple("(1) --1", 2),
        std::tuple("9223372036854775807", std::numeric_limits<i64>::max()),
        std::tuple("-9223372036854775808", std::numeric_limits<i64>::min()),
        std::tuple("10 in (10, 20, 30)", 1),
        std::tuple("10 in 10, 20, 30", 1),
        std::tuple("5+10 in 12+3", 1),
        std::tuple("5+(10 in 12)+3", 8),
        std::tuple("10 in 5, 6, 7+4, 2+8", 1),
        std::tuple("11 in 5, 6, 7+4, 2+8", 1),

        std::tuple("1 + %true", 2),
        std::tuple("%true+1", 2),
        std::tuple("10%%true", 0),
        std::tuple("10 % %true", 0)
));

////////////////////////////////////////////////////////////////////////////////

class TArithmeticFormulaTestParseError
    : public ::testing::Test
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TArithmeticFormulaTestParseError, Test)
{
    const auto& args = GetParam();
    const auto& formula = args;

    EXPECT_THROW(MakeArithmeticFormula(formula), TErrorException);
}

INSTANTIATE_TEST_SUITE_P(
    TArithmeticFormulaTestParseError,
    TArithmeticFormulaTestParseError,
    ::testing::Values(
        "()",
        "1 1",
        ")",
        "(",
        "(1)---1",
        "+1",
        "--",
        "- 1",
        "1var",
        "(-)",
        "(1+)",
        "++a",
        "-(1+2)", // unary minus is not yet supported
        "2//2",
        "(^_^)", // q^_^p is a valid expression btw
        "2=3",
        "!= 10",
        "a = = b",
        "a > = b",
        "a =- b",
        "in + 1",
        "in in",
        "5, 6,",
        ",",
        ", 5",
        "in 2",
        "10 in",
        "10 in ()",
        "1, 1",
        "1, 1 in 1",
        "1, 1 in (1, 1)",
        "(1, 1) in (1, 1)",
        "1, 1 + 2",
        "in/",
        "1,(1,1)",
        "%%true",
        "%true.foo",
        "1%% true",
        "1 %in b"
));

////////////////////////////////////////////////////////////////////////////////

TEST(TArithmeticFormulaTest, Misc)
{
    EXPECT_THROW(MakeArithmeticFormula("9223372036854775808"), TFromStringException);

    auto divideByZero = MakeArithmeticFormula("1/0");
    EXPECT_THROW(divideByZero.Eval({}), TErrorException);

    auto modulusByZero = MakeArithmeticFormula("1%(10-5*2)");
    EXPECT_THROW(modulusByZero.Eval({}), TErrorException);

    auto divideMinIntByMinusOne = MakeArithmeticFormula("a / -1");
    EXPECT_THROW(divideMinIntByMinusOne.Eval({{"a", std::numeric_limits<i64>::min()}}), TErrorException);

    auto minIntModuloMinusOne = MakeArithmeticFormula("a % -1");
    EXPECT_THROW(minIntModuloMinusOne.Eval({{"a", std::numeric_limits<i64>::min()}}), TErrorException);

    auto formulaWithVariables = MakeArithmeticFormula("( a+ b)/c");

    // divide by zero
    EXPECT_THROW(formulaWithVariables.Eval({{"a", 1}, {"b", 2}, {"c", 0}}), TErrorException);

    // not enough variables
    EXPECT_THROW(formulaWithVariables.Eval({{"a", 1}, {"b", 2}}), TErrorException);

    EXPECT_EQ(10, formulaWithVariables.Eval({{"a", 20}, {"b", 30}, {"c", 5}}));

    auto longVars = MakeArithmeticFormula("abacaba ^ variable");
    EXPECT_EQ(435, longVars.Eval({{"variable", 123}, {"abacaba", 456}}));

    auto schedule = MakeArithmeticFormula("hours % 2 == 1 && minutes % 5 == 2");
    int cnt = 0;
    for (int hour = 0; hour < 24; ++hour) {
        for (int minute = 0; minute < 60; ++minute) {
            if (schedule.Eval({{"hours", hour}, {"minutes", minute}})) {
                ++cnt;
            }
        }
    }
    EXPECT_EQ(cnt, (24 / 2) * (60 / 5));

    auto emptyFormula = MakeArithmeticFormula("");
    EXPECT_TRUE(emptyFormula.IsEmpty());
    EXPECT_THROW(emptyFormula.Eval({}), TErrorException);

    auto modulusByTrue = MakeArithmeticFormula("123 %true");
    EXPECT_EQ(modulusByTrue.Eval({{"true", 100}}), 23);
    EXPECT_THROW(modulusByTrue.Eval({}), TErrorException);
}

TEST(TArithmeticFormulaTest, InExpression)
{
    auto formulaWithIn = MakeArithmeticFormula("inf in (1000*1000*1000, 10, 20, 30)");
    EXPECT_EQ(1, formulaWithIn.Eval({{"inf", 1'000'000'000}}));

    auto multilevelIn = MakeArithmeticFormula("a in b in c");
    EXPECT_EQ(1, multilevelIn.Eval({{"a", 1}, {"b", 1}, {"c", 1}}));
    EXPECT_EQ(0, multilevelIn.Eval({{"a", 0}, {"b", 1}, {"c", 1}}));
    // Check associativity.
    EXPECT_EQ(1, multilevelIn.Eval({{"a", 1}, {"b", 2}, {"c", 0}}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TArithmeticFormulaTest, Equality)
{
    auto f1 = MakeArithmeticFormula("a+b");
    auto f2 = MakeArithmeticFormula("a + b");
    auto f3 = MakeArithmeticFormula("(a + (b))");
    auto f4 = MakeArithmeticFormula("b+a");
    auto f5 = MakeArithmeticFormula("a");

    auto fn1 = MakeArithmeticFormula("1");
    auto fn2 = MakeArithmeticFormula("2");

    auto assertComparison = [] (const TArithmeticFormula& lhs, const TArithmeticFormula& rhs, bool equal) {
        if (equal) {
            EXPECT_TRUE(lhs == rhs);
            EXPECT_EQ(lhs.GetHash(), rhs.GetHash());
        } else {
            EXPECT_FALSE(lhs == rhs);
            EXPECT_NE(lhs.GetHash(), rhs.GetHash());
        }
    };

    assertComparison(f1, f2, true);
    assertComparison(f1, f3, true);
    assertComparison(f1, f4, false);
    assertComparison(f1, f5, false);
    assertComparison(f2, f1, true);
    assertComparison(f2, f3, true);
    assertComparison(f2, f4, false);
    assertComparison(f2, f5, false);
    assertComparison(f3, f1, true);
    assertComparison(f3, f2, true);
    assertComparison(f3, f4, false);
    assertComparison(f3, f5, false);
    assertComparison(f4, f1, false);
    assertComparison(f4, f2, false);
    assertComparison(f4, f3, false);
    assertComparison(f4, f5, false);
    assertComparison(f5, f1, false);
    assertComparison(f5, f2, false);
    assertComparison(f5, f3, false);
    assertComparison(f5, f4, false);

    assertComparison(fn1, fn2, false);
    assertComparison(fn1, f5, false);

    assertComparison(MakeArithmeticFormula("1"), MakeArithmeticFormula("%true"), false);
    assertComparison(MakeArithmeticFormula("%false"), MakeArithmeticFormula("%true"), false);
}

TEST(TArithmeticFormulaTest, TestValidateVariable)
{
    ValidateArithmeticFormulaVariable("abc");
    ValidateArithmeticFormulaVariable("abc123");
    ValidateArithmeticFormulaVariable("ABc_123");
    ValidateArithmeticFormulaVariable("IN");
    ValidateArithmeticFormulaVariable("iN");
    ValidateArithmeticFormulaVariable("In");

    EXPECT_THROW(ValidateArithmeticFormulaVariable("123abc"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable(" abc"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable(" abc"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable("ab c"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable(""), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable("dollar$"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable("a+b"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable("in"), TErrorException);

    // OK for boolean, but parse error for arithmetic
    EXPECT_THROW(ValidateArithmeticFormulaVariable("var/2"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable("var-var-var"), TErrorException);
    EXPECT_THROW(ValidateArithmeticFormulaVariable("tablet_common/news-queue"), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
