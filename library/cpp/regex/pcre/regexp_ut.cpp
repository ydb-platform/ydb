#include <library/cpp/testing/unittest/registar.h>

#include <util/string/strip.h>
#include <library/cpp/regex/pcre/regexp.h>
#include <util/stream/output.h>

struct TRegTest {
    const char* Regexp;
    const char* Data;
    const char* Result;
    int CompileOptions;
    int RunOptions;

    TRegTest(const char* re, const char* text, const char* res, int copts = REG_EXTENDED, int ropts = 0)
        : Regexp(re)
        , Data(text)
        , Result(res)
        , CompileOptions(copts)
        , RunOptions(ropts)
    {
    }
};

struct TSubstTest: public TRegTest {
    const char* Replacement;
    const char* Replacement2;

    TSubstTest(const char* re, const char* text, const char* res, const char* repl, const char* repl2)
        : TRegTest(re, text, res, REG_EXTENDED, REGEXP_GLOBAL)
        , Replacement(repl)
        , Replacement2(repl2)
    {
    }
};

const TRegTest REGTEST_DATA[] = {
    TRegTest("test", "its a test and test string.", "6 10", REG_EXTENDED, 0),
    TRegTest("test", "its a test and test string.", "6 10 15 19", REG_EXTENDED, REGEXP_GLOBAL),
    TRegTest("test|[an]{0,0}", "test and test an test string tes", "0 4 4 4 5 5 6 6 7 7 8 8 9 13 13 13 14 14 15 15 16 16 17 21 21 21 22 22 23 23 24 24 25 25 26 26 27 27 28 28 29 29 30 30 31 31 32 32", REG_EXTENDED, REGEXP_GLOBAL),
    TRegTest("test[an]{1,}", "test and test an test string tes", "NM", REG_EXTENDED, REGEXP_GLOBAL)};

const TSubstTest SUBSTTEST_DATA[] = {
    TSubstTest("([a-zA-Z]*[0-9]+) (_[a-z]+)", "Xxx123 534 ___124 bsd _A ZXC _L 141 _sd dsfg QWE123 _bbb", "141 XXX/_sd", "$1 XXX/$2", "$2$2$2 YY$1Y/$2")};

class TRegexpTest: public TTestBase {
private:
    regmatch_t Matches[NMATCHES];

private:
    UNIT_TEST_SUITE(TRegexpTest);
    UNIT_TEST(TestRe)
    UNIT_TEST(TestSubst)
    UNIT_TEST(TestOffEndOfBuffer);
    UNIT_TEST_SUITE_END();

    inline void TestRe() {
        for (const auto& regTest : REGTEST_DATA) {
            memset(Matches, 0, sizeof(Matches));
            TString result;

            TRegExBase re(regTest.Regexp, regTest.CompileOptions);
            if (re.Exec(regTest.Data, Matches, regTest.RunOptions) == 0) {
                for (auto& matche : Matches) {
                    if (matche.rm_so == -1) {
                        break;
                    }
                    result.append(Sprintf("%i %i ", matche.rm_so, matche.rm_eo));
                }
            } else {
                result = "NM";
            }
            StripInPlace(result);
            UNIT_ASSERT_VALUES_EQUAL(result, regTest.Result);
        }
    }

    inline void TestSubst() {
        for (const auto& substTest : SUBSTTEST_DATA) {
            TRegExSubst subst(substTest.Regexp, substTest.CompileOptions);
            subst.ParseReplacement(substTest.Replacement);
            TString result = subst.Replace(substTest.Data, substTest.RunOptions);
            UNIT_ASSERT_VALUES_EQUAL(result, substTest.Result);
            TRegExSubst substCopy = subst;
            subst.ParseReplacement(substTest.Replacement2);
            TString newResult = subst.Replace(substTest.Data, substTest.RunOptions);
            UNIT_ASSERT_VALUES_UNEQUAL(newResult.c_str(), result.c_str());
            TString copyResult = substCopy.Replace(substTest.Data, substTest.RunOptions);
            UNIT_ASSERT_VALUES_EQUAL(copyResult, result);
            substCopy = subst;
            copyResult = substCopy.Replace(substTest.Data, substTest.RunOptions);
            UNIT_ASSERT_VALUES_EQUAL(copyResult, newResult);
        }
    }

    void TestOffEndOfBuffer() {
        const TString needle{".*[^./]gov[.].*"};
        TRegExMatch re{needle, REG_UTF8};
        const TString haystack{"fakty.ictv.ua"};
        UNIT_ASSERT_VALUES_EQUAL(re.Match(haystack.c_str()), false);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TRegexpTest);
