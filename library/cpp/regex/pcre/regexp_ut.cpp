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
    UNIT_TEST(TestSmallArray)
    UNIT_TEST(TestSmallArrayOverflow)
    UNIT_TEST(Test99CaptureGroups)
    UNIT_TEST(Test100CaptureGroups)
    UNIT_TEST(Test300CaptureGroups)
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

    // Test with small array to verify bounds checking works correctly
    // Sanitizers will detect any buffer overflows
    void TestSmallArray() {
        // Create small array of 5 elements
        regmatch_t smallArray[5] = {};

        // Create regex
        TRegExBase re("a+", REG_EXTENDED);

        const char* testString = "aaa bbb aaa";

        // Call Exec with REGEXP_GLOBAL and small array
        // Our fix ensures memset only initializes 5 elements, not NMATCHES
        int result = re.Exec(testString, smallArray, REGEXP_GLOBAL, 5);

        // Should succeed without buffer overflow
        UNIT_ASSERT_VALUES_EQUAL(result, 0);

        // Verify we got matches
        UNIT_ASSERT(smallArray[0].rm_so >= 0);
    }

    // Test that checks pmatch array bounds when number of capture groups
    // exceeds pmatch size but is still much less than NMATCHES
    void TestSmallArrayOverflow() {
        // Create regex with 10 capture groups
        TStringBuilder pattern;
        pattern.Out.Reserve(10 * 4);
        for (int i = 0; i < 10; i++) {
            pattern << "(\\w)";
        }

        TString patternStr = pattern;
        TRegExBase re(patternStr.c_str(), REG_EXTENDED);

        // Allocate small array for only 5 matches (less than 10 groups + 1 full match = 11)
        constexpr int SMALL_SIZE = 5;
        regmatch_t smallArray[SMALL_SIZE] = {};

        // String with 10 characters
        TStringBuilder testString;
        testString.Out.Reserve(10);
        for (int i = 0; i < 10; i++) {
            testString << char('a' + (i % 26));
        }

        TString testStr = testString;

        // Should throw exception because we need 11 matches but array has only 5
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            re.Exec(testStr.c_str(), smallArray, REGEXP_GLOBAL, SMALL_SIZE),
            yexception,
            "Not enough space in pmatch array"
        );
    }

    // Test for 99 capture groups - should succeed
    // With ovecsize=300, pcre_exec can handle up to 100 matches (300/3=100)
    // 99 groups + 1 full match = 100 matches, which fits exactly
    void Test99CaptureGroups() {
        // Create regex with 99 capture groups
        TStringBuilder pattern;
        pattern.Out.Reserve(99 * 4);
        for (int i = 0; i < 99; i++) {
            pattern << "(\\w)";
        }

        TString patternStr = pattern;
        TRegExBase re(patternStr.c_str(), REG_EXTENDED);

        // Allocate array for 100 matches: 1 full match + 99 capture groups
        constexpr int EXPECTED_MATCHES = 100;
        regmatch_t matches[EXPECTED_MATCHES] = {};

        // String with 99 characters
        TStringBuilder testString;
        testString.Out.Reserve(99);
        for (int i = 0; i < 99; i++) {
            testString << char('a' + (i % 26));
        }

        TString testStr = testString;
        Cerr << "Test99CaptureGroups: pattern length = " << patternStr.size() << Endl;

        // Call with REGEXP_GLOBAL to use our code path with bounds checking
        int result = re.Exec(testStr.c_str(), matches, REGEXP_GLOBAL, EXPECTED_MATCHES);

        Cerr << "Test99CaptureGroups result: " << result << Endl;

        // Should succeed with 99 groups
        UNIT_ASSERT_VALUES_EQUAL(result, 0);

        // Count found groups
        int matchCount = 0;
        for (int i = 0; i < EXPECTED_MATCHES; i++) {
            if (matches[i].rm_so == -1) {
                break;
            }
            matchCount++;
        }

        Cerr << "Captured groups: " << matchCount << Endl;

        // Should capture exactly 100 matches (1 full + 99 groups)
        UNIT_ASSERT_VALUES_EQUAL(matchCount, 100);
    }

    // Test for 100 capture groups - should throw exception
    // 100 groups + 1 full match = 101 matches, which exceeds ovecsize/3=100 limit
    // pcre_exec will return rc=0 indicating insufficient space
    void Test100CaptureGroups() {
        // Create regex with 100 capture groups
        TStringBuilder pattern;
        pattern.Out.Reserve(100 * 4);
        for (int i = 0; i < 100; i++) {
            pattern << "(\\w)";
        }

        TString patternStr = pattern;
        TRegExBase re(patternStr.c_str(), REG_EXTENDED);

        // Allocate array for 101 matches: 1 full match + 100 capture groups
        constexpr int EXPECTED_MATCHES = 101;
        regmatch_t matches[EXPECTED_MATCHES] = {};

        // String with 100 characters
        TStringBuilder testString;
        testString.Out.Reserve(100);
        for (int i = 0; i < 100; i++) {
            testString << char('a' + (i % 26));
        }

        TString testStr = testString;
        Cerr << "Test100CaptureGroups: pattern length = " << patternStr.size() << Endl;

        // Should throw exception with message about insufficient space
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            re.Exec(testStr.c_str(), matches, REGEXP_GLOBAL, EXPECTED_MATCHES),
            yexception,
            "Not enough space in internal buffer"
        );
    }

    // Test for 300 capture groups - should throw exception
    // 300 groups + 1 full match = 301 matches, far exceeds ovecsize/3=100 limit
    // pcre_exec will return rc=0 indicating insufficient space
    void Test300CaptureGroups() {
        // Create regex with 300 capture groups
        TStringBuilder pattern;
        pattern.Out.Reserve(300 * 4);
        for (int i = 0; i < 300; i++) {
            pattern << "(\\w)";
        }

        TString patternStr = pattern;
        TRegExBase re(patternStr.c_str(), REG_EXTENDED);

        // Allocate array for 301 matches: 1 full match + 300 capture groups
        constexpr int EXPECTED_MATCHES = 301;
        regmatch_t matches[EXPECTED_MATCHES] = {};

        // String with 300 characters
        TStringBuilder testString;
        testString.Out.Reserve(300);
        for (int i = 0; i < 300; i++) {
            testString << char('a' + (i % 26));
        }

        TString testStr = testString;
        Cerr << "Test300CaptureGroups: pattern length = " << patternStr.size() << Endl;

        // Should throw exception with message about insufficient space
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            re.Exec(testStr.c_str(), matches, REGEXP_GLOBAL, EXPECTED_MATCHES),
            yexception,
            "Not enough space in internal buffer"
        );
    }
};

UNIT_TEST_SUITE_REGISTRATION(TRegexpTest);
