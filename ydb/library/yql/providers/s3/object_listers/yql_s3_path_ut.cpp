#include "yql_s3_path.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NS3 {

Y_UNIT_TEST_SUITE(TPathTests) {
    Y_UNIT_TEST(NormalizeEmpty) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(NormalizePath(""), std::exception, "Path should not be empty");
    }

    Y_UNIT_TEST(NormalizeSlashes) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("/"), "/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("//"), "/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("///"), "/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("////////////"), "/");
    }

    Y_UNIT_TEST(NormalizeNoSlashes) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("a"), "a");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("abc"), "abc");
    }

    Y_UNIT_TEST(NormalizeWithSlashes) {
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("a/b/c"), "a/b/c");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("a/b/c/"), "a/b/c/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("/a/b/c"), "a/b/c");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("/a/b/c/"), "a/b/c/");
        UNIT_ASSERT_VALUES_EQUAL(NormalizePath("///a/b/c///"), "a/b/c/");
    }

    void TestRegexFromWildcardsSuccess(const TString& wildcards, const TString& expectedRegex) {
        TString errorString = ValidateWildcards(wildcards);
        UNIT_ASSERT_C(errorString.empty(), errorString);
        UNIT_ASSERT_VALUES_EQUAL(RegexFromWildcards(wildcards), expectedRegex);
    }

    void TestRegexFromWildcardsFail(const TString& wildcards, const TString& expectedException, const TString& expectedError) {
        UNIT_ASSERT_STRING_CONTAINS(ValidateWildcards(wildcards), expectedError);
        UNIT_ASSERT_EXCEPTION_CONTAINS(RegexFromWildcards(wildcards), yexception, expectedException);
    }

    Y_UNIT_TEST(TestRegexFromWildcards) {
        TestRegexFromWildcardsSuccess("first,test\\_{alt1,alt2}_text", "first\\,test\\\\_(?:alt1|alt2)_text");
        TestRegexFromWildcardsSuccess("hello.*world?str", "hello\\..*world.str");
        TestRegexFromWildcardsSuccess("many_{},{alt1,al?t2,al*t3},{alt4}_alts", "many_(?:)\\,(?:alt1|al.t2|al.*t3)\\,(?:alt4)_alts");
        TestRegexFromWildcardsSuccess("hello}{}}world", "hello\\}(?:)\\}world");
        TestRegexFromWildcardsSuccess("hello{{{}world", "hello(?:\\{\\{)world");

        TestRegexFromWildcardsFail("hello{}}{world", "Found unterminated group", "found unterminated group start at position 8");
    }
}

}
