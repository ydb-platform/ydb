#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

namespace {

std::optional<std::string> ParseAndCollect(const TString& jsonPath) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found: " + issues.ToOneLineString());

    TQueryCollector collector(path);
    auto result = collector.Collect();
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found: " + result.GetError().GetMessage());
    return result.GetQuery();
}

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {
    Y_UNIT_TEST(CorrectJsonPath) {
        // Context object
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$"), "");

        // Member access
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a"), "\1a");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c"), "\1a\1b\1c");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.aba.\"caba\""), "\3aba\4caba");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.\"\".abc"), std::string("\0\3abc", 5));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*"), "\1a");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*.c"), "\1a");

        // Array access
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0]"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1, 2, 3]"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to 3]"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[last]"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0, 2 to last]"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0 to 1].key"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0]"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key1[last].key2"), "\4key1\4key2");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.arr[2 to last]"), "\3arr");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*[2 to last].key"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0].*"), "\3key");

        // Methods
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.abs()"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*.floor()"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1, 2, 3].ceiling()"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.abs()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.floor()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.ceiling()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.double()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.type()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.size()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue()"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*.keyvalue()"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[1, 2, 3].value.size().floor()"), "\3key\5value");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue().name"), "\3key");

        // Starts with predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$ starts with \"lol\""), std::string("\0\3lol", 5));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to last] starts with \"lol\""), std::string("\0\3lol", 5));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[*] starts with \"lol\""), std::string("\0\3lol", 5));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key starts with \"abc\""), std::string("\3key\0\3abc", 9));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c[1, 2, 3] starts with \"abc\""), std::string("\1a\1b\1c\0\3abc", 11));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.type().name starts with \"abc\""), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.* starts with \"abc\""), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*.c[1, 2, 3] starts with \"abc\""),"\1a");

        // Like regex predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$ like_regex \"abc\""), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to 2] like_regex \"abc\""), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[*] like_regex \"abc\""), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key like_regex \"abc\""), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.* like_regex \"abc\""), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[1, 2, 3] like_regex \"abc\""), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue() like_regex \"abc\""), "\3key");
    }

    // Literals are not supported without a preceding ContextObject
    Y_UNIT_TEST(Literals) {
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("1"), std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("1.2345"), std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("true"), std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("false"), std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("null"), std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("\"string\""), std::nullopt);
    }
}

}  // namespace NKikimr::NJsonIndex
