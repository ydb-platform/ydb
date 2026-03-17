#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

namespace {

std::optional<std::string> ParseAndCollect(const TString& jsonPath) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found");

    TQueryCollector collector(path);
    auto result = collector.Collect();
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found: " + result.GetError().GetMessage());
    return result.GetQuery();
}

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {
    Y_UNIT_TEST(CorrectJsonPath) {
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a"), "\1a");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c"), "\1a\1b\1c");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.aba.\"caba\""), "\3aba\4caba");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.\"\".abc"), std::string("\0\3abc", 5));
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0]"), "");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0 to 1].key"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0]"), "\3key");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.k1[last].k2"), "\2k1\2k2");
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.arr[2 to last]"), "\3arr");
    }
}

}  // namespace NKikimr::NJsonIndex
