#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using TQueries = TVector<TString>;

namespace {

TQueries ParseAndCollect(const TString& jsonPath) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found: " + issues.ToOneLineString());

    TQueryCollector collector(path);
    auto result = collector.Collect();
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found: " + result.GetError().GetMessage());
    return result.GetQueries();
}

void ExpectError(const TString& jsonPath, const TString& errorMessage) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found: " + issues.ToOneLineString());

    TQueryCollector collector(path);
    auto result = collector.Collect();
    UNIT_ASSERT_C(result.IsError(), "Expected error: " + errorMessage);

    UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "Expected error message not found: " + errorMessage);
}

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {
    Y_UNIT_TEST(CorrectJsonPath) {
        // Context object
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$"), TQueries{""});

        // Member access
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a"), TQueries{"\1a"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c"), TQueries{"\1a\1b\1c"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.aba.\"caba\""), TQueries{"\3aba\4caba"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.\"\".abc"), TQueries{TString("\0\3abc", 5)});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*"), TQueries{"\1a"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*.c"), TQueries{"\1a"});

        // Array access
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0]"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1, 2, 3]"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to 3]"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[last]"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0, 2 to last]"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0 to 1].key"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0]"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key1[last].key2"), TQueries{"\4key1\4key2"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.arr[2 to last]"), TQueries{"\3arr"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*[2 to last].key"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0].*"), TQueries{"\3key"});

        // Methods
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.abs()"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*.floor()"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1, 2, 3].ceiling()"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.abs()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.floor()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.ceiling()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.double()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.type()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.size()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue()"), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*.keyvalue()"), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[1, 2, 3].value.size().floor()"), TQueries{"\3key\5value"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue().name"), TQueries{"\3key"});

        // Starts with predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$ starts with \"lol\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to last] starts with \"lol\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[*] starts with \"lol\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key starts with \"abc\""), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c[1, 2, 3] starts with \"abc\""), TQueries{"\1a\1b\1c"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.type().name starts with \"abc\""), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.* starts with \"abc\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*.c[1, 2, 3] starts with \"abc\""), TQueries{"\1a"});

        // Like regex predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$ like_regex \"abc\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to 2] like_regex \"abc\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[*] like_regex \"abc\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key like_regex \"abc\""), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.* like_regex \"abc\""), TQueries{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[1, 2, 3] like_regex \"abc\""), TQueries{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue() like_regex \"abc\""), TQueries{"\3key"});
    }

    // Literals are not supported without a preceding ContextObject
    Y_UNIT_TEST(Literals) {
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("1"), TQueries{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("1.2345"), TQueries{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("true"), TQueries{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("false"), TQueries{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("null"), TQueries{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("\"string\""), TQueries{});
    }

    // Variables are not supported now
    Y_UNIT_TEST(Variables) {
        const TString errorMessage = "Variables are not supported at the moment";

        ExpectError("$var", errorMessage);
        ExpectError("$var.key", errorMessage);
        ExpectError("$var[1, 2, 3]", errorMessage);
    }

    Y_UNIT_TEST(TokenizeJson) {
        TString error;

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("\"invalid json", error), TVector<TString>{});
        UNIT_ASSERT(!error.empty());

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("\"literal string\"", error), TVector<TString>{TString("\0\3literal string", 16)});
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString obj = "{\"id\":42042,\"brand\":\"bricks\",\"part_count\":1401,\"price\":null,\"parts\":"
            "[{\"id\":32526,\"count\":7,\"name\":\"3x5\"},{\"id\":32523,\"count\":17,\"name\":\"1x3\"}]}";
        auto tokens = TokenizeJson(obj, error);
        std::sort(tokens.begin(), tokens.end());
        UNIT_ASSERT_VALUES_EQUAL(tokens, (TVector<TString>{
            TString("\2id", 3),
            TString("\2id\0\4\0\0\0\0@\x87\xE4@", 13),
            TString("\5brand", 6),
            TString("\5brand\0\3bricks", 14),
            TString("\5parts", 6),
            TString("\5parts\2id", 9),
            TString("\5parts\2id", 9),
            TString("\5parts\2id\0\4\0\0\0\0\x80\xC3\xDF@", 19),
            TString("\5parts\2id\0\4\0\0\0\0\xC0\xC2\xDF@", 19),
            TString("\5parts\4name", 11),
            TString("\5parts\4name", 11),
            TString("\5parts\4name\0\0031x3", 16),
            TString("\5parts\4name\0\0033x5", 16),
            TString("\5parts\5count", 12),
            TString("\5parts\5count", 12),
            TString("\5parts\5count\0\4\0\0\0\0\0\0\x1C@", 22),
            TString("\5parts\5count\0\4\0\0\0\0\0\0001@", 22),
            TString("\5price", 6),
            TString("\5price\0\2", 8),
            TString("\npart_count", 11),
            TString("\npart_count\0\4\0\0\0\0\0\xE4\x95@", 21)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString emptyKeyObj = "{\"\":{\"a\":\"b\"}}";
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(emptyKeyObj, error), (TVector<TString>{
            TString("\0", 1),
            TString("\0\1a", 3),
            TString("\0\1a\0\3b", 6)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString longKey;
        longKey.resize(1000);
        for (size_t i = 0; i < longKey.size(); i++)
            longKey[i] = 'a';
        TString longKeyObj = "{\"" + longKey + "\":{\"short\":\"b\"}}";
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(longKeyObj, error), (TVector<TString>{
            TString("\xE8\7") + longKey,
            TString("\xE8\7") + longKey + TString("\5short", 6),
            TString("\xE8\7") + longKey + TString("\5short\0\3b", 9)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }
}

}  // namespace NKikimr::NJsonIndex
