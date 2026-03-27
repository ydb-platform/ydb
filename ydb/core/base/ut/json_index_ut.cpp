#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using TQueries = TVector<TString>;

namespace {

TQueries ParseAndCollect(const TString& jsonPath) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

    TQueryCollector collector(path);
    auto result = collector.Collect();
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found for path: " + jsonPath + ": " + result.GetError().GetMessage());
    return result.GetQueries();
}

void ExpectError(const TString& jsonPath, const TString& errorMessage) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

    TQueryCollector collector(path);
    auto result = collector.Collect();
    UNIT_ASSERT_C(result.IsError(), "Expected error for path: " + jsonPath + ": " + errorMessage);

    UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "Expected error message not found: " + errorMessage);
}

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {
    Y_UNIT_TEST(CorrectJsonPath) {
        using T = TQueries;

        // Context object
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$"), T{""});

        // Member access
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a"), T{"\1a"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c"), T{"\1a\1b\1c"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.aba.\"caba\""), T{"\3aba\4caba"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.\"\".abc"), T{TString("\0\3abc", 5)});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*"), T{"\1a"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*.c"), T{"\1a"});

        // Array access
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0]"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1, 2, 3]"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to 3]"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[last]"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0, 2 to last]"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[0 to 1].key"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0]"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key1[last].key2"), T{"\4key1\4key2"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.arr[2 to last]"), T{"\3arr"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*[2 to last].key"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[0].*"), T{"\3key"});

        // Methods
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.abs()"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*.floor()"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1, 2, 3].ceiling()"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.abs()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.floor()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.ceiling()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.double()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.type()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.size()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue()"), T{"\3key"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.*.keyvalue()"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[1, 2, 3].value.size().floor()"), T{"\3key\5value"});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue().name"), T{"\3key"});

        // Starts with predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$ starts with \"lol\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to last] starts with \"lol\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[*] starts with \"lol\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key starts with \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.b.c[1, 2, 3] starts with \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.type().name starts with \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.* starts with \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.a.*.c[1, 2, 3] starts with \"abc\""), T{""});

        // Like regex predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$ like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[1 to 2] like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$[*] like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.* like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key[1, 2, 3] like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key.keyvalue() like_regex \"abc\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key like_regex \"a.c\""), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("$.key like_regex \".*\""), T{""});

        // Exists predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("exists($)"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("exists($.key)"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("exists($.key[1, 2, 3])"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("exists($[*].size())"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("exists($.key.keyvalue().name)"), T{""});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("exists($.key.keyvalue().name starts with \"abc\")"), T{""});

        // Is unknown predicate
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("($ starts with \"abc\") is unknown"), T{""});
    }

    // Literals are not supported without a preceding ContextObject
    Y_UNIT_TEST(Literals) {
        using T = TQueries;

        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("1"), T{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("1.2345"), T{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("true"), T{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("false"), T{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("null"), T{});
        UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect("\"string\""), T{});
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

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("\"literal string\"", error), (TVector<TString>{TString(), TString("\0\3literal string", 16)}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString obj = "{\"id\":42042,\"brand\":\"bricks\",\"part_count\":1401,\"price\":null,\"parts\":"
            "[{\"id\":32526,\"count\":7,\"name\":\"3x5\"},{\"id\":32523,\"count\":17,\"name\":\"1x3\"}]}";
        auto tokens = TokenizeJson(obj, error);
        std::sort(tokens.begin(), tokens.end());
        UNIT_ASSERT_VALUES_EQUAL(tokens, (TVector<TString>{
            TString(),
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
            TString(),
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
            TString(),
            TString("\xE8\7") + longKey,
            TString("\xE8\7") + longKey + TString("\5short", 6),
            TString("\xE8\7") + longKey + TString("\5short\0\3b", 9)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }
}

}  // namespace NKikimr::NJsonIndex
