#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using TQueries = TVector<TString>;
using namespace NYql::NJsonPath;

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

template <bool ParserError = false>
void ValidateError(const TString& jsonPath, const TString& errorMessage) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);

    if constexpr (ParserError) {
        UNIT_ASSERT_STRING_CONTAINS_C(issues.ToOneLineString(), errorMessage, "Expected error message not found: " + errorMessage);
    } else {
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

        TQueryCollector collector(path);
        auto result = collector.Collect();
        UNIT_ASSERT_C(result.IsError(), "Expected error for path: " + jsonPath + ": " + errorMessage);

        UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "Expected error message not found: " + errorMessage);
    }
}

void ValidateQueries(const TString& jsonPath, const TQueries& expectedQueries) {
    UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect(jsonPath), expectedQueries);
}

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {

    // Every path must have a ContextObject ($)
    Y_UNIT_TEST(CollectPath_EmptyPath) {
        ValidateError<true>("", "Too many errors");
    }

    Y_UNIT_TEST(CollectPath_ContextObject) {
        ValidateQueries("$", {""});
    }

    Y_UNIT_TEST(CollectPath_MemberAccess) {
        ValidateQueries("$.a", {"\1a"});
        ValidateQueries("$.a.b.c", {"\1a\1b\1c"});
        ValidateQueries("$.aba.\"caba\"", {"\3aba\4caba"});
        ValidateQueries("$.\"\".abc", {TString("\0\3abc", 5)});
        ValidateQueries("$.*", {""});
        ValidateQueries("$.a.*", {"\1a"});
        ValidateQueries("$.a.*.c", {"\1a"});
    }

    Y_UNIT_TEST(CollectPath_ArrayAccess) {
        ValidateQueries("$[0]", {""});
        ValidateQueries("$[1, 2, 3]", {""});
        ValidateQueries("$[1 to 3]", {""});
        ValidateQueries("$[last]", {""});
        ValidateQueries("$[0, 2 to last]", {""});
        ValidateQueries("$[0 to 1].key", {"\3key"});
        ValidateQueries("$.key[0]", {"\3key"});
        ValidateQueries("$.key1[last].key2", {"\4key1\4key2"});
        ValidateQueries("$.arr[2 to last]", {"\3arr"});
        ValidateQueries("$.*[2 to last].key", {""});
        ValidateQueries("$.key[0].*", {"\3key"});
    }

    // Methods stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_Methods) {
        ValidateQueries("$.abs()", {""});
        ValidateQueries("$.*.floor()", {""});
        ValidateQueries("$[1, 2, 3].ceiling()", {""});
        ValidateQueries("$.key.abs()", {"\3key"});
        ValidateQueries("$.key.floor()", {"\3key"});
        ValidateQueries("$.key.ceiling()", {"\3key"});
        ValidateQueries("$.key.double()", {"\3key"});
        ValidateQueries("$.key.type()", {"\3key"});
        ValidateQueries("$.key.size()", {"\3key"});
        ValidateQueries("$.key.keyvalue()", {"\3key"});
        ValidateQueries("$.*.keyvalue()", {""});
        ValidateQueries("$.key[1, 2, 3].value.size().floor()", {"\3key\5value"});
        ValidateQueries("$.key.keyvalue().name", {"\3key"});
    }

    // Predicates return an empty query because they always return a boolean/null value
    // For JSON_EXISTS, the result is always true even if the path does not exist
    Y_UNIT_TEST(CollectPath_StartsWithPredicate) {
        ValidateQueries("$ starts with \"lol\"", {""});
        ValidateQueries("$[1 to last] starts with \"lol\"", {""});
        ValidateQueries("$[*] starts with \"lol\"", {""});
        ValidateQueries("$.key starts with \"abc\"", {""});
        ValidateQueries("$.a.b.c[1, 2, 3] starts with \"abc\"", {""});
        ValidateQueries("$.key.type().name starts with \"abc\"", {""});
        ValidateQueries("$.* starts with \"abc\"", {""});
        ValidateQueries("$.a.*.c[1, 2, 3] starts with \"abc\"", {""});
    }

    // Predicates return an empty query because they always return a boolean/null value
    // For JSON_EXISTS, the result is always true even if the path does not exist
    Y_UNIT_TEST(CollectPath_LikeRegexPredicate) {
        ValidateQueries("$ like_regex \"abc\"", {""});
        ValidateQueries("$[1 to 2] like_regex \"abc\"", {""});
        ValidateQueries("$[*] like_regex \"abc\"", {""});
        ValidateQueries("$.key like_regex \"abc\"", {""});
        ValidateQueries("$.* like_regex \"abc\"", {""});
        ValidateQueries("$.key[1, 2, 3] like_regex \"abc\"", {""});
        ValidateQueries("$.key.keyvalue() like_regex \"abc\"", {""});
        ValidateQueries("$.key like_regex \"a.c\"", {""});
        ValidateQueries("$.key like_regex \".*\"", {""});
    }

    // Predicates return an empty query because they always return a boolean/null value
    // For JSON_EXISTS, the result is always true even if the path does not exist
    Y_UNIT_TEST(CollectPath_ExistsPredicate) {
        ValidateQueries("exists($)", {""});
        ValidateQueries("exists($.key)", {""});
        ValidateQueries("exists($.key[1, 2, 3])", {""});
        ValidateQueries("exists($[*].size())", {""});
        ValidateQueries("exists($.key.keyvalue().name)", {""});
        ValidateQueries("exists($.key.keyvalue().name starts with \"abc\")", {""});
    }

    // Predicates return an empty query because they always return a boolean/null value
    // For JSON_EXISTS, the result is always true even if the path does not exist
    Y_UNIT_TEST(CollectPath_IsUnknownPredicate) {
        ValidateQueries("($ starts with \"abc\") is unknown", {""});
    }

    // Unary +/- stop further path extraction (same as methods): operand path only
    Y_UNIT_TEST(CollectPath_UnaryPlusMinus) {
        ValidateQueries("-$.key", {"\3key"});
        ValidateQueries("+$.key", {"\3key"});

        ValidateQueries("-$", {""});
        ValidateQueries("+$", {""});

        ValidateQueries("-$.a.b.c", {"\1a\1b\1c"});
        ValidateQueries("+$.a.b.c", {"\1a\1b\1c"});

        ValidateQueries("-$.*", {""});
        ValidateQueries("+$.*", {""});
        ValidateQueries("-$.a.*", {"\1a"});
        ValidateQueries("+$.a.*", {"\1a"});

        ValidateQueries("-$.key[0]", {"\3key"});
        ValidateQueries("+$.key[last]", {"\3key"});

        ValidateQueries("-$.key.abs()", {"\3key"});
        ValidateQueries("+$.key.type()", {"\3key"});

        ValidateQueries("-(-$.key)", {"\3key"});
        ValidateQueries("-(+$.key)", {"\3key"});
        ValidateQueries("+(-$.key)", {"\3key"});
        ValidateQueries("+(+$.key)", {"\3key"});
    }

    // Literals are not supported without a preceding ContextObject
    Y_UNIT_TEST(CollectPath_Literals) {
        ValidateQueries("1", {});
        ValidateQueries("1.2345", {});
        ValidateQueries("true", {});
        ValidateQueries("false", {});
        ValidateQueries("null", {});
        ValidateQueries("\"string\"", {});
    }

    // Variables are not supported now
    Y_UNIT_TEST(CollectPath_Variables) {
        const TString errorMessage = "Variables are not supported at the moment";

        ValidateError("$var", errorMessage);
        ValidateError("$var.key", errorMessage);
        ValidateError("$var[1, 2, 3]", errorMessage);
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
