#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using namespace NYql::NJsonPath;

namespace {

using TCallableType = TQueryCollector::ECallableType;

TVector<TString> ParseAndCollect(const TString& jsonPath, TCallableType callableType = TCallableType::JsonExists) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

    TQueryCollector collector(path, callableType);
    auto result = collector.Collect();
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found for path: " + jsonPath + ": " + result.GetError().GetMessage());
    return result.GetTokens();
}

template <bool ParserError = false>
void ValidateError(
    const TString& jsonPath,
    const TString& errorMessage,
    TCallableType callableType = TCallableType::JsonExists)
{
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);

    if constexpr (ParserError) {
        UNIT_ASSERT_STRING_CONTAINS_C(issues.ToOneLineString(), errorMessage, "Expected error message not found: " + errorMessage);
    } else {
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

        TQueryCollector collector(path, callableType);
        auto result = collector.Collect();
        UNIT_ASSERT_C(result.IsError(), "Expected error for path: " + jsonPath + ": " + errorMessage);

        UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "Expected error message not found: " + errorMessage);
    }
}

void ValidateQueries(
    const TString& jsonPath,
    const TVector<TString>& expectedQueries,
    TCallableType callableType = TCallableType::JsonExists)
{
    UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect(jsonPath, callableType), expectedQueries);
}

}  // namespace

using TCallableType = TQueryCollector::ECallableType;

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

    // Binary arithmetic operators extract tokens from both operands and finish
    Y_UNIT_TEST(CollectPath_BinaryArithmetic) {
        // Path on the left, literal on the right - only left token
        ValidateQueries("$.key + 1", {"\3key"});
        ValidateQueries("$.key - 1", {"\3key"});
        ValidateQueries("$.key * 2", {"\3key"});
        ValidateQueries("$.key / 2", {"\3key"});
        ValidateQueries("$.key % 2", {"\3key"});

        // Literal on the left, path on the right - only right token
        ValidateQueries("1 + $.key", {"\3key"});
        ValidateQueries("1 - $.key", {"\3key"});
        ValidateQueries("1 * $.key", {"\3key"});
        ValidateQueries("1 / $.key", {"\3key"});
        ValidateQueries("1 % $.key", {"\3key"});

        // Context object on the left
        ValidateQueries("$ + 1", {""});
        ValidateQueries("$ * 2", {""});

        // Deeper paths as left operand
        ValidateQueries("$.a.b.c + 1", {"\1a\1b\1c"});
        ValidateQueries("$.a.b - 1", {"\1a\1b"});

        // Array access on the left operand
        ValidateQueries("$.key[0] + 1", {"\3key"});
        ValidateQueries("$.arr[last] * 2", {"\3arr"});

        // Wildcard on the left - collection already finished by wildcard
        ValidateQueries("$.* + 1", {""});
        ValidateQueries("$.a.* - 1", {"\1a"});

        // Both operands are paths - tokens from both are collected (AND)
        ValidateQueries("$.a + $.b", {"\1a", "\1b"});
        ValidateQueries("$.a.b - $.c.d", {"\1a\1b", "\1c\1d"});

        // Both operands are literals - no path to collect
        ValidateQueries("1 + 2", {});
        ValidateQueries("1.5 * 2.0", {});
    }

    // Non-trivial combinations of unary and binary arithmetic operators
    Y_UNIT_TEST(CollectPath_ArithmeticCombinations) {
        // Unary applied to binary: tokens from both binary operands, then Finish
        ValidateQueries("-($.a + 1)", {"\1a"});
        ValidateQueries("+($.a - 1)", {"\1a"});
        ValidateQueries("-($.a * $.b)", {"\1a", "\1b"});
        ValidateQueries("-(1 + $.b)", {"\1b"});

        // Binary with unary left operand
        ValidateQueries("-$.a + $.b", {"\1a", "\1b"});
        ValidateQueries("+$.a - $.b", {"\1a", "\1b"});
        ValidateQueries("-$.key + 1", {"\3key"});
        ValidateQueries("+$.key * 2", {"\3key"});

        // Binary with unary right operand - right token still collected
        ValidateQueries("$.a + (-$.b)", {"\1a", "\1b"});
        ValidateQueries("$.a * (+$.b)", {"\1a", "\1b"});
        ValidateQueries("1 + (-$.b)", {"\1b"});

        // Chained binary (left-associative): all three path tokens collected
        ValidateQueries("$.a + $.b + $.c", {"\1a", "\1b", "\1c"});
        ValidateQueries("$.a - $.b - $.c", {"\1a", "\1b", "\1c"});
        ValidateQueries("$.a * $.b * $.c", {"\1a", "\1b", "\1c"});

        // Mixed precedence: * binds tighter than +, but all paths still collected
        ValidateQueries("$.a + $.b * $.c", {"\1a", "\1b", "\1c"});
        ValidateQueries("$.a * $.b + $.c", {"\1a", "\1b", "\1c"});

        // Double unary combined with binary
        ValidateQueries("-(-$.a) + $.b", {"\1a", "\1b"});
        ValidateQueries("-(+$.a) * 2", {"\1a"});

        // Longer paths on both sides
        ValidateQueries("$.a.b.c + $.x.y.z", {"\1a\1b\1c", "\1x\1y\1z"});
        ValidateQueries("$.a.b.c * 3.14", {"\1a\1b\1c"});

        // Method result used as operand of binary - method finishes, but token still collected
        ValidateQueries("$.key.size() + 1", {"\3key"});
        ValidateQueries("$.key.abs() * 2", {"\3key"});
        ValidateQueries("$.a.size() + $.b.floor()", {"\1a", "\1b"});
    }

    Y_UNIT_TEST(CollectPath_EqualityOperator) {
        const TString compError = "Comparison requires exactly one path and one literal operand";
        const TString varError  = "Variables are not supported at the moment";

        auto strSuffix = [](const TStringBuf s) -> TString {
            return TString("\0\3", 2) + s;
        };

        auto numSuffix = [](double v) -> TString {
            TString s;
            s.push_back('\0');
            s.push_back('\4');
            s.append(reinterpret_cast<const char*>(&v), sizeof(double));
            return s;
        };

        const TString boolTrueSuffix  = TString("\0\1", 2);
        const TString boolFalseSuffix = TString("\0\0", 2);
        const TString nullSuffix      = TString("\0\2", 2);

        // Path == literal, all literal types
        ValidateQueries("$.key == \"hello\"", {"\3key" + strSuffix("hello")});
        ValidateQueries("$.key == \"\"", {"\3key" + strSuffix("")});
        ValidateQueries("$.key == 42", {"\3key" + numSuffix(42)});
        ValidateQueries("$.key == 0", {"\3key" + numSuffix(0)});
        ValidateQueries("$.key == 3.14", {"\3key" + numSuffix(3.14)});
        ValidateQueries("$.key == true", {"\3key" + boolTrueSuffix});
        ValidateQueries("$.key == false", {"\3key" + boolFalseSuffix});
        ValidateQueries("$.key == null", {"\3key" + nullSuffix});

        // Reversed order: literal == path (identical result)
        ValidateQueries("\"hello\" == $.key", {"\3key" + strSuffix("hello")});
        ValidateQueries("42 == $.key", {"\3key" + numSuffix(42)});
        ValidateQueries("true == $.key", {"\3key" + boolTrueSuffix});
        ValidateQueries("null == $.key", {"\3key" + nullSuffix});

        // Context object as path (empty prefix)
        ValidateQueries("$ == \"hello\"", {strSuffix("hello")});
        ValidateQueries("$ == 42", {numSuffix(42)});
        ValidateQueries("$ == true", {boolTrueSuffix});
        ValidateQueries("$ == null", {nullSuffix});
        ValidateQueries("\"hello\" == $", {strSuffix("hello")});

        // Deeper paths
        ValidateQueries("$.a.b == \"x\"", {"\1a\1b" + strSuffix("x")});
        ValidateQueries("$.a.b.c == null", {"\1a\1b\1c" + nullSuffix});
        ValidateQueries("\"x\" == $.a.b.c", {"\1a\1b\1c" + strSuffix("x")});
        ValidateQueries("$.aba.\"caba\" == true", {"\3aba\4caba" + boolTrueSuffix});
        ValidateQueries("$.a.b.c.d == 0", {"\1a\1b\1c\1d" + numSuffix(0)});
        ValidateQueries("$.\"\".\"\" == 0", {TString("\0\0", 2) + numSuffix(0)});

        // Array subscript
        ValidateQueries("$.key[0] == \"x\"", {"\3key" + strSuffix("x")});
        ValidateQueries("$.key[last] == true", {"\3key" + boolTrueSuffix});
        ValidateQueries("$.key[1, 2, 3] == null", {"\3key" + nullSuffix});
        ValidateQueries("$.key[0 to last] == 42", {"\3key" + numSuffix(42)});
        ValidateQueries("$.key[0].sub == \"x\"", {"\3key\3sub" + strSuffix("x")});
        ValidateQueries("$.a.b[0].c == \"x\"", {"\1a\1b\1c" + strSuffix("x")});
        ValidateQueries("$.key[*] == \"x\"", {"\3key" + strSuffix("x")});

        // Wildcard member access finishes the path
        ValidateQueries("$.* == \"x\"", {""});
        ValidateQueries("$.a.* == \"x\"", {"\1a"});
        ValidateQueries("$.a.b.* == \"x\"", {"\1a\1b"});
        ValidateQueries("\"x\" == $.*", {""});
        ValidateQueries("\"x\" == $.a.*", {"\1a"});

        // Methods finish the path
        ValidateQueries("$.key.size() == 3", {"\3key"});
        ValidateQueries("$.key.abs() == 1", {"\3key"});
        ValidateQueries("$.key.type() == \"number\"", {"\3key"});
        ValidateQueries("$.a.b.floor() == 0", {"\1a\1b"});
        ValidateQueries("$.key.keyvalue().name == \"x\"", {"\3key"});

        // Unary arithmetic on path finishes the path
        ValidateQueries("-$.key == 1", {"\3key"});
        ValidateQueries("+$.key == 1", {"\3key"});
        ValidateQueries("-$.a.b == null", {"\1a\1b"});

        // Predicates finish the path - the inner path token is returned (except IsUnknownPredicate)
        ValidateQueries("exists($.key) == true", {"\3key"}, TCallableType::JsonValue);
        ValidateQueries("($.key starts with \"a\") == true", {"\3key"}, TCallableType::JsonValue);
        ValidateQueries("($.key like_regex \"a.*\") == true", {"\3key"}, TCallableType::JsonValue);
        ValidateQueries("($.a.b starts with \"x\") == false", {"\1a\1b"}, TCallableType::JsonValue);
        ValidateQueries("($.key == 10) is unknown", {""}, TCallableType::JsonValue);

        ValidateQueries("exists($.key) == true", {""});
        ValidateQueries("($.key starts with \"a\") == true", {""});
        ValidateQueries("($.key like_regex \"a.*\") == true", {""});
        ValidateQueries("($.a.b starts with \"x\") == false", {""});
        ValidateQueries("($.key == 10) is unknown", {""});

        // Both operands are paths
        ValidateError("$.a == $.b", compError);
        ValidateError("$.key == $", compError);
        ValidateError("$ == $", compError);
        ValidateError("$.a.b == $.c.d", compError);

        // Literals only
        ValidateError("\"x\" == \"y\"", compError);
        ValidateError("1 == 2", compError);
        ValidateError("true == false", compError);
        ValidateError("null == null", compError);
        ValidateError("1 == \"x\"", compError);

        // Without context object
        ValidateError("1 == 1", compError);

        // Variables
        ValidateError("$var == \"x\"", varError);
        ValidateError("\"x\" == $var", varError);
        ValidateError("$var == $var", compError);
        ValidateError("$ == $var", compError);

        // Arithmetic produces multiple tokens
        ValidateQueries("($.a + $.b) == \"x\"", {"\1a", "\1b"});
        ValidateQueries("\"x\" == ($.a + $.b)", {"\1a", "\1b"});
        ValidateQueries("$.key + 1 == \"x\"", {"\3key"});
        ValidateQueries("1 + $.key == \"x\"", {"\3key"});

        // Parenthesized path - no effect
        ValidateQueries("($.a.b) == \"x\"", {"\1a\1b" + strSuffix("x")});
        ValidateQueries("\"x\" == ($.a.b)", {"\1a\1b" + strSuffix("x")});
        ValidateQueries("(((((($).a).b))) == (\"x\"))", {"\1a\1b" + strSuffix("x")});
    }

    Y_UNIT_TEST(CollectPath_BinaryArithmeticWildcard) {
        // Wildcard on left, path on right - both collected
        ValidateQueries("$.a.* + $.b", {"\1a", "\1b"});
        ValidateQueries("$.* + $.b", {"", "\1b"});
        ValidateQueries("$.* - $.a.b", {"", "\1a\1b"});

        // Path on left, wildcard on right
        ValidateQueries("$.a + $.*", {"\1a", ""});
        ValidateQueries("$.a.b + $.*", {"\1a\1b", ""});

        // Wildcard on both sides - two wildcard tokens collected
        ValidateQueries("$.* + $.*", {"", ""});
        ValidateQueries("$.a.* + $.*", {"\1a", ""});
        ValidateQueries("$.* + $.a.*", {"", "\1a"});
        ValidateQueries("$.a.b.*.c + $.a.b.*.d", {"\1a\1b", "\1a\1b"});
    }

    Y_UNIT_TEST(CollectPath_BinaryArithmeticErrors) {
        const TString varError = "Variables are not supported at the moment";

        // Error on left - propagated immediately, right not collected
        ValidateError("$var + $.b", varError);
        ValidateError("$var - $.b", varError);
        ValidateError("$var * $.b", varError);
        ValidateError("$var / $.b", varError);
        ValidateError("$var % $.b", varError);

        // Error on right - left tokens lost, error propagated
        ValidateError("$.a + $var", varError);
        ValidateError("$.a - $var", varError);
        ValidateError("$.a * $var", varError);

        // Both sides error
        ValidateError("$var + $var", varError);

        // Error propagates through chained binary: ($.a + $var) + $.c
        ValidateError("$.a + $var + $.c", varError);
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
