#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using namespace NYql::NJsonPath;

namespace {

using ECallableType = TQueryCollector::ECallableType;

TVector<TString> ParseAndCollect(const TString& jsonPath, ECallableType callableType = ECallableType::JsonExists) {
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
    ECallableType callableType = ECallableType::JsonExists)
{
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);

    if constexpr (ParserError) {
        UNIT_ASSERT_STRING_CONTAINS_C(issues.ToOneLineString(), errorMessage, "for path = " << jsonPath);
    } else {
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

        TQueryCollector collector(path, callableType);
        auto result = collector.Collect();
        UNIT_ASSERT_C(result.IsError(), "Expected error for path: " + jsonPath + ": " + errorMessage);

        UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "for path = " << jsonPath);
    }
}

void ValidateQueries(
    const TString& jsonPath,
    const TVector<TString>& expectedQueries,
    ECallableType callableType = ECallableType::JsonExists)
{
    UNIT_ASSERT_VALUES_EQUAL(ParseAndCollect(jsonPath, callableType), expectedQueries);
}

void ValidateJsonExists(const TString& jsonPath, const TVector<TString>& expectedQueries) {
    ValidateQueries(jsonPath, expectedQueries, ECallableType::JsonExists);
}

void ValidateJsonValue(const TString& jsonPath, const TVector<TString>& expectedQueries) {
    ValidateQueries(jsonPath, expectedQueries, ECallableType::JsonValue);
}

// void ValidateJsonQuery(const TString& jsonPath, const TVector<TString>& expectedQueries) {
//     ValidateQueries(jsonPath, expectedQueries, ECallableType::JsonQuery);
// }

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {

    // Every path must have a ContextObject ($)
    Y_UNIT_TEST(CollectPath_EmptyPath) {
        ValidateError<true>("", "Too many errors");
    }

    Y_UNIT_TEST(CollectPath_ContextObject) {
        ValidateJsonExists("$", {""});
    }

    Y_UNIT_TEST(CollectPath_MemberAccess) {
        ValidateJsonExists("$.a", {"\1a"});
        ValidateJsonExists("$.a.b.c", {"\1a\1b\1c"});
        ValidateJsonExists("$.aba.\"caba\"", {"\3aba\4caba"});
        ValidateJsonExists("$.\"\".abc", {TString("\0\3abc", 5)});
        ValidateJsonExists("$.*", {""});
        ValidateJsonExists("$.a.*", {"\1a"});
        ValidateJsonExists("$.a.*.c", {"\1a"});
    }

    Y_UNIT_TEST(CollectPath_ArrayAccess) {
        ValidateJsonExists("$[0]", {""});
        ValidateJsonExists("$[1, 2, 3]", {""});
        ValidateJsonExists("$[1 to 3]", {""});
        ValidateJsonExists("$[last]", {""});
        ValidateJsonExists("$[0, 2 to last]", {""});
        ValidateJsonExists("$[0 to 1].key", {"\3key"});
        ValidateJsonExists("$.key[0]", {"\3key"});
        ValidateJsonExists("$.key1[last].key2", {"\4key1\4key2"});
        ValidateJsonExists("$.arr[2 to last]", {"\3arr"});
        ValidateJsonExists("$.*[2 to last].key", {""});
        ValidateJsonExists("$.key[0].*", {"\3key"});
    }

    // Methods stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_Methods) {
        ValidateJsonExists("$.abs()", {""});
        ValidateJsonExists("$.*.floor()", {""});
        ValidateJsonExists("$[1, 2, 3].ceiling()", {""});
        ValidateJsonExists("$.key.abs()", {"\3key"});
        ValidateJsonExists("$.key.floor()", {"\3key"});
        ValidateJsonExists("$.key.ceiling()", {"\3key"});
        ValidateJsonExists("$.key.double()", {"\3key"});
        ValidateJsonExists("$.key.type()", {"\3key"});
        ValidateJsonExists("$.key.size()", {"\3key"});
        ValidateJsonExists("$.key.keyvalue()", {"\3key"});
        ValidateJsonExists("$.*.keyvalue()", {""});
        ValidateJsonExists("$.key[1, 2, 3].value.size().floor()", {"\3key\5value"});
        ValidateJsonExists("$.key.keyvalue().name", {"\3key"});
    }

    // StartsWith predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_StartsWithPredicate) {
        ValidateJsonValue("$ starts with \"lol\"", {""});
        ValidateJsonValue("$[1 to last] starts with \"lol\"", {""});
        ValidateJsonValue("$[*] starts with \"lol\"", {""});
        ValidateJsonValue("$.key starts with \"abc\"", {"\3key"});
        ValidateJsonValue("$.a.b.c[1, 2, 3] starts with \"abc\"", {"\1a\1b\1c"});
        ValidateJsonValue("$.key.type().name starts with \"abc\"", {"\3key"});
        ValidateJsonValue("$.* starts with \"abc\"", {""});
        ValidateJsonValue("$.a.*.c[1, 2, 3] starts with \"abc\"", {"\1a"});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key starts with \"lol\"", "Predicates are not allowed in this context", ECallableType::JsonExists);
    }

    // LikeRegex predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_LikeRegexPredicate) {
        ValidateJsonValue("$ like_regex \"abc\"", {""});
        ValidateJsonValue("$[1 to 2] like_regex \"abc\"", {""});
        ValidateJsonValue("$[*] like_regex \"abc\"", {""});
        ValidateJsonValue("$.key like_regex \"abc\"", {"\3key"});
        ValidateJsonValue("$.* like_regex \"abc\"", {""});
        ValidateJsonValue("$.key[1, 2, 3] like_regex \"abc\"", {"\3key"});
        ValidateJsonValue("$.key.keyvalue() like_regex \"abc\"", {"\3key"});
        ValidateJsonValue("$.key like_regex \"a.c\"", {"\3key"});
        ValidateJsonValue("$.key like_regex \".*\"", {"\3key"});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key like_regex \"abc\"", "Predicates are not allowed in this context", ECallableType::JsonExists);
    }

    // Exists predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_ExistsPredicate) {
        ValidateJsonValue("exists($)", {""});
        ValidateJsonValue("exists($.key)", {"\3key"});
        ValidateJsonValue("exists($.key[1, 2, 3])", {"\3key"});
        ValidateJsonValue("exists($[*].size())", {""});
        ValidateJsonValue("exists($.key.keyvalue().name)", {"\3key"});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("exists($)", "Predicates are not allowed in this context", ECallableType::JsonExists);
    }

    // IsUnknown predicates return error because their argument must be a predicate (-> nested predicates are not allowed)
    Y_UNIT_TEST(CollectPath_IsUnknownPredicate) {
        ValidateError("($ starts with \"abc\") is unknown", "Predicates are not allowed in this context", ECallableType::JsonValue);
        ValidateError("($ like_regex \"abc\") is unknown", "Predicates are not allowed in this context", ECallableType::JsonValue);
        ValidateError("(exists($.key)) is unknown", "Predicates are not allowed in this context", ECallableType::JsonValue);

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("($ starts with \"abc\") is unknown", "Predicates are not allowed in this context", ECallableType::JsonExists);
    }

    // Unary +/- stop further path extraction (same as methods): operand path only
    Y_UNIT_TEST(CollectPath_UnaryPlusMinus) {
        ValidateJsonExists("-$.key", {"\3key"});
        ValidateJsonExists("+$.key", {"\3key"});

        ValidateJsonExists("-$", {""});
        ValidateJsonExists("+$", {""});

        ValidateJsonExists("-$.a.b.c", {"\1a\1b\1c"});
        ValidateJsonExists("+$.a.b.c", {"\1a\1b\1c"});

        ValidateJsonExists("-$.*", {""});
        ValidateJsonExists("+$.*", {""});
        ValidateJsonExists("-$.a.*", {"\1a"});
        ValidateJsonExists("+$.a.*", {"\1a"});

        ValidateJsonExists("-$.key[0]", {"\3key"});
        ValidateJsonExists("+$.key[last]", {"\3key"});

        ValidateJsonExists("-$.key.abs()", {"\3key"});
        ValidateJsonExists("+$.key.type()", {"\3key"});

        ValidateJsonExists("-(-$.key)", {"\3key"});
        ValidateJsonExists("-(+$.key)", {"\3key"});
        ValidateJsonExists("+(-$.key)", {"\3key"});
        ValidateJsonExists("+(+$.key)", {"\3key"});

        ValidateJsonValue("exists(-$.a.b)", {"\1a\1b"});
        ValidateJsonValue("exists(+$.a.b)", {"\1a\1b"});

        ValidateJsonValue("-($.double())", {""});
        ValidateJsonValue("+($.double())", {""});
    }

    // Literals are not supported without a preceding ContextObject
    Y_UNIT_TEST(CollectPath_Literals) {
        ValidateError("1", "Literal expressions are not allowed in this context");
        ValidateError("1.2345", "Literal expressions are not allowed in this context");
        ValidateError("true", "Literal expressions are not allowed in this context");
        ValidateError("false", "Literal expressions are not allowed in this context");
        ValidateError("null", "Literal expressions are not allowed in this context");
        ValidateError("\"string\"", "Literal expressions are not allowed in this context");
    }

    // Binary arithmetic operators extract tokens from both operands and finish
    Y_UNIT_TEST(CollectPath_BinaryArithmetic) {
        const TString varError = "Variables are not supported at the moment";

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

        // Wildcard on left, path on right - both collected
        ValidateJsonExists("$.a.* + $.b", {"\1a", "\1b"});
        ValidateJsonExists("$.* + $.b", {"", "\1b"});
        ValidateJsonExists("$.* - $.a.b", {"", "\1a\1b"});

        // Path on left, wildcard on right
        ValidateJsonExists("$.a + $.*", {"\1a", ""});
        ValidateJsonExists("$.a.b + $.*", {"\1a\1b", ""});

        // Wildcard on both sides - two wildcard tokens collected
        ValidateJsonExists("$.* + $.*", {"", ""});
        ValidateJsonExists("$.a.* + $.*", {"\1a", ""});
        ValidateJsonExists("$.* + $.a.*", {"", "\1a"});
        ValidateJsonExists("$.a.b.*.c + $.a.b.*.d", {"\1a\1b", "\1a\1b"});

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
        ValidateQueries("-($.a.*.c) + $.x.y.*", {"\1a", "\1x\1y"});
        ValidateQueries("$.a.b.c * 3.14", {"\1a\1b\1c"});

        // Method result used as operand of binary - method finishes, but token still collected
        ValidateQueries("$.key.size() + 1", {"\3key"});
        ValidateQueries("$.key.abs() * 2", {"\3key"});
        ValidateQueries("$.a.size() + $.b.floor()", {"\1a", "\1b"});
    }

    Y_UNIT_TEST(CollectPath_EqualityOperator) {
        const TString compError = "Comparison requires exactly one path and one literal operand";
        const TString predError = "Predicates are not allowed in this context";
        const TString varError = "Variables are not supported at the moment";

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

        const TString boolTrueSuffix = TString("\0\1", 2);
        const TString boolFalseSuffix = TString("\0\0", 2);
        const TString nullSuffix = TString("\0\2", 2);

        // Path == literal, all literal types
        ValidateJsonValue("$.key == \"hello\"", {"\3key" + strSuffix("hello")});
        ValidateJsonValue("$.key == \"\"", {"\3key" + strSuffix("")});
        ValidateJsonValue("$.key == 42", {"\3key" + numSuffix(42)});
        ValidateJsonValue("$.key == 0", {"\3key" + numSuffix(0)});
        ValidateJsonValue("$.key == 3.14", {"\3key" + numSuffix(3.14)});
        ValidateJsonValue("$.key == true", {"\3key" + boolTrueSuffix});
        ValidateJsonValue("$.key == false", {"\3key" + boolFalseSuffix});
        ValidateJsonValue("$.key == null", {"\3key" + nullSuffix});

        // Reversed order: literal == path (identical result)
        ValidateJsonValue("\"hello\" == $.key", {"\3key" + strSuffix("hello")});
        ValidateJsonValue("42 == $.key", {"\3key" + numSuffix(42)});
        ValidateJsonValue("true == $.key", {"\3key" + boolTrueSuffix});
        ValidateJsonValue("null == $.key", {"\3key" + nullSuffix});

        // Context object as path (empty prefix)
        ValidateJsonValue("$ == \"hello\"", {strSuffix("hello")});
        ValidateJsonValue("$ == 42", {numSuffix(42)});
        ValidateJsonValue("$ == true", {boolTrueSuffix});
        ValidateJsonValue("$ == null", {nullSuffix});
        ValidateJsonValue("\"hello\" == $", {strSuffix("hello")});

        // Deeper paths
        ValidateJsonValue("$.a.b == \"x\"", {"\1a\1b" + strSuffix("x")});
        ValidateJsonValue("$.a.b.c == null", {"\1a\1b\1c" + nullSuffix});
        ValidateJsonValue("\"x\" == $.a.b.c", {"\1a\1b\1c" + strSuffix("x")});
        ValidateJsonValue("$.aba.\"caba\" == true", {"\3aba\4caba" + boolTrueSuffix});
        ValidateJsonValue("$.a.b.c.d == 0", {"\1a\1b\1c\1d" + numSuffix(0)});
        ValidateJsonValue("$.\"\".\"\" == 0", {TString("\0\0", 2) + numSuffix(0)});

        // Array subscript
        ValidateJsonValue("$.key[0] == \"x\"", {"\3key" + strSuffix("x")});
        ValidateJsonValue("$.key[last] == true", {"\3key" + boolTrueSuffix});
        ValidateJsonValue("$.key[1, 2, 3] == null", {"\3key" + nullSuffix});
        ValidateJsonValue("$.key[0 to last] == 42", {"\3key" + numSuffix(42)});
        ValidateJsonValue("$.key[0].sub == \"x\"", {"\3key\3sub" + strSuffix("x")});
        ValidateJsonValue("$.a.b[0].c == \"x\"", {"\1a\1b\1c" + strSuffix("x")});
        ValidateJsonValue("$.key[*] == \"x\"", {"\3key" + strSuffix("x")});

        // Wildcard member access finishes the path
        ValidateJsonValue("$.* == \"x\"", {""});
        ValidateJsonValue("$.a.* == \"x\"", {"\1a"});
        ValidateJsonValue("$.a.b.* == \"x\"", {"\1a\1b"});
        ValidateJsonValue("\"x\" == $.*", {""});
        ValidateJsonValue("\"x\" == $.a.*", {"\1a"});

        // Methods finish the path
        ValidateJsonValue("$.key.size() == 3", {"\3key"});
        ValidateJsonValue("$.key.abs() == 1", {"\3key"});
        ValidateJsonValue("$.key.type() == \"number\"", {"\3key"});
        ValidateJsonValue("$.a.b.floor() == 0", {"\1a\1b"});
        ValidateJsonValue("$.key.keyvalue().name == \"x\"", {"\3key"});

        // Unary arithmetic on path finishes the path
        ValidateJsonValue("-$.key == 1", {"\3key"});
        ValidateJsonValue("+$.key == 1", {"\3key"});
        ValidateJsonValue("-$.a.b == null", {"\1a\1b"});

        // Arithmetic produces multiple tokens
        ValidateJsonValue("($.a + $.b) == \"x\"", {"\1a", "\1b"});
        ValidateJsonValue("\"x\" == ($.a + $.b)", {"\1a", "\1b"});
        ValidateJsonValue("$.key + 1 == \"x\"", {"\3key"});
        ValidateJsonValue("1 + $.key == \"x\"", {"\3key"});

        // Parenthesized path - no effect
        ValidateJsonValue("($.a.b) == \"x\"", {"\1a\1b" + strSuffix("x")});
        ValidateJsonValue("\"x\" == ($.a.b)", {"\1a\1b" + strSuffix("x")});
        ValidateJsonValue("(((((($).a).b))) == (\"x\"))", {"\1a\1b" + strSuffix("x")});

        // Predicates with equality operator -> nested predicates are not allowed
        ValidateError("exists($.key) == true", predError, ECallableType::JsonValue);
        ValidateError("($.key starts with \"a\") == true", predError, ECallableType::JsonValue);
        ValidateError("($.key like_regex \"a.*\") == true", predError, ECallableType::JsonValue);
        ValidateError("($.a.b starts with \"x\") == false", predError, ECallableType::JsonValue);
        ValidateError("($.key == 10) is unknown", predError, ECallableType::JsonValue);
        ValidateError("($.key == 10) == false", predError, ECallableType::JsonValue);
        ValidateError("false == ($.key == 10)", predError, ECallableType::JsonValue);

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key == 10", "Predicates are not allowed in this context", ECallableType::JsonExists);
        ValidateError("false == ($.key == 10)", "Predicates are not allowed in this context", ECallableType::JsonExists);

        // Both operands are paths
        ValidateError("$.a == $.b", compError, ECallableType::JsonValue);
        ValidateError("$.key == $", compError, ECallableType::JsonValue);
        ValidateError("$ == $", compError, ECallableType::JsonValue);
        ValidateError("$.a.b == $.c.d", compError, ECallableType::JsonValue);

        // Literals only
        ValidateError("\"x\" == \"y\"", compError, ECallableType::JsonValue);
        ValidateError("1 == 2", compError, ECallableType::JsonValue);
        ValidateError("true == false", compError, ECallableType::JsonValue);
        ValidateError("null == null", compError, ECallableType::JsonValue);
        ValidateError("1 == \"x\"", compError, ECallableType::JsonValue);

        // Without context object
        ValidateError("1 == 1", compError, ECallableType::JsonValue);

        // Variables
        ValidateError("$var == \"x\"", varError, ECallableType::JsonValue);
        ValidateError("\"x\" == $var", varError, ECallableType::JsonValue);
        ValidateError("$var == $var", compError, ECallableType::JsonValue);
        ValidateError("$ == $var", compError, ECallableType::JsonValue);
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
