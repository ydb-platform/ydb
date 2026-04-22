#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using namespace NYql::NJsonPath;

namespace {

TVector<TString> ParseAndCollect(const TString& jsonPath, ECallableType callableType = ECallableType::JsonExists,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

    auto result = CollectJsonPath(path, callableType);
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found for path: " + jsonPath + ": " + result.GetError().GetMessage());

    if (tokensMode.has_value()) {
        UNIT_ASSERT_C(result.GetTokensMode() == *tokensMode, "for path = " << jsonPath);
    }

    return result.GetTokens();
}

template <bool ParserError = false>
void ValidateError(const TString& jsonPath, const TString& errorMessage, ECallableType callableType = ECallableType::JsonExists) {
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);

    if constexpr (ParserError) {
        UNIT_ASSERT_STRING_CONTAINS_C(issues.ToOneLineString(), errorMessage, "for path = " << jsonPath);
    } else {
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

        auto result = CollectJsonPath(path, callableType);
        UNIT_ASSERT_C(result.IsError(), "Expected error for path: " + jsonPath + ": " + errorMessage);

        UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "for path = " << jsonPath);
    }
}

void ValidateQueries(const TString& jsonPath, const TVector<TString>& expectedQueries, ECallableType callableType = ECallableType::JsonExists,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    UNIT_ASSERT_VALUES_EQUAL_C(ParseAndCollect(jsonPath, callableType, tokensMode), expectedQueries, "for path = " << jsonPath);
}

void ValidateJsonExists(const TString& jsonPath, const TVector<TString>& expectedQueries,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    ValidateQueries(jsonPath, expectedQueries, ECallableType::JsonExists, tokensMode);
}

void ValidateJsonValue(const TString& jsonPath, const TVector<TString>& expectedQueries,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    ValidateQueries(jsonPath, expectedQueries, ECallableType::JsonValue, tokensMode);
}

// void ValidateJsonQuery(const TString& jsonPath, const TVector<TString>& expectedQueries,
// std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
// {
//     ValidateQueries(jsonPath, expectedQueries, ECallableType::JsonQuery, tokensMode);
// }

TString strSuffix(const TStringBuf s) {
    return TString("\0\3", 2) + s;
}

TString numSuffix(double v) {
    TString s;
    s.push_back('\0');
    s.push_back('\4');
    s.append(reinterpret_cast<const char*>(&v), sizeof(double));
    return s;
}

const TString boolTrueSuffix = TString("\0\1", 2);
const TString boolFalseSuffix = TString("\0\0", 2);
const TString nullSuffix = TString("\0\2", 2);

const TString compError = "Comparison is not allowed between literals on both sides";
const TString varError = "Variables are not supported at the moment";
const TString predError = "Predicates are not allowed in this context";
const TString filterError = "'@' is only allowed inside filters";

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
        ValidateError("($ starts with \"abc\") is unknown", predError, ECallableType::JsonValue);
        ValidateError("($ like_regex \"abc\") is unknown", predError, ECallableType::JsonValue);
        ValidateError("(exists($.key)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("($.key == 10) is unknown", predError, ECallableType::JsonValue);
        ValidateError("($.key != 10) is unknown", predError, ECallableType::JsonValue);
        ValidateError("($.key < 10) is unknown", predError, ECallableType::JsonValue);

        // For JSON_EXISTS, predicate mode is denied even earlier (at context level)
        ValidateError("($ starts with \"abc\") is unknown", predError, ECallableType::JsonExists);
        ValidateError("($.key == 10) is unknown", predError, ECallableType::JsonExists);

        // IsUnknown wrapping && - inner AND evaluates its operands (==, starts with, etc.) in EMode::Predicate, blocked
        ValidateError("(($.a == 10) && ($.b == 20)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a starts with \"x\") && ($.b == 1)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(exists($.a) && ($.b like_regex \"y.*\")) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 10) && ($.b == 20) && ($.c == 30)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(exists($.a) && exists($.b)) is unknown", predError, ECallableType::JsonValue);

        // IsUnknown wrapping || - same: inner OR evaluates its predicate operands in EMode::Predicate, blocked
        ValidateError("(($.a == 10) || ($.b == 20)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a starts with \"x\") || ($.b == 1)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(exists($.a) || ($.b like_regex \"y.*\")) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 10) || ($.b == 20) || ($.c == 30)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(exists($.a) || exists($.b)) is unknown", predError, ECallableType::JsonValue);

        // IsUnknown wrapping ! - UnaryNot is in the predicate-type block list, blocked by predicate mode check
        ValidateError("(!($.a == 10)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(!($.a starts with \"x\")) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(!(exists($.a))) is unknown", predError, ECallableType::JsonValue);

        // IsUnknown wrapping && / || that contain !
        ValidateError("(!($.a == 10) && ($.b == 20)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 10) && !($.b == 20)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(!($.a == 10) || ($.b == 20)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 10) || !($.b == 20)) is unknown", predError, ECallableType::JsonValue);
    }

    // Unary NOT always returns predError.
    // For JsonExists: ArePredicatesAllowed(Context) = false, error comes from UnaryNot itself.
    // For JsonValue: inner operand is collected in EMode::Predicate where predicate types are blocked.
    Y_UNIT_TEST(CollectPath_UnaryNot) {
        // Basic cases with JsonExists
        ValidateError("!($.a == 10)", predError, ECallableType::JsonExists);
        ValidateError("!($.key == \"hello\")", predError, ECallableType::JsonExists);
        ValidateError("!($.a == true)", predError, ECallableType::JsonExists);
        ValidateError("!($.a == null)", predError, ECallableType::JsonExists);

        // Basic cases with JsonValue
        ValidateError("!($.a == 10)", predError, ECallableType::JsonValue);
        ValidateError("!($.key == \"hello\")", predError, ECallableType::JsonValue);

        // Deeper paths
        ValidateError("!($.a.b.c == 42)", predError, ECallableType::JsonExists);
        ValidateError("!($.a.b == \"x\")", predError, ECallableType::JsonValue);

        // NOT applied to exists predicate
        ValidateError("!(exists($.key))", predError, ECallableType::JsonValue);

        // NOT applied to starts with predicate
        ValidateError("!($.key starts with \"abc\")", predError, ECallableType::JsonValue);

        // NOT applied to like_regex predicate
        ValidateError("!($.key like_regex \"abc\")", predError, ECallableType::JsonValue);

        // Double NOT
        ValidateError("!(!($.a == 10))", predError, ECallableType::JsonExists);
        ValidateError("!(!($.a == 10))", predError, ECallableType::JsonValue);

        // NOT as left operand of AND - error propagates immediately from left
        ValidateError("!($.a == 10) && ($.b == 20)", predError, ECallableType::JsonValue);
        ValidateError("!($.key starts with \"abc\") && ($.b == 1)", predError, ECallableType::JsonValue);
        ValidateError("!(exists($.key)) && ($.b == 2)", predError, ECallableType::JsonValue);
        ValidateError("!($.a like_regex \".*\") && ($.b == 3)", predError, ECallableType::JsonValue);

        // NOT as right operand of AND - left side succeeds, then error from right
        ValidateError("($.a == 10) && !($.b == 20)", predError, ECallableType::JsonValue);
        ValidateError("($.a starts with \"x\") && !($.b == 1)", predError, ECallableType::JsonValue);
        ValidateError("exists($.a) && !($.b like_regex \"y.*\")", predError, ECallableType::JsonValue);

        // NOT as left operand of OR - error propagates immediately from left
        ValidateError("!($.a == 10) || ($.b == 20)", predError, ECallableType::JsonValue);
        ValidateError("!($.key starts with \"abc\") || ($.b == 1)", predError, ECallableType::JsonValue);
        ValidateError("!(exists($.key)) || ($.b == 2)", predError, ECallableType::JsonValue);

        // NOT as right operand of OR - left side succeeds, then error from right
        ValidateError("($.a == 10) || !($.b == 20)", predError, ECallableType::JsonValue);
        ValidateError("($.a starts with \"x\") || !($.b == 1)", predError, ECallableType::JsonValue);
        ValidateError("exists($.a) || !($.b like_regex \"y.*\")", predError, ECallableType::JsonValue);

        // NOT inside is unknown - is unknown receives error from its argument
        ValidateError("(!($.a == 10)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(!($.key starts with \"abc\")) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(!(exists($.key))) is unknown", predError, ECallableType::JsonValue);

        // NOT in chained AND/OR
        ValidateError("($.a == 1) && !($.b == 2) && ($.c == 3)", predError, ECallableType::JsonValue);
        ValidateError("($.a == 1) || !($.b == 2) || ($.c == 3)", predError, ECallableType::JsonValue);
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
        // Path on the left, literal on the right - only left token
        ValidateQueries("$.key + 1", {"\3key"});
        ValidateQueries("$.key - 1", {"\3key"});
        ValidateQueries("$.key - (-1)", {"\3key"});
        ValidateQueries("$.key * 2", {"\3key"});
        ValidateQueries("$.key / 2", {"\3key"});
        ValidateQueries("$.key % 2", {"\3key"});

        // Literal on the left, path on the right - only right token
        ValidateQueries("1 + $.key", {"\3key"});
        ValidateQueries("-1 - $.key", {"\3key"});
        ValidateQueries("1 * $.key", {"\3key"});
        ValidateQueries("(+(-1)) / $.key", {"\3key"});
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
        ValidateQueries("$.* + (-1)", {""});
        ValidateQueries("$.a.* - 1", {"\1a"});

        // Both operands are paths - tokens from both are collected (AND)
        ValidateQueries("$.a + $.b", {"\1a", "\1b"});
        ValidateQueries("$.a.b - $.c.d", {"\1a\1b", "\1c\1d"});
        ValidateQueries("$.a.b - (-$.c.d)", {"\1a\1b", "\1c\1d"});

        // Both operands are literals - no path to collect
        ValidateQueries("1 + 2", {});
        ValidateQueries("(+(-1.5)) * 2.0", {});

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

    // Arithmetic operators (two-path operands produce And mode) combined with && and ||
    Y_UNIT_TEST(CollectPath_ArithmeticWithBooleanOps) {
        // Two-path arithmetic result (And mode) in AND chain: stays And
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a - $.b == \"x\") && ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a * $.b == \"x\") && ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a / $.b == \"x\") && ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a % $.b == \"x\") && ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.c == 1) && ($.a + $.b == \"x\")", {"\1c" + numSuffix(1), "\1a", "\1b"}, TCollectResult::ETokensMode::And);

        // Two arithmetic results combined via AND: stays And
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c + $.d == \"y\")", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a - $.b == \"x\") && ($.c * $.d == \"y\") && ($.e == 1)", {"\1a", "\1b", "\1c", "\1d", "\1e" + numSuffix(1)},
            TCollectResult::ETokensMode::And);

        // Two-path arithmetic result (And mode) in OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a - $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a * $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a / $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a % $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.c == 1) || ($.a + $.b == \"x\")", {"\1c" + numSuffix(1), "\1a", "\1b"}, TCollectResult::ETokensMode::Or);

        // Two arithmetic results combined via OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c + $.d == \"y\")", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a - $.b == \"x\") || ($.c * $.d == \"y\")", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a / $.b == \"x\") || ($.c % $.d == \"y\")", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);

        // Three-way OR of arithmetic results: all become OR
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c + $.d == \"y\") || ($.e == 1)",
            {"\1a", "\1b", "\1c", "\1d", "\1e" + numSuffix(1)}, TCollectResult::ETokensMode::Or);

        // Arithmetic result with comparison (single-path, NotSet) via AND: compatible, stays And
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c < 5)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.c < 5) && ($.a + $.b == \"x\")", {"\1c", "\1a", "\1b"}, TCollectResult::ETokensMode::And);

        // Arithmetic result with comparison via OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c < 5)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.c < 5) || ($.a + $.b == \"x\")", {"\1c", "\1a", "\1b"}, TCollectResult::ETokensMode::Or);

        // Arithmetic result with starts with / like_regex / exists in AND: compatible
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c starts with \"abc\")", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c like_regex \".*\")", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a + $.b == \"x\") && exists($.c)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);

        // Arithmetic result with starts with / like_regex / exists in OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c starts with \"abc\")", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c like_regex \".*\")", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a + $.b == \"x\") || exists($.c)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);

        // Deeper paths in arithmetic operands
        ValidateJsonValue("($.a.b.c + $.x.y.z == \"val\") && ($.key == 1)", {"\1a\1b\1c", "\1x\1y\1z", "\3key" + numSuffix(1)},
            TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a.b.c + $.x.y.z == \"val\") || ($.key == 1)", {"\1a\1b\1c", "\1x\1y\1z", "\3key" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);

        // Filter: arithmetic with two paths combined via OR with plain path
        ValidateJsonExists("$.key ? (@.a + @.b == 5 || @.c == 1)", {"\3key\1a", "\3key\1b", "\3key\1c" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
        // Filter: two arithmetic results in OR
        ValidateJsonExists("$.key ? (@.a + @.b == 5 || @.c + @.d == 3)", {"\3key\1a", "\3key\1b", "\3key\1c", "\3key\1d"},
            TCollectResult::ETokensMode::Or);
        // Filter: AND chain with OR appended - OR wins
        ValidateJsonExists("$.key ? (@.a + @.b == 5 && @.c == 1 || @.d == 2)",
            {"\3key\1a", "\3key\1b", "\3key\1c" + numSuffix(1), "\3key\1d" + numSuffix(2)}, TCollectResult::ETokensMode::Or);
    }

    Y_UNIT_TEST(CollectPath_EqualityOperator) {
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

        // Literal numeric value folded from unary + / - (same suffix as a plain number literal)
        ValidateJsonValue("$.a == -10", {"\1a" + numSuffix(-10)});
        ValidateJsonValue("$.k == +(-(+(-3)))", {"\1k" + numSuffix(3)});
        ValidateJsonValue("$.key == +42", {"\3key" + numSuffix(42)});
        ValidateJsonValue("$.key == -(-42)", {"\3key" + numSuffix(42)});
        ValidateJsonValue("$.key == +(-15)", {"\3key" + numSuffix(-15)});
        ValidateJsonValue("$.a.b == -(-(-2))", {"\1a\1b" + numSuffix(-2)});
        ValidateJsonValue("$ == +(-(-7))", {numSuffix(7)});
        ValidateJsonValue("-10 == $.a", {"\1a" + numSuffix(-10)});
        ValidateJsonValue("+(-(+(-3))) == $.k", {"\1k" + numSuffix(3)});

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

        // Both operands are paths: merge index tokens with AND (same as comparison ops)
        ValidateJsonValue("$.a == $.b", {"\1a", "\1b"});
        ValidateJsonValue("$.key == $", {"\3key", ""});
        ValidateJsonValue("$ == $", {"", ""});
        ValidateJsonValue("$.a.b == $.c.d", {"\1a\1b", "\1c\1d"});

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
        ValidateError("$var == $var", varError, ECallableType::JsonValue);
        ValidateError("$ == $var", varError, ECallableType::JsonValue);
    }

    // Comparison operators <, <=, >, >=, != collect path tokens from both operands; literals are silently dropped.
    // Mode is set to And only when more than one token is collected (same rule as BinaryArithmeticOp).
    Y_UNIT_TEST(CollectPath_ComparisonOperators) {
        // Literal on the right is dropped, only the path token is returned.
        ValidateJsonValue("$.key < 10", {"\3key"});
        ValidateJsonValue("$.key <= 10", {"\3key"});
        ValidateJsonValue("$.key > 10", {"\3key"});
        ValidateJsonValue("$.key >= 10", {"\3key"});
        ValidateJsonValue("$.key != 10", {"\3key"});
        ValidateJsonValue("$.key != -10", {"\3key"});
        ValidateJsonValue("$.key >= -(+(-10))", {"\3key"});
        ValidateJsonValue("$.key < \"hello\"", {"\3key"});
        ValidateJsonValue("$.key != \"\"", {"\3key"});
        ValidateJsonValue("$.key > 3.14", {"\3key"});
        ValidateJsonValue("$.key >= true", {"\3key"});
        ValidateJsonValue("$.key < null", {"\3key"});

        // Literal on the left, path on the right - literal dropped, path token returned
        ValidateJsonValue("10 < $.key", {"\3key"});
        ValidateJsonValue("10 <= $.key", {"\3key"});
        ValidateJsonValue("10 > $.key", {"\3key"});
        ValidateJsonValue("10 >= $.key", {"\3key"});
        ValidateJsonValue("10 != $.key", {"\3key"});
        ValidateJsonValue("-10 != $.key", {"\3key"});
        ValidateJsonValue("-(+(-10)) != $.key", {"\3key"});

        // Context object as path (empty prefix)
        ValidateJsonValue("$ < 5", {""});
        ValidateJsonValue("$ > \"x\"", {""});
        ValidateJsonValue("$ != null", {""});

        // Deeper member access paths
        ValidateJsonValue("$.a.b.c < 42", {"\1a\1b\1c"});
        ValidateJsonValue("$.a.b > -1", {"\1a\1b"});
        ValidateJsonValue("$.aba.\"caba\" != false", {"\3aba\4caba"});
        ValidateJsonValue("$.a.b.c.d >= 0", {"\1a\1b\1c\1d"});

        // Array access
        ValidateJsonValue("$.key[0] < 5", {"\3key"});
        ValidateJsonValue("$.key[last] > true", {"\3key"});
        ValidateJsonValue("$.key[1, 2, 3] != null", {"\3key"});
        ValidateJsonValue("$.key[0 to last] >= 1", {"\3key"});
        ValidateJsonValue("$.a.b[0].c <= \"x\"", {"\1a\1b\1c"});

        // Wildcard member access finishes the path (literal not appended, but still dropped)
        ValidateJsonValue("$.* < 5", {""});
        ValidateJsonValue("$.a.* > 1", {"\1a"});
        ValidateJsonValue("$.a.b.* != \"x\"", {"\1a\1b"});

        // Wildcard array access
        ValidateJsonValue("$.key[*] < 5", {"\3key"});

        // Methods finish the path
        ValidateJsonValue("$.key.size() < -3", {"\3key"});
        ValidateJsonValue("$.key.abs() >= 1", {"\3key"});
        ValidateJsonValue("$.a.b.floor() != 0", {"\1a\1b"});
        ValidateJsonValue("$.key.keyvalue().name > \"x\"", {"\3key"});

        // Unary arithmetic on path finishes the path
        ValidateJsonValue("-$.key < 1", {"\3key"});
        ValidateJsonValue("+$.key >= 0", {"\3key"});

        // Both sides are paths - tokens from both collected (mode=And)
        ValidateJsonValue("$.a < $.b", {"\1a", "\1b"});
        ValidateJsonValue("$.a.b > $.c.d", {"\1a\1b", "\1c\1d"});
        ValidateJsonValue("$.key != $.other", {"\3key", "\5other"});
        ValidateJsonValue("$ <= $.a", {"", "\1a"});
        ValidateJsonValue("$.a >= $", {"\1a", ""});

        // Both sides are literals - both dropped, empty result
        ValidateJsonValue("1 < 2", {});
        ValidateJsonValue("1.5 >= -2.0", {});
        ValidateJsonValue("true != false", {});

        // Arithmetic expression as operand (same as BinaryArithmeticOp behavior)
        // $.a + $.b produces mode=And, comparison also sets And - compatible
        ValidateJsonValue("$.a + $.b < -5", {"\1a", "\1b"});
        ValidateJsonValue("1 < $.a + $.b", {"\1a", "\1b"});
        ValidateJsonValue("$.key + 1 >= 5", {"\3key"});
        ValidateJsonValue("$.a.size() + $.b.abs() != 0", {"\1a", "\1b"});

        // Comparison predicate nested inside another comparison
        ValidateError("($.a == -10) < -5", predError, ECallableType::JsonValue);
        ValidateError("($.a < 5) > 0", predError, ECallableType::JsonValue);
        ValidateError("($.a <= 5) != 0", predError, ECallableType::JsonValue);
        ValidateError("5 > ($.a == 1)", predError, ECallableType::JsonValue);
        ValidateError("5 != ($.a < 3)", predError, ECallableType::JsonValue);

        // Exists/StartsWith/LikeRegex as operand
        ValidateError("exists($.a) < 5", predError, ECallableType::JsonValue);
        ValidateError("($.a starts with \"x\") != true", predError, ECallableType::JsonValue);
        ValidateError("($.a like_regex \".*\") < 1", predError, ECallableType::JsonValue);

        // AND/OR as operand
        ValidateError("($.a == 1 && $.b == 2) < 5", predError, ECallableType::JsonValue);
        ValidateError("($.a == 1 || $.b == 2) != false", predError, ECallableType::JsonValue);

        // JsonExists: predicate not allowed at top level
        ValidateError("$.key < 10", predError, ECallableType::JsonExists);
        ValidateError("$.key <= -10", predError, ECallableType::JsonExists);
        ValidateError("$.key > 10", predError, ECallableType::JsonExists);
        ValidateError("$.key >= 10", predError, ECallableType::JsonExists);
        ValidateError("$.key != -10", predError, ECallableType::JsonExists);

        // Single-path comparison produces 1 token, NotSet mode, can appear in AND or OR
        ValidateJsonValue("($.a < 5) && ($.b > 1)", {"\1a", "\1b"});
        ValidateJsonValue("($.a <= 5) && ($.b >= 1)", {"\1a", "\1b"});
        ValidateJsonValue("($.a != 5) && ($.b == 1)", {"\1a", "\1b" + numSuffix(1)});
        ValidateJsonValue("($.a < 5) && ($.b > 1) && ($.c != 3)", {"\1a", "\1b", "\1c"});

        ValidateJsonValue("($.a < 5) || ($.b > 1)", {"\1a", "\1b"});
        ValidateJsonValue("($.a >= 5) || ($.b != -1)", {"\1a", "\1b"});
        ValidateJsonValue("($.a != 5) || ($.b == 1)", {"\1a", "\1b" + numSuffix(1)});
        ValidateJsonValue("($.a < -5) || ($.b > 1) || ($.c != 3)", {"\1a", "\1b", "\1c"});

        // Two-path comparison (And mode) mixed with OR: OR wins
        ValidateJsonValue("($.a < $.b) || ($.c > 1)", {"\1a", "\1b", "\1c"});
        ValidateJsonValue("($.a != $.b) || ($.c == -1)", {"\1a", "\1b", "\1c" + numSuffix(-1)});

        // AND chain with OR: OR wins, all tokens become OR
        ValidateJsonValue("($.a < -5) && ($.b == 1) || ($.c > 2)", {"\1a", "\1b" + numSuffix(1), "\1c"});
        ValidateJsonValue("($.a < -5) && ($.b > -1) || ($.c != 3)", {"\1a", "\1b", "\1c"});

        // Variables
        ValidateError("$var < 5", varError, ECallableType::JsonValue);
        ValidateError("5 > $var", varError, ECallableType::JsonValue);
        ValidateError("$var != $var", varError, ECallableType::JsonValue);
    }

    // Comparison operators inside filter predicates (EMode::Filter allows predicates)
    Y_UNIT_TEST(CollectPath_ComparisonOperators_InFilter) {
        // Basic filter with each comparison op
        ValidateJsonExists("$.a ? (@.b < 10)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b <= -10)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b > 10)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b >= +10)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b != 10)", {"\1a\1b"});

        // All literal types as right operand (dropped)
        ValidateJsonExists("$.a ? (@.b < \"hello\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b > -3.14)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b != true)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b >= null)", {"\1a\1b"});

        // Literal on the left, @ path on the right
        ValidateJsonExists("$.a ? (10 < @.b)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (\"x\" != @.b)", {"\1a\1b"});

        // @ itself (filter object) as operand
        ValidateJsonExists("$.a ? (@ < 10)", {"\1a"});
        ValidateJsonExists("$.a ? (@ != \"x\")", {"\1a"});
        ValidateJsonExists("$.a.b ? (@ > 0)", {"\1a\1b"});

        // Deeper filter-object paths
        ValidateJsonExists("$.a ? (@.b.c < -(+(-5)))", {"\1a\1b\1c"});
        ValidateJsonExists("$.a.b ? (@.c.d != null)", {"\1a\1b\1c\1d"});

        // Method on filter-object path (finishes, literal dropped)
        ValidateJsonExists("$.a ? (@.b.size() < -3)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.abs() >= 0)", {"\1a\1b"});

        // Unary on filter-object path (finishes, literal dropped)
        ValidateJsonExists("$.a ? (-@.b < 5)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (+@.b >= 0)", {"\1a\1b"});

        // Both operands are @-paths (both tokens collected)
        ValidateJsonExists("$.key ? (@.a < @.b)", {"\3key\1a", "\3key\1b"});
        ValidateJsonExists("$.key ? (@.x != @.y)", {"\3key\1x", "\3key\1y"});

        // Wildcard on filter-object path
        ValidateJsonExists("$.a ? (@.* < 5)", {"\1a"});
        ValidateJsonExists("$.a ? (@.b.* != 1)", {"\1a\1b"});

        // Comparison in AND inside filter
        ValidateJsonExists("$.a ? (@.b < +10 && @.c == 1)", {"\1a\1b", "\1a\1c" + numSuffix(1)});
        ValidateJsonExists("$.a ? (@.b > 0 && @.b < 100)", {"\1a\1b", "\1a\1b"});
        ValidateJsonExists("$.a ? (@.b != 5 && @.c >= 0 && @.d <= -10)", {"\1a\1b", "\1a\1c", "\1a\1d"});

        // Comparison in OR inside filter
        ValidateJsonExists("$.a ? ((@.b < 5) || (@.c > 10))", {"\1a\1b", "\1a\1c"});
        ValidateJsonExists("$.a ? ((@.b != 1) || (@.c != 2))", {"\1a\1b", "\1a\1c"});

        // Mixing AND and OR inside filter: OR wins
        ValidateJsonExists("$.a ? ((@.b < 5) && ((@.c > 1) || (@.d > 2)))", {"\1a\1b", "\1a\1c", "\1a\1d"});
        ValidateJsonExists("$.a ? (((@.b < 5) || (@.c > 1)) && @.d > 2)", {"\1a\1b", "\1a\1c", "\1a\1d"});

        // Nested predicate in filter operand is blocked (EMode::Predicate on operand)
        ValidateError("$.a ? (($.b == 1) < 5)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (exists(@.b) < 5)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\") != true)", predError, ECallableType::JsonExists);

        // is unknown wrapping comparison inside filter - blocked
        ValidateError("$.a ? ((@.b < 5) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b >= +0) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b != 1) is unknown)", predError, ECallableType::JsonExists);

        // Arithmetic expression as comparison operand in filter
        ValidateJsonExists("$.key ? (@.a + @.b < +5)", {"\3key\1a", "\3key\1b"});
        ValidateJsonExists("$.key ? (@.a * 2 != 0)", {"\3key\1a"});

        // JsonValue also supports comparison in filter
        ValidateJsonValue("$.a ? (@.b < -10)", {"\1a\1b"});
        ValidateJsonValue("$.a ? (@.b != null && @.c >= 1)", {"\1a\1b", "\1a\1c"});
    }

    // Comparison operators (path vs path produce And mode) combined with && and ||
    Y_UNIT_TEST(CollectPath_ComparisonWithBooleanOps) {
        // Two-path comparison (And mode) in AND chain: compatible, stays And
        ValidateJsonValue("($.a < $.b) && ($.c > $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a <= $.b) && ($.c >= $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a != $.b) && ($.c == $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a == $.b) && ($.c < $.d) && ($.e > $.f)", {"\1a", "\1b", "\1c", "\1d", "\1e", "\1f"},
            TCollectResult::ETokensMode::And);

        // Two-path comparison (And mode) in OR: OR wins
        ValidateJsonValue("($.a < $.b) || ($.c > $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a <= $.b) || ($.c >= $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a != $.b) || ($.c == $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a < $.b) || ($.c > $.d) || ($.e != $.f)", {"\1a", "\1b", "\1c", "\1d", "\1e", "\1f"},
            TCollectResult::ETokensMode::Or);

        // Single-path comparison (NotSet mode) in AND chain
        ValidateJsonValue("($.a < 5) && ($.b > 3) && ($.c <= 10)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a > 0) && ($.b >= 1) && ($.c != 0) && ($.d == 2)", {"\1a", "\1b", "\1c", "\1d" + numSuffix(2)},
            TCollectResult::ETokensMode::And);

        // Single-path comparison in OR chain
        ValidateJsonValue("($.a < 5) || ($.b > 3) || ($.c <= 10)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a > 0) || ($.b >= 1) || ($.c != 0) || ($.d == 2)", {"\1a", "\1b", "\1c", "\1d" + numSuffix(2)}, 
            TCollectResult::ETokensMode::Or);

        // Mix of single-path and two-path comparisons in AND: compatible (neither has Or)
        ValidateJsonValue("($.a < 5) && ($.b > $.c)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a < $.b) && ($.c > 5)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a < 5) && ($.b > $.c) && ($.d == 1)", {"\1a", "\1b", "\1c", "\1d" + numSuffix(1)}, 
            TCollectResult::ETokensMode::And);

        // Mix of single-path and two-path comparisons in OR: OR wins
        ValidateJsonValue("($.a < 5) || ($.b > $.c)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a < $.b) || ($.c > 5)", {"\1a", "\1b", "\1c"}, TCollectResult::ETokensMode::Or);

        // AND chain then OR: OR wins
        ValidateJsonValue("($.a < $.b) && ($.c > 1) || ($.d != 0)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a < 5) && ($.b > $.c) || ($.d == 1)", {"\1a", "\1b", "\1c", "\1d" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);

        // Two-path equality combined via AND and OR
        ValidateJsonValue("($.a == $.b) && ($.c == $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a == $.b) || ($.c == $.d)", {"\1a", "\1b", "\1c", "\1d"}, TCollectResult::ETokensMode::Or);

        // Filter: two-path comparison combined with AND/OR
        ValidateJsonExists("$.key ? (@.a < @.b && @.c > 1)", {"\3key\1a", "\3key\1b", "\3key\1c"}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("$.key ? (@.a < @.b || @.c > 1)", {"\3key\1a", "\3key\1b", "\3key\1c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonExists("$.key ? (@.a < @.b && @.c > @.d)", {"\3key\1a", "\3key\1b", "\3key\1c", "\3key\1d"}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("$.key ? (@.a < @.b || @.c > @.d)", {"\3key\1a", "\3key\1b", "\3key\1c", "\3key\1d"}, TCollectResult::ETokensMode::Or);

        // Filter: AND chain with OR - OR wins
        ValidateJsonExists("$.key ? (@.a < @.b && @.c > @.d || @.e == 1)", {"\3key\1a", "\3key\1b", "\3key\1c", "\3key\1d", "\3key\1e" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
    }

    Y_UNIT_TEST(CollectPath_BinaryAnd) {
        // Basic equality on both sides, all literal types
        ValidateJsonValue("($.a == 10) && ($.b == \"hello\")", {"\1a" + numSuffix(10), "\1b" + strSuffix("hello")});
        ValidateJsonValue("(42 == $.key) && ($.val == true)", {"\3key" + numSuffix(42), "\3val" + boolTrueSuffix});
        ValidateJsonValue("($.x == null) && ($.y == false)", {"\1x" + nullSuffix, "\1y" + boolFalseSuffix});
        ValidateJsonValue("($.a == 0) && ($.b == 3.14)", {"\1a" + numSuffix(0), "\1b" + numSuffix(3.14)});
        ValidateJsonValue("(\"hello\" == $.a) && (null == $.b)", {"\1a" + strSuffix("hello"), "\1b" + nullSuffix});

        // Deeper member access paths
        ValidateJsonValue("($.a.b.c == 1) && ($.x.y == \"z\")", {"\1a\1b\1c" + numSuffix(1), "\1x\1y" + strSuffix("z")});
        ValidateJsonValue("($.aba.\"caba\" == true) && ($.d.e.f == 0)", {"\3aba\4caba" + boolTrueSuffix, "\1d\1e\1f" + numSuffix(0)});
        ValidateJsonValue("($.a.b.c.d == 0) && ($.p.q == null)", {"\1a\1b\1c\1d" + numSuffix(0), "\1p\1q" + nullSuffix});

        // Context object as path (empty prefix)
        ValidateJsonValue("($ == \"root\") && ($.b == 2)", {strSuffix("root"), "\1b" + numSuffix(2)});
        ValidateJsonValue("($ == null) && ($ == 42)", {nullSuffix, numSuffix(42)});

        // Array access
        ValidateJsonValue("($.key[0] == 1) && ($.arr[last] == true)", {"\3key" + numSuffix(1), "\3arr" + boolTrueSuffix});
        ValidateJsonValue("($.key[1, 2, 3] == null) && ($.b[0 to last] == \"x\")", {"\3key" + nullSuffix, "\1b" + strSuffix("x")});
        ValidateJsonValue("($.a.b[0].c == \"x\") && ($.d.e == false)", {"\1a\1b\1c" + strSuffix("x"), "\1d\1e" + boolFalseSuffix});
        ValidateJsonValue("($.key[0].sub == \"x\") && ($.v == 1)", {"\3key\3sub" + strSuffix("x"), "\1v" + numSuffix(1)});

        // Wildcard member access (finishes collection, no literal suffix appended)
        ValidateJsonValue("($.a.* == \"x\") && ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.* == \"x\") && ($.b == 2)", {"", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) && ($.b.* == \"z\")", {"\1a" + numSuffix(1), "\1b"});
        ValidateJsonValue("($.a.b.* == true) && ($.c.* == null)", {"\1a\1b", "\1c"});

        // Wildcard array access
        ValidateJsonValue("($.key[*] == \"x\") && ($.b == 2)", {"\3key" + strSuffix("x"), "\1b" + numSuffix(2)});

        // Methods (finish the path, no literal suffix appended)
        ValidateJsonValue("($.key.size() == 3) && ($.val == 1)", {"\3key", "\3val" + numSuffix(1)});
        ValidateJsonValue("($.a == \"x\") && ($.key.abs() == 2)", {"\1a" + strSuffix("x"), "\3key"});
        ValidateJsonValue("($.a.floor() == 0) && ($.b.type() == \"number\")", {"\1a", "\1b"});
        ValidateJsonValue("($.key.keyvalue().name == \"x\") && ($.v == true)", {"\3key", "\1v" + boolTrueSuffix});
        ValidateJsonValue("($.a.size() == 5) && ($.b.ceiling() == 3)", {"\1a", "\1b"});

        // StartsWith predicate on left and right
        ValidateJsonValue("($.a starts with \"x\") && ($.b == 1)", {"\1a", "\1b" + numSuffix(1)});
        ValidateJsonValue("($.a == 1) && ($.b starts with \"y\")", {"\1a" + numSuffix(1), "\1b"});
        ValidateJsonValue("($.a.b.c starts with \"abc\") && ($.d[0] == null)", {"\1a\1b\1c", "\1d" + nullSuffix});
        ValidateJsonValue("($.a starts with \"x\") && ($.b starts with \"y\")", {"\1a", "\1b"});
        ValidateJsonValue("($.a.* starts with \"x\") && ($.b == 1)", {"\1a", "\1b" + numSuffix(1)});

        // LikeRegex predicate on left and right
        ValidateJsonValue("($.a like_regex \".*\") && ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == \"x\") && ($.b like_regex \"[a-z]+\")", {"\1a" + strSuffix("x"), "\1b"});
        ValidateJsonValue("($.a like_regex \"x.*\") && ($.b like_regex \"y.*\")", {"\1a", "\1b"});

        // Exists predicate on left and right
        ValidateJsonValue("exists($.a) && ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) && exists($.b.c)", {"\1a" + numSuffix(1), "\1b\1c"});
        ValidateJsonValue("exists($.a.b[0]) && exists($.c.*)", {"\1a\1b", "\1c"});
        ValidateJsonValue("exists($.a.key.size()) && ($.b == true)", {"\1a\3key", "\1b" + boolTrueSuffix});

        // Unary arithmetic operand (finishes, no literal suffix)
        ValidateJsonValue("(-$.a == 1) && ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) && (+$.b.c == 0)", {"\1a" + numSuffix(1), "\1b\1c"});
        ValidateJsonValue("(-$.a.b.* == 1) && ($.c == 2)", {"\1a\1b", "\1c" + numSuffix(2)});

        // Binary arithmetic with two paths as AND operand (two path tokens, And mode, compatible with AND)
        // $.a + $.b == "x" is parsed as ($.a + $.b) == "x", arithmetic finishes, literal not appended
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
        ValidateJsonValue("($.c == 1) && ($.a + $.b == \"x\")", {"\1c" + numSuffix(1), "\1a", "\1b"});
        ValidateJsonValue("($.a.size() + $.b.abs() == 5) && ($.c == null)", {"\1a", "\1b", "\1c" + nullSuffix});

        // Chained AND (left-associative)
        ValidateJsonValue("($.a == 1) && ($.b == 2) && ($.c == 3)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) && ($.b == 2) && ($.c == 3) && ($.d == 4)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        ValidateJsonValue("($.a starts with \"x\") && ($.b == 1) && exists($.c.d)", {"\1a", "\1b" + numSuffix(1), "\1c\1d"});
        ValidateJsonValue("($.a like_regex \".*\") && ($.b.* == 2) && ($.c.size() == 3) && exists($.d)", {"\1a", "\1b", "\1c", "\1d"});

        // Same path on both sides (two different equality conditions)
        ValidateJsonValue("($.a == 1) && ($.a == 2)", {"\1a" + numSuffix(1), "\1a" + numSuffix(2)});

        // Variables are not supported
        ValidateError("($var == 1) && ($.b == 2)", varError, ECallableType::JsonValue);
        ValidateError("($.a == 1) && ($var == 2)", varError, ECallableType::JsonValue);
        ValidateError("($var == 1) && ($var == 2)", varError, ECallableType::JsonValue);

        // Predicates are not allowed in JsonExists
        ValidateError("($.a == 10) && ($.b == 20)", predError, ECallableType::JsonExists);
        ValidateError("($.a starts with \"x\") && ($.b == 1)", predError, ECallableType::JsonExists);
        ValidateError("exists($.a) && exists($.b)", predError, ECallableType::JsonExists);

        // Mixing AND and OR: OR wins, all tokens become OR
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) || ($.c == 3)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) || (($.b == 2) && ($.c == 3))", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) && (($.b == 2) || ($.c == 3))", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) && ($.c == 3)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});

        // Nested predicates: AND appears in predicate position (inside exists / is unknown / == literal)
        // BinaryAnd inherits EMode::Predicate and its operands (==, starts with, like_regex, exists) are blocked
        ValidateError("exists(($.a == 1) && ($.b == 2))", predError, ECallableType::JsonValue);
        ValidateError("exists(($.a starts with \"x\") && ($.b like_regex \"y\"))", predError, ECallableType::JsonValue);
        ValidateError("exists(exists($.a) && ($.b == 2))", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 1) && ($.b == 2)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 1) && ($.b == 2)) == true", predError, ECallableType::JsonValue);
        ValidateError("false == (($.a == 1) && ($.b == 2))", predError, ECallableType::JsonValue);
    }

    Y_UNIT_TEST(CollectPath_BinaryOr) {
        // Basic equality on both sides, all literal types
        ValidateJsonValue("($.a == 10) || ($.b == \"hello\")", {"\1a" + numSuffix(10), "\1b" + strSuffix("hello")});
        ValidateJsonValue("(42 == $.key) || ($.val == true)", {"\3key" + numSuffix(42), "\3val" + boolTrueSuffix});
        ValidateJsonValue("($.x == null) || ($.y == false)", {"\1x" + nullSuffix, "\1y" + boolFalseSuffix});
        ValidateJsonValue("($.a == 0) || ($.b == 3.14)", {"\1a" + numSuffix(0), "\1b" + numSuffix(3.14)});
        ValidateJsonValue("($ == \"root\") || ($.b == 2)", {strSuffix("root"), "\1b" + numSuffix(2)});

        // Deeper member access paths
        ValidateJsonValue("($.a.b.c == 1) || ($.x.y == \"z\")", {"\1a\1b\1c" + numSuffix(1), "\1x\1y" + strSuffix("z")});
        ValidateJsonValue("($.aba.\"caba\" == true) || ($.d.e.f == 0)", {"\3aba\4caba" + boolTrueSuffix, "\1d\1e\1f" + numSuffix(0)});

        // Array access
        ValidateJsonValue("($.key[0] == 1) || ($.arr[last] == true)", {"\3key" + numSuffix(1), "\3arr" + boolTrueSuffix});
        ValidateJsonValue("($.key[1, 2, 3] == null) || ($.b[0 to last] == \"x\")", {"\3key" + nullSuffix, "\1b" + strSuffix("x")});
        ValidateJsonValue("($.a.b[0].c == \"x\") || ($.d.e == false)", {"\1a\1b\1c" + strSuffix("x"), "\1d\1e" + boolFalseSuffix});

        // Wildcard member and array access (finishes, no literal suffix)
        ValidateJsonValue("($.a.* == \"x\") || ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.* == \"x\") || ($.b == 2)", {"", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) || ($.b.* == \"z\")", {"\1a" + numSuffix(1), "\1b"});
        ValidateJsonValue("($.key[*] == \"x\") || ($.b == 2)", {"\3key" + strSuffix("x"), "\1b" + numSuffix(2)});

        // Methods (finish the path, no literal suffix)
        ValidateJsonValue("($.key.size() == 3) || ($.val == 1)", {"\3key", "\3val" + numSuffix(1)});
        ValidateJsonValue("($.a == \"x\") || ($.key.abs() == 2)", {"\1a" + strSuffix("x"), "\3key"});
        ValidateJsonValue("($.a.floor() == 0) || ($.b.type() == \"number\")", {"\1a", "\1b"});
        ValidateJsonValue("($.key.keyvalue().name == \"x\") || ($.v == true)", {"\3key", "\1v" + boolTrueSuffix});

        // StartsWith predicate
        ValidateJsonValue("($.a starts with \"x\") || ($.b == 1)", {"\1a", "\1b" + numSuffix(1)});
        ValidateJsonValue("($.a == 1) || ($.b starts with \"y\")", {"\1a" + numSuffix(1), "\1b"});
        ValidateJsonValue("($.a starts with \"x\") || ($.b starts with \"y\")", {"\1a", "\1b"});
        ValidateJsonValue("($.a.* starts with \"x\") || ($.b[0] == 1)", {"\1a", "\1b" + numSuffix(1)});

        // LikeRegex predicate
        ValidateJsonValue("($.a like_regex \".*\") || ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == \"x\") || ($.b like_regex \"[a-z]+\")", {"\1a" + strSuffix("x"), "\1b"});
        ValidateJsonValue("($.a like_regex \"x.*\") || ($.b like_regex \"y.*\")", {"\1a", "\1b"});

        // Exists predicate
        ValidateJsonValue("exists($.a) || ($.b == 2)", {"\1a", "\1b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) || exists($.b.c)", {"\1a" + numSuffix(1), "\1b\1c"});
        ValidateJsonValue("exists($.a.b[0]) || exists($.c.*)", {"\1a\1b", "\1c"});

        // Same path on both sides, different values
        ValidateJsonValue("($.a == 1) || ($.a == 2)", {"\1a" + numSuffix(1), "\1a" + numSuffix(2)});
        ValidateJsonValue("($.key == \"a\") || ($.key == \"b\") || ($.key == \"c\")",
            {"\3key" + strSuffix("a"), "\3key" + strSuffix("b"), "\3key" + strSuffix("c")});

        // Chained OR (left-associative)
        ValidateJsonValue("($.a == 1) || ($.b == 2) || ($.c == 3)",{"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) || ($.b == 2) || ($.c == 3) || ($.d == 4)",{"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        ValidateJsonValue("($.a starts with \"x\") || ($.b == 1) || exists($.c.d)",{"\1a", "\1b" + numSuffix(1), "\1c\1d"});

        // Variable on left or right side
        ValidateError("($var == 1) || ($.b == 2)", varError, ECallableType::JsonValue);
        ValidateError("($.a == 1) || ($var == 2)", varError, ECallableType::JsonValue);
        ValidateError("($var == 1) || ($var == 2)", varError, ECallableType::JsonValue);

        // Predicates not allowed in Context for JsonExists
        ValidateError("($.a == 10) || ($.b == 20)", predError, ECallableType::JsonExists);
        ValidateError("($.a starts with \"x\") || ($.b == 1)", predError, ECallableType::JsonExists);
        ValidateError("exists($.a) || exists($.b)", predError, ECallableType::JsonExists);

        // Arithmetic with multiple paths (And mode) mixed with OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
        ValidateJsonValue("($.c == 1) || ($.a + $.b == \"x\")", {"\1c" + numSuffix(1), "\1a", "\1b"});
        ValidateJsonValue("($.a.size() + $.b.abs() == 5) || ($.c == null)", {"\1a", "\1b", "\1c" + nullSuffix});

        // Mixing AND and OR: OR wins
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) || ($.c == 3)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) || (($.b == 2) && ($.c == 3))", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) && (($.b == 2) || ($.c == 3))", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) && ($.c == 3)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3)});

        // Nested predicates: OR appears in predicate position (inside exists / is unknown / == literal)
        ValidateError("exists(($.a == 1) || ($.b == 2))", predError, ECallableType::JsonValue);
        ValidateError("exists(($.a starts with \"x\") || ($.b like_regex \"y\"))", predError, ECallableType::JsonValue);
        ValidateError("exists(exists($.a) || ($.b == 2))", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 1) || ($.b == 2)) is unknown", predError, ECallableType::JsonValue);
        ValidateError("(($.a == 1) || ($.b == 2)) == true", predError, ECallableType::JsonValue);
        ValidateError("false == (($.a == 1) || ($.b == 2))", predError, ECallableType::JsonValue);
    }

    // Verifies that TokensMode (And/Or) propagates correctly through nesting,
    // and that mix errors are detected regardless of nesting depth or structure
    Y_UNIT_TEST(CollectPath_ModePropagation) {
        // ((A && B) && (C && D)) - And+And combined at top level
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) && (($.c == 3) && ($.d == 4))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // ((A || B) || (C || D)) - Or+Or combined at top level
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) || (($.c == 3) || ($.d == 4))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // A && (B && (C && D))
        ValidateJsonValue("($.a == 1) && (($.b == 2) && (($.c == 3) && ($.d == 4)))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // A || (B || (C || D))
        ValidateJsonValue("($.a == 1) || (($.b == 2) || (($.c == 3) || ($.d == 4)))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});

        // Arithmetic with two paths (mode=And) is compatible with AND chains
        // Two arithmetic operands inside AND: ($.a+$.b == "x") && ($.c+$.d == "y")
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c + $.d == \"y\")",
            {"\1a", "\1b", "\1c", "\1d"});
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c + $.d == \"y\") && ($.e == 1)",
            {"\1a", "\1b", "\1c", "\1d", "\1e" + numSuffix(1)});

        // (A && B) || (C && D): OR wins, all tokens become OR
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) || (($.c == 3) && ($.d == 4))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // (A || B) && (C || D): OR wins, all tokens become OR
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) && (($.c == 3) || ($.d == 4))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});

        // A && (B || (C || D)): OR wins
        ValidateJsonValue("($.a == 1) && (($.b == 2) || (($.c == 3) || ($.d == 4)))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // A && (B && (C || D)): OR wins
        ValidateJsonValue("($.a == 1) && (($.b == 2) && (($.c == 3) || ($.d == 4)))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});

        // A || (B && (C && D)): OR wins
        ValidateJsonValue("($.a == 1) || (($.b == 2) && (($.c == 3) && ($.d == 4)))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // A || (B || (C && D)): OR wins
        ValidateJsonValue("($.a == 1) || (($.b == 2) || (($.c == 3) && ($.d == 4)))",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});

        // && binds tighter than ||
        // A && B && C || D  =>  ((A && B) && C) || D: OR wins
        ValidateJsonValue("($.a == 1) && ($.b == 2) && ($.c == 3) || ($.d == 4)",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // A || B || C && D  =>  (A || B) || (C && D): OR wins
        ValidateJsonValue("($.a == 1) || ($.b == 2) || ($.c == 3) && ($.d == 4)",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});
        // A || B && C || D  =>  A || (B && C) || D  =>  (A || (B && C)) || D: OR wins
        ValidateJsonValue("($.a == 1) || ($.b == 2) && ($.c == 3) || ($.d == 4)",
            {"\1a" + numSuffix(1), "\1b" + numSuffix(2), "\1c" + numSuffix(3), "\1d" + numSuffix(4)});

        // ($.a + $.b == "x") has mode=And, nested as part of OR: OR wins
        ValidateJsonValue("(($.a + $.b == \"x\") || ($.c == 1)) && ($.d == 2)",
            {"\1a", "\1b", "\1c" + numSuffix(1), "\1d" + numSuffix(2)});
        ValidateJsonValue("($.d == 2) && (($.a + $.b == \"x\") || ($.c == 1))",
            {"\1d" + numSuffix(2), "\1a", "\1b", "\1c" + numSuffix(1)});

        // ($.a[0].b == 1) && ((-$.c.d == 2) && ($.e.* starts with "x"))
        // -$.c.d == 2: unary makes path Finished, no literal appended, token "\1c\1d"
        // $.e.* starts with "x": wildcard makes path Finished, token "\1e"
        ValidateJsonValue("($.a[0].b == 1) && ((-$.c.d == 2) && ($.e.* starts with \"x\"))",
            {"\1a\1b" + numSuffix(1), "\1c\1d", "\1e"});

        // ($.a.b.c starts with "x") && ($.d.size() == 3) && ($.e[0] + $.f.abs() == 5)
        // $.e[0] + $.f.abs(): both sub-paths collected, mode=And, arithmetic Finished, no literal appended
        ValidateJsonValue("($.a.b.c starts with \"x\") && ($.d.size() == 3) && ($.e[0] + $.f.abs() == 5)",
            {"\1a\1b\1c", "\1d", "\1e", "\1f"});

        // (exists($.a.b[0]) && ($.c.d == "x")) && (($.e like_regex ".*") && (+$.f.g.h == 0))
        // +$.f.g.h == 0: unary makes path Finished, no literal appended, token "\1f\1g\1h"
        ValidateJsonValue("(exists($.a.b[0]) && ($.c.d == \"x\")) && (($.e like_regex \".*\") && (+$.f.g.h == 0))",
            {"\1a\1b", "\1c\1d" + strSuffix("x"), "\1e", "\1f\1g\1h"});

        // All five binary arithmetic ops as AND operands - each produces mode=And, all compatible
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c - $.d == \"y\") && ($.e * $.f == \"z\")",
            {"\1a", "\1b", "\1c", "\1d", "\1e", "\1f"});
        ValidateJsonValue("($.a / $.b == \"x\") && ($.c % $.d == \"y\") && ($.e == 1)",
            {"\1a", "\1b", "\1c", "\1d", "\1e" + numSuffix(1)});

        // Unary on both sides of AND - each makes path Finished, literal not appended
        ValidateJsonValue("(-$.a.b == 1) && (+$.c.d.e == 2) && (-$.f[0] == 3)",
            {"\1a\1b", "\1c\1d\1e", "\1f"});

        // ($.a.size() == 3) || (($.b[0] == "x") || (-$.c.d.e == 1))
        // Right inner OR: two NotSet operands, Or, outer OR: left=NotSet, right=Or, Or
        ValidateJsonValue("($.a.size() == 3) || (($.b[0] == \"x\") || (-$.c.d.e == 1))",
            {"\1a", "\1b" + strSuffix("x"), "\1c\1d\1e"});

        // ($.a starts with "x") || ($.b.* == "y") || exists($.c.d[0])
        // $.b.* == "y": wildcard Finished, no literal, token "\1b"
        ValidateJsonValue("($.a starts with \"x\") || ($.b.* == \"y\") || exists($.c.d[0])",
            {"\1a", "\1b", "\1c\1d"});

        // ($.a[1 to 3].b like_regex ".*") || ((-$.c == 0) || ($.d.e.keyvalue() == "f"))
        // $.d.e.keyvalue() == "f": method Finished, no literal, token "\1d\1e"
        ValidateJsonValue("($.a[1 to 3].b like_regex \".*\") || ((-$.c == 0) || ($.d.e.keyvalue() == \"f\"))",
            {"\1a\1b", "\1c", "\1d\1e"});

        // Unary on both sides of OR
        ValidateJsonValue("(-$.a.b == 1) || (+$.c.d == 2) || (-$.e.f.g == 3)",
            {"\1a\1b", "\1c\1d", "\1e\1f\1g"});

        // ((-$.a.b == 1) && ($.c.size() == 2)) || (exists($.d) && ($.e starts with "x")): OR wins
        ValidateJsonValue("((-$.a.b == 1) && ($.c.size() == 2)) || (exists($.d) && ($.e starts with \"x\"))",
            {"\1a\1b", "\1c", "\1d", "\1e"});

        // ($.a like_regex "x.*") || (($.b.abs() == 1) && ($.c[0] starts with "y")): OR wins
        ValidateJsonValue("($.a like_regex \"x.*\") || ($.b.abs() == 1) && ($.c[0] starts with \"y\")",
            {"\1a", "\1b", "\1c"});

        // (($.a + $.b == "x") && (-$.c.d == 1)) || ($.e.f == 2): OR wins
        ValidateJsonValue("($.a + $.b == \"x\") && (-$.c.d == 1) || ($.e.f == 2)",
            {"\1a", "\1b", "\1c\1d", "\1e\1f" + numSuffix(2)});

        // ($.a[0] starts with "x") || (($.b.c + $.d.e == 3) && exists($.f.g.*)): OR wins
        ValidateJsonValue("($.a[0] starts with \"x\") || ($.b.c + $.d.e == 3) && exists($.f.g.*)",
            {"\1a", "\1b\1c", "\1d\1e", "\1f\1g"});

        // (($.a.b[0].c == 1) && ($.d.* starts with "x")) || (-$.e.f.g == 2): OR wins
        ValidateJsonValue("($.a.b[0].c == 1) && ($.d.* starts with \"x\") || (-$.e.f.g == 2)",
            {"\1a\1b\1c" + numSuffix(1), "\1d", "\1e\1f\1g"});

        // (($.a like_regex ".*") && ($.b.size() == 0) && ($.c[last] == true)) || ($.d.floor() == 0): OR wins
        ValidateJsonValue("($.a like_regex \".*\") && ($.b.size() == 0) && ($.c[last] == true) || ($.d.floor() == 0)",
            {"\1a", "\1b", "\1c" + boolTrueSuffix, "\1d"});

        // ($.a == 1) || ((-$.b.c.d == 2) && ($.e.size() == 3)): OR wins
        ValidateJsonValue("($.a == 1) || ((-$.b.c.d == 2) && ($.e.size() == 3))",
            {"\1a" + numSuffix(1), "\1b\1c\1d", "\1e"});

        // All five arithmetic ops with two paths inside OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
        ValidateJsonValue("($.a - $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
        ValidateJsonValue("($.a * $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
        ValidateJsonValue("($.a / $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
        ValidateJsonValue("($.a % $.b == \"x\") || ($.c == 1)", {"\1a", "\1b", "\1c" + numSuffix(1)});
    }

    // Filter predicates allow the collector to use predicate constraints for path narrowing
    // $.a ? (@.b == 10)  =>  ["\1a\1b" + numSuffix(10)]
    Y_UNIT_TEST(CollectPath_FilterPredicate) {
        // Basic: simple path before ?, simple equality predicate
        ValidateJsonExists("$.a ? (@.b == 10)", {"\1a\1b" + numSuffix(10)});
        ValidateJsonExists("$.a ? (@.b == -(+10))", {"\1a\1b" + numSuffix(-10)});
        ValidateJsonExists("$.a ? (@.b == \"hello\")", {"\1a\1b" + strSuffix("hello")});
        ValidateJsonExists("$.a ? (@.b == true)", {"\1a\1b" + boolTrueSuffix});
        ValidateJsonExists("$.a ? (@.b == false)", {"\1a\1b" + boolFalseSuffix});
        ValidateJsonExists("$.a ? (@.b == null)", {"\1a\1b" + nullSuffix});
        ValidateJsonExists("$.a ? (-10 == @.b)", {"\1a\1b" + numSuffix(-10)});
        ValidateJsonExists("$.a ? (\"hello\" == @.b)", {"\1a\1b" + strSuffix("hello")});
        ValidateJsonExists("$ ? (@.a == 1)", {"\1a" + numSuffix(1)});
        ValidateJsonExists("$ ? (@.key == \"x\")", {"\3key" + strSuffix("x")});

        // @ == literal: equality on the filter object itself, prefix becomes the full token
        ValidateJsonExists("$.a ? (@ == \"hello\")", {"\1a" + strSuffix("hello")});
        ValidateJsonExists("$.a ? (@ == -42)", {"\1a" + numSuffix(-42)});
        ValidateJsonExists("$.a ? (@ == true)", {"\1a" + boolTrueSuffix});
        ValidateJsonExists("$.a ? (@ == null)", {"\1a" + nullSuffix});
        ValidateJsonExists("$.a.b ? (@ == \"x\")", {"\1a\1b" + strSuffix("x")});
        ValidateJsonExists("$ ? (@ == 0)", {numSuffix(0)});

        // @ starts with / like_regex: predicate on the filter object itself
        ValidateJsonExists("$.a ? (@ starts with \"x\")", {"\1a"});
        ValidateJsonExists("$.a.b ? (@ starts with \"hello\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@ like_regex \"[a-z]+\")", {"\1a"});
        ValidateJsonExists("$.a.b.c ? (@ like_regex \".*\")", {"\1a\1b\1c"});

        // exists(@): filter predicate is exists on the filter object
        ValidateJsonExists("$.a ? (exists(@))", {"\1a"});
        ValidateJsonExists("$.a.b ? (exists(@))", {"\1a\1b"});

        // @[0].b: array subscript on filter object, then member access
        ValidateJsonExists("$.a ? (@[0].b == 1)", {"\1a\1b" + numSuffix(1)});
        ValidateJsonExists("$.a ? (@[last].b == \"x\")", {"\1a\1b" + strSuffix("x")});
        ValidateJsonExists("$.a ? (@[*].b == true)", {"\1a\1b" + boolTrueSuffix});

        // FilterObject via wildcard member access, finishes the path, so literal is not appended
        ValidateJsonExists("$.a ? (@.* == \"x\")", {"\1a"});
        ValidateJsonExists("$.a ? (@.b.* starts with \"x\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.* == 1)", {"\1a\1b"});

        // Methods finish the path, so literal is not appended
        ValidateJsonExists("$.a ? (@.b.size() == 3)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.abs() == 1)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.floor() == 0)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.ceiling() == 5)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.type() == \"number\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.double() == 1)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.keyvalue() == \"x\")", {"\1a\1b"});
        // method result checked in AND: both paths collected
        ValidateJsonExists("$.a ? (@.b.size() == +3 && @.c == -1)", {"\1a\1b", "\1a\1c" + numSuffix(-1)});

        // Unary finishes the path, so literal is not appended
        ValidateJsonExists("$.a ? (-@.b == 5)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (+@.b == 5)", {"\1a\1b"});
        ValidateJsonExists("$.a ? (-@.b.c.d == 0)", {"\1a\1b\1c\1d"});
        ValidateJsonExists("$.a ? (-@.b == 5 && @.c == 1)", {"\1a\1b", "\1a\1c" + numSuffix(1)});
        ValidateJsonExists("$.a ? (-@.b == 5 || +@.c == 2)", {"\1a\1b", "\1a\1c"});

        // All five arithmetic operators: path + path, both tokens, no literal suffix
        ValidateJsonExists("$.key ? (@.a + @.b == +5)", {"\3key\1a", "\3key\1b"});
        ValidateJsonExists("$.key ? (@.a - @.b == 0)", {"\3key\1a", "\3key\1b"});
        ValidateJsonExists("$.key ? (@.a * @.b == 10)", {"\3key\1a", "\3key\1b"});
        ValidateJsonExists("$.key ? (@.a / @.b == 2)", {"\3key\1a", "\3key\1b"});
        ValidateJsonExists("$.key ? (@.a % @.b == 1)", {"\3key\1a", "\3key\1b"});
        // path + literal: literal side dropped by CollectArithmeticOperand, only path token
        ValidateJsonExists("$.key ? (@.a + 1 == 5)", {"\3key\1a"});
        ValidateJsonExists("$.key ? (1 - @.a == 5)", {"\3key\1a"});
        ValidateJsonExists("$.key ? (@.a * (-2) == -10)", {"\3key\1a"});
        // three paths via chained arithmetic
        ValidateJsonExists("$.key ? (@.a + @.b + @.c == 0)", {"\3key\1a", "\3key\1b", "\3key\1c"});
        // arithmetic with deeper filter object paths
        ValidateJsonExists("$.x ? (@.a.b + @.c.d == 0)", {"\1x\1a\1b", "\1x\1c\1d"});
        // arithmetic with two paths produces mode=And, compatible with AND
        ValidateJsonExists("$.key ? (@.a + @.b == 5 && @.c == 1)", {"\3key\1a", "\3key\1b", "\3key\1c" + numSuffix(1)});

        // StartsWith finishes the path
        ValidateJsonExists("$.a ? (@.b starts with \"x\")", {"\1a\1b"});
        ValidateJsonExists("$.a.b.c ? (@.d starts with \"abc\")", {"\1a\1b\1c\1d"});
        ValidateJsonValue("$.a ? (@.b starts with \"x\")", {"\1a\1b"});

        // LikeRegex finishes the path
        ValidateJsonExists("$.a ? (@.b like_regex \".*\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b.c like_regex \"[0-9]+\")", {"\1a\1b\1c"});
        ValidateJsonValue("$.a ? (@.b like_regex \"[a-z]+\")", {"\1a\1b"});

        // Exists finishes the path
        ValidateJsonExists("$.a ? (exists(@.b))", {"\1a\1b"});
        ValidateJsonExists("$.a ? (exists(@.b.c.d))", {"\1a\1b\1c\1d"});
        ValidateJsonExists("$.a ? (exists(@.b[0]))", {"\1a\1b"});
        ValidateJsonValue("$.a ? (exists(@.b))", {"\1a\1b"});

        // Deeper paths before and inside filter
        ValidateJsonExists("$.a.b ? (@.c == \"x\")", {"\1a\1b\1c" + strSuffix("x")});
        ValidateJsonExists("$.a ? (@.b.c == true)", {"\1a\1b\1c" + boolTrueSuffix});
        ValidateJsonExists("$.a.b.c ? (@.d.e == null)", {"\1a\1b\1c\1d\1e" + nullSuffix});
        ValidateJsonExists("$.a ? (@.b.c.d == 3.14)", {"\1a\1b\1c\1d" + numSuffix(3.14)});

        // Array access in the input path
        ValidateJsonExists("$.a[0] ? (@.b == 1)", {"\1a\1b" + numSuffix(1)});
        ValidateJsonExists("$.key[1, 2, 3] ? (@.sub == \"x\")", {"\3key\3sub" + strSuffix("x")});
        ValidateJsonExists("$.a[0 to last] ? (@.b == true)", {"\1a\1b" + boolTrueSuffix});


        // AND: Two equality conditions
        ValidateJsonExists("$.a ? (@.b == +10 && @.c == +13)", {"\1a\1b" + numSuffix(10), "\1a\1c" + numSuffix(13)});
        ValidateJsonExists("$.a.b ? (@.c == \"x\" && @.d == 1)", {"\1a\1b\1c" + strSuffix("x"), "\1a\1b\1d" + numSuffix(1)});
        ValidateJsonExists("$ ? (@.x == null && @.y == true)", {"\1x" + nullSuffix, "\1y" + boolTrueSuffix});

        // AND: Three conditions chained with AND
        ValidateJsonExists("$.key ? (@.a == 1 && @.b == -2 && @.c == 3)", {"\3key\1a" + numSuffix(1), "\3key\1b" + numSuffix(-2), "\3key\1c" + numSuffix(3)});

        // AND: Four conditions chained with AND (two pairs)
        ValidateJsonExists("$.a ? ((@.b == 1 && @.c == 2) && (@.d == 3 && @.e == 4))",
            {"\1a\1b" + numSuffix(1), "\1a\1c" + numSuffix(2),
             "\1a\1d" + numSuffix(3), "\1a\1e" + numSuffix(4)});

        // AND: mixing predicate types
        ValidateJsonExists("$.a ? ((@.b == 1) && (@.c starts with \"x\") && (@.d like_regex \"y.*\") && exists(@.e))",
            {"\1a\1b" + numSuffix(1), "\1a\1c", "\1a\1d", "\1a\1e"});

        // AND: mixing methods, unary, wildcard, equality
        ValidateJsonExists("$.key ? ((@.a == 1) && (@.b.size() == 3) && (-@.c == 0) && (@.d.* starts with \"x\"))",
            {"\3key\1a" + numSuffix(1), "\3key\1b", "\3key\1c", "\3key\1d"});

        // OR: Two equality conditions
        ValidateJsonExists("$.a ? ((@.b == 10) || (@.c == 13))", {"\1a\1b" + numSuffix(10), "\1a\1c" + numSuffix(13)});
        ValidateJsonExists("$.key ? ((@.x == \"a\") || (@.y == \"b\"))", {"\3key\1x" + strSuffix("a"), "\3key\1y" + strSuffix("b")});

        // OR: Three conditions chained with OR
        ValidateJsonExists("$.a ? ((@.b == 1) || (@.c == 2) || (@.d == 3))", {"\1a\1b" + numSuffix(1), "\1a\1c" + numSuffix(2), "\1a\1d" + numSuffix(3)});

        // OR: Four conditions chained with OR (two pairs)
        ValidateJsonExists("$.a ? ((@.b == 1 || @.c == 2) || (@.d == 3 || @.e == 4))",
            {"\1a\1b" + numSuffix(1), "\1a\1c" + numSuffix(2),
             "\1a\1d" + numSuffix(3), "\1a\1e" + numSuffix(4)});

        // OR: mixing predicate types
        ValidateJsonExists("$.a ? ((@.b == 1) || (@.c starts with \"x\") || (@.d like_regex \"y.*\") || exists(@.e))",
            {"\1a\1b" + numSuffix(1), "\1a\1c", "\1a\1d", "\1a\1e"});

        // AND on left of OR: OR wins
        ValidateJsonExists("$.a ? ((@.b == 1 && @.c == 2) || @.d == 3)", {"\1a\1b" + numSuffix(1), "\1a\1c" + numSuffix(2), "\1a\1d" + numSuffix(3)});
        // OR on right of AND: OR wins
        ValidateJsonExists("$.a ? (@.b == 1 && ((@.c == 2) || (@.d == 3)))", {"\1a\1b" + numSuffix(1), "\1a\1c" + numSuffix(2), "\1a\1d" + numSuffix(3)});
        // Arithmetic with two paths (mode=And) on left of OR: OR wins
        ValidateJsonExists("$.a ? ((@.b + @.c == 5) || @.d == 3)", {"\1a\1b", "\1a\1c", "\1a\1d" + numSuffix(3)});
        // Arithmetic with two paths (mode=And) on right of OR: OR wins
        ValidateJsonExists("$.a ? (@.b == 1 || (@.c + @.d == 5))", {"\1a\1b" + numSuffix(1), "\1a\1c", "\1a\1d"});
        // (A || B) inside AND chain: OR wins
        ValidateJsonExists("$.a ? (((@.b == 1) || (@.c == 2)) && @.d == 3)", {"\1a\1b" + numSuffix(1), "\1a\1c" + numSuffix(2), "\1a\1d" + numSuffix(3)});

        // Finished input path (wildcard/method) - filter predicate can't narrow
        ValidateJsonExists("$.* ? (@.b == 1)", {""});
        ValidateJsonExists("$.a.* ? (@.b == 1)", {"\1a"});
        ValidateJsonExists("$.a.b.* ? (@.c == \"x\")", {"\1a\1b"});

        // Filter is Finished - further member access is dropped
        ValidateJsonExists("$.a ? (@.b == 10) .c", {"\1a\1b" + numSuffix(10)});

        // JsonExists explicitly: filter allows all predicate types even though
        ValidateJsonExists("$.a ? (@.b == 10)", {"\1a\1b" + numSuffix(10)});
        ValidateJsonExists("$.a ? (@.b starts with \"x\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b like_regex \".*\")", {"\1a\1b"});
        ValidateJsonExists("$.a ? (exists(@.b))", {"\1a\1b"});
        ValidateJsonExists("$.a ? (@.b == 10 && @.c starts with \"x\" && exists(@.d))", {"\1a\1b" + numSuffix(10), "\1a\1c", "\1a\1d"});
        ValidateJsonExists("$.a ? ((@.b == 10) || (@.c starts with \"x\") || exists(@.d))", {"\1a\1b" + numSuffix(10), "\1a\1c", "\1a\1d"});
        ValidateJsonExists("$.a ? (@.b + @.c == 5)", {"\1a\1b", "\1a\1c"});

        // JsonValue also works (filter allowed predicates in both callable types)
        ValidateJsonValue("$.a ? (@.b == 10)", {"\1a\1b" + numSuffix(10)});
        ValidateJsonValue("$.a ? (@.b == 10 && @.c == 13)", {"\1a\1b" + numSuffix(10), "\1a\1c" + numSuffix(13)});
        ValidateJsonValue("$.a ? ((@.b == 10) || (@.c == 13))", {"\1a\1b" + numSuffix(10), "\1a\1c" + numSuffix(13)});
        ValidateJsonValue("$.a ? (@.b + @.c == 5)", {"\1a\1b", "\1a\1c"});
        ValidateJsonValue("$.a ? (@.b == @.c)", {"\1a\1b", "\1a\1c"});
        ValidateJsonValue("$.a ? (@.b == $.c)", {"\1a\1b", "\1c"});
        ValidateJsonValue("$.a ? (@ == @.b)", {"\1a", "\1a\1b"});

        // Nested filter: exists(@.b ? (@.c == 1)) inside an outer filter
        ValidateJsonExists("$.a ? (exists(@.b ? (@.c == 1)))", {"\1a\1b\1c" + numSuffix(1)});
        ValidateJsonExists("$.key ? (exists(@.sub ? (@.val == \"x\")))", {"\3key\3sub\3val" + strSuffix("x")});

        // @ outside filter context is an error
        ValidateError("@", filterError);
        ValidateError("@.a", filterError);
        ValidateError("@.a == 1", filterError, ECallableType::JsonValue);
        ValidateError("exists(@.a)", filterError, ECallableType::JsonValue);
        ValidateError("@ starts with \"x\"", filterError, ECallableType::JsonValue);

        // Both sides of == are paths: AND-merge of filter-relative paths
        ValidateJsonExists("$.a ? (@.b == @.c)", {"\1a\1b", "\1a\1c"});
        ValidateJsonExists("$.a ? (@.b == $.c)", {"\1a\1b", "\1c"});
        ValidateJsonExists("$.a ? (@ == @.b)", {"\1a", "\1a\1b"});
        // Both sides are literals
        ValidateError("$.a ? (1 == 2)", compError, ECallableType::JsonExists);
        ValidateError("$.a ? (\"x\" == \"y\")", compError, ECallableType::JsonExists);

        // IsUnknown inside filter: EMode::Filter allows predicates, but IsUnknown evaluates its
        // inner argument in EMode::Predicate, where predicate types (==, starts with, etc.) are blocked
        ValidateError("$.a ? ((@.b == 10) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\") is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b like_regex \".*\") is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b)) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b != 10) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b < 5) is unknown)", predError, ECallableType::JsonExists);
        // deeper paths
        ValidateError("$.a ? ((@.b.c == 10) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a.b ? ((@.c.d starts with \"x\") is unknown)", predError, ECallableType::JsonExists);

        // IsUnknown wrapping && inside filter: && evaluates its operands (==, etc.) in EMode::Predicate, blocked
        ValidateError("$.a ? ((@.b == 10 && @.c == 20) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\" && @.c == 1) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) && @.c like_regex \"y.*\") is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) && exists(@.c)) is unknown)", predError, ECallableType::JsonExists);

        // IsUnknown wrapping || inside filter: same, || evaluates operands in EMode::Predicate
        ValidateError("$.a ? ((@.b == 10 || @.c == 20) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\" || @.c == 1) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) || @.c like_regex \"y.*\") is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) || exists(@.c)) is unknown)", predError, ECallableType::JsonExists);

        // Unary NOT inside filter - UnaryNot always returns predError regardless of mode
        ValidateError("$.a ? (!(@.b == 10))", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b starts with \"x\"))", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (!(exists(@.b)))", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b like_regex \".*\"))", predError, ECallableType::JsonExists);
        // deeper paths
        ValidateError("$.a ? (!(@.b.c == 10))", predError, ECallableType::JsonExists);
        ValidateError("$.key ? (!(@.sub != \"x\"))", predError, ECallableType::JsonExists);

        // Unary NOT on left / right of && and || inside filter
        ValidateError("$.a ? (!(@.b == 10) && @.c == 20)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (@.b == 10 && !(@.c == 20))", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b starts with \"x\") && @.c == 1)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (exists(@.b) && !(@.c like_regex \"y.*\"))", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b == 10) || @.c == 20)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (@.b == 10 || !(@.c == 20))", predError, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b starts with \"x\") || exists(@.c))", predError, ECallableType::JsonExists);

        // Unary NOT inside is unknown inside filter
        ValidateError("$.a ? ((!(@.b == 10)) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((!(@.b starts with \"x\")) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((!(exists(@.b))) is unknown)", predError, ECallableType::JsonExists);

        // Unary NOT inside && / || which are wrapped by is unknown inside filter
        ValidateError("$.a ? ((!(@.b == 10) && @.c == 20) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b == 10 && !(@.c == 20)) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((!(@.b == 10) || @.c == 20) is unknown)", predError, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b == 10 || !(@.c == 20)) is unknown)", predError, ECallableType::JsonExists);
    }

    // Nested filter: (@ ? (predicate)).member == value
    Y_UNIT_TEST(CollectPath_NestedFilter) {
        // Basic: inner == predicate, outer comparison dropped
        ValidateJsonExists("$ ? ((@ ? (@.a == 1)).b == 2)", {"\1a" + numSuffix(1)});
        ValidateJsonExists("$ ? ((@ ? (@.a == \"x\")).b == \"y\")", {"\1a" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? (@.a == true)).b == false)", {"\1a" + boolTrueSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == false)).b == true)", {"\1a" + boolFalseSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == null)).b == null)", {"\1a" + nullSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == -3.14)).b == 0)", {"\1a" + numSuffix(-3.14)});

        // Reversed literal in inner predicate (literal == @.path)
        ValidateJsonExists("$ ? ((@ ? (1 == @.a)).b == 2)", {"\1a" + numSuffix(1)});
        ValidateJsonExists("$ ? ((@ ? (\"x\" == @.a)).b == \"y\")", {"\1a" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? (null == @.a)).b == 0)", {"\1a" + nullSuffix});

        // Outer path contributes to the index prefix
        ValidateJsonExists("$.key ? ((@ ? (@.sub == \"x\")).other == \"y\")", {"\3key\3sub" + strSuffix("x")});
        ValidateJsonExists("$.a.b ? ((@ ? (@.c == true)).d == false)", {"\1a\1b\1c" + boolTrueSuffix});
        ValidateJsonExists("$.arr ? ((@ ? (@.id == 9)).name == \"x\")", {"\3arr\2id" + numSuffix(9)});
        ValidateJsonExists("$.items ? ((@ ? (@.type == null)).value > 0)", {"\5items\4type" + nullSuffix});

        // Comparison operators != == in inner predicate: literal not appended, only path
        ValidateJsonExists("$ ? ((@ ? (@.n < 10)).label == \"x\")", {"\1n"});
        ValidateJsonExists("$ ? ((@ ? (@.n > 0)).label == \"x\")", {"\1n"});
        ValidateJsonExists("$ ? ((@ ? (@.n != 0)).label == \"x\")", {"\1n"});
        ValidateJsonExists("$ ? ((@ ? (@.n >= 0)).label == \"x\")", {"\1n"});
        ValidateJsonExists("$ ? ((@ ? (@.n <= 100)).label == \"x\")", {"\1n"});
        ValidateJsonExists("$.arr ? ((@ ? (@.score >= 5)).rank == 1)", {"\3arr\5score"});

        // Deeper inner path
        ValidateJsonExists("$ ? ((@ ? (@.a.b == 1)).c == 2)", {"\1a\1b" + numSuffix(1)});
        ValidateJsonExists("$.key ? ((@ ? (@.a.b.c == \"x\")).d == \"y\")", {"\3key\1a\1b\1c" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? (@.x.y == null)).z == true)", {"\1x\1y" + nullSuffix});

        // Array subscript in inner predicate path: subscript is dropped for the index
        ValidateJsonExists("$ ? ((@ ? (@.a[0] == 1)).b == 2)", {"\1a" + numSuffix(1)});
        ValidateJsonExists("$ ? ((@ ? (@.a[last] == true)).b == null)", {"\1a" + boolTrueSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a[0].b == \"x\")).c == 1)", {"\1a\1b" + strSuffix("x")});

        // Array subscript on @ before inner filter: subscript is dropped for the index
        ValidateJsonExists("$ ? ((@[0] ? (@.id == 9)).name == \"x\")", {"\2id" + numSuffix(9)});
        ValidateJsonExists("$ ? ((@[*] ? (@.tag == \"foo\")).value > 0)", {"\3tag" + strSuffix("foo")});
        ValidateJsonExists("$.items ? ((@[*] ? (@.tag == \"foo\")).value > 0)", {"\5items\3tag" + strSuffix("foo")});
        ValidateJsonExists("$.k ? ((@[1] ? (@.x == true)).y == false)", {"\1k\1x" + boolTrueSuffix});

        // Wildcard in inner predicate: path finishes, literal not appended
        ValidateJsonExists("$ ? ((@ ? (@.* == 1)).x == 2)", {""});
        ValidateJsonExists("$.key ? ((@ ? (@.* == \"x\")).y == 1)", {"\3key"});
        ValidateJsonExists("$ ? ((@ ? (@.a.* == null)).b == 1)", {"\1a"});

        // Inner AND: both tokens propagate (filter result has multiple tokens)
        ValidateJsonExists("$ ? ((@ ? (@.a == 1 && @.b == 2)).c == 3)", {"\1a" + numSuffix(1), "\1b" + numSuffix(2)});
        ValidateJsonExists("$.key ? ((@ ? (@.x == \"v\" && @.y == true)).z == null)", {"\3key\1x" + strSuffix("v"), "\3key\1y" + boolTrueSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == null && @.b == false)).c == 1)", {"\1a" + nullSuffix, "\1b" + boolFalseSuffix});

        // Inner OR: both tokens propagate
        ValidateJsonExists("$ ? ((@ ? (@.a == 1 || @.a == 2)).b == \"x\")", {"\1a" + numSuffix(1), "\1a" + numSuffix(2)});
        ValidateJsonExists("$ ? ((@ ? (@.tag == \"foo\" || @.tag == \"bar\")).value == 0)", {"\3tag" + strSuffix("foo"), "\3tag" + strSuffix("bar")});
        ValidateJsonExists("$.arr ? ((@ ? (@.id == 1 || @.id == 2)).val == true)", {"\3arr\2id" + numSuffix(1), "\3arr\2id" + numSuffix(2)});

        // Double nesting: only deepest (innermost) inner filter determines the tokens
        ValidateJsonExists("$ ? ((@ ? ((@ ? (@.z == 1)).w == 2)).val == 3)", {"\1z" + numSuffix(1)});
        ValidateJsonExists("$.a ? ((@ ? ((@ ? (@.b == \"x\")).c == \"y\")).d == \"z\")", {"\1a\1b" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? ((@ ? (@.p == null)).q == true)).r == false)", {"\1p" + nullSuffix});
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
