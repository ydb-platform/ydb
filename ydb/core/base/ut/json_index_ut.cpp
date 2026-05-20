#include "json_index.h"

#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJsonIndex {

using namespace NYql::NJsonPath;

namespace {

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

const TString compError = "Comparison is not allowed between literals/variables on both sides";
const TString predError = "Predicates are not allowed in this context";
const TString filterError = "'@' is only allowed inside filters";
const TString emptyError = "Cannot collect tokens for the given JSON path"; 
const TString varContextError = "Variables are not allowed in this context";

using EMode = TCollectResult::ETokensMode;
using TVarMap = std::unordered_map<TString, TString>;

// Successful parsing and collection of JSON path with mode validation
TTokens ParseAndCollect(const TString& jsonPath, ECallableType callableType, const TVarMap& variables = {},
    const TVarMap& paramVariables = {}, std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
    UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

    auto result = CollectJsonPath(path, callableType, variables, paramVariables);
    UNIT_ASSERT_C(!result.IsError(), "Collect errors found for path: " + jsonPath + ": " + result.GetError().GetMessage());

    if (tokensMode.has_value()) {
        UNIT_ASSERT_C(result.GetTokensMode() == *tokensMode, "for path = " << jsonPath);
    }

    return result.GetTokens();
}

// Compare expected tokens with collected tokens
void ValidateTokens(const TString& jsonPath, const std::vector<TToken>& expected, const TVarMap& variables = {},
    const TVarMap& paramVariables = {}, ECallableType callableType = ECallableType::JsonValue,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    auto expectedTokens = TTokens{expected.begin(), expected.end()};
    auto result = ParseAndCollect(jsonPath, callableType, variables, paramVariables, tokensMode);

    for (const auto& token : result) {
        UNIT_ASSERT_C(expectedTokens.contains(token), "for path = " << jsonPath);
    }
    UNIT_ASSERT_VALUES_EQUAL_C(result.size(), expectedTokens.size(), "for path = " << jsonPath);
}

void ValidateTokens(const TString& jsonPath, const std::vector<TString>& expected, const TVarMap& variables = {},
    const TVarMap& paramVariables = {}, ECallableType callableType = ECallableType::JsonValue,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    std::vector<TToken> tokenList;
    tokenList.reserve(expected.size());
    for (const auto& token : expected) {
        tokenList.emplace_back(token, "");
    }
    ValidateTokens(jsonPath, tokenList, variables, paramVariables, callableType, tokensMode);
}

// Validate error for the given JSON path
template <bool ParserError = false>
void ValidateError(const TString& jsonPath, const TString& errorMessage, const TVarMap& variables = {},
    const TVarMap& paramVariables = {}, ECallableType callableType = ECallableType::JsonValue)
{
    NYql::TIssues issues;
    const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);

    if constexpr (ParserError) {
        UNIT_ASSERT_STRING_CONTAINS_C(issues.ToOneLineString(), errorMessage, "for path = " << jsonPath);
    } else {
        UNIT_ASSERT_C(issues.Empty(), "Parse errors found for path: " + jsonPath + ": " + issues.ToOneLineString());

        auto result = CollectJsonPath(path, callableType, variables, paramVariables);
        UNIT_ASSERT_C(result.IsError(), "Expected error for path: " + jsonPath + ": " + errorMessage);

        UNIT_ASSERT_STRING_CONTAINS_C(result.GetError().GetMessage(), errorMessage, "for path = " << jsonPath);
    }
}

// Simple JSON_EXISTS wrapper without variables
void ValidateJsonExists(const TString& jsonPath, const std::vector<TString>& expected,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    ValidateTokens(jsonPath, expected, {}, {}, ECallableType::JsonExists, tokensMode);
}

// Simple JSON_VALUE wrapper without variables
void ValidateJsonValue(const TString& jsonPath, const std::vector<TString>& expected,
    std::optional<TCollectResult::ETokensMode> tokensMode = std::nullopt)
{
    ValidateTokens(jsonPath, expected, {}, {}, ECallableType::JsonValue, tokensMode);
}

// Make a collect result with the given tokens and mode
TCollectResult MakeParamTokens(const std::vector<TToken>& tokens, EMode mode = EMode::NotSet) {
    TTokens tokenSet(tokens.begin(), tokens.end());
    TCollectResult result(std::move(tokenSet));
    result.SetTokensMode(mode);
    return result;
}

// Make a collect result with the given strings (without params) and mode
TCollectResult MakeTokens(const std::vector<TString>& tokens, EMode mode = EMode::NotSet) {
    std::vector<TToken> tokenList;
    tokenList.reserve(tokens.size());
    for (const auto& token : tokens) {
        tokenList.emplace_back(token, "");
    }
    return MakeParamTokens(tokenList, mode);
}

// Make a collect result with the given error message
TCollectResult MakeError(const TString& message) {
    return TCollectResult(NYql::TIssue(message));
}

// Check the merge result with the given expected tokens and mode
void CheckMergeFull(const TCollectResult& result, const std::vector<TToken>& expectedTokens, EMode expectedMode, const TString& description) {
    UNIT_ASSERT_C(!result.IsError(), description << ": got error: " << result.GetError().GetMessage());
    UNIT_ASSERT_C(result.GetTokensMode() == expectedMode, description << ": modes differ");

    TTokens expected(expectedTokens.begin(), expectedTokens.end());

    for (const auto& token : result.GetTokens()) {
        UNIT_ASSERT_C(expected.contains(token), description << ": token " << token.PathToken << " with param " << token.ParamName << " is not expected");
    }
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetTokens().size(), expected.size(), description << ": token sets differ");
}

// Check the merge result with the given expected strings (without params) and mode
void CheckMerge(const TCollectResult& result, std::vector<TString> expectedTokens, EMode expectedMode) {
    std::vector<TToken> expectedTokenList;
    expectedTokenList.reserve(expectedTokens.size());
    for (const auto& token : expectedTokens) {
        expectedTokenList.emplace_back(token, "");
    }
    CheckMergeFull(result, expectedTokenList, expectedMode, "CheckMerge");
}

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
        ValidateJsonExists("$.a", {"\2a"});
        ValidateJsonExists("$.a.b.c", {"\2a\2b\2c"});
        ValidateJsonExists("$.aba.\"caba\"", {"\4aba\5caba"});
        ValidateJsonExists("$.\"\".abc", {"\1\4abc"});
        ValidateJsonExists("$.*", {""});
        ValidateJsonExists("$.a.*", {"\2a"});
        ValidateJsonExists("$.a.*.c", {"\2a"});
    }

    Y_UNIT_TEST(CollectPath_ArrayAccess) {
        ValidateJsonExists("$[0]", {""});
        ValidateJsonExists("$[1, 2, 3]", {""});
        ValidateJsonExists("$[1 to 3]", {""});
        ValidateJsonExists("$[last]", {""});
        ValidateJsonExists("$[0, 2 to last]", {""});
        ValidateJsonExists("$[0 to 1].key", {"\4key"});
        ValidateJsonExists("$.key[0]", {"\4key"});
        ValidateJsonExists("$.key1[last].key2", {"\5key1\5key2"});
        ValidateJsonExists("$.arr[2 to last]", {"\4arr"});
        ValidateJsonExists("$.*[2 to last].key", {""});
        ValidateJsonExists("$.key[0].*", {"\4key"});
    }

    // Methods stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_Methods) {
        ValidateJsonExists("$.abs()", {""});
        ValidateJsonExists("$.*.floor()", {""});
        ValidateJsonExists("$[1, 2, 3].ceiling()", {""});
        ValidateJsonExists("$.key.abs()", {"\4key"});
        ValidateJsonExists("$.key.floor()", {"\4key"});
        ValidateJsonExists("$.key.ceiling()", {"\4key"});
        ValidateJsonExists("$.key.double()", {"\4key"});
        ValidateJsonExists("$.key.type()", {"\4key"});
        ValidateJsonExists("$.key.size()", {"\4key"});
        ValidateJsonExists("$.key.keyvalue()", {"\4key"});
        ValidateJsonExists("$.*.keyvalue()", {""});
        ValidateJsonExists("$.key[1, 2, 3].value.size().floor()", {"\4key\6value"});
        ValidateJsonExists("$.key.keyvalue().name", {"\4key"});
    }

    // StartsWith predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_StartsWithPredicate) {
        ValidateJsonValue("$ starts with \"lol\"", {""});
        ValidateJsonValue("$[1 to last] starts with \"lol\"", {""});
        ValidateJsonValue("$[*] starts with \"lol\"", {""});
        ValidateJsonValue("$.key starts with \"abc\"", {"\4key"});
        ValidateJsonValue("$.a.b.c[1, 2, 3] starts with \"abc\"", {"\2a\2b\2c"});
        ValidateJsonValue("$.key.type().name starts with \"abc\"", {"\4key"});
        ValidateJsonValue("$.* starts with \"abc\"", {""});
        ValidateJsonValue("$.a.*.c[1, 2, 3] starts with \"abc\"", {"\2a"});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key starts with \"lol\"", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);
    }

    // LikeRegex predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_LikeRegexPredicate) {
        ValidateJsonValue("$ like_regex \"abc\"", {""});
        ValidateJsonValue("$[1 to 2] like_regex \"abc\"", {""});
        ValidateJsonValue("$[*] like_regex \"abc\"", {""});
        ValidateJsonValue("$.key like_regex \"abc\"", {"\4key"});
        ValidateJsonValue("$.* like_regex \"abc\"", {""});
        ValidateJsonValue("$.key[1, 2, 3] like_regex \"abc\"", {"\4key"});
        ValidateJsonValue("$.key.keyvalue() like_regex \"abc\"", {"\4key"});
        ValidateJsonValue("$.key like_regex \"a.c\"", {"\4key"});
        ValidateJsonValue("$.key like_regex \".*\"", {"\4key"});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key like_regex \"abc\"", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);
    }

    // Exists predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_ExistsPredicate) {
        ValidateJsonValue("exists($)", {""});
        ValidateJsonValue("exists($.key)", {"\4key"});
        ValidateJsonValue("exists($.key[1, 2, 3])", {"\4key"});
        ValidateJsonValue("exists($[*].size())", {""});
        ValidateJsonValue("exists($.key.keyvalue().name)", {"\4key"});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("exists($)", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);
    }

    // IsUnknown predicates return error because their argument must be a predicate (-> nested predicates are not allowed)
    Y_UNIT_TEST(CollectPath_IsUnknownPredicate) {
        ValidateError("($ starts with \"abc\") is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($ like_regex \"abc\") is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(exists($.key)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key == 10) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key != 10) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key < 10) is unknown", predError, {}, {}, ECallableType::JsonValue);

        // For JSON_EXISTS, predicate mode is denied even earlier (at context level)
        ValidateError("($ starts with \"abc\") is unknown", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("($.key == 10) is unknown", predError, {}, {}, ECallableType::JsonExists);

        // IsUnknown wrapping && - inner AND evaluates its operands (==, starts with, etc.) in EMode::Predicate, blocked
        ValidateError("(($.a == 10) && ($.b == 20)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a starts with \"x\") && ($.b == 1)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(exists($.a) && ($.b like_regex \"y.*\")) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 10) && ($.b == 20) && ($.c == 30)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(exists($.a) && exists($.b)) is unknown", predError, {}, {}, ECallableType::JsonValue);

        // IsUnknown wrapping || - same: inner OR evaluates its predicate operands in EMode::Predicate, blocked
        ValidateError("(($.a == 10) || ($.b == 20)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a starts with \"x\") || ($.b == 1)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(exists($.a) || ($.b like_regex \"y.*\")) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 10) || ($.b == 20) || ($.c == 30)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(exists($.a) || exists($.b)) is unknown", predError, {}, {}, ECallableType::JsonValue);

        // IsUnknown wrapping ! - UnaryNot is in the predicate-type block list, blocked by predicate mode check
        ValidateError("(!($.a == 10)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(!($.a starts with \"x\")) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(!(exists($.a))) is unknown", predError, {}, {}, ECallableType::JsonValue);

        // IsUnknown wrapping && / || that contain !
        ValidateError("(!($.a == 10) && ($.b == 20)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 10) && !($.b == 20)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(!($.a == 10) || ($.b == 20)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 10) || !($.b == 20)) is unknown", predError, {}, {}, ECallableType::JsonValue);
    }

    // Unary NOT always returns predError.
    // For JsonExists: ArePredicatesAllowed(Context) = false, error comes from UnaryNot itself.
    // For JsonValue: inner operand is collected in EMode::Predicate where predicate types are blocked.
    Y_UNIT_TEST(CollectPath_UnaryNot) {
        // Basic cases with JsonExists
        ValidateError("!($.a == 10)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("!($.key == \"hello\")", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("!($.a == true)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("!($.a == null)", predError, {}, {}, ECallableType::JsonExists);

        // Basic cases with JsonValue
        ValidateError("!($.a == 10)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("!($.key == \"hello\")", predError, {}, {}, ECallableType::JsonValue);

        // Deeper paths
        ValidateError("!($.a.b.c == 42)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("!($.a.b == \"x\")", predError, {}, {}, ECallableType::JsonValue);

        // NOT applied to exists predicate
        ValidateError("!(exists($.key))", predError, {}, {}, ECallableType::JsonValue);

        // NOT applied to starts with predicate
        ValidateError("!($.key starts with \"abc\")", predError, {}, {}, ECallableType::JsonValue);

        // NOT applied to like_regex predicate
        ValidateError("!($.key like_regex \"abc\")", predError, {}, {}, ECallableType::JsonValue);

        // Double NOT
        ValidateError("!(!($.a == 10))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("!(!($.a == 10))", predError, {}, {}, ECallableType::JsonValue);

        // NOT as left operand of AND - error propagates immediately from left
        ValidateError("!($.a == 10) && ($.b == 20)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("!($.key starts with \"abc\") && ($.b == 1)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("!(exists($.key)) && ($.b == 2)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("!($.a like_regex \".*\") && ($.b == 3)", predError, {}, {}, ECallableType::JsonValue);

        // NOT as right operand of AND - left side succeeds, then error from right
        ValidateError("($.a == 10) && !($.b == 20)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a starts with \"x\") && !($.b == 1)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists($.a) && !($.b like_regex \"y.*\")", predError, {}, {}, ECallableType::JsonValue);

        // NOT as left operand of OR - error propagates immediately from left
        ValidateError("!($.a == 10) || ($.b == 20)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("!($.key starts with \"abc\") || ($.b == 1)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("!(exists($.key)) || ($.b == 2)", predError, {}, {}, ECallableType::JsonValue);

        // NOT as right operand of OR - left side succeeds, then error from right
        ValidateError("($.a == 10) || !($.b == 20)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a starts with \"x\") || !($.b == 1)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists($.a) || !($.b like_regex \"y.*\")", predError, {}, {}, ECallableType::JsonValue);

        // NOT inside is unknown - is unknown receives error from its argument
        ValidateError("(!($.a == 10)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(!($.key starts with \"abc\")) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(!(exists($.key))) is unknown", predError, {}, {}, ECallableType::JsonValue);

        // NOT in chained AND/OR
        ValidateError("($.a == 1) && !($.b == 2) && ($.c == 3)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a == 1) || !($.b == 2) || ($.c == 3)", predError, {}, {}, ECallableType::JsonValue);
    }

    // Unary +/- stop further path extraction (same as methods): operand path only
    Y_UNIT_TEST(CollectPath_UnaryPlusMinus) {
        ValidateJsonExists("-$.key", {"\4key"});
        ValidateJsonExists("+$.key", {"\4key"});

        ValidateJsonExists("-$", {""});
        ValidateJsonExists("+$", {""});

        ValidateJsonExists("-$.a.b.c", {"\2a\2b\2c"});
        ValidateJsonExists("+$.a.b.c", {"\2a\2b\2c"});

        ValidateJsonExists("-$.*", {""});
        ValidateJsonExists("+$.*", {""});
        ValidateJsonExists("-$.a.*", {"\2a"});
        ValidateJsonExists("+$.a.*", {"\2a"});

        ValidateJsonExists("-$.key[0]", {"\4key"});
        ValidateJsonExists("+$.key[last]", {"\4key"});

        ValidateJsonExists("-$.key.abs()", {"\4key"});
        ValidateJsonExists("+$.key.type()", {"\4key"});

        ValidateJsonExists("-(-$.key)", {"\4key"});
        ValidateJsonExists("-(+$.key)", {"\4key"});
        ValidateJsonExists("+(-$.key)", {"\4key"});
        ValidateJsonExists("+(+$.key)", {"\4key"});

        ValidateJsonValue("exists(-$.a.b)", {"\2a\2b"});
        ValidateJsonValue("exists(+$.a.b)", {"\2a\2b"});

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
        ValidateTokens("$.key + 1", {"\4key"});
        ValidateTokens("$.key - 1", {"\4key"});
        ValidateTokens("$.key - (-1)", {"\4key"});
        ValidateTokens("$.key * 2", {"\4key"});
        ValidateTokens("$.key / 2", {"\4key"});
        ValidateTokens("$.key % 2", {"\4key"});

        // Literal on the left, path on the right - only right token
        ValidateTokens("1 + $.key", {"\4key"});
        ValidateTokens("-1 - $.key", {"\4key"});
        ValidateTokens("1 * $.key", {"\4key"});
        ValidateTokens("(+(-1)) / $.key", {"\4key"});
        ValidateTokens("1 % $.key", {"\4key"});

        // Context object on the left
        ValidateTokens("$ + 1", {""});
        ValidateTokens("$ * 2", {""});

        // Deeper paths as left operand
        ValidateTokens("$.a.b.c + 1", {"\2a\2b\2c"});
        ValidateTokens("$.a.b - 1", {"\2a\2b"});

        // Array access on the left operand
        ValidateTokens("$.key[0] + 1", {"\4key"});
        ValidateTokens("$.arr[last] * 2", {"\4arr"});

        // Wildcard on the left - collection already finished by wildcard
        ValidateTokens("$.* + 1", {""});
        ValidateTokens("$.* + (-1)", {""});
        ValidateTokens("$.a.* - 1", {"\2a"});

        // Both operands are paths - tokens from both are collected (AND)
        ValidateTokens("$.a + $.b", {"\2a", "\2b"});
        ValidateTokens("$.a.b - $.c.d", {"\2a\2b", "\2c\2d"});
        ValidateTokens("$.a.b - (-$.c.d)", {"\2a\2b", "\2c\2d"});

        // Both operands are literals - no path to collect
        ValidateError("1 + 2", emptyError);
        ValidateError("(+(-1.5)) * 2.0", emptyError);

        // Wildcard on left, path on right - both collected
        ValidateJsonExists("$.a.* + $.b", {"\2a", "\2b"});
        ValidateJsonExists("$.* + $.b", {"", "\2b"});
        ValidateJsonExists("$.* - $.a.b", {"", "\2a\2b"});

        // Path on left, wildcard on right
        ValidateJsonExists("$.a + $.*", {"\2a", ""});
        ValidateJsonExists("$.a.b + $.*", {"\2a\2b", ""});

        // Wildcard on both sides - two wildcard tokens collected
        ValidateJsonExists("$.* + $.*", {""});
        ValidateJsonExists("$.a.* + $.*", {"\2a", ""});
        ValidateJsonExists("$.* + $.a.*", {"", "\2a"});
        ValidateJsonExists("$.a.b.*.c + $.a.b.*.d", {"\2a\2b"});

        // Variable on left - propagated immediately, right not collected
        ValidateJsonExists("$var + $.b", {"\2b"});
        ValidateJsonExists("$var - $.b", {"\2b"});
        ValidateJsonExists("$var * $.b", {"\2b"});
        ValidateJsonExists("$var / $.b", {"\2b"});
        ValidateJsonExists("$var % $.b", {"\2b"});

        // Variable on right - left tokens lost, variable not collected
        ValidateJsonExists("$.a + $var", {"\2a"});
        ValidateJsonExists("$.a - $var", {"\2a"});
        ValidateJsonExists("$.a * $var", {"\2a"});

        // Variable propagates through chained binary: ($.a + $var) + $.c
        ValidateJsonExists("$.a + $var + $.c", {"\2a", "\2c"});
    }

    // Non-trivial combinations of unary and binary arithmetic operators
    Y_UNIT_TEST(CollectPath_ArithmeticCombinations) {
        // Unary applied to binary: tokens from both binary operands, then Finish
        ValidateTokens("-($.a + 1)", {"\2a"});
        ValidateTokens("+($.a - 1)", {"\2a"});
        ValidateTokens("-($.a * $.b)", {"\2a", "\2b"});
        ValidateTokens("-(1 + $.b)", {"\2b"});

        // Binary with unary left operand
        ValidateTokens("-$.a + $.b", {"\2a", "\2b"});
        ValidateTokens("+$.a - $.b", {"\2a", "\2b"});
        ValidateTokens("-$.key + 1", {"\4key"});
        ValidateTokens("+$.key * 2", {"\4key"});

        // Binary with unary right operand - right token still collected
        ValidateTokens("$.a + (-$.b)", {"\2a", "\2b"});
        ValidateTokens("$.a * (+$.b)", {"\2a", "\2b"});
        ValidateTokens("1 + (-$.b)", {"\2b"});

        // Chained binary (left-associative): all three path tokens collected
        ValidateTokens("$.a + $.b + $.c", {"\2a", "\2b", "\2c"});
        ValidateTokens("$.a - $.b - $.c", {"\2a", "\2b", "\2c"});
        ValidateTokens("$.a * $.b * $.c", {"\2a", "\2b", "\2c"});

        // Mixed precedence: * binds tighter than +, but all paths still collected
        ValidateTokens("$.a + $.b * $.c", {"\2a", "\2b", "\2c"});
        ValidateTokens("$.a * $.b + $.c", {"\2a", "\2b", "\2c"});

        // Double unary combined with binary
        ValidateTokens("-(-$.a) + $.b", {"\2a", "\2b"});
        ValidateTokens("-(+$.a) * 2", {"\2a"});

        // Longer paths on both sides
        ValidateTokens("$.a.b.c + $.x.y.z", {"\2a\2b\2c", "\2x\2y\2z"});
        ValidateTokens("-($.a.*.c) + $.x.y.*", {"\2a", "\2x\2y"});
        ValidateTokens("$.a.b.c * 3.14", {"\2a\2b\2c"});

        // Method result used as operand of binary - method finishes, but token still collected
        ValidateTokens("$.key.size() + 1", {"\4key"});
        ValidateTokens("$.key.abs() * 2", {"\4key"});
        ValidateTokens("$.a.size() + $.b.floor()", {"\2a", "\2b"});
    }

    // Arithmetic operators (two-path operands produce And mode) combined with && and ||
    Y_UNIT_TEST(CollectPath_ArithmeticWithBooleanOps) {
        // Two-path arithmetic result (And mode) in AND chain: stays And
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a - $.b == \"x\") && ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a * $.b == \"x\") && ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a / $.b == \"x\") && ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a % $.b == \"x\") && ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.c == 1) && ($.a + $.b == \"x\")", {"\2c" + numSuffix(1), "\2a", "\2b"}, TCollectResult::ETokensMode::And);

        // Two arithmetic results combined via AND: stays And
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c + $.d == \"y\")", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a - $.b == \"x\") && ($.c * $.d == \"y\") && ($.e == 1)", {"\2a", "\2b", "\2c", "\2d", "\2e" + numSuffix(1)},
            TCollectResult::ETokensMode::And);

        // Two-path arithmetic result (And mode) in OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a - $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a * $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a / $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a % $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.c == 1) || ($.a + $.b == \"x\")", {"\2c" + numSuffix(1), "\2a", "\2b"}, TCollectResult::ETokensMode::Or);

        // Two arithmetic results combined via OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c + $.d == \"y\")", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a - $.b == \"x\") || ($.c * $.d == \"y\")", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a / $.b == \"x\") || ($.c % $.d == \"y\")", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);

        // Three-way OR of arithmetic results: all become OR
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c + $.d == \"y\") || ($.e == 1)",
            {"\2a", "\2b", "\2c", "\2d", "\2e" + numSuffix(1)}, TCollectResult::ETokensMode::Or);

        // Arithmetic result with comparison (single-path, NotSet) via AND: compatible, stays And
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c < 5)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.c < 5) && ($.a + $.b == \"x\")", {"\2c", "\2a", "\2b"}, TCollectResult::ETokensMode::And);

        // Arithmetic result with comparison via OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c < 5)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.c < 5) || ($.a + $.b == \"x\")", {"\2c", "\2a", "\2b"}, TCollectResult::ETokensMode::Or);

        // Arithmetic result with starts with / like_regex / exists in AND: compatible
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c starts with \"abc\")", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c like_regex \".*\")", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a + $.b == \"x\") && exists($.c)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);

        // Arithmetic result with starts with / like_regex / exists in OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c starts with \"abc\")", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c like_regex \".*\")", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a + $.b == \"x\") || exists($.c)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);

        // Deeper paths in arithmetic operands
        ValidateJsonValue("($.a.b.c + $.x.y.z == \"val\") && ($.key == 1)", {"\2a\2b\2c", "\2x\2y\2z", "\4key" + numSuffix(1)},
            TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a.b.c + $.x.y.z == \"val\") || ($.key == 1)", {"\2a\2b\2c", "\2x\2y\2z", "\4key" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);

        // Filter: arithmetic with two paths combined via OR with plain path
        ValidateJsonExists("$.key ? (@.a + @.b == 5 || @.c == 1)", {"\4key\2a", "\4key\2b", "\4key\2c" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
        // Filter: two arithmetic results in OR
        ValidateJsonExists("$.key ? (@.a + @.b == 5 || @.c + @.d == 3)", {"\4key\2a", "\4key\2b", "\4key\2c", "\4key\2d"},
            TCollectResult::ETokensMode::Or);
        // Filter: AND chain with OR appended - OR wins
        ValidateJsonExists("$.key ? (@.a + @.b == 5 && @.c == 1 || @.d == 2)",
            {"\4key\2a", "\4key\2b", "\4key\2c" + numSuffix(1), "\4key\2d" + numSuffix(2)}, TCollectResult::ETokensMode::Or);
    }

    Y_UNIT_TEST(CollectPath_EqualityOperator) {
        // Path == literal, all literal types
        ValidateJsonValue("$.key == \"hello\"", {"\4key" + strSuffix("hello")});
        ValidateJsonValue("$.key == \"\"", {"\4key" + strSuffix("")});
        ValidateJsonValue("$.key == 42", {"\4key" + numSuffix(42)});
        ValidateJsonValue("$.key == 0", {"\4key" + numSuffix(0)});
        ValidateJsonValue("$.key == 3.14", {"\4key" + numSuffix(3.14)});
        ValidateJsonValue("$.key == true", {"\4key" + boolTrueSuffix});
        ValidateJsonValue("$.key == false", {"\4key" + boolFalseSuffix});
        ValidateJsonValue("$.key == null", {"\4key" + nullSuffix});

        // Reversed order: literal == path (identical result)
        ValidateJsonValue("\"hello\" == $.key", {"\4key" + strSuffix("hello")});
        ValidateJsonValue("42 == $.key", {"\4key" + numSuffix(42)});
        ValidateJsonValue("true == $.key", {"\4key" + boolTrueSuffix});
        ValidateJsonValue("null == $.key", {"\4key" + nullSuffix});

        // Context object as path (empty prefix)
        ValidateJsonValue("$ == \"hello\"", {strSuffix("hello")});
        ValidateJsonValue("$ == 42", {numSuffix(42)});
        ValidateJsonValue("$ == true", {boolTrueSuffix});
        ValidateJsonValue("$ == null", {nullSuffix});
        ValidateJsonValue("\"hello\" == $", {strSuffix("hello")});

        // Deeper paths
        ValidateJsonValue("$.a.b == \"x\"", {"\2a\2b" + strSuffix("x")});
        ValidateJsonValue("$.a.b.c == null", {"\2a\2b\2c" + nullSuffix});
        ValidateJsonValue("\"x\" == $.a.b.c", {"\2a\2b\2c" + strSuffix("x")});
        ValidateJsonValue("$.aba.\"caba\" == true", {"\4aba\5caba" + boolTrueSuffix});
        ValidateJsonValue("$.a.b.c.d == 0", {"\2a\2b\2c\2d" + numSuffix(0)});
        ValidateJsonValue("$.\"\".\"\" == 0", {"\1\1" + numSuffix(0)});

        // Array subscript
        ValidateJsonValue("$.key[0] == \"x\"", {"\4key" + strSuffix("x")});
        ValidateJsonValue("$.key[last] == true", {"\4key" + boolTrueSuffix});
        ValidateJsonValue("$.key[1, 2, 3] == null", {"\4key" + nullSuffix});
        ValidateJsonValue("$.key[0 to last] == 42", {"\4key" + numSuffix(42)});
        ValidateJsonValue("$.key[0].sub == \"x\"", {"\4key\4sub" + strSuffix("x")});
        ValidateJsonValue("$.a.b[0].c == \"x\"", {"\2a\2b\2c" + strSuffix("x")});
        ValidateJsonValue("$.key[*] == \"x\"", {"\4key" + strSuffix("x")});

        // Wildcard member access finishes the path
        ValidateJsonValue("$.* == \"x\"", {""});
        ValidateJsonValue("$.a.* == \"x\"", {"\2a"});
        ValidateJsonValue("$.a.b.* == \"x\"", {"\2a\2b"});
        ValidateJsonValue("\"x\" == $.*", {""});
        ValidateJsonValue("\"x\" == $.a.*", {"\2a"});

        // Methods finish the path
        ValidateJsonValue("$.key.size() == 3", {"\4key"});
        ValidateJsonValue("$.key.abs() == 1", {"\4key"});
        ValidateJsonValue("$.key.type() == \"number\"", {"\4key"});
        ValidateJsonValue("$.a.b.floor() == 0", {"\2a\2b"});
        ValidateJsonValue("$.key.keyvalue().name == \"x\"", {"\4key"});

        // Unary arithmetic on path finishes the path
        ValidateJsonValue("-$.key == 1", {"\4key"});
        ValidateJsonValue("+$.key == 1", {"\4key"});
        ValidateJsonValue("-$.a.b == null", {"\2a\2b"});

        // Literal numeric value folded from unary + / - (same suffix as a plain number literal)
        ValidateJsonValue("$.a == -10", {"\2a" + numSuffix(-10)});
        ValidateJsonValue("$.k == +(-(+(-3)))", {"\2k" + numSuffix(3)});
        ValidateJsonValue("$.key == +42", {"\4key" + numSuffix(42)});
        ValidateJsonValue("$.key == -(-42)", {"\4key" + numSuffix(42)});
        ValidateJsonValue("$.key == +(-15)", {"\4key" + numSuffix(-15)});
        ValidateJsonValue("$.a.b == -(-(-2))", {"\2a\2b" + numSuffix(-2)});
        ValidateJsonValue("$ == +(-(-7))", {numSuffix(7)});
        ValidateJsonValue("-10 == $.a", {"\2a" + numSuffix(-10)});
        ValidateJsonValue("+(-(+(-3))) == $.k", {"\2k" + numSuffix(3)});

        // Arithmetic produces multiple tokens
        ValidateJsonValue("($.a + $.b) == \"x\"", {"\2a", "\2b"});
        ValidateJsonValue("\"x\" == ($.a + $.b)", {"\2a", "\2b"});
        ValidateJsonValue("$.key + 1 == \"x\"", {"\4key"});
        ValidateJsonValue("1 + $.key == \"x\"", {"\4key"});

        // Parenthesized path - no effect
        ValidateJsonValue("($.a.b) == \"x\"", {"\2a\2b" + strSuffix("x")});
        ValidateJsonValue("\"x\" == ($.a.b)", {"\2a\2b" + strSuffix("x")});
        ValidateJsonValue("(((((($).a).b))) == (\"x\"))", {"\2a\2b" + strSuffix("x")});

        // Predicates with equality operator -> nested predicates are not allowed
        ValidateError("exists($.key) == true", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key starts with \"a\") == true", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key like_regex \"a.*\") == true", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a.b starts with \"x\") == false", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key == 10) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.key == 10) == false", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("false == ($.key == 10)", predError, {}, {}, ECallableType::JsonValue);

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key == 10", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);
        ValidateError("false == ($.key == 10)", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);

        // Both operands are paths: merge index tokens with AND (same as comparison ops)
        ValidateJsonValue("$.a == $.b", {"\2a", "\2b"});
        ValidateJsonValue("$.key == $", {"\4key", ""});
        ValidateJsonValue("$ == $", {""});
        ValidateJsonValue("$.a.b == $.c.d", {"\2a\2b", "\2c\2d"});

        // Literals only
        ValidateError("\"x\" == \"y\"", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("1 == 2", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("true == false", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("null == null", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("1 == \"x\"", compError, {}, {}, ECallableType::JsonValue);

        // Without context object
        ValidateError("1 == 1", compError, {}, {}, ECallableType::JsonValue);

        // Variables
        ValidateError("$var == \"x\"", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("\"x\" == $var", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("$var == $var", compError, {}, {}, ECallableType::JsonValue);
        ValidateJsonValue("$ == $var", {""});
    }

    // Comparison operators <, <=, >, >=, != collect path tokens from both operands; literals are silently dropped.
    // Mode is set to And only when more than one token is collected (same rule as BinaryArithmeticOp).
    Y_UNIT_TEST(CollectPath_ComparisonOperators) {
        // Literal on the right is dropped, only the path token is returned.
        ValidateJsonValue("$.key < 10", {"\4key"});
        ValidateJsonValue("$.key <= 10", {"\4key"});
        ValidateJsonValue("$.key > 10", {"\4key"});
        ValidateJsonValue("$.key >= 10", {"\4key"});
        ValidateJsonValue("$.key != 10", {"\4key"});
        ValidateJsonValue("$.key != -10", {"\4key"});
        ValidateJsonValue("$.key >= -(+(-10))", {"\4key"});
        ValidateJsonValue("$.key < \"hello\"", {"\4key"});
        ValidateJsonValue("$.key != \"\"", {"\4key"});
        ValidateJsonValue("$.key > 3.14", {"\4key"});
        ValidateJsonValue("$.key >= true", {"\4key"});
        ValidateJsonValue("$.key < null", {"\4key"});

        // Literal on the left, path on the right - literal dropped, path token returned
        ValidateJsonValue("10 < $.key", {"\4key"});
        ValidateJsonValue("10 <= $.key", {"\4key"});
        ValidateJsonValue("10 > $.key", {"\4key"});
        ValidateJsonValue("10 >= $.key", {"\4key"});
        ValidateJsonValue("10 != $.key", {"\4key"});
        ValidateJsonValue("-10 != $.key", {"\4key"});
        ValidateJsonValue("-(+(-10)) != $.key", {"\4key"});

        // Context object as path (empty prefix)
        ValidateJsonValue("$ < 5", {""});
        ValidateJsonValue("$ > \"x\"", {""});
        ValidateJsonValue("$ != null", {""});

        // Deeper member access paths
        ValidateJsonValue("$.a.b.c < 42", {"\2a\2b\2c"});
        ValidateJsonValue("$.a.b > -1", {"\2a\2b"});
        ValidateJsonValue("$.aba.\"caba\" != false", {"\4aba\5caba"});
        ValidateJsonValue("$.a.b.c.d >= 0", {"\2a\2b\2c\2d"});

        // Array access
        ValidateJsonValue("$.key[0] < 5", {"\4key"});
        ValidateJsonValue("$.key[last] > true", {"\4key"});
        ValidateJsonValue("$.key[1, 2, 3] != null", {"\4key"});
        ValidateJsonValue("$.key[0 to last] >= 1", {"\4key"});
        ValidateJsonValue("$.a.b[0].c <= \"x\"", {"\2a\2b\2c"});

        // Wildcard member access finishes the path (literal not appended, but still dropped)
        ValidateJsonValue("$.* < 5", {""});
        ValidateJsonValue("$.a.* > 1", {"\2a"});
        ValidateJsonValue("$.a.b.* != \"x\"", {"\2a\2b"});

        // Wildcard array access
        ValidateJsonValue("$.key[*] < 5", {"\4key"});

        // Methods finish the path
        ValidateJsonValue("$.key.size() < -3", {"\4key"});
        ValidateJsonValue("$.key.abs() >= 1", {"\4key"});
        ValidateJsonValue("$.a.b.floor() != 0", {"\2a\2b"});
        ValidateJsonValue("$.key.keyvalue().name > \"x\"", {"\4key"});

        // Unary arithmetic on path finishes the path
        ValidateJsonValue("-$.key < 1", {"\4key"});
        ValidateJsonValue("+$.key >= 0", {"\4key"});

        // Both sides are paths - tokens from both collected (mode=And)
        ValidateJsonValue("$.a < $.b", {"\2a", "\2b"});
        ValidateJsonValue("$.a.b > $.c.d", {"\2a\2b", "\2c\2d"});
        ValidateJsonValue("$.key != $.other", {"\4key", "\6other"});
        ValidateJsonValue("$ <= $.a", {"", "\2a"});
        ValidateJsonValue("$.a >= $", {"\2a", ""});

        // Both sides are literals - error
        ValidateError("1 < 2", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("1.5 >= -2.0", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("true != false", emptyError, {}, {}, ECallableType::JsonValue);

        // Arithmetic expression as operand (same as BinaryArithmeticOp behavior)
        // $.a + $.b produces mode=And, comparison also sets And - compatible
        ValidateJsonValue("$.a + $.b < -5", {"\2a", "\2b"});
        ValidateJsonValue("1 < $.a + $.b", {"\2a", "\2b"});
        ValidateJsonValue("$.key + 1 >= 5", {"\4key"});
        ValidateJsonValue("$.a.size() + $.b.abs() != 0", {"\2a", "\2b"});

        // Comparison predicate nested inside another comparison
        ValidateError("($.a == -10) < -5", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a < 5) > 0", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a <= 5) != 0", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("5 > ($.a == 1)", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("5 != ($.a < 3)", predError, {}, {}, ECallableType::JsonValue);

        // Exists/StartsWith/LikeRegex as operand
        ValidateError("exists($.a) < 5", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a starts with \"x\") != true", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a like_regex \".*\") < 1", predError, {}, {}, ECallableType::JsonValue);

        // AND/OR as operand
        ValidateError("($.a == 1 && $.b == 2) < 5", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a == 1 || $.b == 2) != false", predError, {}, {}, ECallableType::JsonValue);

        // JsonExists: predicate not allowed at top level
        ValidateError("$.key < 10", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.key <= -10", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.key > 10", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.key >= 10", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.key != -10", predError, {}, {}, ECallableType::JsonExists);

        // Single-path comparison produces 1 token, NotSet mode, can appear in AND or OR
        ValidateJsonValue("($.a < 5) && ($.b > 1)", {"\2a", "\2b"});
        ValidateJsonValue("($.a <= 5) && ($.b >= 1)", {"\2a", "\2b"});
        ValidateJsonValue("($.a != 5) && ($.b == 1)", {"\2a", "\2b" + numSuffix(1)});
        ValidateJsonValue("($.a < 5) && ($.b > 1) && ($.c != 3)", {"\2a", "\2b", "\2c"});

        ValidateJsonValue("($.a < 5) || ($.b > 1)", {"\2a", "\2b"});
        ValidateJsonValue("($.a >= 5) || ($.b != -1)", {"\2a", "\2b"});
        ValidateJsonValue("($.a != 5) || ($.b == 1)", {"\2a", "\2b" + numSuffix(1)});
        ValidateJsonValue("($.a < -5) || ($.b > 1) || ($.c != 3)", {"\2a", "\2b", "\2c"});

        // Two-path comparison (And mode) mixed with OR: OR wins
        ValidateJsonValue("($.a < $.b) || ($.c > 1)", {"\2a", "\2b", "\2c"});
        ValidateJsonValue("($.a != $.b) || ($.c == -1)", {"\2a", "\2b", "\2c" + numSuffix(-1)});

        // AND chain with OR: OR wins, all tokens become OR
        ValidateJsonValue("($.a < -5) && ($.b == 1) || ($.c > 2)", {"\2a", "\2b" + numSuffix(1), "\2c"});
        ValidateJsonValue("($.a < -5) && ($.b > -1) || ($.c != 3)", {"\2a", "\2b", "\2c"});

        // Variables
        ValidateError("$var < 5", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("5 > $var", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("$var != $var", emptyError, {}, {}, ECallableType::JsonValue);
    }

    // Comparison operators inside filter predicates (EMode::Filter allows predicates)
    Y_UNIT_TEST(CollectPath_ComparisonOperators_InFilter) {
        // Basic filter with each comparison op
        ValidateJsonExists("$.a ? (@.b < 10)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b <= -10)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b > 10)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b >= +10)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b != 10)", {"\2a\2b"});

        // All literal types as right operand (dropped)
        ValidateJsonExists("$.a ? (@.b < \"hello\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b > -3.14)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b != true)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b >= null)", {"\2a\2b"});

        // Literal on the left, @ path on the right
        ValidateJsonExists("$.a ? (10 < @.b)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (\"x\" != @.b)", {"\2a\2b"});

        // @ itself (filter object) as operand
        ValidateJsonExists("$.a ? (@ < 10)", {"\2a"});
        ValidateJsonExists("$.a ? (@ != \"x\")", {"\2a"});
        ValidateJsonExists("$.a.b ? (@ > 0)", {"\2a\2b"});

        // Deeper filter-object paths
        ValidateJsonExists("$.a ? (@.b.c < -(+(-5)))", {"\2a\2b\2c"});
        ValidateJsonExists("$.a.b ? (@.c.d != null)", {"\2a\2b\2c\2d"});

        // Method on filter-object path (finishes, literal dropped)
        ValidateJsonExists("$.a ? (@.b.size() < -3)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.abs() >= 0)", {"\2a\2b"});

        // Unary on filter-object path (finishes, literal dropped)
        ValidateJsonExists("$.a ? (-@.b < 5)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (+@.b >= 0)", {"\2a\2b"});

        // Both operands are @-paths (both tokens collected)
        ValidateJsonExists("$.key ? (@.a < @.b)", {"\4key\2a", "\4key\2b"});
        ValidateJsonExists("$.key ? (@.x != @.y)", {"\4key\2x", "\4key\2y"});

        // Wildcard on filter-object path
        ValidateJsonExists("$.a ? (@.* < 5)", {"\2a"});
        ValidateJsonExists("$.a ? (@.b.* != 1)", {"\2a\2b"});

        // Comparison in AND inside filter
        ValidateJsonExists("$.a ? (@.b < +10 && @.c == 1)", {"\2a\2b", "\2a\2c" + numSuffix(1)});
        ValidateJsonExists("$.a ? (@.b > 0 && @.b < 100)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b != 5 && @.c >= 0 && @.d <= -10)", {"\2a\2b", "\2a\2c", "\2a\2d"});

        // Comparison in OR inside filter
        ValidateJsonExists("$.a ? ((@.b < 5) || (@.c > 10))", {"\2a\2b", "\2a\2c"});
        ValidateJsonExists("$.a ? ((@.b != 1) || (@.c != 2))", {"\2a\2b", "\2a\2c"});

        // Mixing AND and OR inside filter: OR wins
        ValidateJsonExists("$.a ? ((@.b < 5) && ((@.c > 1) || (@.d > 2)))", {"\2a\2b", "\2a\2c", "\2a\2d"});
        ValidateJsonExists("$.a ? (((@.b < 5) || (@.c > 1)) && @.d > 2)", {"\2a\2b", "\2a\2c", "\2a\2d"});

        // Nested predicate in filter operand is blocked (EMode::Predicate on operand)
        ValidateError("$.a ? (($.b == 1) < 5)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (exists(@.b) < 5)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\") != true)", predError, {}, {}, ECallableType::JsonExists);

        // is unknown wrapping comparison inside filter - blocked
        ValidateError("$.a ? ((@.b < 5) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b >= +0) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b != 1) is unknown)", predError, {}, {}, ECallableType::JsonExists);

        // Arithmetic expression as comparison operand in filter
        ValidateJsonExists("$.key ? (@.a + @.b < +5)", {"\4key\2a", "\4key\2b"});
        ValidateJsonExists("$.key ? (@.a * 2 != 0)", {"\4key\2a"});

        // JsonValue also supports comparison in filter
        ValidateJsonValue("$.a ? (@.b < -10)", {"\2a\2b"});
        ValidateJsonValue("$.a ? (@.b != null && @.c >= 1)", {"\2a\2b", "\2a\2c"});
    }

    // Comparison operators (path vs path produce And mode) combined with && and ||
    Y_UNIT_TEST(CollectPath_ComparisonWithBooleanOps) {
        // Two-path comparison (And mode) in AND chain: compatible, stays And
        ValidateJsonValue("($.a < $.b) && ($.c > $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a <= $.b) && ($.c >= $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a != $.b) && ($.c == $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a == $.b) && ($.c < $.d) && ($.e > $.f)", {"\2a", "\2b", "\2c", "\2d", "\2e", "\2f"},
            TCollectResult::ETokensMode::And);

        // Two-path comparison (And mode) in OR: OR wins
        ValidateJsonValue("($.a < $.b) || ($.c > $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a <= $.b) || ($.c >= $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a != $.b) || ($.c == $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a < $.b) || ($.c > $.d) || ($.e != $.f)", {"\2a", "\2b", "\2c", "\2d", "\2e", "\2f"},
            TCollectResult::ETokensMode::Or);

        // Single-path comparison (NotSet mode) in AND chain
        ValidateJsonValue("($.a < 5) && ($.b > 3) && ($.c <= 10)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a > 0) && ($.b >= 1) && ($.c != 0) && ($.d == 2)", {"\2a", "\2b", "\2c", "\2d" + numSuffix(2)},
            TCollectResult::ETokensMode::And);

        // Single-path comparison in OR chain
        ValidateJsonValue("($.a < 5) || ($.b > 3) || ($.c <= 10)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a > 0) || ($.b >= 1) || ($.c != 0) || ($.d == 2)", {"\2a", "\2b", "\2c", "\2d" + numSuffix(2)}, 
            TCollectResult::ETokensMode::Or);

        // Mix of single-path and two-path comparisons in AND: compatible (neither has Or)
        ValidateJsonValue("($.a < 5) && ($.b > $.c)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a < $.b) && ($.c > 5)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a < 5) && ($.b > $.c) && ($.d == 1)", {"\2a", "\2b", "\2c", "\2d" + numSuffix(1)}, 
            TCollectResult::ETokensMode::And);

        // Mix of single-path and two-path comparisons in OR: OR wins
        ValidateJsonValue("($.a < 5) || ($.b > $.c)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a < $.b) || ($.c > 5)", {"\2a", "\2b", "\2c"}, TCollectResult::ETokensMode::Or);

        // AND chain then OR: OR wins
        ValidateJsonValue("($.a < $.b) && ($.c > 1) || ($.d != 0)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("($.a < 5) && ($.b > $.c) || ($.d == 1)", {"\2a", "\2b", "\2c", "\2d" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);

        // Two-path equality combined via AND and OR
        ValidateJsonValue("($.a == $.b) && ($.c == $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("($.a == $.b) || ($.c == $.d)", {"\2a", "\2b", "\2c", "\2d"}, TCollectResult::ETokensMode::Or);

        // Filter: two-path comparison combined with AND/OR
        ValidateJsonExists("$.key ? (@.a < @.b && @.c > 1)", {"\4key\2a", "\4key\2b", "\4key\2c"}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("$.key ? (@.a < @.b || @.c > 1)", {"\4key\2a", "\4key\2b", "\4key\2c"}, TCollectResult::ETokensMode::Or);
        ValidateJsonExists("$.key ? (@.a < @.b && @.c > @.d)", {"\4key\2a", "\4key\2b", "\4key\2c", "\4key\2d"}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("$.key ? (@.a < @.b || @.c > @.d)", {"\4key\2a", "\4key\2b", "\4key\2c", "\4key\2d"}, TCollectResult::ETokensMode::Or);

        // Filter: AND chain with OR - OR wins
        ValidateJsonExists("$.key ? (@.a < @.b && @.c > @.d || @.e == 1)", {"\4key\2a", "\4key\2b", "\4key\2c", "\4key\2d", "\4key\2e" + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
    }

    Y_UNIT_TEST(CollectPath_BinaryAnd) {
        // Basic equality on both sides, all literal types
        ValidateJsonValue("($.a == 10) && ($.b == \"hello\")", {"\2a" + numSuffix(10), "\2b" + strSuffix("hello")});
        ValidateJsonValue("(42 == $.key) && ($.val == true)", {"\4key" + numSuffix(42), "\4val" + boolTrueSuffix});
        ValidateJsonValue("($.x == null) && ($.y == false)", {"\2x" + nullSuffix, "\2y" + boolFalseSuffix});
        ValidateJsonValue("($.a == 0) && ($.b == 3.14)", {"\2a" + numSuffix(0), "\2b" + numSuffix(3.14)});
        ValidateJsonValue("(\"hello\" == $.a) && (null == $.b)", {"\2a" + strSuffix("hello"), "\2b" + nullSuffix});

        // Deeper member access paths
        ValidateJsonValue("($.a.b.c == 1) && ($.x.y == \"z\")", {"\2a\2b\2c" + numSuffix(1), "\2x\2y" + strSuffix("z")});
        ValidateJsonValue("($.aba.\"caba\" == true) && ($.d.e.f == 0)", {"\4aba\5caba" + boolTrueSuffix, "\2d\2e\2f" + numSuffix(0)});
        ValidateJsonValue("($.a.b.c.d == 0) && ($.p.q == null)", {"\2a\2b\2c\2d" + numSuffix(0), "\2p\2q" + nullSuffix});

        // Context object as path (empty prefix)
        ValidateJsonValue("($ == \"root\") && ($.b == 2)", {strSuffix("root"), "\2b" + numSuffix(2)});
        ValidateJsonValue("($ == null) && ($ == 42)", {nullSuffix, numSuffix(42)});

        // Array access
        ValidateJsonValue("($.key[0] == 1) && ($.arr[last] == true)", {"\4key" + numSuffix(1), "\4arr" + boolTrueSuffix});
        ValidateJsonValue("($.key[1, 2, 3] == null) && ($.b[0 to last] == \"x\")", {"\4key" + nullSuffix, "\2b" + strSuffix("x")});
        ValidateJsonValue("($.a.b[0].c == \"x\") && ($.d.e == false)", {"\2a\2b\2c" + strSuffix("x"), "\2d\2e" + boolFalseSuffix});
        ValidateJsonValue("($.key[0].sub == \"x\") && ($.v == 1)", {"\4key\4sub" + strSuffix("x"), "\2v" + numSuffix(1)});

        // Wildcard member access (finishes collection, no literal suffix appended)
        ValidateJsonValue("($.a.* == \"x\") && ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.* == \"x\") && ($.b == 2)", {"\2b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) && ($.b.* == \"z\")", {"\2a" + numSuffix(1), "\2b"});
        ValidateJsonValue("($.a.b.* == true) && ($.c.* == null)", {"\2a\2b", "\2c"});

        // Wildcard array access
        ValidateJsonValue("($.key[*] == \"x\") && ($.b == 2)", {"\4key" + strSuffix("x"), "\2b" + numSuffix(2)});

        // Methods (finish the path, no literal suffix appended)
        ValidateJsonValue("($.key.size() == 3) && ($.val == 1)", {"\4key", "\4val" + numSuffix(1)});
        ValidateJsonValue("($.a == \"x\") && ($.key.abs() == 2)", {"\2a" + strSuffix("x"), "\4key"});
        ValidateJsonValue("($.a.floor() == 0) && ($.b.type() == \"number\")", {"\2a", "\2b"});
        ValidateJsonValue("($.key.keyvalue().name == \"x\") && ($.v == true)", {"\4key", "\2v" + boolTrueSuffix});
        ValidateJsonValue("($.a.size() == 5) && ($.b.ceiling() == 3)", {"\2a", "\2b"});

        // StartsWith predicate on left and right
        ValidateJsonValue("($.a starts with \"x\") && ($.b == 1)", {"\2a", "\2b" + numSuffix(1)});
        ValidateJsonValue("($.a == 1) && ($.b starts with \"y\")", {"\2a" + numSuffix(1), "\2b"});
        ValidateJsonValue("($.a.b.c starts with \"abc\") && ($.d[0] == null)", {"\2a\2b\2c", "\2d" + nullSuffix});
        ValidateJsonValue("($.a starts with \"x\") && ($.b starts with \"y\")", {"\2a", "\2b"});
        ValidateJsonValue("($.a.* starts with \"x\") && ($.b == 1)", {"\2a", "\2b" + numSuffix(1)});

        // LikeRegex predicate on left and right
        ValidateJsonValue("($.a like_regex \".*\") && ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.a == \"x\") && ($.b like_regex \"[a-z]+\")", {"\2a" + strSuffix("x"), "\2b"});
        ValidateJsonValue("($.a like_regex \"x.*\") && ($.b like_regex \"y.*\")", {"\2a", "\2b"});

        // Exists predicate on left and right
        ValidateJsonValue("exists($.a) && ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) && exists($.b.c)", {"\2a" + numSuffix(1), "\2b\2c"});
        ValidateJsonValue("exists($.a.b[0]) && exists($.c.*)", {"\2a\2b", "\2c"});
        ValidateJsonValue("exists($.a.key.size()) && ($.b == true)", {"\2a\4key", "\2b" + boolTrueSuffix});

        // Unary arithmetic operand (finishes, no literal suffix)
        ValidateJsonValue("(-$.a == 1) && ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) && (+$.b.c == 0)", {"\2a" + numSuffix(1), "\2b\2c"});
        ValidateJsonValue("(-$.a.b.* == 1) && ($.c == 2)", {"\2a\2b", "\2c" + numSuffix(2)});

        // Binary arithmetic with two paths as AND operand (two path tokens, And mode, compatible with AND)
        // $.a + $.b == "x" is parsed as ($.a + $.b) == "x", arithmetic finishes, literal not appended
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
        ValidateJsonValue("($.c == 1) && ($.a + $.b == \"x\")", {"\2c" + numSuffix(1), "\2a", "\2b"});
        ValidateJsonValue("($.a.size() + $.b.abs() == 5) && ($.c == null)", {"\2a", "\2b", "\2c" + nullSuffix});

        // Chained AND (left-associative)
        ValidateJsonValue("($.a == 1) && ($.b == 2) && ($.c == 3)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) && ($.b == 2) && ($.c == 3) && ($.d == 4)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        ValidateJsonValue("($.a starts with \"x\") && ($.b == 1) && exists($.c.d)", {"\2a", "\2b" + numSuffix(1), "\2c\2d"});
        ValidateJsonValue("($.a like_regex \".*\") && ($.b.* == 2) && ($.c.size() == 3) && exists($.d)", {"\2a", "\2b", "\2c", "\2d"});

        // Same path on both sides (two different equality conditions)
        ValidateJsonValue("($.a == 1) && ($.a == 2)", {"\2a" + numSuffix(1), "\2a" + numSuffix(2)});

        // Variables with literals
        ValidateError("($var == 1) && ($.b == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a == 1) && ($var == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($var == 1) && ($var == 2)", compError, {}, {}, ECallableType::JsonValue);

        // Predicates are not allowed in JsonExists
        ValidateError("($.a == 10) && ($.b == 20)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("($.a starts with \"x\") && ($.b == 1)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("exists($.a) && exists($.b)", predError, {}, {}, ECallableType::JsonExists);

        // Mixing AND and OR: OR wins, all tokens become OR
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) || ($.c == 3)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) || (($.b == 2) && ($.c == 3))", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) && (($.b == 2) || ($.c == 3))", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) && ($.c == 3)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});

        // Nested predicates: AND appears in predicate position (inside exists / is unknown / == literal)
        // BinaryAnd inherits EMode::Predicate and its operands (==, starts with, like_regex, exists) are blocked
        ValidateError("exists(($.a == 1) && ($.b == 2))", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists(($.a starts with \"x\") && ($.b like_regex \"y\"))", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists(exists($.a) && ($.b == 2))", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 1) && ($.b == 2)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 1) && ($.b == 2)) == true", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("false == (($.a == 1) && ($.b == 2))", predError, {}, {}, ECallableType::JsonValue);
    }

    Y_UNIT_TEST(CollectPath_BinaryOr) {
        // Basic equality on both sides, all literal types
        ValidateJsonValue("($.a == 10) || ($.b == \"hello\")", {"\2a" + numSuffix(10), "\2b" + strSuffix("hello")});
        ValidateJsonValue("(42 == $.key) || ($.val == true)", {"\4key" + numSuffix(42), "\4val" + boolTrueSuffix});
        ValidateJsonValue("($.x == null) || ($.y == false)", {"\2x" + nullSuffix, "\2y" + boolFalseSuffix});
        ValidateJsonValue("($.a == 0) || ($.b == 3.14)", {"\2a" + numSuffix(0), "\2b" + numSuffix(3.14)});
        ValidateJsonValue("($ == \"root\") || ($.b == 2)", {strSuffix("root"), "\2b" + numSuffix(2)});

        // Deeper member access paths
        ValidateJsonValue("($.a.b.c == 1) || ($.x.y == \"z\")", {"\2a\2b\2c" + numSuffix(1), "\2x\2y" + strSuffix("z")});
        ValidateJsonValue("($.aba.\"caba\" == true) || ($.d.e.f == 0)", {"\4aba\5caba" + boolTrueSuffix, "\2d\2e\2f" + numSuffix(0)});

        // Array access
        ValidateJsonValue("($.key[0] == 1) || ($.arr[last] == true)", {"\4key" + numSuffix(1), "\4arr" + boolTrueSuffix});
        ValidateJsonValue("($.key[1, 2, 3] == null) || ($.b[0 to last] == \"x\")", {"\4key" + nullSuffix, "\2b" + strSuffix("x")});
        ValidateJsonValue("($.a.b[0].c == \"x\") || ($.d.e == false)", {"\2a\2b\2c" + strSuffix("x"), "\2d\2e" + boolFalseSuffix});

        // Wildcard member and array access (finishes, no literal suffix)
        ValidateJsonValue("($.a.* == \"x\") || ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.* == \"x\") || ($.b == 2)", {""});
        ValidateJsonValue("($.a == 1) || ($.b.* == \"z\")", {"\2a" + numSuffix(1), "\2b"});
        ValidateJsonValue("($.key[*] == \"x\") || ($.b == 2)", {"\4key" + strSuffix("x"), "\2b" + numSuffix(2)});

        // Methods (finish the path, no literal suffix)
        ValidateJsonValue("($.key.size() == 3) || ($.val == 1)", {"\4key", "\4val" + numSuffix(1)});
        ValidateJsonValue("($.a == \"x\") || ($.key.abs() == 2)", {"\2a" + strSuffix("x"), "\4key"});
        ValidateJsonValue("($.a.floor() == 0) || ($.b.type() == \"number\")", {"\2a", "\2b"});
        ValidateJsonValue("($.key.keyvalue().name == \"x\") || ($.v == true)", {"\4key", "\2v" + boolTrueSuffix});

        // StartsWith predicate
        ValidateJsonValue("($.a starts with \"x\") || ($.b == 1)", {"\2a", "\2b" + numSuffix(1)});
        ValidateJsonValue("($.a == 1) || ($.b starts with \"y\")", {"\2a" + numSuffix(1), "\2b"});
        ValidateJsonValue("($.a starts with \"x\") || ($.b starts with \"y\")", {"\2a", "\2b"});
        ValidateJsonValue("($.a.* starts with \"x\") || ($.b[0] == 1)", {"\2a", "\2b" + numSuffix(1)});

        // LikeRegex predicate
        ValidateJsonValue("($.a like_regex \".*\") || ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.a == \"x\") || ($.b like_regex \"[a-z]+\")", {"\2a" + strSuffix("x"), "\2b"});
        ValidateJsonValue("($.a like_regex \"x.*\") || ($.b like_regex \"y.*\")", {"\2a", "\2b"});

        // Exists predicate
        ValidateJsonValue("exists($.a) || ($.b == 2)", {"\2a", "\2b" + numSuffix(2)});
        ValidateJsonValue("($.a == 1) || exists($.b.c)", {"\2a" + numSuffix(1), "\2b\2c"});
        ValidateJsonValue("exists($.a.b[0]) || exists($.c.*)", {"\2a\2b", "\2c"});

        // Same path on both sides, different values
        ValidateJsonValue("($.a == 1) || ($.a == 2)", {"\2a" + numSuffix(1), "\2a" + numSuffix(2)});
        ValidateJsonValue("($.key == \"a\") || ($.key == \"b\") || ($.key == \"c\")",
            {"\4key" + strSuffix("a"), "\4key" + strSuffix("b"), "\4key" + strSuffix("c")});

        // Chained OR (left-associative)
        ValidateJsonValue("($.a == 1) || ($.b == 2) || ($.c == 3)",{"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) || ($.b == 2) || ($.c == 3) || ($.d == 4)",{"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        ValidateJsonValue("($.a starts with \"x\") || ($.b == 1) || exists($.c.d)",{"\2a", "\2b" + numSuffix(1), "\2c\2d"});

        // Variable on left or right side
        ValidateError("($var == 1) || ($.b == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a == 1) || ($var == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($var == 1) || ($var == 2)", compError, {}, {}, ECallableType::JsonValue);

        // Predicates not allowed in Context for JsonExists
        ValidateError("($.a == 10) || ($.b == 20)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("($.a starts with \"x\") || ($.b == 1)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("exists($.a) || exists($.b)", predError, {}, {}, ECallableType::JsonExists);

        // Arithmetic with multiple paths (And mode) mixed with OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
        ValidateJsonValue("($.c == 1) || ($.a + $.b == \"x\")", {"\2c" + numSuffix(1), "\2a", "\2b"});
        ValidateJsonValue("($.a.size() + $.b.abs() == 5) || ($.c == null)", {"\2a", "\2b", "\2c" + nullSuffix});

        // Mixing AND and OR: OR wins
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) || ($.c == 3)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) || (($.b == 2) && ($.c == 3))", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("($.a == 1) && (($.b == 2) || ($.c == 3))", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) && ($.c == 3)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3)});

        // Nested predicates: OR appears in predicate position (inside exists / is unknown / == literal)
        ValidateError("exists(($.a == 1) || ($.b == 2))", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists(($.a starts with \"x\") || ($.b like_regex \"y\"))", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists(exists($.a) || ($.b == 2))", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 1) || ($.b == 2)) is unknown", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("(($.a == 1) || ($.b == 2)) == true", predError, {}, {}, ECallableType::JsonValue);
        ValidateError("false == (($.a == 1) || ($.b == 2))", predError, {}, {}, ECallableType::JsonValue);
    }

    // Verifies that TokensMode (And/Or) propagates correctly through nesting,
    // and that mix errors are detected regardless of nesting depth or structure
    Y_UNIT_TEST(CollectPath_ModePropagation) {
        // ((A && B) && (C && D)) - And+And combined at top level
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) && (($.c == 3) && ($.d == 4))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // ((A || B) || (C || D)) - Or+Or combined at top level
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) || (($.c == 3) || ($.d == 4))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // A && (B && (C && D))
        ValidateJsonValue("($.a == 1) && (($.b == 2) && (($.c == 3) && ($.d == 4)))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // A || (B || (C || D))
        ValidateJsonValue("($.a == 1) || (($.b == 2) || (($.c == 3) || ($.d == 4)))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});

        // Arithmetic with two paths (mode=And) is compatible with AND chains
        // Two arithmetic operands inside AND: ($.a+$.b == "x") && ($.c+$.d == "y")
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c + $.d == \"y\")",
            {"\2a", "\2b", "\2c", "\2d"});
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c + $.d == \"y\") && ($.e == 1)",
            {"\2a", "\2b", "\2c", "\2d", "\2e" + numSuffix(1)});

        // (A && B) || (C && D): OR wins, all tokens become OR
        ValidateJsonValue("(($.a == 1) && ($.b == 2)) || (($.c == 3) && ($.d == 4))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // (A || B) && (C || D): OR wins, all tokens become OR
        ValidateJsonValue("(($.a == 1) || ($.b == 2)) && (($.c == 3) || ($.d == 4))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});

        // A && (B || (C || D)): OR wins
        ValidateJsonValue("($.a == 1) && (($.b == 2) || (($.c == 3) || ($.d == 4)))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // A && (B && (C || D)): OR wins
        ValidateJsonValue("($.a == 1) && (($.b == 2) && (($.c == 3) || ($.d == 4)))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});

        // A || (B && (C && D)): OR wins
        ValidateJsonValue("($.a == 1) || (($.b == 2) && (($.c == 3) && ($.d == 4)))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // A || (B || (C && D)): OR wins
        ValidateJsonValue("($.a == 1) || (($.b == 2) || (($.c == 3) && ($.d == 4)))",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});

        // && binds tighter than ||
        // A && B && C || D  =>  ((A && B) && C) || D: OR wins
        ValidateJsonValue("($.a == 1) && ($.b == 2) && ($.c == 3) || ($.d == 4)",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // A || B || C && D  =>  (A || B) || (C && D): OR wins
        ValidateJsonValue("($.a == 1) || ($.b == 2) || ($.c == 3) && ($.d == 4)",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});
        // A || B && C || D  =>  A || (B && C) || D  =>  (A || (B && C)) || D: OR wins
        ValidateJsonValue("($.a == 1) || ($.b == 2) && ($.c == 3) || ($.d == 4)",
            {"\2a" + numSuffix(1), "\2b" + numSuffix(2), "\2c" + numSuffix(3), "\2d" + numSuffix(4)});

        // ($.a + $.b == "x") has mode=And, nested as part of OR: OR wins
        ValidateJsonValue("(($.a + $.b == \"x\") || ($.c == 1)) && ($.d == 2)",
            {"\2a", "\2b", "\2c" + numSuffix(1), "\2d" + numSuffix(2)});
        ValidateJsonValue("($.d == 2) && (($.a + $.b == \"x\") || ($.c == 1))",
            {"\2d" + numSuffix(2), "\2a", "\2b", "\2c" + numSuffix(1)});

        // ($.a[0].b == 1) && ((-$.c.d == 2) && ($.e.* starts with "x"))
        // -$.c.d == 2: unary makes path Finished, no literal appended, token "\2c\2d"
        // $.e.* starts with "x": wildcard makes path Finished, token "\2e"
        ValidateJsonValue("($.a[0].b == 1) && ((-$.c.d == 2) && ($.e.* starts with \"x\"))",
            {"\2a\2b" + numSuffix(1), "\2c\2d", "\2e"});

        // ($.a.b.c starts with "x") && ($.d.size() == 3) && ($.e[0] + $.f.abs() == 5)
        // $.e[0] + $.f.abs(): both sub-paths collected, mode=And, arithmetic Finished, no literal appended
        ValidateJsonValue("($.a.b.c starts with \"x\") && ($.d.size() == 3) && ($.e[0] + $.f.abs() == 5)",
            {"\2a\2b\2c", "\2d", "\2e", "\2f"});

        // (exists($.a.b[0]) && ($.c.d == "x")) && (($.e like_regex ".*") && (+$.f.g.h == 0))
        // +$.f.g.h == 0: unary makes path Finished, no literal appended, token "\2f\2g\2h"
        ValidateJsonValue("(exists($.a.b[0]) && ($.c.d == \"x\")) && (($.e like_regex \".*\") && (+$.f.g.h == 0))",
            {"\2a\2b", "\2c\2d" + strSuffix("x"), "\2e", "\2f\2g\2h"});

        // All five binary arithmetic ops as AND operands - each produces mode=And, all compatible
        ValidateJsonValue("($.a + $.b == \"x\") && ($.c - $.d == \"y\") && ($.e * $.f == \"z\")",
            {"\2a", "\2b", "\2c", "\2d", "\2e", "\2f"});
        ValidateJsonValue("($.a / $.b == \"x\") && ($.c % $.d == \"y\") && ($.e == 1)",
            {"\2a", "\2b", "\2c", "\2d", "\2e" + numSuffix(1)});

        // Unary on both sides of AND - each makes path Finished, literal not appended
        ValidateJsonValue("(-$.a.b == 1) && (+$.c.d.e == 2) && (-$.f[0] == 3)",
            {"\2a\2b", "\2c\2d\2e", "\2f"});

        // ($.a.size() == 3) || (($.b[0] == "x") || (-$.c.d.e == 1))
        // Right inner OR: two NotSet operands, Or, outer OR: left=NotSet, right=Or, Or
        ValidateJsonValue("($.a.size() == 3) || (($.b[0] == \"x\") || (-$.c.d.e == 1))",
            {"\2a", "\2b" + strSuffix("x"), "\2c\2d\2e"});

        // ($.a starts with "x") || ($.b.* == "y") || exists($.c.d[0])
        // $.b.* == "y": wildcard Finished, no literal, token "\2b"
        ValidateJsonValue("($.a starts with \"x\") || ($.b.* == \"y\") || exists($.c.d[0])",
            {"\2a", "\2b", "\2c\2d"});

        // ($.a[1 to 3].b like_regex ".*") || ((-$.c == 0) || ($.d.e.keyvalue() == "f"))
        // $.d.e.keyvalue() == "f": method Finished, no literal, token "\2d\2e"
        ValidateJsonValue("($.a[1 to 3].b like_regex \".*\") || ((-$.c == 0) || ($.d.e.keyvalue() == \"f\"))",
            {"\2a\2b", "\2c", "\2d\2e"});

        // Unary on both sides of OR
        ValidateJsonValue("(-$.a.b == 1) || (+$.c.d == 2) || (-$.e.f.g == 3)",
            {"\2a\2b", "\2c\2d", "\2e\2f\2g"});

        // ((-$.a.b == 1) && ($.c.size() == 2)) || (exists($.d) && ($.e starts with "x")): OR wins
        ValidateJsonValue("((-$.a.b == 1) && ($.c.size() == 2)) || (exists($.d) && ($.e starts with \"x\"))",
            {"\2a\2b", "\2c", "\2d", "\2e"});

        // ($.a like_regex "x.*") || (($.b.abs() == 1) && ($.c[0] starts with "y")): OR wins
        ValidateJsonValue("($.a like_regex \"x.*\") || ($.b.abs() == 1) && ($.c[0] starts with \"y\")",
            {"\2a", "\2b", "\2c"});

        // (($.a + $.b == "x") && (-$.c.d == 1)) || ($.e.f == 2): OR wins
        ValidateJsonValue("($.a + $.b == \"x\") && (-$.c.d == 1) || ($.e.f == 2)",
            {"\2a", "\2b", "\2c\2d", "\2e\2f" + numSuffix(2)});

        // ($.a[0] starts with "x") || (($.b.c + $.d.e == 3) && exists($.f.g.*)): OR wins
        ValidateJsonValue("($.a[0] starts with \"x\") || ($.b.c + $.d.e == 3) && exists($.f.g.*)",
            {"\2a", "\2b\2c", "\2d\2e", "\2f\2g"});

        // (($.a.b[0].c == 1) && ($.d.* starts with "x")) || (-$.e.f.g == 2): OR wins
        ValidateJsonValue("($.a.b[0].c == 1) && ($.d.* starts with \"x\") || (-$.e.f.g == 2)",
            {"\2a\2b\2c" + numSuffix(1), "\2d", "\2e\2f\2g"});

        // (($.a like_regex ".*") && ($.b.size() == 0) && ($.c[last] == true)) || ($.d.floor() == 0): OR wins
        ValidateJsonValue("($.a like_regex \".*\") && ($.b.size() == 0) && ($.c[last] == true) || ($.d.floor() == 0)",
            {"\2a", "\2b", "\2c" + boolTrueSuffix, "\2d"});

        // ($.a == 1) || ((-$.b.c.d == 2) && ($.e.size() == 3)): OR wins
        ValidateJsonValue("($.a == 1) || ((-$.b.c.d == 2) && ($.e.size() == 3))",
            {"\2a" + numSuffix(1), "\2b\2c\2d", "\2e"});

        // All five arithmetic ops with two paths inside OR: OR wins
        ValidateJsonValue("($.a + $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
        ValidateJsonValue("($.a - $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
        ValidateJsonValue("($.a * $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
        ValidateJsonValue("($.a / $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
        ValidateJsonValue("($.a % $.b == \"x\") || ($.c == 1)", {"\2a", "\2b", "\2c" + numSuffix(1)});
    }

    // Filter predicates allow the collector to use predicate constraints for path narrowing
    // $.a ? (@.b == 10)  =>  ["\2a\2b" + numSuffix(10)]
    Y_UNIT_TEST(CollectPath_FilterPredicate) {
        // Basic: simple path before ?, simple equality predicate
        ValidateJsonExists("$.a ? (@.b == 10)", {"\2a\2b" + numSuffix(10)});
        ValidateJsonExists("$.a ? (@.b == -(+10))", {"\2a\2b" + numSuffix(-10)});
        ValidateJsonExists("$.a ? (@.b == \"hello\")", {"\2a\2b" + strSuffix("hello")});
        ValidateJsonExists("$.a ? (@.b == true)", {"\2a\2b" + boolTrueSuffix});
        ValidateJsonExists("$.a ? (@.b == false)", {"\2a\2b" + boolFalseSuffix});
        ValidateJsonExists("$.a ? (@.b == null)", {"\2a\2b" + nullSuffix});
        ValidateJsonExists("$.a ? (-10 == @.b)", {"\2a\2b" + numSuffix(-10)});
        ValidateJsonExists("$.a ? (\"hello\" == @.b)", {"\2a\2b" + strSuffix("hello")});
        ValidateJsonExists("$ ? (@.a == 1)", {"\2a" + numSuffix(1)});
        ValidateJsonExists("$ ? (@.key == \"x\")", {"\4key" + strSuffix("x")});

        // @ == literal: equality on the filter object itself, prefix becomes the full token
        ValidateJsonExists("$.a ? (@ == \"hello\")", {"\2a" + strSuffix("hello")});
        ValidateJsonExists("$.a ? (@ == -42)", {"\2a" + numSuffix(-42)});
        ValidateJsonExists("$.a ? (@ == true)", {"\2a" + boolTrueSuffix});
        ValidateJsonExists("$.a ? (@ == null)", {"\2a" + nullSuffix});
        ValidateJsonExists("$.a.b ? (@ == \"x\")", {"\2a\2b" + strSuffix("x")});
        ValidateJsonExists("$ ? (@ == 0)", {numSuffix(0)});

        // @ starts with / like_regex: predicate on the filter object itself
        ValidateJsonExists("$.a ? (@ starts with \"x\")", {"\2a"});
        ValidateJsonExists("$.a.b ? (@ starts with \"hello\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@ like_regex \"[a-z]+\")", {"\2a"});
        ValidateJsonExists("$.a.b.c ? (@ like_regex \".*\")", {"\2a\2b\2c"});

        // exists(@): filter predicate is exists on the filter object
        ValidateJsonExists("$.a ? (exists(@))", {"\2a"});
        ValidateJsonExists("$.a.b ? (exists(@))", {"\2a\2b"});

        // @[0].b: array subscript on filter object, then member access
        ValidateJsonExists("$.a ? (@[0].b == 1)", {"\2a\2b" + numSuffix(1)});
        ValidateJsonExists("$.a ? (@[last].b == \"x\")", {"\2a\2b" + strSuffix("x")});
        ValidateJsonExists("$.a ? (@[*].b == true)", {"\2a\2b" + boolTrueSuffix});

        // FilterObject via wildcard member access, finishes the path, so literal is not appended
        ValidateJsonExists("$.a ? (@.* == \"x\")", {"\2a"});
        ValidateJsonExists("$.a ? (@.b.* starts with \"x\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.* == 1)", {"\2a\2b"});

        // Methods finish the path, so literal is not appended
        ValidateJsonExists("$.a ? (@.b.size() == 3)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.abs() == 1)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.floor() == 0)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.ceiling() == 5)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.type() == \"number\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.double() == 1)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.keyvalue() == \"x\")", {"\2a\2b"});
        // method result checked in AND: both paths collected
        ValidateJsonExists("$.a ? (@.b.size() == +3 && @.c == -1)", {"\2a\2b", "\2a\2c" + numSuffix(-1)});

        // Unary finishes the path, so literal is not appended
        ValidateJsonExists("$.a ? (-@.b == 5)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (+@.b == 5)", {"\2a\2b"});
        ValidateJsonExists("$.a ? (-@.b.c.d == 0)", {"\2a\2b\2c\2d"});
        ValidateJsonExists("$.a ? (-@.b == 5 && @.c == 1)", {"\2a\2b", "\2a\2c" + numSuffix(1)});
        ValidateJsonExists("$.a ? (-@.b == 5 || +@.c == 2)", {"\2a\2b", "\2a\2c"});

        // All five arithmetic operators: path + path, both tokens, no literal suffix
        ValidateJsonExists("$.key ? (@.a + @.b == +5)", {"\4key\2a", "\4key\2b"});
        ValidateJsonExists("$.key ? (@.a - @.b == 0)", {"\4key\2a", "\4key\2b"});
        ValidateJsonExists("$.key ? (@.a * @.b == 10)", {"\4key\2a", "\4key\2b"});
        ValidateJsonExists("$.key ? (@.a / @.b == 2)", {"\4key\2a", "\4key\2b"});
        ValidateJsonExists("$.key ? (@.a % @.b == 1)", {"\4key\2a", "\4key\2b"});
        // path + literal: literal side dropped by CollectArithmeticOperand, only path token
        ValidateJsonExists("$.key ? (@.a + 1 == 5)", {"\4key\2a"});
        ValidateJsonExists("$.key ? (1 - @.a == 5)", {"\4key\2a"});
        ValidateJsonExists("$.key ? (@.a * (-2) == -10)", {"\4key\2a"});
        // three paths via chained arithmetic
        ValidateJsonExists("$.key ? (@.a + @.b + @.c == 0)", {"\4key\2a", "\4key\2b", "\4key\2c"});
        // arithmetic with deeper filter object paths
        ValidateJsonExists("$.x ? (@.a.b + @.c.d == 0)", {"\2x\2a\2b", "\2x\2c\2d"});
        // arithmetic with two paths produces mode=And, compatible with AND
        ValidateJsonExists("$.key ? (@.a + @.b == 5 && @.c == 1)", {"\4key\2a", "\4key\2b", "\4key\2c" + numSuffix(1)});

        // StartsWith finishes the path
        ValidateJsonExists("$.a ? (@.b starts with \"x\")", {"\2a\2b"});
        ValidateJsonExists("$.a.b.c ? (@.d starts with \"abc\")", {"\2a\2b\2c\2d"});
        ValidateJsonValue("$.a ? (@.b starts with \"x\")", {"\2a\2b"});

        // LikeRegex finishes the path
        ValidateJsonExists("$.a ? (@.b like_regex \".*\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b.c like_regex \"[0-9]+\")", {"\2a\2b\2c"});
        ValidateJsonValue("$.a ? (@.b like_regex \"[a-z]+\")", {"\2a\2b"});

        // Exists finishes the path
        ValidateJsonExists("$.a ? (exists(@.b))", {"\2a\2b"});
        ValidateJsonExists("$.a ? (exists(@.b.c.d))", {"\2a\2b\2c\2d"});
        ValidateJsonExists("$.a ? (exists(@.b[0]))", {"\2a\2b"});
        ValidateJsonValue("$.a ? (exists(@.b))", {"\2a\2b"});

        // Deeper paths before and inside filter
        ValidateJsonExists("$.a.b ? (@.c == \"x\")", {"\2a\2b\2c" + strSuffix("x")});
        ValidateJsonExists("$.a ? (@.b.c == true)", {"\2a\2b\2c" + boolTrueSuffix});
        ValidateJsonExists("$.a.b.c ? (@.d.e == null)", {"\2a\2b\2c\2d\2e" + nullSuffix});
        ValidateJsonExists("$.a ? (@.b.c.d == 3.14)", {"\2a\2b\2c\2d" + numSuffix(3.14)});

        // Array access in the input path
        ValidateJsonExists("$.a[0] ? (@.b == 1)", {"\2a\2b" + numSuffix(1)});
        ValidateJsonExists("$.key[1, 2, 3] ? (@.sub == \"x\")", {"\4key\4sub" + strSuffix("x")});
        ValidateJsonExists("$.a[0 to last] ? (@.b == true)", {"\2a\2b" + boolTrueSuffix});


        // AND: Two equality conditions
        ValidateJsonExists("$.a ? (@.b == +10 && @.c == +13)", {"\2a\2b" + numSuffix(10), "\2a\2c" + numSuffix(13)});
        ValidateJsonExists("$.a.b ? (@.c == \"x\" && @.d == 1)", {"\2a\2b\2c" + strSuffix("x"), "\2a\2b\2d" + numSuffix(1)});
        ValidateJsonExists("$ ? (@.x == null && @.y == true)", {"\2x" + nullSuffix, "\2y" + boolTrueSuffix});

        // AND: Three conditions chained with AND
        ValidateJsonExists("$.key ? (@.a == 1 && @.b == -2 && @.c == 3)", {"\4key\2a" + numSuffix(1), "\4key\2b" + numSuffix(-2), "\4key\2c" + numSuffix(3)});

        // AND: Four conditions chained with AND (two pairs)
        ValidateJsonExists("$.a ? ((@.b == 1 && @.c == 2) && (@.d == 3 && @.e == 4))",
            {"\2a\2b" + numSuffix(1), "\2a\2c" + numSuffix(2),
             "\2a\2d" + numSuffix(3), "\2a\2e" + numSuffix(4)});

        // AND: mixing predicate types
        ValidateJsonExists("$.a ? ((@.b == 1) && (@.c starts with \"x\") && (@.d like_regex \"y.*\") && exists(@.e))",
            {"\2a\2b" + numSuffix(1), "\2a\2c", "\2a\2d", "\2a\2e"});

        // AND: mixing methods, unary, wildcard, equality
        ValidateJsonExists("$.key ? ((@.a == 1) && (@.b.size() == 3) && (-@.c == 0) && (@.d.* starts with \"x\"))",
            {"\4key\2a" + numSuffix(1), "\4key\2b", "\4key\2c", "\4key\2d"});

        // OR: Two equality conditions
        ValidateJsonExists("$.a ? ((@.b == 10) || (@.c == 13))", {"\2a\2b" + numSuffix(10), "\2a\2c" + numSuffix(13)});
        ValidateJsonExists("$.key ? ((@.x == \"a\") || (@.y == \"b\"))", {"\4key\2x" + strSuffix("a"), "\4key\2y" + strSuffix("b")});

        // OR: Three conditions chained with OR
        ValidateJsonExists("$.a ? ((@.b == 1) || (@.c == 2) || (@.d == 3))", {"\2a\2b" + numSuffix(1), "\2a\2c" + numSuffix(2), "\2a\2d" + numSuffix(3)});

        // OR: Four conditions chained with OR (two pairs)
        ValidateJsonExists("$.a ? ((@.b == 1 || @.c == 2) || (@.d == 3 || @.e == 4))",
            {"\2a\2b" + numSuffix(1), "\2a\2c" + numSuffix(2),
             "\2a\2d" + numSuffix(3), "\2a\2e" + numSuffix(4)});

        // OR: mixing predicate types
        ValidateJsonExists("$.a ? ((@.b == 1) || (@.c starts with \"x\") || (@.d like_regex \"y.*\") || exists(@.e))",
            {"\2a\2b" + numSuffix(1), "\2a\2c", "\2a\2d", "\2a\2e"});

        // AND on left of OR: OR wins
        ValidateJsonExists("$.a ? ((@.b == 1 && @.c == 2) || @.d == 3)", {"\2a\2b" + numSuffix(1), "\2a\2c" + numSuffix(2), "\2a\2d" + numSuffix(3)});
        // OR on right of AND: OR wins
        ValidateJsonExists("$.a ? (@.b == 1 && ((@.c == 2) || (@.d == 3)))", {"\2a\2b" + numSuffix(1), "\2a\2c" + numSuffix(2), "\2a\2d" + numSuffix(3)});
        // Arithmetic with two paths (mode=And) on left of OR: OR wins
        ValidateJsonExists("$.a ? ((@.b + @.c == 5) || @.d == 3)", {"\2a\2b", "\2a\2c", "\2a\2d" + numSuffix(3)});
        // Arithmetic with two paths (mode=And) on right of OR: OR wins
        ValidateJsonExists("$.a ? (@.b == 1 || (@.c + @.d == 5))", {"\2a\2b" + numSuffix(1), "\2a\2c", "\2a\2d"});
        // (A || B) inside AND chain: OR wins
        ValidateJsonExists("$.a ? (((@.b == 1) || (@.c == 2)) && @.d == 3)", {"\2a\2b" + numSuffix(1), "\2a\2c" + numSuffix(2), "\2a\2d" + numSuffix(3)});

        // Finished input path (wildcard/method) - filter predicate can't narrow
        ValidateJsonExists("$.* ? (@.b == 1)", {""});
        ValidateJsonExists("$.a.* ? (@.b == 1)", {"\2a"});
        ValidateJsonExists("$.a.b.* ? (@.c == \"x\")", {"\2a\2b"});

        // Filter is Finished - further member access is dropped
        ValidateJsonExists("$.a ? (@.b == 10) .c", {"\2a\2b" + numSuffix(10)});

        // JsonExists explicitly: filter allows all predicate types even though
        ValidateJsonExists("$.a ? (@.b == 10)", {"\2a\2b" + numSuffix(10)});
        ValidateJsonExists("$.a ? (@.b starts with \"x\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b like_regex \".*\")", {"\2a\2b"});
        ValidateJsonExists("$.a ? (exists(@.b))", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b == 10 && @.c starts with \"x\" && exists(@.d))", {"\2a\2b" + numSuffix(10), "\2a\2c", "\2a\2d"});
        ValidateJsonExists("$.a ? ((@.b == 10) || (@.c starts with \"x\") || exists(@.d))", {"\2a\2b" + numSuffix(10), "\2a\2c", "\2a\2d"});
        ValidateJsonExists("$.a ? (@.b + @.c == 5)", {"\2a\2b", "\2a\2c"});

        // JsonValue also works (filter allowed predicates in both callable types)
        ValidateJsonValue("$.a ? (@.b == 10)", {"\2a\2b" + numSuffix(10)});
        ValidateJsonValue("$.a ? (@.b == 10 && @.c == 13)", {"\2a\2b" + numSuffix(10), "\2a\2c" + numSuffix(13)});
        ValidateJsonValue("$.a ? ((@.b == 10) || (@.c == 13))", {"\2a\2b" + numSuffix(10), "\2a\2c" + numSuffix(13)});
        ValidateJsonValue("$.a ? (@.b + @.c == 5)", {"\2a\2b", "\2a\2c"});
        ValidateJsonValue("$.a ? (@.b == @.c)", {"\2a\2b", "\2a\2c"});
        ValidateJsonValue("$.a ? (@.b == $.c)", {"\2a\2b", "\2c"});
        ValidateJsonValue("$.a ? (@ == @.b)", {"\2a", "\2a\2b"});

        // Nested filter: exists(@.b ? (@.c == 1)) inside an outer filter
        ValidateJsonExists("$.a ? (exists(@.b ? (@.c == 1)))", {"\2a\2b\2c" + numSuffix(1)});
        ValidateJsonExists("$.key ? (exists(@.sub ? (@.val == \"x\")))", {"\4key\4sub\4val" + strSuffix("x")});

        // @ outside filter context is an error
        ValidateError("@", filterError);
        ValidateError("@.a", filterError);
        ValidateError("@.a == 1", filterError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists(@.a)", filterError, {}, {}, ECallableType::JsonValue);
        ValidateError("@ starts with \"x\"", filterError, {}, {}, ECallableType::JsonValue);

        // Both sides of == are paths: AND-merge of filter-relative paths
        ValidateJsonExists("$.a ? (@.b == @.c)", {"\2a\2b", "\2a\2c"});
        ValidateJsonExists("$.a ? (@.b == $.c)", {"\2a\2b", "\2c"});
        ValidateJsonExists("$.a ? (@ == @.b)", {"\2a", "\2a\2b"});
        // Both sides are literals
        ValidateError("$.a ? (1 == 2)", compError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (\"x\" == \"y\")", compError, {}, {}, ECallableType::JsonExists);

        // IsUnknown inside filter: EMode::Filter allows predicates, but IsUnknown evaluates its
        // inner argument in EMode::Predicate, where predicate types (==, starts with, etc.) are blocked
        ValidateError("$.a ? ((@.b == 10) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\") is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b like_regex \".*\") is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b)) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b != 10) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b < 5) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        // deeper paths
        ValidateError("$.a ? ((@.b.c == 10) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a.b ? ((@.c.d starts with \"x\") is unknown)", predError, {}, {}, ECallableType::JsonExists);

        // IsUnknown wrapping && inside filter: && evaluates its operands (==, etc.) in EMode::Predicate, blocked
        ValidateError("$.a ? ((@.b == 10 && @.c == 20) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\" && @.c == 1) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) && @.c like_regex \"y.*\") is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) && exists(@.c)) is unknown)", predError, {}, {}, ECallableType::JsonExists);

        // IsUnknown wrapping || inside filter: same, || evaluates operands in EMode::Predicate
        ValidateError("$.a ? ((@.b == 10 || @.c == 20) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\" || @.c == 1) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) || @.c like_regex \"y.*\") is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((exists(@.b) || exists(@.c)) is unknown)", predError, {}, {}, ECallableType::JsonExists);

        // Unary NOT inside filter - UnaryNot always returns predError regardless of mode
        ValidateError("$.a ? (!(@.b == 10))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b starts with \"x\"))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (!(exists(@.b)))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b like_regex \".*\"))", predError, {}, {}, ECallableType::JsonExists);
        // deeper paths
        ValidateError("$.a ? (!(@.b.c == 10))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.key ? (!(@.sub != \"x\"))", predError, {}, {}, ECallableType::JsonExists);

        // Unary NOT on left / right of && and || inside filter
        ValidateError("$.a ? (!(@.b == 10) && @.c == 20)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (@.b == 10 && !(@.c == 20))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b starts with \"x\") && @.c == 1)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (exists(@.b) && !(@.c like_regex \"y.*\"))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b == 10) || @.c == 20)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (@.b == 10 || !(@.c == 20))", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (!(@.b starts with \"x\") || exists(@.c))", predError, {}, {}, ECallableType::JsonExists);

        // Unary NOT inside is unknown inside filter
        ValidateError("$.a ? ((!(@.b == 10)) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((!(@.b starts with \"x\")) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((!(exists(@.b))) is unknown)", predError, {}, {}, ECallableType::JsonExists);

        // Unary NOT inside && / || which are wrapped by is unknown inside filter
        ValidateError("$.a ? ((!(@.b == 10) && @.c == 20) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b == 10 && !(@.c == 20)) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((!(@.b == 10) || @.c == 20) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b == 10 || !(@.c == 20)) is unknown)", predError, {}, {}, ECallableType::JsonExists);
    }

    // Nested filter: (@ ? (predicate)).member == value
    Y_UNIT_TEST(CollectPath_NestedFilter) {
        // Basic: inner == predicate, outer comparison dropped
        ValidateJsonExists("$ ? ((@ ? (@.a == 1)).b == 2)", {"\2a" + numSuffix(1)});
        ValidateJsonExists("$ ? ((@ ? (@.a == \"x\")).b == \"y\")", {"\2a" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? (@.a == true)).b == false)", {"\2a" + boolTrueSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == false)).b == true)", {"\2a" + boolFalseSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == null)).b == null)", {"\2a" + nullSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == -3.14)).b == 0)", {"\2a" + numSuffix(-3.14)});

        // Reversed literal in inner predicate (literal == @.path)
        ValidateJsonExists("$ ? ((@ ? (1 == @.a)).b == 2)", {"\2a" + numSuffix(1)});
        ValidateJsonExists("$ ? ((@ ? (\"x\" == @.a)).b == \"y\")", {"\2a" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? (null == @.a)).b == 0)", {"\2a" + nullSuffix});

        // Outer path contributes to the index prefix
        ValidateJsonExists("$.key ? ((@ ? (@.sub == \"x\")).other == \"y\")", {"\4key\4sub" + strSuffix("x")});
        ValidateJsonExists("$.a.b ? ((@ ? (@.c == true)).d == false)", {"\2a\2b\2c" + boolTrueSuffix});
        ValidateJsonExists("$.arr ? ((@ ? (@.id == 9)).name == \"x\")", {"\4arr\3id" + numSuffix(9)});
        ValidateJsonExists("$.items ? ((@ ? (@.type == null)).value > 0)", {"\6items\5type" + nullSuffix});

        // Comparison operators != == in inner predicate: literal not appended, only path
        ValidateJsonExists("$ ? ((@ ? (@.n < 10)).label == \"x\")", {"\2n"});
        ValidateJsonExists("$ ? ((@ ? (@.n > 0)).label == \"x\")", {"\2n"});
        ValidateJsonExists("$ ? ((@ ? (@.n != 0)).label == \"x\")", {"\2n"});
        ValidateJsonExists("$ ? ((@ ? (@.n >= 0)).label == \"x\")", {"\2n"});
        ValidateJsonExists("$ ? ((@ ? (@.n <= 100)).label == \"x\")", {"\2n"});
        ValidateJsonExists("$.arr ? ((@ ? (@.score >= 5)).rank == 1)", {"\4arr\6score"});

        // Deeper inner path
        ValidateJsonExists("$ ? ((@ ? (@.a.b == 1)).c == 2)", {"\2a\2b" + numSuffix(1)});
        ValidateJsonExists("$.key ? ((@ ? (@.a.b.c == \"x\")).d == \"y\")", {"\4key\2a\2b\2c" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? (@.x.y == null)).z == true)", {"\2x\2y" + nullSuffix});

        // Array subscript in inner predicate path: subscript is dropped for the index
        ValidateJsonExists("$ ? ((@ ? (@.a[0] == 1)).b == 2)", {"\2a" + numSuffix(1)});
        ValidateJsonExists("$ ? ((@ ? (@.a[last] == true)).b == null)", {"\2a" + boolTrueSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a[0].b == \"x\")).c == 1)", {"\2a\2b" + strSuffix("x")});

        // Array subscript on @ before inner filter: subscript is dropped for the index
        ValidateJsonExists("$ ? ((@[0] ? (@.id == 9)).name == \"x\")", {"\3id" + numSuffix(9)});
        ValidateJsonExists("$ ? ((@[*] ? (@.tag == \"foo\")).value > 0)", {"\4tag" + strSuffix("foo")});
        ValidateJsonExists("$.items ? ((@[*] ? (@.tag == \"foo\")).value > 0)", {"\6items\4tag" + strSuffix("foo")});
        ValidateJsonExists("$.k ? ((@[1] ? (@.x == true)).y == false)", {"\2k\2x" + boolTrueSuffix});

        // Wildcard in inner predicate: path finishes, literal not appended
        ValidateJsonExists("$ ? ((@ ? (@.* == 1)).x == 2)", {""});
        ValidateJsonExists("$.key ? ((@ ? (@.* == \"x\")).y == 1)", {"\4key"});
        ValidateJsonExists("$ ? ((@ ? (@.a.* == null)).b == 1)", {"\2a"});

        // Inner AND: both tokens propagate (filter result has multiple tokens)
        ValidateJsonExists("$ ? ((@ ? (@.a == 1 && @.b == 2)).c == 3)", {"\2a" + numSuffix(1), "\2b" + numSuffix(2)});
        ValidateJsonExists("$.key ? ((@ ? (@.x == \"v\" && @.y == true)).z == null)", {"\4key\2x" + strSuffix("v"), "\4key\2y" + boolTrueSuffix});
        ValidateJsonExists("$ ? ((@ ? (@.a == null && @.b == false)).c == 1)", {"\2a" + nullSuffix, "\2b" + boolFalseSuffix});

        // Inner OR: both tokens propagate
        ValidateJsonExists("$ ? ((@ ? (@.a == 1 || @.a == 2)).b == \"x\")", {"\2a" + numSuffix(1), "\2a" + numSuffix(2)});
        ValidateJsonExists("$ ? ((@ ? (@.tag == \"foo\" || @.tag == \"bar\")).value == 0)", {"\4tag" + strSuffix("foo"), "\4tag" + strSuffix("bar")});
        ValidateJsonExists("$.arr ? ((@ ? (@.id == 1 || @.id == 2)).val == true)", {"\4arr\3id" + numSuffix(1), "\4arr\3id" + numSuffix(2)});

        // Double nesting: only deepest (innermost) inner filter determines the tokens
        ValidateJsonExists("$ ? ((@ ? ((@ ? (@.z == 1)).w == 2)).val == 3)", {"\2z" + numSuffix(1)});
        ValidateJsonExists("$.a ? ((@ ? ((@ ? (@.b == \"x\")).c == \"y\")).d == \"z\")", {"\2a\2b" + strSuffix("x")});
        ValidateJsonExists("$ ? ((@ ? ((@ ? (@.p == null)).q == true)).r == false)", {"\2p" + nullSuffix});
    }

    Y_UNIT_TEST(CollectPath_Variables) {
        // Equality: variable on the right side, all scalar types
        ValidateTokens("$.key == $var", {"\4key" + strSuffix("hello")}, {{"var", strSuffix("hello")}});
        ValidateTokens("$.key == $var", {"\4key" + strSuffix("")}, {{"var", strSuffix("")}});
        ValidateTokens("$.key == $var", {"\4key" + numSuffix(42)}, {{"var", numSuffix(42)}});
        ValidateTokens("$.key == $var", {"\4key" + numSuffix(0)}, {{"var", numSuffix(0)}});
        ValidateTokens("$.key == $var", {"\4key" + numSuffix(3.14)}, {{"var", numSuffix(3.14)}});
        ValidateTokens("$.key == $var", {"\4key" + numSuffix(-10)}, {{"var", numSuffix(-10)}});
        ValidateTokens("$.key == $var", {"\4key" + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("$.key == $var", {"\4key" + boolFalseSuffix}, {{"var", boolFalseSuffix}});
        ValidateTokens("$.key == $var", {"\4key" + nullSuffix}, {{"var", nullSuffix}});

        // Equality: variable on the left side
        ValidateTokens("$var == $.key", {"\4key" + strSuffix("hello")}, {{"var", strSuffix("hello")}});
        ValidateTokens("$var == $.key", {"\4key" + numSuffix(5)}, {{"var", numSuffix(5)}});
        ValidateTokens("$var == $.key", {"\4key" + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("$var == $.key", {"\4key" + nullSuffix}, {{"var", nullSuffix}});

        // Context object as path
        ValidateTokens("$ == $var", {strSuffix("root")}, {{"var", strSuffix("root")}});
        ValidateTokens("$var == $", {numSuffix(0)}, {{"var", numSuffix(0)}});

        // Deeper member access paths
        ValidateTokens("$.a.b == $var", {"\2a\2b" + strSuffix("x")}, {{"var", strSuffix("x")}});
        ValidateTokens("$.a.b.c == $var", {"\2a\2b\2c" + numSuffix(1)}, {{"var", numSuffix(1)}});
        ValidateTokens("$.aba.\"caba\" == $var", {"\4aba\5caba" + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("$var == $.a.b.c", {"\2a\2b\2c" + nullSuffix}, {{"var", nullSuffix}});

        // Array access
        ValidateTokens("$.key[0] == $var", {"\4key" + strSuffix("x")}, {{"var", strSuffix("x")}});
        ValidateTokens("$.key[last] == $var", {"\4key" + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("$.key[1, 2, 3] == $var", {"\4key" + nullSuffix}, {{"var", nullSuffix}});
        ValidateTokens("$.a.b[0].c == $var", {"\2a\2b\2c" + numSuffix(7)}, {{"var", numSuffix(7)}});

        // Wildcard member access: path finishes, literal not appended
        ValidateTokens("$.* == $var", {""}, {{"var", strSuffix("x")}});
        ValidateTokens("$.a.* == $var", {"\2a"}, {{"var", numSuffix(1)}});
        ValidateTokens("$var == $.*", {""}, {{"var", strSuffix("x")}});
        ValidateTokens("$var == $.a.*", {"\2a"}, {{"var", numSuffix(1)}});

        // Methods: path finishes, literal not appended
        ValidateTokens("$.key.size() == $var", {"\4key"}, {{"var", numSuffix(3)}});
        ValidateTokens("$.key.abs() == $var", {"\4key"}, {{"var", numSuffix(1)}});
        ValidateTokens("$.key.type() == $var", {"\4key"}, {{"var", strSuffix("number")}});
        ValidateTokens("$.a.b.floor() == $var", {"\2a\2b"}, {{"var", numSuffix(0)}});

        // Unary arithmetic on path: path finishes, literal not appended
        ValidateTokens("-$.key == $var", {"\4key"}, {{"var", numSuffix(1)}});
        ValidateTokens("+$.key == $var", {"\4key"}, {{"var", numSuffix(0)}});

        // Multiple variables: AND
        ValidateTokens("($.a == $v1) && ($.b == $v2)",
            {"\2a" + strSuffix("x"), "\2b" + numSuffix(1)},
            {{"v1", strSuffix("x")}, {"v2", numSuffix(1)}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == $v) && ($.b == $v)",
            {"\2a" + nullSuffix, "\2b" + nullSuffix},
            {{"v", nullSuffix}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == $v1) && ($.b == $v2) && ($.c == $v3)",
            {"\2a" + strSuffix("a"), "\2b" + numSuffix(2), "\2c" + boolTrueSuffix},
            {{"v1", strSuffix("a")}, {"v2", numSuffix(2)}, {"v3", boolTrueSuffix}}, {},
            ECallableType::JsonValue, EMode::And);

        // Multiple variables: OR
        ValidateTokens("($.a == $v1) || ($.b == $v2)",
            {"\2a" + strSuffix("x"), "\2b" + numSuffix(1)},
            {{"v1", strSuffix("x")}, {"v2", numSuffix(1)}}, {},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("($.key == $v1) || ($.key == $v2)",
            {"\4key" + strSuffix("a"), "\4key" + strSuffix("b")},
            {{"v1", strSuffix("a")}, {"v2", strSuffix("b")}}, {},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("($.a == $v1) || ($.b == $v2) || ($.c == $v3)",
            {"\2a" + boolTrueSuffix, "\2b" + nullSuffix, "\2c" + numSuffix(0)},
            {{"v1", boolTrueSuffix}, {"v2", nullSuffix}, {"v3", numSuffix(0)}}, {},
            ECallableType::JsonValue, EMode::Or);

        // Mixed: variable and literal
        ValidateTokens("($.a == $var) && ($.b == 42)",
            {"\2a" + strSuffix("x"), "\2b" + numSuffix(42)},
            {{"var", strSuffix("x")}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == \"hello\") && ($.b == $var)",
            {"\2a" + strSuffix("hello"), "\2b" + numSuffix(3.14)},
            {{"var", numSuffix(3.14)}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == $var) || ($.b == true)",
            {"\2a" + nullSuffix, "\2b" + boolTrueSuffix},
            {{"var", nullSuffix}}, {},
            ECallableType::JsonValue, EMode::Or);

        // Variable not in map
        ValidateTokens("$.key == $var", {"\4key"}, {}, {}, ECallableType::JsonValue);
        ValidateTokens("$var == $.key", {"\4key"}, {}, {}, ECallableType::JsonValue);

        // Variable exists but queried variable is missing
        ValidateTokens("$.key == $missing", {"\4key"},
            {{"other", strSuffix("x")}}, {}, ECallableType::JsonValue);

        // One variable present, other missing in AND
        ValidateTokens("($.a == $v1) && ($.b == $v2)", {"\2a" + strSuffix("x"), "\2b"},
            {{"v1", strSuffix("x")}}, {}, ECallableType::JsonValue, EMode::And);

        // Variable in non-literal context: standalone path
        ValidateError("$var", varContextError, {{"var", strSuffix("x")}});
        ValidateError("$var.key", varContextError, {{"var", strSuffix("x")}});
        ValidateError("$var[0]", varContextError, {{"var", strSuffix("x")}});
        ValidateError("$var.a.b.c", varContextError, {{"var", strSuffix("x")}});

        // Variable in arithmetic: treated as empty literal, path only
        ValidateTokens("$.key + $var", {"\4key"}, {{"var", numSuffix(1)}});
        ValidateTokens("$var + $.key", {"\4key"}, {{"var", numSuffix(1)}});
        ValidateTokens("$.key - $var", {"\4key"}, {{"var", numSuffix(1)}});
        ValidateTokens("$.key * $var", {"\4key"}, {{"var", numSuffix(2)}});
        ValidateTokens("$.key / $var", {"\4key"}, {{"var", numSuffix(2)}});
        ValidateTokens("$.key % $var", {"\4key"}, {{"var", numSuffix(2)}});
        ValidateTokens("$.a.b + $var", {"\2a\2b"}, {{"var", numSuffix(5)}});

        // Both sides are variables: treated as two empty literals -> emptyError
        ValidateError("$v1 + $v2", emptyError, {{"v1", numSuffix(1)}, {"v2", numSuffix(2)}});
        ValidateError("$v1 * $v2", emptyError, {{"v1", numSuffix(3)}, {"v2", numSuffix(4)}});

        // Variable in comparison operators: variable is treated as a dropped literal
        ValidateTokens("$.key < $var", {"\4key"}, {{"var", numSuffix(5)}});
        ValidateTokens("$.key <= $var", {"\4key"}, {{"var", numSuffix(5)}});
        ValidateTokens("$.key > $var", {"\4key"}, {{"var", numSuffix(0)}});
        ValidateTokens("$.key >= $var", {"\4key"}, {{"var", numSuffix(0)}});
        ValidateTokens("$.key != $var", {"\4key"}, {{"var", strSuffix("x")}});

        // Variable on left side
        ValidateTokens("$var < $.key", {"\4key"}, {{"var", numSuffix(5)}});
        ValidateTokens("$var > $.key", {"\4key"}, {{"var", numSuffix(0)}});
        ValidateTokens("$var != $.key", {"\4key"}, {{"var", strSuffix("x")}});
        ValidateTokens("$var <= $.a.b", {"\2a\2b"}, {{"var", numSuffix(3)}});

        // Both sides variables: emptyError
        ValidateError("$v1 < $v2", emptyError,
            {{"v1", numSuffix(1)}, {"v2", numSuffix(2)}}, {}, ECallableType::JsonValue);
        ValidateError("$v1 != $v2", emptyError,
            {{"v1", strSuffix("a")}, {"v2", strSuffix("b")}}, {}, ECallableType::JsonValue);

        // Filter predicate with variable
        ValidateTokens("$.a ? (@.b == $var)", {"\2a\2b" + strSuffix("x")},
            {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);
        ValidateTokens("$.a ? (@.b == $var)", {"\2a\2b" + numSuffix(42)},
            {{"var", numSuffix(42)}}, {}, ECallableType::JsonExists);
        ValidateTokens("$.a ? (@.b == $var)", {"\2a\2b" + boolTrueSuffix},
            {{"var", boolTrueSuffix}}, {}, ECallableType::JsonExists);
        ValidateTokens("$.a ? (@.b == $var)", {"\2a\2b" + nullSuffix},
            {{"var", nullSuffix}}, {}, ECallableType::JsonExists);

        // Variable on left side in filter equality
        ValidateTokens("$.a ? ($var == @.b)", {"\2a\2b" + strSuffix("x")},
            {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);

        // Deeper filter paths
        ValidateTokens("$.a.b ? (@.c == $var)", {"\2a\2b\2c" + numSuffix(1)},
            {{"var", numSuffix(1)}}, {}, ECallableType::JsonExists);
        ValidateTokens("$.key ? (@.sub == $var)", {"\4key\4sub" + strSuffix("val")},
            {{"var", strSuffix("val")}}, {}, ECallableType::JsonExists);

        // Variable in filter comparison (dropped, path only)
        ValidateTokens("$.a ? (@.b < $var)", {"\2a\2b"},
            {{"var", numSuffix(10)}}, {}, ECallableType::JsonExists);
        ValidateTokens("$.a ? (@.b != $var)", {"\2a\2b"},
            {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);

        // Multiple variables in filter AND
        ValidateTokens("$.a ? (@.b == $v1 && @.c == $v2)",
            {"\2a\2b" + strSuffix("x"), "\2a\2c" + numSuffix(1)},
            {{"v1", strSuffix("x")}, {"v2", numSuffix(1)}}, {}, ECallableType::JsonExists);

        // Multiple variables in filter OR
        ValidateTokens("$.a ? ((@.b == $v1) || (@.b == $v2))",
            {"\2a\2b" + strSuffix("a"), "\2a\2b" + strSuffix("b")},
            {{"v1", strSuffix("a")}, {"v2", strSuffix("b")}}, {}, ECallableType::JsonExists);

        // Missing variable in filter -> skipped
        ValidateTokens("$.a ? (@.b == $var)", {"\2a\2b"}, {}, {}, ECallableType::JsonExists);
    }

    // ParamVariables map: var name -> YQL param name (e.g. "$value")
    Y_UNIT_TEST(CollectPath_ParamVariables) {
        // Basic
        ValidateTokens("$.key == $var", {TToken{"\4key", "$value"}}, {}, {{"var", "$value"}});
        ValidateTokens("$.key == $var", {TToken{"\4key", "$p"}}, {}, {{"var", "$p"}});

        // Reversed order
        ValidateTokens("$var == $.key", {TToken{"\4key", "$value"}}, {}, {{"var", "$value"}});

        // Context object as path
        ValidateTokens("$ == $var", {TToken{"", "$p"}}, {}, {{"var", "$p"}});
        ValidateTokens("$var == $", {TToken{"", "$p"}}, {}, {{"var", "$p"}});

        // Deeper member access paths
        ValidateTokens("$.a.b == $var", {TToken{"\2a\2b", "$p"}}, {}, {{"var", "$p"}});
        ValidateTokens("$.a.b.c == $var", {TToken{"\2a\2b\2c", "$param"}}, {}, {{"var", "$param"}});
        ValidateTokens("$.aba.\"caba\" == $var", {TToken{"\4aba\5caba", "$val"}}, {}, {{"var", "$val"}});
        ValidateTokens("$var == $.a.b.c", {TToken{"\2a\2b\2c", "$p"}}, {}, {{"var", "$p"}});

        // Array access
        ValidateTokens("$.key[0] == $var", {TToken{"\4key", "$v"}}, {}, {{"var", "$v"}});
        ValidateTokens("$.key[last] == $var", {TToken{"\4key", "$v"}}, {}, {{"var", "$v"}});
        ValidateTokens("$.key[1, 2, 3] == $var", {TToken{"\4key", "$v"}}, {}, {{"var", "$v"}});
        ValidateTokens("$.a.b[0].c == $var", {TToken{"\2a\2b\2c", "$v"}}, {}, {{"var", "$v"}});

        // Wildcard member access
        ValidateTokens("$.* == $var", {TToken{"", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("$.a.* == $var", {TToken{"\2a", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("$var == $.*", {TToken{"", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("$var == $.a.*", {TToken{"\2a", ""}}, {}, {{"var", "$v"}});

        // Methods finish the path
        ValidateTokens("$.key.size() == $var", {TToken{"\4key", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("$.key.abs() == $var", {TToken{"\4key", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("$.key.type() == $var", {TToken{"\4key", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("$.a.b.floor() == $var", {TToken{"\2a\2b", ""}}, {}, {{"var", "$v"}});

        // Unary arithmetic on path
        ValidateTokens("-$.key == $var", {TToken{"\4key", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("+$.key == $var", {TToken{"\4key", ""}}, {}, {{"var", "$v"}});

        // Multiple param variables: AND
        ValidateTokens("($.a == $v1) && ($.b == $v2)",
            {TToken{"\2a", "$p1"}, TToken{"\2b", "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == $v) && ($.b == $v)",
            {TToken{"\2a", "$p"}, TToken{"\2b", "$p"}}, {},
            {{"v", "$p"}},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == $v1) && ($.b == $v2) && ($.c == $v3)",
            {TToken{"\2a", "$p1"}, TToken{"\2b", "$p2"}, TToken{"\2c", "$p3"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}, {"v3", "$p3"}},
            ECallableType::JsonValue, EMode::And);

        // Multiple param variables: OR
        ValidateTokens("($.a == $v1) || ($.b == $v2)",
            {TToken{"\2a", "$p1"}, TToken{"\2b", "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("($.key == $v1) || ($.key == $v2)",
            {TToken{"\4key", "$p1"}, TToken{"\4key", "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("($.a == $v1) || ($.b == $v2) || ($.c == $v3)",
            {TToken{"\2a", "$p1"}, TToken{"\2b", "$p2"}, TToken{"\2c", "$p3"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}, {"v3", "$p3"}},
            ECallableType::JsonValue, EMode::Or);

        // Mixed: param variable and plain literal in AND
        ValidateTokens("($.a == $var) && ($.b == 42)",
            {TToken{"\2a", "$p"}, TToken{"\2b" + numSuffix(42), ""}}, {},
            {{"var", "$p"}},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("($.a == \"hello\") && ($.b == $var)",
            {TToken{"\2a" + strSuffix("hello"), ""}, TToken{"\2b", "$p"}}, {},
            {{"var", "$p"}},
            ECallableType::JsonValue, EMode::And);

        // Mixed: param variable and plain literal in OR
        ValidateTokens("($.a == $var) || ($.b == true)",
            {TToken{"\2a", "$p"}, TToken{"\2b" + boolTrueSuffix, ""}}, {},
            {{"var", "$p"}},
            ECallableType::JsonValue, EMode::Or);

        // Param variable in filter predicate
        ValidateTokens("$.a ? (@.b == $var)", {TToken{"\2a\2b", "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("$.a ? ($var == @.b)", {TToken{"\2a\2b", "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);

        // Deeper filter paths
        ValidateTokens("$.a.b ? (@.c == $var)", {TToken{"\2a\2b\2c", "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("$.key ? (@.sub == $var)", {TToken{"\4key\4sub", "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);

        // Param variable in filter comparison
        ValidateTokens("$.a ? (@.b < $var)", {TToken{"\2a\2b", ""}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("$.a ? (@.b != $var)", {TToken{"\2a\2b", ""}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);

        // Multiple param variables in filter AND
        ValidateTokens("$.a ? (@.b == $v1 && @.c == $v2)",
            {TToken{"\2a\2b", "$p1"}, TToken{"\2a\2c", "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonExists, EMode::And);

        // Multiple param variables in filter OR
        ValidateTokens("$.a ? ((@.b == $v1) || (@.b == $v2))",
            {TToken{"\2a\2b", "$p1"}, TToken{"\2a\2b", "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonExists, EMode::Or);

        // Param variable in arithmetic
        ValidateTokens("$.key + $var", {TToken{"\4key", ""}}, {}, {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("$var + $.key", {TToken{"\4key", ""}}, {}, {{"var", "$p"}}, ECallableType::JsonExists);

        // Param variable in comparison operators
        ValidateTokens("$.key < $var", {TToken{"\4key", ""}}, {}, {{"var", "$p"}}, ECallableType::JsonValue);
        ValidateTokens("$.key != $var", {TToken{"\4key", ""}}, {}, {{"var", "$p"}}, ECallableType::JsonValue);
        ValidateTokens("$var < $.key", {TToken{"\4key", ""}}, {}, {{"var", "$p"}}, ECallableType::JsonValue);

        // Param variable not in the map: variable is ignored
        ValidateTokens("$.key == $var", {TToken{"\4key", ""}});
        ValidateTokens("$var == $.key", {TToken{"\4key", ""}});

        // Variable in non-literal context is still an error with paramVariables
        ValidateError("$var", varContextError, {}, {{"var", "$p"}}, ECallableType::JsonValue);
        ValidateError("$var.key", varContextError, {}, {{"var", "$p"}}, ECallableType::JsonValue);

        // param variable on both sides of == is an error (two literals)
        ValidateError("$v1 == $v2", compError, {}, {{"v1", "$p1"}, {"v2", "$p2"}}, ECallableType::JsonValue);

        // Deduplication
        ValidateTokens("($.a.b.c == $var) && ($.a.b.c == $var)",
            {TToken{"\2a\2b\2c", "$p"}}, {}, {{"var", "$p"}});
        ValidateTokens("($.a.b.c == $var) || ($.a.b.c == $var)",
            {TToken{"\2a\2b\2c", "$p"}}, {}, {{"var", "$p"}});

        ValidateTokens("($.key == $v1) && ($.key == $v2)",
            {TToken{"\4key", "$p"}}, {}, {{"v1", "$p"}, {"v2", "$p"}});
        ValidateTokens("($.key == $v1) || ($.key == $v2)",
            {TToken{"\4key", "$p"}}, {}, {{"v1", "$p"}, {"v2", "$p"}});
    }

    // Method calls on literal values (string, number, bool, null) and on variables
    Y_UNIT_TEST(CollectPath_MethodsOnLiteralsAndVariables) {
        // Standalone: method on a literal/variable produces no indexable path
        ValidateError("\"hello\".type()", emptyError);
        ValidateError("\"hello\".size()", emptyError);
        ValidateError("42.0.abs()", emptyError);
        ValidateError("3.14.floor()", emptyError);
        ValidateError("3.14.ceiling()", emptyError);
        ValidateError("true.type()", emptyError);
        ValidateError("null.type()", emptyError);
        ValidateError("(-1).abs()", emptyError);
        ValidateError("(+1.5).ceiling()", emptyError);

        // Chained method on literal
        ValidateError("\"hello\".type().size()", emptyError);

        // Standalone: method on a variable
        ValidateError("$var.type()", emptyError, {{"var", strSuffix("hello")}});
        ValidateError("$var.abs()", emptyError, {{"var", numSuffix(1)}});
        ValidateError("$var.floor()", emptyError, {{"var", numSuffix(3.14)}});
        ValidateError("$var.size()", emptyError, {{"var", strSuffix("hello")}});

        // Standalone: method on a param variable
        ValidateError("$var.type()", emptyError, {}, {{"var", "$p"}});
        ValidateError("$var.abs()", emptyError, {}, {{"var", "$p"}});

        // Comparison: path == literal.method()
        ValidateJsonValue("$.key == \"hello\".type()", {"\4key"});
        ValidateJsonValue("\"hello\".type() == $.key", {"\4key"});
        ValidateJsonValue("$.key == 42.0.abs()", {"\4key"});
        ValidateJsonValue("$.a == true.type()", {"\2a"});
        ValidateJsonValue("$.a == null.type()", {"\2a"});
        ValidateJsonValue("$.a.b == (-1).abs()", {"\2a\2b"});

        // Comparison: path.method() == literal.method()
        ValidateJsonValue("$.key.abs() == \"hello\".type()", {"\4key"});
        ValidateJsonValue("$.a.b.floor() == 42.0.abs()", {"\2a\2b"});

        // Comparison: path == variable.method()
        ValidateTokens("$.key == $var.type()", {"\4key"}, {{"var", strSuffix("number")}});
        ValidateTokens("$var.type() == $.key", {"\4key"}, {{"var", strSuffix("number")}});
        ValidateTokens("$.key == $var.abs()", {"\4key"}, {{"var", numSuffix(5)}});
        ValidateTokens("$.a.b == $var.floor()", {"\2a\2b"}, {{"var", numSuffix(0)}});

        // Comparison: path == param-variable.method()
        ValidateTokens("$.key == $var.type()", {"\4key"}, {}, {{"var", "$p"}});
        ValidateTokens("$var.type() == $.key", {"\4key"}, {}, {{"var", "$p"}});

        // Both sides non-path (literal.method() == literal)
        ValidateError(R"("hello".type() == "string")", emptyError);
        ValidateError("42.0.abs() == 42", emptyError);

        // Filter: @.b == literal.method()
        ValidateJsonExists("$.a ? (@.b == \"hello\".type())", {"\2a\2b"});
        ValidateJsonExists("$.a ? (@.b == 42.0.abs())", {"\2a\2b"});
        ValidateTokens("$.a ? (@.b == $var.type())", {"\2a\2b"}, {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);
    }

    Y_UNIT_TEST(CollectPath_MultiTokenSuffix) {
        // MemberAccess
        ValidateJsonValue("($.a + $.b).c == 1", {"\2a", "\2b"});
        ValidateJsonValue("exists(($.a + $.b).c)", {"\2a", "\2b"});

        // WildcardMemberAccess
        ValidateJsonValue("exists(($.a + $.b).*)", {"\2a", "\2b"});
        ValidateJsonValue("($.a + $.b).* starts with \"x\"", {"\2a", "\2b"});

        // ArrayAccess
        ValidateJsonValue("exists(($.a + $.b)[0])", {"\2a", "\2b"});
        ValidateJsonValue("exists(($.a + $.b)[0].key)", {"\2a", "\2b"});
        ValidateJsonValue("exists(($.a + $.b)[*])", {"\2a", "\2b"});

        // Binary operations
        ValidateJsonValue("($.a + $.b) == 3", {"\2a", "\2b"});
        ValidateJsonValue("($.a - $.b) == true", {"\2a", "\2b"});

        // Filter
        ValidateJsonValue("($ ? (@.a > 0) ? (@.b < 10)).c", {"\2a"});
        ValidateJsonValue("($ ? (@.a > 0) ? (@.b < 10)) == 1", {"\2a"});
        ValidateJsonValue("$ ? (@.a > 0 && @.b < 10).c", {"\2a", "\2b"});
        ValidateJsonValue("$ ? (@.a > 0 && @.b < 10) == 1", {"\2a", "\2b"});

        // Methods
        ValidateJsonValue("($.k1 > $.k2).abs()", {"\3k1", "\3k2"});
        ValidateJsonValue("$ ? (@.a > 0 && @.b < 10).type()", {"\2a", "\2b"});
        ValidateJsonValue("$ ? (@.a > 0 && @.b < 10).type() == \"object\"", {"\2a", "\2b"});
    }

    // Tokens with no ancestor–descendant relation survive both AND and OR merge intact.
    Y_UNIT_TEST(MergeAndOr_DisjointPaths) {
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b"}), MakeTokens({"\2c\2d"})),
            {"\2a\2b", "\2c\2d"}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b"}), MakeTokens({"\2c\2d"})),
            {"\2a\2b", "\2c\2d"}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({"\2a", "\2b"}, EMode::And), MakeTokens({"\2c"})),
            {"\2a", "\2b", "\2c"}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({"\2a", "\2b"}, EMode::Or), MakeTokens({"\2c"})),
            {"\2a", "\2b", "\2c"}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b", "\2c\2d"}, EMode::And),
                     MakeTokens({"\2e\2f", "\2g\2h"}, EMode::And)),
            {"\2a\2b", "\2c\2d", "\2e\2f", "\2g\2h"}, EMode::And);

    }

    // AND keeps the deepest descendant (leaf); OR keeps the shallowest ancestor (root).
    Y_UNIT_TEST(MergeAndOr_DirectAncestorDescendant) {
        // parent in left operand
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b"}), MakeTokens({"\2a\2b\2c"})),
            {"\2a\2b\2c"}, EMode::And);

        // parent in right operand
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b\2c"}), MakeTokens({"\2a\2b"})),
            {"\2a\2b\2c"}, EMode::And);

        // grandparent pruned (two levels up)
        CheckMerge(
            MergeAnd(MakeTokens({"\2a"}), MakeTokens({"\2a\2b\2c"})),
            {"\2a\2b\2c"}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b"}), MakeTokens({"\2a\2b\2c"})),
            {"\2a\2b"}, EMode::Or);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b\2c"}), MakeTokens({"\2a\2b"})),
            {"\2a\2b"}, EMode::Or);

        CheckMerge(
            MergeOr(MakeTokens({"\2a"}), MakeTokens({"\2a\2b\2c"})),
            {"\2a"}, EMode::Or);
    }

    // Identical tokens collapse to a single entry; equal tokens are NOT each other's prefix.
    Y_UNIT_TEST(MergeAndOr_IdenticalTokens) {
        // single-token sets: set deduplication leaves 1 token -> mode stays NotSet
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b"}), MakeTokens({"\2a\2b"})),
            {"\2a\2b"}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b"}), MakeTokens({"\2a\2b"})),
            {"\2a\2b"}, EMode::NotSet);

        // multi-token sets: deduplication, no pruning (b and c are siblings, not prefix-related)
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b", "\2c"}, EMode::And),
                     MakeTokens({"\2a\2b", "\2c"}, EMode::And)),
            {"\2a\2b", "\2c"}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b", "\2c"}, EMode::Or),
                    MakeTokens({"\2a\2b", "\2c"}, EMode::Or)),
            {"\2a\2b", "\2c"}, EMode::Or);
    }

    // The five-term example from the task description.
    // Terms: a.b, a.b.c, a.d, a.b.c.e, e.f
    //
    // Tree:
    //   a               e
    //  / \              |
    // b   d             f
    // |
    // c
    // |
    // e
    //
    // AND -> leaves: a.b.c.e, a.d, e.f
    // OR  -> roots:  a.b,     a.d, e.f
    Y_UNIT_TEST(MergeAndOr_FullExample) {
        auto left = MakeTokens({"\2a\2b", "\2a\2b\2c", "\2a\2d"}, EMode::And);
        auto right = MakeTokens({"\2a\2b\2c\2e", "\2e\2f"}, EMode::And);
        CheckMerge(
            MergeAnd(std::move(left), std::move(right)),
            {"\2a\2b\2c\2e", "\2a\2d", "\2e\2f"}, EMode::And);

        left = MakeTokens({"\2a\2b", "\2a\2b\2c", "\2a\2d"}, EMode::Or);
        right = MakeTokens({"\2a\2b\2c\2e", "\2e\2f"}, EMode::Or);
        CheckMerge(
            MergeOr(std::move(left), std::move(right)),
            {"\2a\2b", "\2a\2d", "\2e\2f"}, EMode::Or);
    }

    // Two completely independent subtrees each with an ancestor–descendant pair.
    Y_UNIT_TEST(MergeAndOr_MultipleBranches) {
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b", "\2a\2b\2c"}, EMode::And),
                     MakeTokens({"\2x\2y", "\2x\2y\2z"}, EMode::And)),
            {"\2a\2b\2c", "\2x\2y\2z"}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b", "\2a\2b\2c"}, EMode::Or),
                    MakeTokens({"\2x\2y", "\2x\2y\2z"}, EMode::Or)),
            {"\2a\2b", "\2x\2y"}, EMode::Or);
    }

    // A path-only token is a string prefix of a path+literal token for the same field.
    // AND should keep the value-specific (longer) token; OR should keep the path-only (shorter).
    Y_UNIT_TEST(MergeAndOr_LiteralSuffix) {
        const TString ab = "\2a\2b";
        const TString abNum = ab + numSuffix(5.0);
        const TString abNum2 = ab + numSuffix(7.0);

        CheckMerge(
            MergeAnd(MakeTokens({ab}), MakeTokens({abNum})),
            {abNum}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({ab}), MakeTokens({abNum})),
            {ab}, EMode::Or);

        // Two different values for the same path: neither is a prefix of the other -> both kept
        CheckMerge(
            MergeOr(MakeTokens({abNum}), MakeTokens({abNum2})),
            {abNum, abNum2}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({abNum}), MakeTokens({abNum2})),
            {abNum, abNum2}, EMode::And);
    }

    // Pruning rules for tokens that carry a runtime parameter (Param != "")
    Y_UNIT_TEST(MergeAndOr_ParamTokens) {
        // Deduplication: two equal param tokens -> 1 entry
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{"\2a\2b", "$p"}}),
                     MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b", "$p"}}, EMode::NotSet, "AND dedup: equal param tokens collapse to one");

        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{"\2a\2b", "$p"}}),
                    MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b", "$p"}}, EMode::NotSet, "OR dedup: equal param tokens collapse to one");

        // Same path, same Param, in a multi-token set: dedup still collapses them.
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{"\2a", "$p"}, TToken{"\2b", "$q"}}, EMode::And),
                     MakeParamTokens({TToken{"\2a", "$p"}, TToken{"\2b", "$q"}}, EMode::And)),
            {TToken{"\2a", "$p"}, TToken{"\2b", "$q"}}, EMode::And, "AND dedup: multi-token param set collapses duplicates");

        // AND pruning with param as leaf
        CheckMergeFull(
            MergeAnd(MakeTokens({"\2a\2b"}),
                     MakeParamTokens({TToken{"\2a\2b\2c", "$p"}})),
            {TToken{"\2a\2b\2c", "$p"}}, EMode::And, "AND pruning: non-param ancestor dropped, param leaf kept");

        CheckMergeFull(
            MergeAnd(MakeTokens({"\2a\2b"}),
                     MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b", "$p"}}, EMode::And, "AND pruning: path-only token dropped, param token kept");

        // Order does not matter for AND
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{"\2a\2b\2c", "$p"}}),
                     MakeTokens({"\2a\2b"})),
            {TToken{"\2a\2b\2c", "$p"}}, EMode::And, "AND pruning: order reversed, non-param ancestor still dropped");

        // Two levels up
        CheckMergeFull(
            MergeAnd(MakeTokens({"\2a"}),
                     MakeParamTokens({TToken{"\2a\2b\2c", "$p"}})),
            {TToken{"\2a\2b\2c", "$p"}}, EMode::And, "AND pruning: grandparent non-param dropped by param grandchild");

        // OR pruning with non-param ancestor
        CheckMergeFull(
            MergeOr(MakeTokens({"\2a\2b"}),
                    MakeParamTokens({TToken{"\2a\2b\2c", "$p"}})),
            {TToken{"\2a\2b", ""}}, EMode::Or, "OR pruning: non-param ancestor kept, param descendant dropped");

        CheckMergeFull(
            MergeOr(MakeTokens({"\2a\2b"}),
                    MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b"}}, EMode::Or, "OR pruning: path-only token kept, param token dropped");

        // Order does not matter for OR
        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{"\2a\2b\2c", "$p"}}),
                    MakeTokens({"\2a\2b"})),
            {TToken{"\2a\2b", ""}}, EMode::Or, "OR pruning: order reversed, non-param ancestor still kept");

        // Two levels down
        CheckMergeFull(
            MergeOr(MakeTokens({"\2a"}),
                    MakeParamTokens({TToken{"\2a\2b\2c", "$p"}})),
            {TToken{"\2a", ""}}, EMode::Or, "OR pruning: root non-param kept, two-level-deep param descendant dropped");

        // Disjoint paths
        CheckMergeFull(
            MergeAnd(MakeTokens({"\2a\2b"}),
                     MakeParamTokens({TToken{"\2c\2d", "$p"}})),
            {TToken{"\2a\2b", ""}, TToken{"\2c\2d", "$p"}}, EMode::And, "AND disjoint: unrelated paths both kept");

        CheckMergeFull(
            MergeOr(MakeTokens({"\2a\2b"}),
                    MakeParamTokens({TToken{"\2c\2d", "$p"}})),
            {TToken{"\2a\2b", ""}, TToken{"\2c\2d", "$p"}}, EMode::Or, "OR disjoint: unrelated paths both kept");

        // Disjoint param tokens
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{"\2a", "$p1"}}),
                     MakeParamTokens({TToken{"\2b", "$p2"}})),
            {TToken{"\2a", "$p1"}, TToken{"\2b", "$p2"}}, EMode::And, "AND disjoint params: two unrelated param tokens both kept");

        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{"\2a", "$p1"}}),
                    MakeParamTokens({TToken{"\2b", "$p2"}})),
            {TToken{"\2a", "$p1"}, TToken{"\2b", "$p2"}}, EMode::Or, "OR disjoint params: two unrelated param tokens both kept");

        // Same path, different Params
        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{"\4key", "$p1"}}),
                    MakeParamTokens({TToken{"\4key", "$p2"}})),
            {TToken{"\4key", "$p1"}, TToken{"\4key", "$p2"}}, EMode::Or, "OR same path different params: both kept as distinct constraints");

        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{"\4key", "$p1"}}),
                     MakeParamTokens({TToken{"\4key", "$p2"}})),
            {TToken{"\4key", "$p1"}, TToken{"\4key", "$p2"}}, EMode::And, "AND same path different params: both kept as distinct constraints");

        // Param token with literal-suffixed non-param sibling on the same path
        CheckMergeFull(
            MergeAnd(MakeTokens({"\2a\2b" + numSuffix(5.0)}),
                     MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b", "$p"}, TToken{"\2a\2b" + numSuffix(5.0), ""}}, EMode::And, "AND literal-suffix sibling: param and literal-suffixed token both kept");

        CheckMergeFull(
            MergeOr(MakeTokens({"\2a\2b" + numSuffix(5.0)}),
                    MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b", "$p"}, TToken{"\2a\2b" + numSuffix(5.0), ""}}, EMode::Or, "OR literal-suffix sibling: param and literal-suffixed token both kept");

        // Non-param path-only token is a prefix of the literal-suffixed token AND of the param token with the same base path
        CheckMergeFull(
            MergeAnd(MakeTokens({"\2a\2b", "\2a\2b" + numSuffix(5.0)}, EMode::And),
                     MakeParamTokens({TToken{"\2a\2b", "$p"}})),
            {TToken{"\2a\2b", "$p"}, TToken{"\2a\2b" + numSuffix(5.0), ""}}, EMode::And, "AND prefix+literal+param: path-only prefix dropped, leaf and param kept");

        CheckMergeFull(
            MergeOr(MakeTokens({"\2a\2b"}),
                    MakeParamTokens({TToken{"\2a\2b", "$p"}, TToken{"\2a\2b" + numSuffix(5.0), ""}})),
            {TToken{"\2a\2b", ""}}, EMode::Or, "OR prefix+literal+param: root non-param kept, descendants dropped");
    }

    // When one operand carries an incompatible mode, the merge falls back to OR mode,
    // so OR pruning (keep roots) is applied even inside MergeAnd.
    Y_UNIT_TEST(MergeAnd_ModeMixAppliesOrPruning) {
        // Left has Or mode -> hasMix -> final mode Or -> OR pruning keeps the shorter token
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b"}, EMode::Or),
                     MakeTokens({"\2a\2b\2c"}, EMode::And)),
            {"\2a\2b"}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b\2c"}, EMode::And),
                     MakeTokens({"\2a\2b"}, EMode::Or)),
            {"\2a\2b"}, EMode::Or);
    }

    // A chain of three levels (grandparent -> parent -> child) in a single merge call.
    Y_UNIT_TEST(MergeAndOr_DeepChain) {
        // AND: grandparent and parent are both prefixes of child -> only child survives
        CheckMerge(
            MergeAnd(MakeTokens({"\2a", "\2a\2b"}, EMode::And),
                     MakeTokens({"\2a\2b\2c"})),
            {"\2a\2b\2c"}, EMode::And);

        // OR: root covers all -> only root survives
        CheckMerge(
            MergeOr(MakeTokens({"\2a"}),
                    MakeTokens({"\2a\2b", "\2a\2b\2c"}, EMode::Or)),
            {"\2a"}, EMode::Or);
    }

    // Hierarchical tokens mixed with completely unrelated tokens.
    Y_UNIT_TEST(MergeAndOr_MixedHierarchyAndDisjoint) {
        // {a.b, a.b.c, x} AND {a.b.c.d, y} -> {a.b.c.d, x, y}
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b", "\2a\2b\2c", "\2x"}, EMode::And),
                     MakeTokens({"\2a\2b\2c\2d", "\2y"}, EMode::And)),
            {"\2a\2b\2c\2d", "\2x", "\2y"}, EMode::And);

        // {a, a.b, x} OR {a.b.c, y} -> {a, x, y}
        CheckMerge(
            MergeOr(MakeTokens({"\2a", "\2a\2b", "\2x"}, EMode::Or),
                    MakeTokens({"\2a\2b\2c", "\2y"}, EMode::Or)),
            {"\2a", "\2x", "\2y"}, EMode::Or);
    }

    Y_UNIT_TEST(MergeAndOr_EmptyOperands) {
        // One operand is empty: result is the other operand's single token, NotSet mode
        CheckMerge(
            MergeAnd(MakeTokens({}), MakeTokens({"\2a\2b"})),
            {"\2a\2b"}, EMode::NotSet);

        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b"}), MakeTokens({})),
            {"\2a\2b"}, EMode::NotSet);

        CheckMerge(
            MergeAnd(MakeTokens({}), MakeTokens({})),
            {}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({}), MakeTokens({"\2a\2b"})),
            {"\2a\2b"}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b"}), MakeTokens({})),
            {"\2a\2b"}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({}), MakeTokens({})),
            {}, EMode::NotSet);
    }

    Y_UNIT_TEST(MergeAndOr_ErrorPropagation) {
        {
            auto r = MergeAnd(MakeError("left error"), MakeTokens({"\2a\2b"}));
            UNIT_ASSERT_C(r.IsError(), "AND: expected error from left");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "left error");
        }
        {
            auto r = MergeAnd(MakeTokens({"\2a\2b"}), MakeError("right error"));
            UNIT_ASSERT_C(r.IsError(), "AND: expected error from right");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "right error");
        }
        {
            auto r = MergeOr(MakeError("left error"), MakeTokens({"\2a\2b"}));
            UNIT_ASSERT_C(r.IsError(), "OR: expected error from left");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "left error");
        }
        {
            auto r = MergeOr(MakeTokens({"\2a\2b"}), MakeError("right error"));
            UNIT_ASSERT_C(r.IsError(), "OR: expected error from right");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "right error");
        }
    }

    // Sibling paths sharing only a common ancestor but not a prefix relation between themselves.
    Y_UNIT_TEST(MergeAndOr_Siblings) {
        // a.b and a.c share ancestor a but neither is a prefix of the other
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b"}), MakeTokens({"\2a\2c"})),
            {"\2a\2b", "\2a\2c"}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b"}), MakeTokens({"\2a\2c"})),
            {"\2a\2b", "\2a\2c"}, EMode::Or);

        // Mix: one sibling has a deeper descendant
        // {a.b, a.c} AND {a.b.d, a.c} -> AND: a.b.d covers a.b; a.c deduplicates -> {a.b.d, a.c}
        CheckMerge(
            MergeAnd(MakeTokens({"\2a\2b", "\2a\2c"}, EMode::And),
                     MakeTokens({"\2a\2b\2d", "\2a\2c"}, EMode::And)),
            {"\2a\2b\2d", "\2a\2c"}, EMode::And);

        // {a.b, a.c} OR {a.b.d, a.c} -> OR: a.b covers a.b.d; a.c deduplicates -> {a.b, a.c}
        CheckMerge(
            MergeOr(MakeTokens({"\2a\2b", "\2a\2c"}, EMode::Or),
                    MakeTokens({"\2a\2b\2d", "\2a\2c"}, EMode::Or)),
            {"\2a\2b", "\2a\2c"}, EMode::Or);
    }

    // Ensure different-length key names don't create false prefix matches.
    // Key "ab" (2 bytes, length prefix \x02) must NOT match as a prefix for key "a" (\x01).
    Y_UNIT_TEST(MergeAndOr_DifferentLengthKeys) {
        // \2a\2b  = path $.a.b  (keys "a" and "b", each 1 char, encoded as 2)
        // \3ab\2c = path $.ab.c (key "ab" is 2 chars, encoded as 3)
        const TString pathAB  = "\2a\2b";
        const TString pathABC = "\3ab\2c";  // $.ab.c — unrelated to $.a.b

        CheckMerge(
            MergeAnd(MakeTokens({pathAB}), MakeTokens({pathABC})),
            {pathAB, pathABC}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({pathAB}), MakeTokens({pathABC})),
            {pathAB, pathABC}, EMode::Or);
    }

    Y_UNIT_TEST(MergeAndOr_ZeroPath) {
        const TString first  = "\1"; // $.""
        const TString second = boolTrueSuffix;

        CheckMerge(
            MergeAnd(MakeTokens({first}), MakeTokens({second})),
            {first, second}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({first}), MakeTokens({second})),
            {first, second}, EMode::Or);
    }

    // The empty string token ("") represents the root context object ($)
    Y_UNIT_TEST(MergeAndOr_EmptyPathToken) {
        const TString root = "";
        const TString a = "\2a";
        const TString ab = "\2a\2b";
        const TString b = "\2b";

        // OR: root token covers any other token -> only root survives
        CheckMerge(
            MergeOr(MakeTokens({root}), MakeTokens({a})),
            {root}, EMode::Or);

        CheckMerge(
            MergeOr(MakeTokens({a}), MakeTokens({root})),
            {root}, EMode::Or);

        // OR: root in a multi-token set — all others pruned
        CheckMerge(
            MergeOr(MakeTokens({root, a}, EMode::Or), MakeTokens({ab, b}, EMode::Or)),
            {root}, EMode::Or);

        // AND: root is more general than any other token -> root is pruned
        CheckMerge(
            MergeAnd(MakeTokens({root}), MakeTokens({a})),
            {a}, EMode::And);

        CheckMerge(
            MergeAnd(MakeTokens({a}), MakeTokens({root})),
            {a}, EMode::And);

        // AND: root with multiple non-empty tokens -> root pruned, others kept
        CheckMerge(
            MergeAnd(MakeTokens({root, a}, EMode::And), MakeTokens({ab, b}, EMode::And)),
            {ab, b}, EMode::And);

        // Root on both sides: deduplication leaves a single token -> NotSet mode, no pruning
        CheckMerge(
            MergeOr(MakeTokens({root}), MakeTokens({root})),
            {root}, EMode::NotSet);

        CheckMerge(
            MergeAnd(MakeTokens({root}), MakeTokens({root})),
            {root}, EMode::NotSet);

        // Root with a literal-suffixed token (path + value): root is still a prefix -> same rules
        const TString aNum = a + numSuffix(1.0);
        CheckMerge(
            MergeOr(MakeTokens({root}), MakeTokens({aNum})),
            {root}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({root}), MakeTokens({aNum})),
            {aNum}, EMode::And);

        // Root with a sibling pair: both siblings extend root, so root covers both in OR
        CheckMerge(
            MergeOr(MakeTokens({root}), MakeTokens({a, b}, EMode::Or)),
            {root}, EMode::Or);

        // AND: root with siblings -> root pruned, siblings kept
        CheckMerge(
            MergeAnd(MakeTokens({root}), MakeTokens({a, b}, EMode::And)),
            {a, b}, EMode::And);
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
            TString("\3id", 3),
            TString("\3id\0\4\0\0\0\0@\x87\xE4@", 13),
            TString("\6brand", 6),
            TString("\6brand\0\3bricks", 14),
            TString("\6parts", 6),
            TString("\6parts\3id", 9),
            TString("\6parts\3id", 9),
            TString("\6parts\3id\0\4\0\0\0\0\x80\xC3\xDF@", 19),
            TString("\6parts\3id\0\4\0\0\0\0\xC0\xC2\xDF@", 19),
            TString("\6parts\5name", 11),
            TString("\6parts\5name", 11),
            TString("\6parts\5name\0\0031x3", 16),
            TString("\6parts\5name\0\0033x5", 16),
            TString("\6parts\6count", 12),
            TString("\6parts\6count", 12),
            TString("\6parts\6count\0\4\0\0\0\0\0\0\x1C@", 22),
            TString("\6parts\6count\0\4\0\0\0\0\0\0001@", 22),
            TString("\6price", 6),
            TString("\6price\0\2", 8),
            TString("\x0Bpart_count", 11),
            TString("\x0Bpart_count\0\4\0\0\0\0\0\xE4\x95@", 21)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString emptyKeyObj = "{\"\":{\"a\":\"b\"}}";
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(emptyKeyObj, error), (TVector<TString>{
            TString(),
            TString("\1", 1),
            TString("\1\2a", 3),
            TString("\1\2a\0\3b", 6)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString longKey;
        longKey.resize(1000);
        for (size_t i = 0; i < longKey.size(); i++)
            longKey[i] = 'a';
        TString longKeyObj = "{\"" + longKey + "\":{\"short\":\"b\"}}";
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(longKeyObj, error), (TVector<TString>{
            TString(),
            TString("\xE9\7") + longKey,
            TString("\xE9\7") + longKey + TString("\6short", 6),
            TString("\xE9\7") + longKey + TString("\6short\0\3b", 9)
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    Y_UNIT_TEST(FormatJsonIndexToken) {
        auto encodeKey = [](const TString& key) {
            TString result;
            size_t size = key.size() + 1;
            do {
                if (size < 0x80) {
                    result.push_back(static_cast<char>(size));
                } else {
                    result.push_back(static_cast<char>(0x80 | (size & 0x7F)));
                }
                size >>= 7;
            } while (size > 0);
            result += key;
            return result;
        };

        auto encodePath = [&](const std::vector<TString>& keys) {
            TString result;
            for (const auto& key : keys) {
                result += encodeKey(key);
            }
            return result;
        };

        // path only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"k1", "k2"}), ""), R"({"path":"k1.k2"})");

        // path + bool true literal
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"k1", "k2"}) + boolTrueSuffix, ""), R"({"path":"k1.k2","literal":true})");

        // path + param
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"k1", "k2"}), "$var"), R"({"path":"k1.k2","param":"$var"})");

        // bool false literal only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(boolFalseSuffix, ""), R"({"literal":false})");

        // param only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken("", "$var"), R"({"param":"$var"})");

        // empty
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken("", ""), "{}");

        // bool true literal only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(boolTrueSuffix, ""), R"({"literal":true})");

        // null literal only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(nullSuffix, ""), R"({"literal":null})");

        // string literal only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(strSuffix("hello"), ""), R"({"literal":"hello"})");

        // numeric literal only
        {
            double v = 42.0;
            UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(numSuffix(v), ""), TStringBuilder() << R"({"literal":)" << v << "}");
        }

        // path + string literal
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"a"}) + strSuffix("x"), ""), R"({"path":"a","literal":"x"})");

        // path + numeric literal
        {
            double v = 3.14;
            UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"x"}) + numSuffix(v), ""), TStringBuilder() << R"({"path":"x","literal":)" << v << "}");
        }

        // path + null literal
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"a"}) + nullSuffix, ""), R"({"path":"a","literal":null})");

        // single-segment path
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"key"}), ""), R"({"path":"key"})");

        // three-segment path
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"a", "b", "c"}), ""), R"({"path":"a.b.c"})");

        // empty key segment in path
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"", "b"}), ""), R"({"path":".b"})");

        // long key (forces multi-byte LEB128)
        {
            TString longKey(200, 'x');
            UNIT_ASSERT_VALUES_EQUAL(
                FormatJsonIndexToken(encodePath({longKey}), ""),
                TStringBuilder() << R"({"path":")" << longKey << R"("})");
        }
    }
}

}  // namespace NKikimr::NJsonIndex
