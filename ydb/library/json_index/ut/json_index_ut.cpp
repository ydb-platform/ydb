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
const TString arrayItemSuffix = TString("\1");
const TString laxMarker = TString("\2");

TString encodeKey(TStringBuf key) {
    TString result;
    size_t size = key.size() + (ui8)EPathSeparator::Max;
    do {
        if (size < 0x80) {
            result.push_back(static_cast<char>(size));
        } else {
            result.push_back(static_cast<char>(0x80 | (size & 0x7F)));
        }
        size >>= 7;
    } while (size > 0);
    result.append(key.data(), key.size());
    return result;
}

TString encodePath(const std::vector<TString>& keys) {
    TString result;
    for (const auto& key : keys) {
        result += encodeKey(key);
    }
    return result;
}

TString keyWithNull(TStringBuf before, TStringBuf after) {
    TString key;
    key.append(before);
    key.push_back('\0');
    key.append(after);
    return key;
}

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
        UNIT_ASSERT_C(expectedTokens.contains(token), token.PathToken << " for path = " << jsonPath);
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

void CheckPathAndOrMerge(const TString& a, const TString& b,
    const std::vector<TString>& andExpected, const std::vector<TString>& orExpected)
{
    CheckMerge(MergeAnd(MakeTokens({a}), MakeTokens({b})), andExpected, EMode::And);
    CheckMerge(MergeAnd(MakeTokens({b}), MakeTokens({a})), andExpected, EMode::And);
    CheckMerge(MergeOr(MakeTokens({a}), MakeTokens({b})), orExpected, EMode::Or);
    CheckMerge(MergeOr(MakeTokens({b}), MakeTokens({a})), orExpected, EMode::Or);
}

void CheckMergeSymmetric(const TString& a, const TString& b, const std::vector<TString>& expected) {
    CheckPathAndOrMerge(a, b, expected, expected);
}

}  // namespace

Y_UNIT_TEST_SUITE(NJsonIndex) {
    Y_UNIT_TEST(TokenizeJsonDoesNotEmitEmptyToken) {
        TString error;

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(R"({"a":1})", error), (TVector<TString>{
            encodeKey("a"), encodeKey("a") + numSuffix(1),

            laxMarker + encodeKey("a"), laxMarker + encodeKey("a") + numSuffix(1),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[]", error), TVector<TString>{arrayItemSuffix});
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("42", error), (TVector<TString>{numSuffix(42), laxMarker + numSuffix(42)}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }


    // Every path must have a ContextObject ($)
    Y_UNIT_TEST(CollectPath_EmptyPath) {
        ValidateError<true>("", "Too many errors");
    }

    Y_UNIT_TEST(CollectPath_ContextObject) {
        ValidateJsonExists("strict $", {""});
    }

    Y_UNIT_TEST(CollectPath_MemberAccess) {
        ValidateJsonExists("strict $.a", {encodeKey("a")});
        ValidateJsonExists("strict $.a.b.c", {encodeKey("a") + encodeKey("b") + encodeKey("c")});
        ValidateJsonExists("strict $.aba.\"caba\"", {encodeKey("aba") + encodeKey("caba")});
        ValidateJsonExists("strict $.\"\".abc", {encodeKey("") + encodeKey("abc")});
        ValidateJsonExists("strict $.*", {""});
        ValidateJsonExists("strict $.a.*", {encodeKey("a")});
        ValidateJsonExists("strict $.a.*.c", {encodeKey("a")});
    }

    Y_UNIT_TEST(CollectPath_ArrayAccess) {
        ValidateJsonExists("strict $[0]", {arrayItemSuffix});
        ValidateJsonExists("strict $[1, 2, 3]", {arrayItemSuffix});
        ValidateJsonExists("strict $[1 to 3]", {arrayItemSuffix});
        ValidateJsonExists("strict $[last]", {arrayItemSuffix});
        ValidateJsonExists("strict $[0, 2 to last]", {arrayItemSuffix});
        ValidateJsonExists("strict $[0 to 1].key", {arrayItemSuffix + encodeKey("key")});
        ValidateJsonExists("strict $.key[0]", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonExists("strict $.key1[last].key2", {encodeKey("key1") + arrayItemSuffix + encodeKey("key2")});
        ValidateJsonExists("strict $.arr[2 to last]", {encodeKey("arr") + arrayItemSuffix});
        ValidateJsonExists("strict $.*[2 to last].key", {""});
        ValidateJsonExists("strict $.key[0].*", {encodeKey("key") + arrayItemSuffix});
    }

    // Methods stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_Methods) {
        ValidateJsonExists("strict $.abs()", {""});
        ValidateJsonExists("strict $.*.floor()", {""});
        ValidateJsonExists("strict $[1, 2, 3].ceiling()", {arrayItemSuffix});
        ValidateJsonExists("strict $.key.abs()", {encodeKey("key")});
        ValidateJsonExists("strict $.key.floor()", {encodeKey("key")});
        ValidateJsonExists("strict $.key.ceiling()", {encodeKey("key")});
        ValidateJsonExists("strict $.key.double()", {encodeKey("key")});
        ValidateJsonExists("strict $.key.type()", {encodeKey("key")});
        ValidateJsonExists("strict $.key.size()", {encodeKey("key")});
        ValidateJsonExists("strict $.key.keyvalue()", {encodeKey("key")});
        ValidateJsonExists("strict $.*.keyvalue()", {""});
        ValidateJsonExists("strict $.key[1, 2, 3].value.size().floor()", {encodeKey("key") + arrayItemSuffix + encodeKey("value")});
        ValidateJsonExists("strict $.key.keyvalue().name", {encodeKey("key")});
    }

    // StartsWith predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_StartsWithPredicate) {
        ValidateJsonValue("strict $ starts with \"lol\"", {""});
        ValidateJsonValue("strict $[1 to last] starts with \"lol\"", {arrayItemSuffix});
        ValidateJsonValue("strict $[*] starts with \"lol\"", {arrayItemSuffix});
        ValidateJsonValue("strict $.key starts with \"abc\"", {encodeKey("key")});
        ValidateJsonValue("strict $.a.b.c[1, 2, 3] starts with \"abc\"", {encodeKey("a") + encodeKey("b") + encodeKey("c") + arrayItemSuffix});
        ValidateJsonValue("strict $.key.type().name starts with \"abc\"", {encodeKey("key")});
        ValidateJsonValue("strict $.* starts with \"abc\"", {""});
        ValidateJsonValue("strict $.a.*.c[1, 2, 3] starts with \"abc\"", {encodeKey("a")});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key starts with \"lol\"", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);
    }

    // LikeRegex predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_LikeRegexPredicate) {
        ValidateJsonValue("strict $ like_regex \"abc\"", {""});
        ValidateJsonValue("strict $[1 to 2] like_regex \"abc\"", {arrayItemSuffix});
        ValidateJsonValue("strict $[*] like_regex \"abc\"", {arrayItemSuffix});
        ValidateJsonValue("strict $.key like_regex \"abc\"", {encodeKey("key")});
        ValidateJsonValue("strict $.* like_regex \"abc\"", {""});
        ValidateJsonValue("strict $.key[1, 2, 3] like_regex \"abc\"", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonValue("strict $.key.keyvalue() like_regex \"abc\"", {encodeKey("key")});
        ValidateJsonValue("strict $.key like_regex \"a.c\"", {encodeKey("key")});
        ValidateJsonValue("strict $.key like_regex \".*\"", {encodeKey("key")});

        // For JSON_EXISTS, the result is always true even if the path does not exist
        ValidateError("$.key like_regex \"abc\"", "Predicates are not allowed in this context", {}, {}, ECallableType::JsonExists);
    }

    // Exists predicates stop further path extraction: operand path only
    Y_UNIT_TEST(CollectPath_ExistsPredicate) {
        ValidateJsonValue("strict exists($)", {""});
        ValidateJsonValue("strict exists($.key)", {encodeKey("key")});
        ValidateJsonValue("strict exists($.key[1, 2, 3])", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonValue("strict exists($[*].size())", {arrayItemSuffix});
        ValidateJsonValue("strict exists($.key.keyvalue().name)", {encodeKey("key")});

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
        ValidateJsonExists("strict -$.key", {encodeKey("key")});
        ValidateJsonExists("strict +$.key", {encodeKey("key")});

        ValidateJsonExists("strict -$", {""});
        ValidateJsonExists("strict +$", {""});

        ValidateJsonExists("strict -$.a.b.c", {encodeKey("a") + encodeKey("b") + encodeKey("c")});
        ValidateJsonExists("strict +$.a.b.c", {encodeKey("a") + encodeKey("b") + encodeKey("c")});

        ValidateJsonExists("strict -$.*", {""});
        ValidateJsonExists("strict +$.*", {""});
        ValidateJsonExists("strict -$.a.*", {encodeKey("a")});
        ValidateJsonExists("strict +$.a.*", {encodeKey("a")});

        ValidateJsonExists("strict -$.key[0]", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonExists("strict +$.key[last]", {encodeKey("key") + arrayItemSuffix});

        ValidateJsonExists("strict -$.key.abs()", {encodeKey("key")});
        ValidateJsonExists("strict +$.key.type()", {encodeKey("key")});

        ValidateJsonExists("strict -(-$.key)", {encodeKey("key")});
        ValidateJsonExists("strict -(+$.key)", {encodeKey("key")});
        ValidateJsonExists("strict +(-$.key)", {encodeKey("key")});
        ValidateJsonExists("strict +(+$.key)", {encodeKey("key")});

        ValidateJsonValue("strict exists(-$.a.b)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonValue("strict exists(+$.a.b)", {encodeKey("a") + encodeKey("b")});

        ValidateJsonValue("strict -($.double())", {""});
        ValidateJsonValue("strict +($.double())", {""});
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
        ValidateTokens("strict $.key + 1", {encodeKey("key")});
        ValidateTokens("strict $.key - 1", {encodeKey("key")});
        ValidateTokens("strict $.key - (-1)", {encodeKey("key")});
        ValidateTokens("strict $.key * 2", {encodeKey("key")});
        ValidateTokens("strict $.key / 2", {encodeKey("key")});
        ValidateTokens("strict $.key % 2", {encodeKey("key")});

        // Literal on the left, path on the right - only right token
        ValidateTokens("strict 1 + $.key", {encodeKey("key")});
        ValidateTokens("strict -1 - $.key", {encodeKey("key")});
        ValidateTokens("strict 1 * $.key", {encodeKey("key")});
        ValidateTokens("strict (+(-1)) / $.key", {encodeKey("key")});
        ValidateTokens("strict 1 % $.key", {encodeKey("key")});

        // Context object on the left
        ValidateTokens("strict $ + 1", {""});
        ValidateTokens("strict $ * 2", {""});

        // Deeper paths as left operand
        ValidateTokens("strict $.a.b.c + 1", {encodeKey("a") + encodeKey("b") + encodeKey("c")});
        ValidateTokens("strict $.a.b - 1", {encodeKey("a") + encodeKey("b")});

        // Array access on the left operand
        ValidateTokens("strict $.key[0] + 1", {encodeKey("key") + arrayItemSuffix});
        ValidateTokens("strict $.arr[last] * 2", {encodeKey("arr") + arrayItemSuffix});

        // Wildcard on the left - collection already finished by wildcard
        ValidateTokens("strict $.* + 1", {""});
        ValidateTokens("strict $.* + (-1)", {""});
        ValidateTokens("strict $.a.* - 1", {encodeKey("a")});

        // Both operands are paths - tokens from both are collected (AND)
        ValidateTokens("strict $.a + $.b", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict $.a.b - $.c.d", {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")});
        ValidateTokens("strict $.a.b - (-$.c.d)", {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")});

        // Both operands are literals - no path to collect
        ValidateError("1 + 2", emptyError);
        ValidateError("(+(-1.5)) * 2.0", emptyError);

        // Wildcard on left, path on right - both collected
        ValidateJsonExists("strict $.a.* + $.b", {encodeKey("a"), encodeKey("b")});
        ValidateJsonExists("strict $.* + $.b", {"", encodeKey("b")});
        ValidateJsonExists("strict $.* - $.a.b", {"", encodeKey("a") + encodeKey("b")});

        // Path on left, wildcard on right
        ValidateJsonExists("strict $.a + $.*", {encodeKey("a"), ""});
        ValidateJsonExists("strict $.a.b + $.*", {encodeKey("a") + encodeKey("b"), ""});

        // Wildcard on both sides - two wildcard tokens collected
        ValidateJsonExists("strict $.* + $.*", {""});
        ValidateJsonExists("strict $.a.* + $.*", {encodeKey("a"), ""});
        ValidateJsonExists("strict $.* + $.a.*", {"", encodeKey("a")});
        ValidateJsonExists("strict $.a.b.*.c + $.a.b.*.d", {encodeKey("a") + encodeKey("b")});

        // Variable on left - propagated immediately, right not collected
        ValidateJsonExists("strict $var + $.b", {encodeKey("b")});
        ValidateJsonExists("strict $var - $.b", {encodeKey("b")});
        ValidateJsonExists("strict $var * $.b", {encodeKey("b")});
        ValidateJsonExists("strict $var / $.b", {encodeKey("b")});
        ValidateJsonExists("strict $var % $.b", {encodeKey("b")});

        // Variable on right - left tokens lost, variable not collected
        ValidateJsonExists("strict $.a + $var", {encodeKey("a")});
        ValidateJsonExists("strict $.a - $var", {encodeKey("a")});
        ValidateJsonExists("strict $.a * $var", {encodeKey("a")});

        // Variable propagates through chained binary: ($.a + $var) + $.c
        ValidateJsonExists("strict $.a + $var + $.c", {encodeKey("a"), encodeKey("c")});
    }

    // Non-trivial combinations of unary and binary arithmetic operators
    Y_UNIT_TEST(CollectPath_ArithmeticCombinations) {
        // Unary applied to binary: tokens from both binary operands, then Finish
        ValidateTokens("strict -($.a + 1)", {encodeKey("a")});
        ValidateTokens("strict +($.a - 1)", {encodeKey("a")});
        ValidateTokens("strict -($.a * $.b)", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict -(1 + $.b)", {encodeKey("b")});

        // Binary with unary left operand
        ValidateTokens("strict -$.a + $.b", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict +$.a - $.b", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict -$.key + 1", {encodeKey("key")});
        ValidateTokens("strict +$.key * 2", {encodeKey("key")});

        // Binary with unary right operand - right token still collected
        ValidateTokens("strict $.a + (-$.b)", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict $.a * (+$.b)", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict 1 + (-$.b)", {encodeKey("b")});

        // Chained binary (left-associative): all three path tokens collected
        ValidateTokens("strict $.a + $.b + $.c", {encodeKey("a"), encodeKey("b"), encodeKey("c")});
        ValidateTokens("strict $.a - $.b - $.c", {encodeKey("a"), encodeKey("b"), encodeKey("c")});
        ValidateTokens("strict $.a * $.b * $.c", {encodeKey("a"), encodeKey("b"), encodeKey("c")});

        // Mixed precedence: * binds tighter than +, but all paths still collected
        ValidateTokens("strict $.a + $.b * $.c", {encodeKey("a"), encodeKey("b"), encodeKey("c")});
        ValidateTokens("strict $.a * $.b + $.c", {encodeKey("a"), encodeKey("b"), encodeKey("c")});

        // Double unary combined with binary
        ValidateTokens("strict -(-$.a) + $.b", {encodeKey("a"), encodeKey("b")});
        ValidateTokens("strict -(+$.a) * 2", {encodeKey("a")});

        // Longer paths on both sides
        ValidateTokens("strict $.a.b.c + $.x.y.z", {encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("x") + encodeKey("y") + encodeKey("z")});
        ValidateTokens("strict -($.a.*.c) + $.x.y.*", {encodeKey("a"), encodeKey("x") + encodeKey("y")});
        ValidateTokens("strict $.a.b.c * 3.14", {encodeKey("a") + encodeKey("b") + encodeKey("c")});

        // Method result used as operand of binary - method finishes, but token still collected
        ValidateTokens("strict $.key.size() + 1", {encodeKey("key")});
        ValidateTokens("strict $.key.abs() * 2", {encodeKey("key")});
        ValidateTokens("strict $.a.size() + $.b.floor()", {encodeKey("a"), encodeKey("b")});
    }

    // Arithmetic operators (two-path operands produce And mode) combined with && and ||
    Y_UNIT_TEST(CollectPath_ArithmeticWithBooleanOps) {
        // Two-path arithmetic result (And mode) in AND chain: stays And
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a - $.b == \"x\") && ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a * $.b == \"x\") && ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a / $.b == \"x\") && ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a % $.b == \"x\") && ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.c == 1) && ($.a + $.b == \"x\")", {encodeKey("c") + numSuffix(1), encodeKey("a"), encodeKey("b")}, TCollectResult::ETokensMode::And);

        // Two arithmetic results combined via AND: stays And
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c + $.d == \"y\")", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a - $.b == \"x\") && ($.c * $.d == \"y\") && ($.e == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e") + numSuffix(1)},
            TCollectResult::ETokensMode::And);

        // Two-path arithmetic result (And mode) in OR: OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a - $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a * $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a / $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a % $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.c == 1) || ($.a + $.b == \"x\")", {encodeKey("c") + numSuffix(1), encodeKey("a"), encodeKey("b")}, TCollectResult::ETokensMode::Or);

        // Two arithmetic results combined via OR: OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c + $.d == \"y\")", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a - $.b == \"x\") || ($.c * $.d == \"y\")", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a / $.b == \"x\") || ($.c % $.d == \"y\")", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);

        // Three-way OR of arithmetic results: all become OR
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c + $.d == \"y\") || ($.e == 1)",
            {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e") + numSuffix(1)}, TCollectResult::ETokensMode::Or);

        // Arithmetic result with comparison (single-path, NotSet) via AND: compatible, stays And
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c < 5)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.c < 5) && ($.a + $.b == \"x\")", {encodeKey("c"), encodeKey("a"), encodeKey("b")}, TCollectResult::ETokensMode::And);

        // Arithmetic result with comparison via OR: OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c < 5)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.c < 5) || ($.a + $.b == \"x\")", {encodeKey("c"), encodeKey("a"), encodeKey("b")}, TCollectResult::ETokensMode::Or);

        // Arithmetic result with starts with / like_regex / exists in AND: compatible
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c starts with \"abc\")", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c like_regex \".*\")", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a + $.b == \"x\") && exists($.c)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);

        // Arithmetic result with starts with / like_regex / exists in OR: OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c starts with \"abc\")", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c like_regex \".*\")", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a + $.b == \"x\") || exists($.c)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);

        // Deeper paths in arithmetic operands
        ValidateJsonValue("strict ($.a.b.c + $.x.y.z == \"val\") && ($.key == 1)", {encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("x") + encodeKey("y") + encodeKey("z"), encodeKey("key") + numSuffix(1)},
            TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a.b.c + $.x.y.z == \"val\") || ($.key == 1)", {encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("x") + encodeKey("y") + encodeKey("z"), encodeKey("key") + numSuffix(1)},
            TCollectResult::ETokensMode::Or);

        // Filter: arithmetic with two paths combined via OR with plain path
        ValidateJsonExists("strict $.key ? (@.a + @.b == 5 || @.c == 1)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c") + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
        // Filter: two arithmetic results in OR
        ValidateJsonExists("strict $.key ? (@.a + @.b == 5 || @.c + @.d == 3)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c"), encodeKey("key") + encodeKey("d")},
            TCollectResult::ETokensMode::Or);
        // Filter: AND chain with OR appended - OR wins
        ValidateJsonExists("strict $.key ? (@.a + @.b == 5 && @.c == 1 || @.d == 2)",
            {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c") + numSuffix(1), encodeKey("key") + encodeKey("d") + numSuffix(2)}, TCollectResult::ETokensMode::Or);
    }

    Y_UNIT_TEST(CollectPath_EqualityOperator) {
        // Path == literal, all literal types
        ValidateJsonValue("strict $.key == \"hello\"", {encodeKey("key") + strSuffix("hello")});
        ValidateJsonValue("strict $.key == \"\"", {encodeKey("key") + strSuffix("")});
        ValidateJsonValue("strict $.key == 42", {encodeKey("key") + numSuffix(42)});
        ValidateJsonValue("strict $.key == 0", {encodeKey("key") + numSuffix(0)});
        ValidateJsonValue("strict $.key == 3.14", {encodeKey("key") + numSuffix(3.14)});
        ValidateJsonValue("strict $.key == true", {encodeKey("key") + boolTrueSuffix});
        ValidateJsonValue("strict $.key == false", {encodeKey("key") + boolFalseSuffix});
        ValidateJsonValue("strict $.key == null", {encodeKey("key") + nullSuffix});

        // Reversed order: literal == path (identical result)
        ValidateJsonValue("strict \"hello\" == $.key", {encodeKey("key") + strSuffix("hello")});
        ValidateJsonValue("strict 42 == $.key", {encodeKey("key") + numSuffix(42)});
        ValidateJsonValue("strict true == $.key", {encodeKey("key") + boolTrueSuffix});
        ValidateJsonValue("strict null == $.key", {encodeKey("key") + nullSuffix});

        // Context object as path (empty prefix)
        ValidateJsonValue("strict $ == \"hello\"", {strSuffix("hello")});
        ValidateJsonValue("strict $ == 42", {numSuffix(42)});
        ValidateJsonValue("strict $ == true", {boolTrueSuffix});
        ValidateJsonValue("strict $ == null", {nullSuffix});
        ValidateJsonValue("strict \"hello\" == $", {strSuffix("hello")});

        // Deeper paths
        ValidateJsonValue("strict $.a.b == \"x\"", {encodeKey("a") + encodeKey("b") + strSuffix("x")});
        ValidateJsonValue("strict $.a.b.c == null", {encodeKey("a") + encodeKey("b") + encodeKey("c") + nullSuffix});
        ValidateJsonValue("strict \"x\" == $.a.b.c", {encodeKey("a") + encodeKey("b") + encodeKey("c") + strSuffix("x")});
        ValidateJsonValue("strict $.aba.\"caba\" == true", {encodeKey("aba") + encodeKey("caba") + boolTrueSuffix});
        ValidateJsonValue("strict $.a.b.c.d == 0", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d") + numSuffix(0)});
        ValidateJsonValue("strict $.\"\".\"\" == 0", {encodeKey("") + encodeKey("") + numSuffix(0)});

        // Array subscript
        ValidateJsonValue("strict $.key[0] == \"x\"", {encodeKey("key") + arrayItemSuffix + strSuffix("x")});
        ValidateJsonValue("strict $.key[last] == true", {encodeKey("key") + arrayItemSuffix + boolTrueSuffix});
        ValidateJsonValue("strict $.key[1, 2, 3] == null", {encodeKey("key") + arrayItemSuffix + nullSuffix});
        ValidateJsonValue("strict $.key[0 to last] == 42", {encodeKey("key") + arrayItemSuffix + numSuffix(42)});
        ValidateJsonValue("strict $.key[0].sub == \"x\"", {encodeKey("key") + arrayItemSuffix + encodeKey("sub") + strSuffix("x")});
        ValidateJsonValue("strict $.a.b[0].c == \"x\"", {encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c") + strSuffix("x")});
        ValidateJsonValue("strict $.key[*] == \"x\"", {encodeKey("key") + arrayItemSuffix + strSuffix("x")});

        // Wildcard member access finishes the path
        ValidateJsonValue("strict $.* == \"x\"", {""});
        ValidateJsonValue("strict $.a.* == \"x\"", {encodeKey("a")});
        ValidateJsonValue("strict $.a.b.* == \"x\"", {encodeKey("a") + encodeKey("b")});
        ValidateJsonValue("strict \"x\" == $.*", {""});
        ValidateJsonValue("strict \"x\" == $.a.*", {encodeKey("a")});

        // Methods finish the path
        ValidateJsonValue("strict $.key.size() == 3", {encodeKey("key")});
        ValidateJsonValue("strict $.key.abs() == 1", {encodeKey("key")});
        ValidateJsonValue("strict $.key.type() == \"number\"", {encodeKey("key")});
        ValidateJsonValue("strict $.a.b.floor() == 0", {encodeKey("a") + encodeKey("b")});
        ValidateJsonValue("strict $.key.keyvalue().name == \"x\"", {encodeKey("key")});

        // Unary arithmetic on path finishes the path
        ValidateJsonValue("strict -$.key == 1", {encodeKey("key")});
        ValidateJsonValue("strict +$.key == 1", {encodeKey("key")});
        ValidateJsonValue("strict -$.a.b == null", {encodeKey("a") + encodeKey("b")});

        // Literal numeric value folded from unary + / - (same suffix as a plain number literal)
        ValidateJsonValue("strict $.a == -10", {encodeKey("a") + numSuffix(-10)});
        ValidateJsonValue("strict $.k == +(-(+(-3)))", {encodeKey("k") + numSuffix(3)});
        ValidateJsonValue("strict $.key == +42", {encodeKey("key") + numSuffix(42)});
        ValidateJsonValue("strict $.key == -(-42)", {encodeKey("key") + numSuffix(42)});
        ValidateJsonValue("strict $.key == +(-15)", {encodeKey("key") + numSuffix(-15)});
        ValidateJsonValue("strict $.a.b == -(-(-2))", {encodeKey("a") + encodeKey("b") + numSuffix(-2)});
        ValidateJsonValue("strict $ == +(-(-7))", {numSuffix(7)});
        ValidateJsonValue("strict -10 == $.a", {encodeKey("a") + numSuffix(-10)});
        ValidateJsonValue("strict +(-(+(-3))) == $.k", {encodeKey("k") + numSuffix(3)});

        // Arithmetic produces multiple tokens
        ValidateJsonValue("strict ($.a + $.b) == \"x\"", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict \"x\" == ($.a + $.b)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $.key + 1 == \"x\"", {encodeKey("key")});
        ValidateJsonValue("strict 1 + $.key == \"x\"", {encodeKey("key")});

        // Parenthesized path - no effect
        ValidateJsonValue("strict ($.a.b) == \"x\"", {encodeKey("a") + encodeKey("b") + strSuffix("x")});
        ValidateJsonValue("strict \"x\" == ($.a.b)", {encodeKey("a") + encodeKey("b") + strSuffix("x")});
        ValidateJsonValue("strict (((((($).a).b))) == (\"x\"))", {encodeKey("a") + encodeKey("b") + strSuffix("x")});

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
        ValidateJsonValue("strict $.a == $.b", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $.key == $", {encodeKey("key"), ""});
        ValidateJsonValue("strict $ == $", {""});
        ValidateJsonValue("strict $.a.b == $.c.d", {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")});

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
        ValidateJsonValue("strict $ == $var", {""});
    }

    // Comparison operators <, <=, >, >=, != collect path tokens from both operands; literals are silently dropped.
    // Mode is set to And only when more than one token is collected (same rule as BinaryArithmeticOp).
    Y_UNIT_TEST(CollectPath_ComparisonOperators) {
        // Literal on the right is dropped, only the path token is returned.
        ValidateJsonValue("strict $.key < 10", {encodeKey("key")});
        ValidateJsonValue("strict $.key <= 10", {encodeKey("key")});
        ValidateJsonValue("strict $.key > 10", {encodeKey("key")});
        ValidateJsonValue("strict $.key >= 10", {encodeKey("key")});
        ValidateJsonValue("strict $.key != 10", {encodeKey("key")});
        ValidateJsonValue("strict $.key != -10", {encodeKey("key")});
        ValidateJsonValue("strict $.key >= -(+(-10))", {encodeKey("key")});
        ValidateJsonValue("strict $.key < \"hello\"", {encodeKey("key")});
        ValidateJsonValue("strict $.key != \"\"", {encodeKey("key")});
        ValidateJsonValue("strict $.key > 3.14", {encodeKey("key")});
        ValidateJsonValue("strict $.key >= true", {encodeKey("key")});
        ValidateJsonValue("strict $.key < null", {encodeKey("key")});

        // Literal on the left, path on the right - literal dropped, path token returned
        ValidateJsonValue("strict 10 < $.key", {encodeKey("key")});
        ValidateJsonValue("strict 10 <= $.key", {encodeKey("key")});
        ValidateJsonValue("strict 10 > $.key", {encodeKey("key")});
        ValidateJsonValue("strict 10 >= $.key", {encodeKey("key")});
        ValidateJsonValue("strict 10 != $.key", {encodeKey("key")});
        ValidateJsonValue("strict -10 != $.key", {encodeKey("key")});
        ValidateJsonValue("strict -(+(-10)) != $.key", {encodeKey("key")});

        // Context object as path (empty prefix)
        ValidateJsonValue("strict $ < 5", {""});
        ValidateJsonValue("strict $ > \"x\"", {""});
        ValidateJsonValue("strict $ != null", {""});

        // Deeper member access paths
        ValidateJsonValue("strict $.a.b.c < 42", {encodeKey("a") + encodeKey("b") + encodeKey("c")});
        ValidateJsonValue("strict $.a.b > -1", {encodeKey("a") + encodeKey("b")});
        ValidateJsonValue("strict $.aba.\"caba\" != false", {encodeKey("aba") + encodeKey("caba")});
        ValidateJsonValue("strict $.a.b.c.d >= 0", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d")});

        // Array access
        ValidateJsonValue("strict $.key[0] < 5", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonValue("strict $.key[last] > true", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonValue("strict $.key[1, 2, 3] != null", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonValue("strict $.key[0 to last] >= 1", {encodeKey("key") + arrayItemSuffix});
        ValidateJsonValue("strict $.a.b[0].c <= \"x\"", {encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c")});

        // Wildcard member access finishes the path (literal not appended, but still dropped)
        ValidateJsonValue("strict $.* < 5", {""});
        ValidateJsonValue("strict $.a.* > 1", {encodeKey("a")});
        ValidateJsonValue("strict $.a.b.* != \"x\"", {encodeKey("a") + encodeKey("b")});

        // Wildcard array access
        ValidateJsonValue("strict $.key[*] < 5", {encodeKey("key") + arrayItemSuffix});

        // Methods finish the path
        ValidateJsonValue("strict $.key.size() < -3", {encodeKey("key")});
        ValidateJsonValue("strict $.key.abs() >= 1", {encodeKey("key")});
        ValidateJsonValue("strict $.a.b.floor() != 0", {encodeKey("a") + encodeKey("b")});
        ValidateJsonValue("strict $.key.keyvalue().name > \"x\"", {encodeKey("key")});

        // Unary arithmetic on path finishes the path
        ValidateJsonValue("strict -$.key < 1", {encodeKey("key")});
        ValidateJsonValue("strict +$.key >= 0", {encodeKey("key")});

        // Both sides are paths - tokens from both collected (mode=And)
        ValidateJsonValue("strict $.a < $.b", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $.a.b > $.c.d", {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")});
        ValidateJsonValue("strict $.key != $.other", {encodeKey("key"), encodeKey("other")});
        ValidateJsonValue("strict $ <= $.a", {"", encodeKey("a")});
        ValidateJsonValue("strict $.a >= $", {encodeKey("a"), ""});

        // Both sides are literals - error
        ValidateError("1 < 2", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("1.5 >= -2.0", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("true != false", emptyError, {}, {}, ECallableType::JsonValue);

        // Arithmetic expression as operand (same as BinaryArithmeticOp behavior)
        // $.a + $.b produces mode=And, comparison also sets And - compatible
        ValidateJsonValue("strict $.a + $.b < -5", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict 1 < $.a + $.b", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $.key + 1 >= 5", {encodeKey("key")});
        ValidateJsonValue("strict $.a.size() + $.b.abs() != 0", {encodeKey("a"), encodeKey("b")});

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
        ValidateJsonValue("strict ($.a < 5) && ($.b > 1)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a <= 5) && ($.b >= 1)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a != 5) && ($.b == 1)", {encodeKey("a"), encodeKey("b") + numSuffix(1)});
        ValidateJsonValue("strict ($.a < 5) && ($.b > 1) && ($.c != 3)", {encodeKey("a"), encodeKey("b"), encodeKey("c")});

        ValidateJsonValue("strict ($.a < 5) || ($.b > 1)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a >= 5) || ($.b != -1)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a != 5) || ($.b == 1)", {encodeKey("a"), encodeKey("b") + numSuffix(1)});
        ValidateJsonValue("strict ($.a < -5) || ($.b > 1) || ($.c != 3)", {encodeKey("a"), encodeKey("b"), encodeKey("c")});

        // Two-path comparison (And mode) mixed with OR: OR wins
        ValidateJsonValue("strict ($.a < $.b) || ($.c > 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c")});
        ValidateJsonValue("strict ($.a != $.b) || ($.c == -1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(-1)});

        // AND chain with OR: OR wins, all tokens become OR
        ValidateJsonValue("strict ($.a < -5) && ($.b == 1) || ($.c > 2)", {encodeKey("a"), encodeKey("b") + numSuffix(1), encodeKey("c")});
        ValidateJsonValue("strict ($.a < -5) && ($.b > -1) || ($.c != 3)", {encodeKey("a"), encodeKey("b"), encodeKey("c")});

        // Variables
        ValidateError("$var < 5", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("5 > $var", emptyError, {}, {}, ECallableType::JsonValue);
        ValidateError("$var != $var", emptyError, {}, {}, ECallableType::JsonValue);
    }

    // Comparison operators inside filter predicates (EMode::Filter allows predicates)
    Y_UNIT_TEST(CollectPath_ComparisonOperators_InFilter) {
        // Basic filter with each comparison op
        ValidateJsonExists("strict $.a ? (@.b < 10)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b <= -10)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b > 10)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b >= +10)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b != 10)", {encodeKey("a") + encodeKey("b")});

        // All literal types as right operand (dropped)
        ValidateJsonExists("strict $.a ? (@.b < \"hello\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b > -3.14)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b != true)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b >= null)", {encodeKey("a") + encodeKey("b")});

        // Literal on the left, @ path on the right
        ValidateJsonExists("strict $.a ? (10 < @.b)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (\"x\" != @.b)", {encodeKey("a") + encodeKey("b")});

        // @ itself (filter object) as operand
        ValidateJsonExists("strict $.a ? (@ < 10)", {encodeKey("a")});
        ValidateJsonExists("strict $.a ? (@ != \"x\")", {encodeKey("a")});
        ValidateJsonExists("strict $.a.b ? (@ > 0)", {encodeKey("a") + encodeKey("b")});

        // Deeper filter-object paths
        ValidateJsonExists("strict $.a ? (@.b.c < -(+(-5)))", {encodeKey("a") + encodeKey("b") + encodeKey("c")});
        ValidateJsonExists("strict $.a.b ? (@.c.d != null)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d")});

        // Method on filter-object path (finishes, literal dropped)
        ValidateJsonExists("strict $.a ? (@.b.size() < -3)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.abs() >= 0)", {encodeKey("a") + encodeKey("b")});

        // Unary on filter-object path (finishes, literal dropped)
        ValidateJsonExists("strict $.a ? (-@.b < 5)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (+@.b >= 0)", {encodeKey("a") + encodeKey("b")});

        // Both operands are @-paths (both tokens collected)
        ValidateJsonExists("strict $.key ? (@.a < @.b)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        ValidateJsonExists("strict $.key ? (@.x != @.y)", {encodeKey("key") + encodeKey("x"), encodeKey("key") + encodeKey("y")});

        // Wildcard on filter-object path
        ValidateJsonExists("strict $.a ? (@.* < 5)", {encodeKey("a")});
        ValidateJsonExists("strict $.a ? (@.b.* != 1)", {encodeKey("a") + encodeKey("b")});

        // Comparison in AND inside filter
        ValidateJsonExists("strict $.a ? (@.b < +10 && @.c == 1)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c") + numSuffix(1)});
        ValidateJsonExists("strict $.a ? (@.b > 0 && @.b < 100)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b != 5 && @.c >= 0 && @.d <= -10)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});

        // Comparison in OR inside filter
        ValidateJsonExists("strict $.a ? ((@.b < 5) || (@.c > 10))", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});
        ValidateJsonExists("strict $.a ? ((@.b != 1) || (@.c != 2))", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});

        // Mixing AND and OR inside filter: OR wins
        ValidateJsonExists("strict $.a ? ((@.b < 5) && ((@.c > 1) || (@.d > 2)))", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});
        ValidateJsonExists("strict $.a ? (((@.b < 5) || (@.c > 1)) && @.d > 2)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});

        // Nested predicate in filter operand is blocked (EMode::Predicate on operand)
        ValidateError("$.a ? (($.b == 1) < 5)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? (exists(@.b) < 5)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b starts with \"x\") != true)", predError, {}, {}, ECallableType::JsonExists);

        // is unknown wrapping comparison inside filter - blocked
        ValidateError("$.a ? ((@.b < 5) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b >= +0) is unknown)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("$.a ? ((@.b != 1) is unknown)", predError, {}, {}, ECallableType::JsonExists);

        // Arithmetic expression as comparison operand in filter
        ValidateJsonExists("strict $.key ? (@.a + @.b < +5)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        ValidateJsonExists("strict $.key ? (@.a * 2 != 0)", {encodeKey("key") + encodeKey("a")});

        // JsonValue also supports comparison in filter
        ValidateJsonValue("strict $.a ? (@.b < -10)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonValue("strict $.a ? (@.b != null && @.c >= 1)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});
    }

    // Comparison operators (path vs path produce And mode) combined with && and ||
    Y_UNIT_TEST(CollectPath_ComparisonWithBooleanOps) {
        // Two-path comparison (And mode) in AND chain: compatible, stays And
        ValidateJsonValue("strict ($.a < $.b) && ($.c > $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a <= $.b) && ($.c >= $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a != $.b) && ($.c == $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a == $.b) && ($.c < $.d) && ($.e > $.f)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e"), encodeKey("f")},
            TCollectResult::ETokensMode::And);

        // Two-path comparison (And mode) in OR: OR wins
        ValidateJsonValue("strict ($.a < $.b) || ($.c > $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a <= $.b) || ($.c >= $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a != $.b) || ($.c == $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a < $.b) || ($.c > $.d) || ($.e != $.f)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e"), encodeKey("f")},
            TCollectResult::ETokensMode::Or);

        // Single-path comparison (NotSet mode) in AND chain
        ValidateJsonValue("strict ($.a < 5) && ($.b > 3) && ($.c <= 10)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a > 0) && ($.b >= 1) && ($.c != 0) && ($.d == 2)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d") + numSuffix(2)},
            TCollectResult::ETokensMode::And);

        // Single-path comparison in OR chain
        ValidateJsonValue("strict ($.a < 5) || ($.b > 3) || ($.c <= 10)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a > 0) || ($.b >= 1) || ($.c != 0) || ($.d == 2)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d") + numSuffix(2)}, 
            TCollectResult::ETokensMode::Or);

        // Mix of single-path and two-path comparisons in AND: compatible (neither has Or)
        ValidateJsonValue("strict ($.a < 5) && ($.b > $.c)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a < $.b) && ($.c > 5)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a < 5) && ($.b > $.c) && ($.d == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d") + numSuffix(1)}, 
            TCollectResult::ETokensMode::And);

        // Mix of single-path and two-path comparisons in OR: OR wins
        ValidateJsonValue("strict ($.a < 5) || ($.b > $.c)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a < $.b) || ($.c > 5)", {encodeKey("a"), encodeKey("b"), encodeKey("c")}, TCollectResult::ETokensMode::Or);

        // AND chain then OR: OR wins
        ValidateJsonValue("strict ($.a < $.b) && ($.c > 1) || ($.d != 0)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict ($.a < 5) && ($.b > $.c) || ($.d == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d") + numSuffix(1)},
            TCollectResult::ETokensMode::Or);

        // Two-path equality combined via AND and OR
        ValidateJsonValue("strict ($.a == $.b) && ($.c == $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict ($.a == $.b) || ($.c == $.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")}, TCollectResult::ETokensMode::Or);

        // Filter: two-path comparison combined with AND/OR
        ValidateJsonExists("strict $.key ? (@.a < @.b && @.c > 1)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c")}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("strict $.key ? (@.a < @.b || @.c > 1)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c")}, TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.key ? (@.a < @.b && @.c > @.d)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c"), encodeKey("key") + encodeKey("d")}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("strict $.key ? (@.a < @.b || @.c > @.d)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c"), encodeKey("key") + encodeKey("d")}, TCollectResult::ETokensMode::Or);

        // Filter: AND chain with OR - OR wins
        ValidateJsonExists("strict $.key ? (@.a < @.b && @.c > @.d || @.e == 1)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c"), encodeKey("key") + encodeKey("d"), encodeKey("key") + encodeKey("e") + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
    }

    Y_UNIT_TEST(CollectPath_BinaryAnd) {
        // Basic equality on both sides, all literal types
        ValidateJsonValue("strict ($.a == 10) && ($.b == \"hello\")", {encodeKey("a") + numSuffix(10), encodeKey("b") + strSuffix("hello")});
        ValidateJsonValue("strict (42 == $.key) && ($.val == true)", {encodeKey("key") + numSuffix(42), encodeKey("val") + boolTrueSuffix});
        ValidateJsonValue("strict ($.x == null) && ($.y == false)", {encodeKey("x") + nullSuffix, encodeKey("y") + boolFalseSuffix});
        ValidateJsonValue("strict ($.a == 0) && ($.b == 3.14)", {encodeKey("a") + numSuffix(0), encodeKey("b") + numSuffix(3.14)});
        ValidateJsonValue("strict (\"hello\" == $.a) && (null == $.b)", {encodeKey("a") + strSuffix("hello"), encodeKey("b") + nullSuffix});

        // Deeper member access paths
        ValidateJsonValue("strict ($.a.b.c == 1) && ($.x.y == \"z\")", {encodeKey("a") + encodeKey("b") + encodeKey("c") + numSuffix(1), encodeKey("x") + encodeKey("y") + strSuffix("z")});
        ValidateJsonValue("strict ($.aba.\"caba\" == true) && ($.d.e.f == 0)", {encodeKey("aba") + encodeKey("caba") + boolTrueSuffix, encodeKey("d") + encodeKey("e") + encodeKey("f") + numSuffix(0)});
        ValidateJsonValue("strict ($.a.b.c.d == 0) && ($.p.q == null)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d") + numSuffix(0), encodeKey("p") + encodeKey("q") + nullSuffix});

        // Context object as path (empty prefix)
        ValidateJsonValue("strict ($ == \"root\") && ($.b == 2)", {strSuffix("root"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($ == null) && ($ == 42)", {nullSuffix, numSuffix(42)});

        // Array access
        ValidateJsonValue("strict ($.key[0] == 1) && ($.arr[last] == true)", {encodeKey("key") + arrayItemSuffix + numSuffix(1), encodeKey("arr") + arrayItemSuffix + boolTrueSuffix});
        ValidateJsonValue("strict ($.key[1, 2, 3] == null) && ($.b[0 to last] == \"x\")", {encodeKey("key") + arrayItemSuffix + nullSuffix, encodeKey("b") + arrayItemSuffix + strSuffix("x")});
        ValidateJsonValue("strict ($.a.b[0].c == \"x\") && ($.d.e == false)", {encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c") + strSuffix("x"), encodeKey("d") + encodeKey("e") + boolFalseSuffix});
        ValidateJsonValue("strict ($.key[0].sub == \"x\") && ($.v == 1)", {encodeKey("key") + arrayItemSuffix + encodeKey("sub") + strSuffix("x"), encodeKey("v") + numSuffix(1)});

        // Wildcard member access (finishes collection, no literal suffix appended)
        ValidateJsonValue("strict ($.a.* == \"x\") && ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.* == \"x\") && ($.b == 2)", {encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.a == 1) && ($.b.* == \"z\")", {encodeKey("a") + numSuffix(1), encodeKey("b")});
        ValidateJsonValue("strict ($.a.b.* == true) && ($.c.* == null)", {encodeKey("a") + encodeKey("b"), encodeKey("c")});

        // Wildcard array access
        ValidateJsonValue("strict ($.key[*] == \"x\") && ($.b == 2)", {encodeKey("key") + arrayItemSuffix + strSuffix("x"), encodeKey("b") + numSuffix(2)});

        // Methods (finish the path, no literal suffix appended)
        ValidateJsonValue("strict ($.key.size() == 3) && ($.val == 1)", {encodeKey("key"), encodeKey("val") + numSuffix(1)});
        ValidateJsonValue("strict ($.a == \"x\") && ($.key.abs() == 2)", {encodeKey("a") + strSuffix("x"), encodeKey("key")});
        ValidateJsonValue("strict ($.a.floor() == 0) && ($.b.type() == \"number\")", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.key.keyvalue().name == \"x\") && ($.v == true)", {encodeKey("key"), encodeKey("v") + boolTrueSuffix});
        ValidateJsonValue("strict ($.a.size() == 5) && ($.b.ceiling() == 3)", {encodeKey("a"), encodeKey("b")});

        // StartsWith predicate on left and right
        ValidateJsonValue("strict ($.a starts with \"x\") && ($.b == 1)", {encodeKey("a"), encodeKey("b") + numSuffix(1)});
        ValidateJsonValue("strict ($.a == 1) && ($.b starts with \"y\")", {encodeKey("a") + numSuffix(1), encodeKey("b")});
        ValidateJsonValue("strict ($.a.b.c starts with \"abc\") && ($.d[0] == null)", {encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("d") + arrayItemSuffix + nullSuffix});
        ValidateJsonValue("strict ($.a starts with \"x\") && ($.b starts with \"y\")", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a.* starts with \"x\") && ($.b == 1)", {encodeKey("a"), encodeKey("b") + numSuffix(1)});

        // LikeRegex predicate on left and right
        ValidateJsonValue("strict ($.a like_regex \".*\") && ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.a == \"x\") && ($.b like_regex \"[a-z]+\")", {encodeKey("a") + strSuffix("x"), encodeKey("b")});
        ValidateJsonValue("strict ($.a like_regex \"x.*\") && ($.b like_regex \"y.*\")", {encodeKey("a"), encodeKey("b")});

        // Exists predicate on left and right
        ValidateJsonValue("strict exists($.a) && ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.a == 1) && exists($.b.c)", {encodeKey("a") + numSuffix(1), encodeKey("b") + encodeKey("c")});
        ValidateJsonValue("strict exists($.a.b[0]) && exists($.c.*)", {encodeKey("a") + encodeKey("b") + arrayItemSuffix, encodeKey("c")});
        ValidateJsonValue("strict exists($.a.key.size()) && ($.b == true)", {encodeKey("a") + encodeKey("key"), encodeKey("b") + boolTrueSuffix});

        // Unary arithmetic operand (finishes, no literal suffix)
        ValidateJsonValue("strict (-$.a == 1) && ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.a == 1) && (+$.b.c == 0)", {encodeKey("a") + numSuffix(1), encodeKey("b") + encodeKey("c")});
        ValidateJsonValue("strict (-$.a.b.* == 1) && ($.c == 2)", {encodeKey("a") + encodeKey("b"), encodeKey("c") + numSuffix(2)});

        // Binary arithmetic with two paths as AND operand (two path tokens, And mode, compatible with AND)
        // $.a + $.b == "x" is parsed as ($.a + $.b) == "x", arithmetic finishes, literal not appended
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
        ValidateJsonValue("strict ($.c == 1) && ($.a + $.b == \"x\")", {encodeKey("c") + numSuffix(1), encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a.size() + $.b.abs() == 5) && ($.c == null)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + nullSuffix});

        // Chained AND (left-associative)
        ValidateJsonValue("strict ($.a == 1) && ($.b == 2) && ($.c == 3)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict ($.a == 1) && ($.b == 2) && ($.c == 3) && ($.d == 4)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        ValidateJsonValue("strict ($.a starts with \"x\") && ($.b == 1) && exists($.c.d)", {encodeKey("a"), encodeKey("b") + numSuffix(1), encodeKey("c") + encodeKey("d")});
        ValidateJsonValue("strict ($.a like_regex \".*\") && ($.b.* == 2) && ($.c.size() == 3) && exists($.d)", {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")});

        // Same path on both sides (two different equality conditions)
        ValidateJsonValue("strict ($.a == 1) && ($.a == 2)", {encodeKey("a") + numSuffix(1), encodeKey("a") + numSuffix(2)});

        // Variables with literals
        ValidateError("($var == 1) && ($.b == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a == 1) && ($var == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($var == 1) && ($var == 2)", compError, {}, {}, ECallableType::JsonValue);

        // Predicates are not allowed in JsonExists
        ValidateError("($.a == 10) && ($.b == 20)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("($.a starts with \"x\") && ($.b == 1)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("exists($.a) && exists($.b)", predError, {}, {}, ECallableType::JsonExists);

        // Mixing AND and OR: OR wins, all tokens become OR
        ValidateJsonValue("strict (($.a == 1) && ($.b == 2)) || ($.c == 3)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict ($.a == 1) || (($.b == 2) && ($.c == 3))", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict ($.a == 1) && (($.b == 2) || ($.c == 3))", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict (($.a == 1) || ($.b == 2)) && ($.c == 3)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});

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
        ValidateJsonValue("strict ($.a == 10) || ($.b == \"hello\")", {encodeKey("a") + numSuffix(10), encodeKey("b") + strSuffix("hello")});
        ValidateJsonValue("strict (42 == $.key) || ($.val == true)", {encodeKey("key") + numSuffix(42), encodeKey("val") + boolTrueSuffix});
        ValidateJsonValue("strict ($.x == null) || ($.y == false)", {encodeKey("x") + nullSuffix, encodeKey("y") + boolFalseSuffix});
        ValidateJsonValue("strict ($.a == 0) || ($.b == 3.14)", {encodeKey("a") + numSuffix(0), encodeKey("b") + numSuffix(3.14)});
        ValidateJsonValue("strict ($ == \"root\") || ($.b == 2)", {strSuffix("root"), encodeKey("b") + numSuffix(2)});

        // Deeper member access paths
        ValidateJsonValue("strict ($.a.b.c == 1) || ($.x.y == \"z\")", {encodeKey("a") + encodeKey("b") + encodeKey("c") + numSuffix(1), encodeKey("x") + encodeKey("y") + strSuffix("z")});
        ValidateJsonValue("strict ($.aba.\"caba\" == true) || ($.d.e.f == 0)", {encodeKey("aba") + encodeKey("caba") + boolTrueSuffix, encodeKey("d") + encodeKey("e") + encodeKey("f") + numSuffix(0)});

        // Array access
        ValidateJsonValue("strict ($.key[0] == 1) || ($.arr[last] == true)", {encodeKey("key") + arrayItemSuffix + numSuffix(1), encodeKey("arr") + arrayItemSuffix + boolTrueSuffix});
        ValidateJsonValue("strict ($.key[1, 2, 3] == null) || ($.b[0 to last] == \"x\")", {encodeKey("key") + arrayItemSuffix + nullSuffix, encodeKey("b") + arrayItemSuffix + strSuffix("x")});
        ValidateJsonValue("strict ($.a.b[0].c == \"x\") || ($.d.e == false)", {encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c") + strSuffix("x"), encodeKey("d") + encodeKey("e") + boolFalseSuffix});

        // Wildcard member and array access (finishes, no literal suffix)
        ValidateJsonValue("strict ($.a.* == \"x\") || ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.* == \"x\") || ($.b == 2)", {""});
        ValidateJsonValue("strict ($.a == 1) || ($.b.* == \"z\")", {encodeKey("a") + numSuffix(1), encodeKey("b")});
        ValidateJsonValue("strict ($.key[*] == \"x\") || ($.b == 2)", {encodeKey("key") + arrayItemSuffix + strSuffix("x"), encodeKey("b") + numSuffix(2)});

        // Methods (finish the path, no literal suffix)
        ValidateJsonValue("strict ($.key.size() == 3) || ($.val == 1)", {encodeKey("key"), encodeKey("val") + numSuffix(1)});
        ValidateJsonValue("strict ($.a == \"x\") || ($.key.abs() == 2)", {encodeKey("a") + strSuffix("x"), encodeKey("key")});
        ValidateJsonValue("strict ($.a.floor() == 0) || ($.b.type() == \"number\")", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.key.keyvalue().name == \"x\") || ($.v == true)", {encodeKey("key"), encodeKey("v") + boolTrueSuffix});

        // StartsWith predicate
        ValidateJsonValue("strict ($.a starts with \"x\") || ($.b == 1)", {encodeKey("a"), encodeKey("b") + numSuffix(1)});
        ValidateJsonValue("strict ($.a == 1) || ($.b starts with \"y\")", {encodeKey("a") + numSuffix(1), encodeKey("b")});
        ValidateJsonValue("strict ($.a starts with \"x\") || ($.b starts with \"y\")", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a.* starts with \"x\") || ($.b[0] == 1)", {encodeKey("a"), encodeKey("b") + arrayItemSuffix + numSuffix(1)});

        // LikeRegex predicate
        ValidateJsonValue("strict ($.a like_regex \".*\") || ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.a == \"x\") || ($.b like_regex \"[a-z]+\")", {encodeKey("a") + strSuffix("x"), encodeKey("b")});
        ValidateJsonValue("strict ($.a like_regex \"x.*\") || ($.b like_regex \"y.*\")", {encodeKey("a"), encodeKey("b")});

        // Exists predicate
        ValidateJsonValue("strict exists($.a) || ($.b == 2)", {encodeKey("a"), encodeKey("b") + numSuffix(2)});
        ValidateJsonValue("strict ($.a == 1) || exists($.b.c)", {encodeKey("a") + numSuffix(1), encodeKey("b") + encodeKey("c")});
        ValidateJsonValue("strict exists($.a.b[0]) || exists($.c.*)", {encodeKey("a") + encodeKey("b") + arrayItemSuffix, encodeKey("c")});

        // Same path on both sides, different values
        ValidateJsonValue("strict ($.a == 1) || ($.a == 2)", {encodeKey("a") + numSuffix(1), encodeKey("a") + numSuffix(2)});
        ValidateJsonValue("strict ($.key == \"a\") || ($.key == \"b\") || ($.key == \"c\")",
            {encodeKey("key") + strSuffix("a"), encodeKey("key") + strSuffix("b"), encodeKey("key") + strSuffix("c")});

        // Chained OR (left-associative)
        ValidateJsonValue("strict ($.a == 1) || ($.b == 2) || ($.c == 3)",{encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict ($.a == 1) || ($.b == 2) || ($.c == 3) || ($.d == 4)",{encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        ValidateJsonValue("strict ($.a starts with \"x\") || ($.b == 1) || exists($.c.d)",{encodeKey("a"), encodeKey("b") + numSuffix(1), encodeKey("c") + encodeKey("d")});

        // Variable on left or right side
        ValidateError("($var == 1) || ($.b == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($.a == 1) || ($var == 2)", compError, {}, {}, ECallableType::JsonValue);
        ValidateError("($var == 1) || ($var == 2)", compError, {}, {}, ECallableType::JsonValue);

        // Predicates not allowed in Context for JsonExists
        ValidateError("($.a == 10) || ($.b == 20)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("($.a starts with \"x\") || ($.b == 1)", predError, {}, {}, ECallableType::JsonExists);
        ValidateError("exists($.a) || exists($.b)", predError, {}, {}, ECallableType::JsonExists);

        // Arithmetic with multiple paths (And mode) mixed with OR: OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
        ValidateJsonValue("strict ($.c == 1) || ($.a + $.b == \"x\")", {encodeKey("c") + numSuffix(1), encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a.size() + $.b.abs() == 5) || ($.c == null)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + nullSuffix});

        // Mixing AND and OR: OR wins
        ValidateJsonValue("strict (($.a == 1) && ($.b == 2)) || ($.c == 3)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict ($.a == 1) || (($.b == 2) && ($.c == 3))", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict ($.a == 1) && (($.b == 2) || ($.c == 3))", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});
        ValidateJsonValue("strict (($.a == 1) || ($.b == 2)) && ($.c == 3)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3)});

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
        ValidateJsonValue("strict (($.a == 1) && ($.b == 2)) && (($.c == 3) && ($.d == 4))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // ((A || B) || (C || D)) - Or+Or combined at top level
        ValidateJsonValue("strict (($.a == 1) || ($.b == 2)) || (($.c == 3) || ($.d == 4))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // A && (B && (C && D))
        ValidateJsonValue("strict ($.a == 1) && (($.b == 2) && (($.c == 3) && ($.d == 4)))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // A || (B || (C || D))
        ValidateJsonValue("strict ($.a == 1) || (($.b == 2) || (($.c == 3) || ($.d == 4)))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});

        // Arithmetic with two paths (mode=And) is compatible with AND chains
        // Two arithmetic operands inside AND: ($.a+$.b == "x") && ($.c+$.d == "y")
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c + $.d == \"y\")",
            {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d")});
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c + $.d == \"y\") && ($.e == 1)",
            {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e") + numSuffix(1)});

        // (A && B) || (C && D): OR wins, all tokens become OR
        ValidateJsonValue("strict (($.a == 1) && ($.b == 2)) || (($.c == 3) && ($.d == 4))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // (A || B) && (C || D): OR wins, all tokens become OR
        ValidateJsonValue("strict (($.a == 1) || ($.b == 2)) && (($.c == 3) || ($.d == 4))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});

        // A && (B || (C || D)): OR wins
        ValidateJsonValue("strict ($.a == 1) && (($.b == 2) || (($.c == 3) || ($.d == 4)))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // A && (B && (C || D)): OR wins
        ValidateJsonValue("strict ($.a == 1) && (($.b == 2) && (($.c == 3) || ($.d == 4)))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});

        // A || (B && (C && D)): OR wins
        ValidateJsonValue("strict ($.a == 1) || (($.b == 2) && (($.c == 3) && ($.d == 4)))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // A || (B || (C && D)): OR wins
        ValidateJsonValue("strict ($.a == 1) || (($.b == 2) || (($.c == 3) && ($.d == 4)))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});

        // && binds tighter than ||
        // A && B && C || D  =>  ((A && B) && C) || D: OR wins
        ValidateJsonValue("strict ($.a == 1) && ($.b == 2) && ($.c == 3) || ($.d == 4)",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // A || B || C && D  =>  (A || B) || (C && D): OR wins
        ValidateJsonValue("strict ($.a == 1) || ($.b == 2) || ($.c == 3) && ($.d == 4)",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});
        // A || B && C || D  =>  A || (B && C) || D  =>  (A || (B && C)) || D: OR wins
        ValidateJsonValue("strict ($.a == 1) || ($.b == 2) && ($.c == 3) || ($.d == 4)",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2), encodeKey("c") + numSuffix(3), encodeKey("d") + numSuffix(4)});

        // ($.a + $.b == "x") has mode=And, nested as part of OR: OR wins
        ValidateJsonValue("strict (($.a + $.b == \"x\") || ($.c == 1)) && ($.d == 2)",
            {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1), encodeKey("d") + numSuffix(2)});
        ValidateJsonValue("strict ($.d == 2) && (($.a + $.b == \"x\") || ($.c == 1))",
            {encodeKey("d") + numSuffix(2), encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});

        // ($.a[0].b == 1) && ((-$.c.d == 2) && ($.e.* starts with "x"))
        // -$.c.d == 2: unary makes path Finished, no literal appended, token encodeKey("c") + encodeKey("d")
        // $.e.* starts with "x": wildcard makes path Finished, token encodeKey("e")
        ValidateJsonValue("strict ($.a[0].b == 1) && ((-$.c.d == 2) && ($.e.* starts with \"x\"))",
            {encodeKey("a") + arrayItemSuffix + encodeKey("b") + numSuffix(1), encodeKey("c") + encodeKey("d"), encodeKey("e")});

        // ($.a.b.c starts with "x") && ($.d.size() == 3) && ($.e[0] + $.f.abs() == 5)
        // $.e[0] + $.f.abs(): both sub-paths collected, mode=And, arithmetic Finished, no literal appended
        ValidateJsonValue("strict ($.a.b.c starts with \"x\") && ($.d.size() == 3) && ($.e[0] + $.f.abs() == 5)",
            {encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("d"), encodeKey("e") + arrayItemSuffix, encodeKey("f")});

        // (exists($.a.b[0]) && ($.c.d == "x")) && (($.e like_regex ".*") && (+$.f.g.h == 0))
        // +$.f.g.h == 0: unary makes path Finished, no literal appended, token encodeKey("f") + encodeKey("g") + encodeKey("h")
        ValidateJsonValue("strict (exists($.a.b[0]) && ($.c.d == \"x\")) && (($.e like_regex \".*\") && (+$.f.g.h == 0))",
            {encodeKey("a") + encodeKey("b") + arrayItemSuffix, encodeKey("c") + encodeKey("d") + strSuffix("x"), encodeKey("e"), encodeKey("f") + encodeKey("g") + encodeKey("h")});

        // All five binary arithmetic ops as AND operands - each produces mode=And, all compatible
        ValidateJsonValue("strict ($.a + $.b == \"x\") && ($.c - $.d == \"y\") && ($.e * $.f == \"z\")",
            {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e"), encodeKey("f")});
        ValidateJsonValue("strict ($.a / $.b == \"x\") && ($.c % $.d == \"y\") && ($.e == 1)",
            {encodeKey("a"), encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e") + numSuffix(1)});

        // Unary on both sides of AND - each makes path Finished, literal not appended
        ValidateJsonValue("strict (-$.a.b == 1) && (+$.c.d.e == 2) && (-$.f[0] == 3)",
            {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d") + encodeKey("e"), encodeKey("f") + arrayItemSuffix});

        // ($.a.size() == 3) || (($.b[0] == "x") || (-$.c.d.e == 1))
        // Right inner OR: two NotSet operands, Or, outer OR: left=NotSet, right=Or, Or
        ValidateJsonValue("strict ($.a.size() == 3) || (($.b[0] == \"x\") || (-$.c.d.e == 1))",
            {encodeKey("a"), encodeKey("b") + arrayItemSuffix + strSuffix("x"), encodeKey("c") + encodeKey("d") + encodeKey("e")});

        // ($.a starts with "x") || ($.b.* == "y") || exists($.c.d[0])
        // $.b.* == "y": wildcard Finished, no literal, token encodeKey("b")
        ValidateJsonValue("strict ($.a starts with \"x\") || ($.b.* == \"y\") || exists($.c.d[0])",
            {encodeKey("a"), encodeKey("b"), encodeKey("c") + encodeKey("d") + arrayItemSuffix});

        // ($.a[1 to 3].b like_regex ".*") || ((-$.c == 0) || ($.d.e.keyvalue() == "f"))
        // $.d.e.keyvalue() == "f": method Finished, no literal, token encodeKey("d") + encodeKey("e")
        ValidateJsonValue("strict ($.a[1 to 3].b like_regex \".*\") || ((-$.c == 0) || ($.d.e.keyvalue() == \"f\"))",
            {encodeKey("a") + arrayItemSuffix + encodeKey("b"), encodeKey("c"), encodeKey("d") + encodeKey("e")});

        // Unary on both sides of OR
        ValidateJsonValue("strict (-$.a.b == 1) || (+$.c.d == 2) || (-$.e.f.g == 3)",
            {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d"), encodeKey("e") + encodeKey("f") + encodeKey("g")});

        // ((-$.a.b == 1) && ($.c.size() == 2)) || (exists($.d) && ($.e starts with "x")): OR wins
        ValidateJsonValue("strict ((-$.a.b == 1) && ($.c.size() == 2)) || (exists($.d) && ($.e starts with \"x\"))",
            {encodeKey("a") + encodeKey("b"), encodeKey("c"), encodeKey("d"), encodeKey("e")});

        // ($.a like_regex "x.*") || (($.b.abs() == 1) && ($.c[0] starts with "y")): OR wins
        ValidateJsonValue("strict ($.a like_regex \"x.*\") || ($.b.abs() == 1) && ($.c[0] starts with \"y\")",
            {encodeKey("a"), encodeKey("b"), encodeKey("c") + arrayItemSuffix});

        // (($.a + $.b == "x") && (-$.c.d == 1)) || ($.e.f == 2): OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") && (-$.c.d == 1) || ($.e.f == 2)",
            {encodeKey("a"), encodeKey("b"), encodeKey("c") + encodeKey("d"), encodeKey("e") + encodeKey("f") + numSuffix(2)});

        // ($.a[0] starts with "x") || (($.b.c + $.d.e == 3) && exists($.f.g.*)): OR wins
        ValidateJsonValue("strict ($.a[0] starts with \"x\") || ($.b.c + $.d.e == 3) && exists($.f.g.*)",
            {encodeKey("a") + arrayItemSuffix, encodeKey("b") + encodeKey("c"), encodeKey("d") + encodeKey("e"), encodeKey("f") + encodeKey("g")});

        // (($.a.b[0].c == 1) && ($.d.* starts with "x")) || (-$.e.f.g == 2): OR wins
        ValidateJsonValue("strict ($.a.b[0].c == 1) && ($.d.* starts with \"x\") || (-$.e.f.g == 2)",
            {encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c") + numSuffix(1), encodeKey("d"), encodeKey("e") + encodeKey("f") + encodeKey("g")});

        // (($.a like_regex ".*") && ($.b.size() == 0) && ($.c[last] == true)) || ($.d.floor() == 0): OR wins
        ValidateJsonValue("strict ($.a like_regex \".*\") && ($.b.size() == 0) && ($.c[last] == true) || ($.d.floor() == 0)",
            {encodeKey("a"), encodeKey("b"), encodeKey("c") + arrayItemSuffix + boolTrueSuffix, encodeKey("d")});

        // ($.a == 1) || ((-$.b.c.d == 2) && ($.e.size() == 3)): OR wins
        ValidateJsonValue("strict ($.a == 1) || ((-$.b.c.d == 2) && ($.e.size() == 3))",
            {encodeKey("a") + numSuffix(1), encodeKey("b") + encodeKey("c") + encodeKey("d"), encodeKey("e")});

        // All five arithmetic ops with two paths inside OR: OR wins
        ValidateJsonValue("strict ($.a + $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
        ValidateJsonValue("strict ($.a - $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
        ValidateJsonValue("strict ($.a * $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
        ValidateJsonValue("strict ($.a / $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
        ValidateJsonValue("strict ($.a % $.b == \"x\") || ($.c == 1)", {encodeKey("a"), encodeKey("b"), encodeKey("c") + numSuffix(1)});
    }

    // Filter predicates allow the collector to use predicate constraints for path narrowing
    // $.a ? (@.b == 10)  =>  [encodeKey("a") + encodeKey("b") + numSuffix(10)]
    Y_UNIT_TEST(CollectPath_FilterPredicate) {
        // Basic: simple path before ?, simple equality predicate
        ValidateJsonExists("strict $.a ? (@.b == 10)", {encodeKey("a") + encodeKey("b") + numSuffix(10)});
        ValidateJsonExists("strict $.a ? (@.b == -(+10))", {encodeKey("a") + encodeKey("b") + numSuffix(-10)});
        ValidateJsonExists("strict $.a ? (@.b == \"hello\")", {encodeKey("a") + encodeKey("b") + strSuffix("hello")});
        ValidateJsonExists("strict $.a ? (@.b == true)", {encodeKey("a") + encodeKey("b") + boolTrueSuffix});
        ValidateJsonExists("strict $.a ? (@.b == false)", {encodeKey("a") + encodeKey("b") + boolFalseSuffix});
        ValidateJsonExists("strict $.a ? (@.b == null)", {encodeKey("a") + encodeKey("b") + nullSuffix});
        ValidateJsonExists("strict $.a ? (-10 == @.b)", {encodeKey("a") + encodeKey("b") + numSuffix(-10)});
        ValidateJsonExists("strict $.a ? (\"hello\" == @.b)", {encodeKey("a") + encodeKey("b") + strSuffix("hello")});
        ValidateJsonExists("strict $ ? (@.a == 1)", {encodeKey("a") + numSuffix(1)});
        ValidateJsonExists("strict $ ? (@.key == \"x\")", {encodeKey("key") + strSuffix("x")});

        // @ == literal: equality on the filter object itself, prefix becomes the full token
        ValidateJsonExists("strict $.a ? (@ == \"hello\")", {encodeKey("a") + strSuffix("hello")});
        ValidateJsonExists("strict $.a ? (@ == -42)", {encodeKey("a") + numSuffix(-42)});
        ValidateJsonExists("strict $.a ? (@ == true)", {encodeKey("a") + boolTrueSuffix});
        ValidateJsonExists("strict $.a ? (@ == null)", {encodeKey("a") + nullSuffix});
        ValidateJsonExists("strict $.a.b ? (@ == \"x\")", {encodeKey("a") + encodeKey("b") + strSuffix("x")});
        ValidateJsonExists("strict $ ? (@ == 0)", {numSuffix(0)});

        // @ starts with / like_regex: predicate on the filter object itself
        ValidateJsonExists("strict $.a ? (@ starts with \"x\")", {encodeKey("a")});
        ValidateJsonExists("strict $.a.b ? (@ starts with \"hello\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@ like_regex \"[a-z]+\")", {encodeKey("a")});
        ValidateJsonExists("strict $.a.b.c ? (@ like_regex \".*\")", {encodeKey("a") + encodeKey("b") + encodeKey("c")});

        // exists(@): filter predicate is exists on the filter object
        ValidateJsonExists("strict $.a ? (exists(@))", {encodeKey("a")});
        ValidateJsonExists("strict $.a.b ? (exists(@))", {encodeKey("a") + encodeKey("b")});

        // @[0].b: array subscript on filter object, then member access
        ValidateJsonExists("strict $.a ? (@[0].b == 1)", {encodeKey("a") + arrayItemSuffix + encodeKey("b") + numSuffix(1)});
        ValidateJsonExists("strict $.a ? (@[last].b == \"x\")", {encodeKey("a") + arrayItemSuffix + encodeKey("b") + strSuffix("x")});
        ValidateJsonExists("strict $.a ? (@[*].b == true)", {encodeKey("a") + arrayItemSuffix + encodeKey("b") + boolTrueSuffix});

        // FilterObject via wildcard member access, finishes the path, so literal is not appended
        ValidateJsonExists("strict $.a ? (@.* == \"x\")", {encodeKey("a")});
        ValidateJsonExists("strict $.a ? (@.b.* starts with \"x\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.* == 1)", {encodeKey("a") + encodeKey("b")});

        // Methods finish the path, so literal is not appended
        ValidateJsonExists("strict $.a ? (@.b.size() == 3)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.abs() == 1)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.floor() == 0)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.ceiling() == 5)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.type() == \"number\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.double() == 1)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.keyvalue() == \"x\")", {encodeKey("a") + encodeKey("b")});
        // method result checked in AND: both paths collected
        ValidateJsonExists("strict $.a ? (@.b.size() == +3 && @.c == -1)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c") + numSuffix(-1)});

        // Unary finishes the path, so literal is not appended
        ValidateJsonExists("strict $.a ? (-@.b == 5)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (+@.b == 5)", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (-@.b.c.d == 0)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d")});
        ValidateJsonExists("strict $.a ? (-@.b == 5 && @.c == 1)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c") + numSuffix(1)});
        ValidateJsonExists("strict $.a ? (-@.b == 5 || +@.c == 2)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});

        // All five arithmetic operators: path + path, both tokens, no literal suffix
        ValidateJsonExists("strict $.key ? (@.a + @.b == +5)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        ValidateJsonExists("strict $.key ? (@.a - @.b == 0)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        ValidateJsonExists("strict $.key ? (@.a * @.b == 10)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        ValidateJsonExists("strict $.key ? (@.a / @.b == 2)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        ValidateJsonExists("strict $.key ? (@.a % @.b == 1)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b")});
        // path + literal: literal side dropped by CollectArithmeticOperand, only path token
        ValidateJsonExists("strict $.key ? (@.a + 1 == 5)", {encodeKey("key") + encodeKey("a")});
        ValidateJsonExists("strict $.key ? (1 - @.a == 5)", {encodeKey("key") + encodeKey("a")});
        ValidateJsonExists("strict $.key ? (@.a * (-2) == -10)", {encodeKey("key") + encodeKey("a")});
        // three paths via chained arithmetic
        ValidateJsonExists("strict $.key ? (@.a + @.b + @.c == 0)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c")});
        // arithmetic with deeper filter object paths
        ValidateJsonExists("strict $.x ? (@.a.b + @.c.d == 0)", {encodeKey("x") + encodeKey("a") + encodeKey("b"), encodeKey("x") + encodeKey("c") + encodeKey("d")});
        // arithmetic with two paths produces mode=And, compatible with AND
        ValidateJsonExists("strict $.key ? (@.a + @.b == 5 && @.c == 1)", {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c") + numSuffix(1)});

        // StartsWith finishes the path
        ValidateJsonExists("strict $.a ? (@.b starts with \"x\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a.b.c ? (@.d starts with \"abc\")", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d")});
        ValidateJsonValue("strict $.a ? (@.b starts with \"x\")", {encodeKey("a") + encodeKey("b")});

        // LikeRegex finishes the path
        ValidateJsonExists("strict $.a ? (@.b like_regex \".*\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b.c like_regex \"[0-9]+\")", {encodeKey("a") + encodeKey("b") + encodeKey("c")});
        ValidateJsonValue("strict $.a ? (@.b like_regex \"[a-z]+\")", {encodeKey("a") + encodeKey("b")});

        // Exists finishes the path
        ValidateJsonExists("strict $.a ? (exists(@.b))", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (exists(@.b.c.d))", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d")});
        ValidateJsonExists("strict $.a ? (exists(@.b[0]))", {encodeKey("a") + encodeKey("b") + arrayItemSuffix});
        ValidateJsonValue("strict $.a ? (exists(@.b))", {encodeKey("a") + encodeKey("b")});

        // Deeper paths before and inside filter
        ValidateJsonExists("strict $.a.b ? (@.c == \"x\")", {encodeKey("a") + encodeKey("b") + encodeKey("c") + strSuffix("x")});
        ValidateJsonExists("strict $.a ? (@.b.c == true)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + boolTrueSuffix});
        ValidateJsonExists("strict $.a.b.c ? (@.d.e == null)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d") + encodeKey("e") + nullSuffix});
        ValidateJsonExists("strict $.a ? (@.b.c.d == 3.14)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d") + numSuffix(3.14)});

        // Array access in the input path
        ValidateJsonExists("strict $.a[0] ? (@.b == 1)", {encodeKey("a") + arrayItemSuffix + encodeKey("b") + numSuffix(1)});
        ValidateJsonExists("strict $.key[1, 2, 3] ? (@.sub == \"x\")", {encodeKey("key") + arrayItemSuffix + encodeKey("sub") + strSuffix("x")});
        ValidateJsonExists("strict $.a[0 to last] ? (@.b == true)", {encodeKey("a") + arrayItemSuffix + encodeKey("b") + boolTrueSuffix});


        // AND: Two equality conditions
        ValidateJsonExists("strict $.a ? (@.b == +10 && @.c == +13)", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c") + numSuffix(13)});
        ValidateJsonExists("strict $.a.b ? (@.c == \"x\" && @.d == 1)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + strSuffix("x"), encodeKey("a") + encodeKey("b") + encodeKey("d") + numSuffix(1)});
        ValidateJsonExists("strict $ ? (@.x == null && @.y == true)", {encodeKey("x") + nullSuffix, encodeKey("y") + boolTrueSuffix});

        // AND: Three conditions chained with AND
        ValidateJsonExists("strict $.key ? (@.a == 1 && @.b == -2 && @.c == 3)", {encodeKey("key") + encodeKey("a") + numSuffix(1), encodeKey("key") + encodeKey("b") + numSuffix(-2), encodeKey("key") + encodeKey("c") + numSuffix(3)});

        // AND: Four conditions chained with AND (two pairs)
        ValidateJsonExists("strict $.a ? ((@.b == 1 && @.c == 2) && (@.d == 3 && @.e == 4))",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2),
             encodeKey("a") + encodeKey("d") + numSuffix(3), encodeKey("a") + encodeKey("e") + numSuffix(4)});

        // AND: mixing predicate types
        ValidateJsonExists("strict $.a ? ((@.b == 1) && (@.c starts with \"x\") && (@.d like_regex \"y.*\") && exists(@.e))",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d"), encodeKey("a") + encodeKey("e")});

        // AND: mixing methods, unary, wildcard, equality
        ValidateJsonExists("strict $.key ? ((@.a == 1) && (@.b.size() == 3) && (-@.c == 0) && (@.d.* starts with \"x\"))",
            {encodeKey("key") + encodeKey("a") + numSuffix(1), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c"), encodeKey("key") + encodeKey("d")});

        // OR: Two equality conditions
        ValidateJsonExists("strict $.a ? ((@.b == 10) || (@.c == 13))", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c") + numSuffix(13)});
        ValidateJsonExists("strict $.key ? ((@.x == \"a\") || (@.y == \"b\"))", {encodeKey("key") + encodeKey("x") + strSuffix("a"), encodeKey("key") + encodeKey("y") + strSuffix("b")});

        // OR: Three conditions chained with OR
        ValidateJsonExists("strict $.a ? ((@.b == 1) || (@.c == 2) || (@.d == 3))", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)});

        // OR: Four conditions chained with OR (two pairs)
        ValidateJsonExists("strict $.a ? ((@.b == 1 || @.c == 2) || (@.d == 3 || @.e == 4))",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2),
             encodeKey("a") + encodeKey("d") + numSuffix(3), encodeKey("a") + encodeKey("e") + numSuffix(4)});

        // OR: mixing predicate types
        ValidateJsonExists("strict $.a ? ((@.b == 1) || (@.c starts with \"x\") || (@.d like_regex \"y.*\") || exists(@.e))",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d"), encodeKey("a") + encodeKey("e")});

        // AND on left of OR: OR wins
        ValidateJsonExists("strict $.a ? ((@.b == 1 && @.c == 2) || @.d == 3)", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)});
        // OR on right of AND: OR wins
        ValidateJsonExists("strict $.a ? (@.b == 1 && ((@.c == 2) || (@.d == 3)))", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)});
        // Arithmetic with two paths (mode=And) on left of OR: OR wins
        ValidateJsonExists("strict $.a ? ((@.b + @.c == 5) || @.d == 3)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d") + numSuffix(3)});
        // Arithmetic with two paths (mode=And) on right of OR: OR wins
        ValidateJsonExists("strict $.a ? (@.b == 1 || (@.c + @.d == 5))", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});
        // (A || B) inside AND chain: OR wins
        ValidateJsonExists("strict $.a ? (((@.b == 1) || (@.c == 2)) && @.d == 3)", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)});

        // Finished input path (wildcard/method) - filter predicate can't narrow
        ValidateJsonExists("strict $.* ? (@.b == 1)", {""});
        ValidateJsonExists("strict $.a.* ? (@.b == 1)", {encodeKey("a")});
        ValidateJsonExists("strict $.a.b.* ? (@.c == \"x\")", {encodeKey("a") + encodeKey("b")});

        // Filter is Finished - further member access is dropped
        ValidateJsonExists("strict $.a ? (@.b == 10) .c", {encodeKey("a") + encodeKey("b") + numSuffix(10)});

        // JsonExists explicitly: filter allows all predicate types even though
        ValidateJsonExists("strict $.a ? (@.b == 10)", {encodeKey("a") + encodeKey("b") + numSuffix(10)});
        ValidateJsonExists("strict $.a ? (@.b starts with \"x\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b like_regex \".*\")", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (exists(@.b))", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b == 10 && @.c starts with \"x\" && exists(@.d))", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});
        ValidateJsonExists("strict $.a ? ((@.b == 10) || (@.c starts with \"x\") || exists(@.d))", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});
        ValidateJsonExists("strict $.a ? (@.b + @.c == 5)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});

        // JsonValue also works (filter allowed predicates in both callable types)
        ValidateJsonValue("strict $.a ? (@.b == 10)", {encodeKey("a") + encodeKey("b") + numSuffix(10)});
        ValidateJsonValue("strict $.a ? (@.b == 10 && @.c == 13)", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c") + numSuffix(13)});
        ValidateJsonValue("strict $.a ? ((@.b == 10) || (@.c == 13))", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c") + numSuffix(13)});
        ValidateJsonValue("strict $.a ? (@.b + @.c == 5)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});
        ValidateJsonValue("strict $.a ? (@.b == @.c)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});
        ValidateJsonValue("strict $.a ? (@.b == $.c)", {encodeKey("a") + encodeKey("b"), encodeKey("c")});
        ValidateJsonValue("strict $.a ? (@ == @.b)", {encodeKey("a"), encodeKey("a") + encodeKey("b")});

        // Nested filter: exists(@.b ? (@.c == 1)) inside an outer filter
        ValidateJsonExists("strict $.a ? (exists(@.b ? (@.c == 1)))", {encodeKey("a") + encodeKey("b") + encodeKey("c") + numSuffix(1)});
        ValidateJsonExists("strict $.key ? (exists(@.sub ? (@.val == \"x\")))", {encodeKey("key") + encodeKey("sub") + encodeKey("val") + strSuffix("x")});

        // @ outside filter context is an error
        ValidateError("@", filterError);
        ValidateError("@.a", filterError);
        ValidateError("@.a == 1", filterError, {}, {}, ECallableType::JsonValue);
        ValidateError("exists(@.a)", filterError, {}, {}, ECallableType::JsonValue);
        ValidateError("@ starts with \"x\"", filterError, {}, {}, ECallableType::JsonValue);

        // Both sides of == are paths: AND-merge of filter-relative paths
        ValidateJsonExists("strict $.a ? (@.b == @.c)", {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")});
        ValidateJsonExists("strict $.a ? (@.b == $.c)", {encodeKey("a") + encodeKey("b"), encodeKey("c")});
        ValidateJsonExists("strict $.a ? (@ == @.b)", {encodeKey("a"), encodeKey("a") + encodeKey("b")});
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

    // Chained filters -> AND
    Y_UNIT_TEST(CollectPath_FilterChain) {
        // Basic comparisons
        ValidateJsonExists("strict $.key ? (@.v1 >= 10) ? (@.v2 <= 20)", {encodeKey("key") + encodeKey("v1"), encodeKey("key") + encodeKey("v2")}, TCollectResult::ETokensMode::And);
        ValidateJsonExists("strict $.key ? (@.v1 >= 10 && @.v2 <= 20)", {encodeKey("key") + encodeKey("v1"), encodeKey("key") + encodeKey("v2")}, TCollectResult::ETokensMode::And);

        // Equality with literals
        ValidateJsonExists("strict $.key ? (@.v1 == 10) ? (@.v2 == 20)", {encodeKey("key") + encodeKey("v1") + numSuffix(10), encodeKey("key") + encodeKey("v2") + numSuffix(20)});
        ValidateJsonExists("strict $.key ? (@.v1 == 10 && @.v2 == 20)", {encodeKey("key") + encodeKey("v1") + numSuffix(10), encodeKey("key") + encodeKey("v2") + numSuffix(20)});

        // Three filters in a chain
        ValidateJsonExists("strict $.a ? (@.b == 1) ? (@.c == 2) ? (@.d == 3)", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)});
        ValidateJsonExists("strict $.a ? (@.b == 1 && @.c == 2 && @.d == 3)", {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)});

        // Mixed predicate types
        ValidateJsonExists("strict $.a ? (@.b == 10) ? (@.c starts with \"x\") ? (exists(@.d))", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});
        ValidateJsonExists("strict $.a ? (@.b == 10 && @.c starts with \"x\" && exists(@.d))", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d")});

        // @ on filter object itself
        ValidateJsonExists("strict $.a ? (@ == \"x\") ? (@.b == 1)", {encodeKey("a") + strSuffix("x"), encodeKey("a") + encodeKey("b") + numSuffix(1)});

        // Deeper input path
        ValidateJsonExists("strict $.a.b ? (@.c == 1) ? (@.d == 2)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + numSuffix(1), encodeKey("a") + encodeKey("b") + encodeKey("d") + numSuffix(2)});

        // JsonValue
        ValidateJsonValue("strict $.key ? (@.v1 > 0) ? (@.v2 < 10)", {encodeKey("key") + encodeKey("v1"), encodeKey("key") + encodeKey("v2")}, TCollectResult::ETokensMode::And);

        // Chained filter then member access
        ValidateJsonValue("strict $.a ? (@.b == 10) ? (@.c == 20).d", {encodeKey("a") + encodeKey("b") + numSuffix(10), encodeKey("a") + encodeKey("c") + numSuffix(20)});

        // Wildcard in first filter, equality in second
        ValidateJsonExists("strict $.a ? (@.* == \"x\") ? (@.b == 1)", {encodeKey("a") + encodeKey("b") + numSuffix(1)});

        // && inside jsonpath
        ValidateJsonExists("strict $.a ? (@.b == 1 && @.c == 2) ? (@.d == 3)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)},
            TCollectResult::ETokensMode::And);
        ValidateJsonExists("strict $.a ? (@.b == 1) ? (@.c == 2 && @.d == 3)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)},
            TCollectResult::ETokensMode::And);
        ValidateJsonExists("strict $.a ? (@.b == 1 && @.c == 2) ? (@.d == 3 && @.e == 4)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3), encodeKey("a") + encodeKey("e") + numSuffix(4)},
            TCollectResult::ETokensMode::And);

        // || inside jsonpath
        ValidateJsonExists("strict $.a ? (@.b == 1 || @.c == 2) ? (@.d == 3)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)},
            TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.a ? (@.b == 1) ? (@.c == 2 || @.d == 3)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)},
            TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.a ? (@.b == 1 || @.c == 2) ? (@.d == 3 || @.e == 4)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3), encodeKey("a") + encodeKey("e") + numSuffix(4)},
            TCollectResult::ETokensMode::Or);

        // Mixed && and || inside
        ValidateJsonExists("strict $.a ? (@.b == 1 && @.c == 2) ? (@.d == 3 || @.e == 4)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3), encodeKey("a") + encodeKey("e") + numSuffix(4)},
            TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.a ? (@.b == 1 || @.c == 2) ? (@.d == 3 && @.e == 4)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3), encodeKey("a") + encodeKey("e") + numSuffix(4)},
            TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.a ? ((@.b < 5) && ((@.c > 1) || (@.d > 2))) ? (@.e == 0)",
            {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d"), encodeKey("a") + encodeKey("e") + numSuffix(0)},
            TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.a ? (@.b == 1) ? (((@.c < 5) || (@.d > 1)) && @.e > 2)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c"), encodeKey("a") + encodeKey("d"), encodeKey("a") + encodeKey("e")},
            TCollectResult::ETokensMode::Or);
        ValidateJsonExists("strict $.key ? (@.a < @.b || @.c > @.d) ? (@.e == 1)",
            {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c"), encodeKey("key") + encodeKey("d"), encodeKey("key") + encodeKey("e") + numSuffix(1)},
            TCollectResult::ETokensMode::Or);
        ValidateJsonValue("strict $.a ? (@.b == 1 && @.c == 2) ? (@.d == 3)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)},
            TCollectResult::ETokensMode::And);
        ValidateJsonValue("strict $.a ? (@.b == 1 || @.c == 2) ? (@.d == 3)",
            {encodeKey("a") + encodeKey("b") + numSuffix(1), encodeKey("a") + encodeKey("c") + numSuffix(2), encodeKey("a") + encodeKey("d") + numSuffix(3)},
            TCollectResult::ETokensMode::Or);

        // Arithmetic two-path in chain
        ValidateJsonExists("strict $.key ? (@.a + @.b == 5) ? (@.c == 1 || @.d == 2)",
            {encodeKey("key") + encodeKey("a"), encodeKey("key") + encodeKey("b"), encodeKey("key") + encodeKey("c") + numSuffix(1), encodeKey("key") + encodeKey("d") + numSuffix(2)},
            TCollectResult::ETokensMode::Or);
    }

    // Nested filter: (@ ? (predicate)).member == value
    Y_UNIT_TEST(CollectPath_NestedFilter) {
        // Basic: inner == predicate, outer comparison dropped
        ValidateJsonExists("strict $ ? ((@ ? (@.a == 1)).b == 2)", {encodeKey("a") + numSuffix(1)});
        ValidateJsonExists("strict $ ? ((@ ? (@.a == \"x\")).b == \"y\")", {encodeKey("a") + strSuffix("x")});
        ValidateJsonExists("strict $ ? ((@ ? (@.a == true)).b == false)", {encodeKey("a") + boolTrueSuffix});
        ValidateJsonExists("strict $ ? ((@ ? (@.a == false)).b == true)", {encodeKey("a") + boolFalseSuffix});
        ValidateJsonExists("strict $ ? ((@ ? (@.a == null)).b == null)", {encodeKey("a") + nullSuffix});
        ValidateJsonExists("strict $ ? ((@ ? (@.a == -3.14)).b == 0)", {encodeKey("a") + numSuffix(-3.14)});

        // Reversed literal in inner predicate (literal == @.path)
        ValidateJsonExists("strict $ ? ((@ ? (1 == @.a)).b == 2)", {encodeKey("a") + numSuffix(1)});
        ValidateJsonExists("strict $ ? ((@ ? (\"x\" == @.a)).b == \"y\")", {encodeKey("a") + strSuffix("x")});
        ValidateJsonExists("strict $ ? ((@ ? (null == @.a)).b == 0)", {encodeKey("a") + nullSuffix});

        // Outer path contributes to the index prefix
        ValidateJsonExists("strict $.key ? ((@ ? (@.sub == \"x\")).other == \"y\")", {encodeKey("key") + encodeKey("sub") + strSuffix("x")});
        ValidateJsonExists("strict $.a.b ? ((@ ? (@.c == true)).d == false)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + boolTrueSuffix});
        ValidateJsonExists("strict $.arr ? ((@ ? (@.id == 9)).name == \"x\")", {encodeKey("arr") + encodeKey("id") + numSuffix(9)});
        ValidateJsonExists("strict $.items ? ((@ ? (@.type == null)).value > 0)", {encodeKey("items") + encodeKey("type") + nullSuffix});

        // Comparison operators != == in inner predicate: literal not appended, only path
        ValidateJsonExists("strict $ ? ((@ ? (@.n < 10)).label == \"x\")", {encodeKey("n")});
        ValidateJsonExists("strict $ ? ((@ ? (@.n > 0)).label == \"x\")", {encodeKey("n")});
        ValidateJsonExists("strict $ ? ((@ ? (@.n != 0)).label == \"x\")", {encodeKey("n")});
        ValidateJsonExists("strict $ ? ((@ ? (@.n >= 0)).label == \"x\")", {encodeKey("n")});
        ValidateJsonExists("strict $ ? ((@ ? (@.n <= 100)).label == \"x\")", {encodeKey("n")});
        ValidateJsonExists("strict $.arr ? ((@ ? (@.score >= 5)).rank == 1)", {encodeKey("arr") + encodeKey("score")});

        // Deeper inner path
        ValidateJsonExists("strict $ ? ((@ ? (@.a.b == 1)).c == 2)", {encodeKey("a") + encodeKey("b") + numSuffix(1)});
        ValidateJsonExists("strict $.key ? ((@ ? (@.a.b.c == \"x\")).d == \"y\")", {encodeKey("key") + encodeKey("a") + encodeKey("b") + encodeKey("c") + strSuffix("x")});
        ValidateJsonExists("strict $ ? ((@ ? (@.x.y == null)).z == true)", {encodeKey("x") + encodeKey("y") + nullSuffix});

        // Array subscript in inner predicate path: subscript is dropped for the index
        ValidateJsonExists("strict $ ? ((@ ? (@.a[0] == 1)).b == 2)", {encodeKey("a") + arrayItemSuffix + numSuffix(1)});
        ValidateJsonExists("strict $ ? ((@ ? (@.a[last] == true)).b == null)", {encodeKey("a") + arrayItemSuffix + boolTrueSuffix});
        ValidateJsonExists("strict $ ? ((@ ? (@.a[0].b == \"x\")).c == 1)", {encodeKey("a") + arrayItemSuffix + encodeKey("b") + strSuffix("x")});

        // Array subscript on @ before inner filter: subscript is dropped for the index
        ValidateJsonExists("strict $ ? ((@[0] ? (@.id == 9)).name == \"x\")", {arrayItemSuffix + encodeKey("id") + numSuffix(9)});
        ValidateJsonExists("strict $ ? ((@[*] ? (@.tag == \"foo\")).value > 0)", {arrayItemSuffix + encodeKey("tag") + strSuffix("foo")});
        ValidateJsonExists("strict $.items ? ((@[*] ? (@.tag == \"foo\")).value > 0)", {encodeKey("items") + arrayItemSuffix + encodeKey("tag") + strSuffix("foo")});
        ValidateJsonExists("strict $.k ? ((@[1] ? (@.x == true)).y == false)", {encodeKey("k") + arrayItemSuffix + encodeKey("x") + boolTrueSuffix});

        // Wildcard in inner predicate: path finishes, literal not appended
        ValidateJsonExists("strict $ ? ((@ ? (@.* == 1)).x == 2)", {""});
        ValidateJsonExists("strict $.key ? ((@ ? (@.* == \"x\")).y == 1)", {encodeKey("key")});
        ValidateJsonExists("strict $ ? ((@ ? (@.a.* == null)).b == 1)", {encodeKey("a")});

        // Inner AND: both tokens propagate (filter result has multiple tokens)
        ValidateJsonExists("strict $ ? ((@ ? (@.a == 1 && @.b == 2)).c == 3)", {encodeKey("a") + numSuffix(1), encodeKey("b") + numSuffix(2)});
        ValidateJsonExists("strict $.key ? ((@ ? (@.x == \"v\" && @.y == true)).z == null)", {encodeKey("key") + encodeKey("x") + strSuffix("v"), encodeKey("key") + encodeKey("y") + boolTrueSuffix});
        ValidateJsonExists("strict $ ? ((@ ? (@.a == null && @.b == false)).c == 1)", {encodeKey("a") + nullSuffix, encodeKey("b") + boolFalseSuffix});

        // Inner OR: both tokens propagate
        ValidateJsonExists("strict $ ? ((@ ? (@.a == 1 || @.a == 2)).b == \"x\")", {encodeKey("a") + numSuffix(1), encodeKey("a") + numSuffix(2)});
        ValidateJsonExists("strict $ ? ((@ ? (@.tag == \"foo\" || @.tag == \"bar\")).value == 0)", {encodeKey("tag") + strSuffix("foo"), encodeKey("tag") + strSuffix("bar")});
        ValidateJsonExists("strict $.arr ? ((@ ? (@.id == 1 || @.id == 2)).val == true)", {encodeKey("arr") + encodeKey("id") + numSuffix(1), encodeKey("arr") + encodeKey("id") + numSuffix(2)});

        // Double nesting: only deepest (innermost) inner filter determines the tokens
        ValidateJsonExists("strict $ ? ((@ ? ((@ ? (@.z == 1)).w == 2)).val == 3)", {encodeKey("z") + numSuffix(1)});
        ValidateJsonExists("strict $.a ? ((@ ? ((@ ? (@.b == \"x\")).c == \"y\")).d == \"z\")", {encodeKey("a") + encodeKey("b") + strSuffix("x")});
        ValidateJsonExists("strict $ ? ((@ ? ((@ ? (@.p == null)).q == true)).r == false)", {encodeKey("p") + nullSuffix});
    }

    Y_UNIT_TEST(CollectPath_Variables) {
        // Equality: variable on the right side, all scalar types
        ValidateTokens("strict $.key == $var", {encodeKey("key") + strSuffix("hello")}, {{"var", strSuffix("hello")}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + strSuffix("")}, {{"var", strSuffix("")}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + numSuffix(42)}, {{"var", numSuffix(42)}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + numSuffix(0)}, {{"var", numSuffix(0)}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + numSuffix(3.14)}, {{"var", numSuffix(3.14)}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + numSuffix(-10)}, {{"var", numSuffix(-10)}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + boolFalseSuffix}, {{"var", boolFalseSuffix}});
        ValidateTokens("strict $.key == $var", {encodeKey("key") + nullSuffix}, {{"var", nullSuffix}});

        // Equality: variable on the left side
        ValidateTokens("strict $var == $.key", {encodeKey("key") + strSuffix("hello")}, {{"var", strSuffix("hello")}});
        ValidateTokens("strict $var == $.key", {encodeKey("key") + numSuffix(5)}, {{"var", numSuffix(5)}});
        ValidateTokens("strict $var == $.key", {encodeKey("key") + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("strict $var == $.key", {encodeKey("key") + nullSuffix}, {{"var", nullSuffix}});

        // Context object as path
        ValidateTokens("strict $ == $var", {strSuffix("root")}, {{"var", strSuffix("root")}});
        ValidateTokens("strict $var == $", {numSuffix(0)}, {{"var", numSuffix(0)}});

        // Deeper member access paths
        ValidateTokens("strict $.a.b == $var", {encodeKey("a") + encodeKey("b") + strSuffix("x")}, {{"var", strSuffix("x")}});
        ValidateTokens("strict $.a.b.c == $var", {encodeKey("a") + encodeKey("b") + encodeKey("c") + numSuffix(1)}, {{"var", numSuffix(1)}});
        ValidateTokens("strict $.aba.\"caba\" == $var", {encodeKey("aba") + encodeKey("caba") + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("strict $var == $.a.b.c", {encodeKey("a") + encodeKey("b") + encodeKey("c") + nullSuffix}, {{"var", nullSuffix}});

        // Array access
        ValidateTokens("strict $.key[0] == $var", {encodeKey("key") + arrayItemSuffix + strSuffix("x")}, {{"var", strSuffix("x")}});
        ValidateTokens("strict $.key[last] == $var", {encodeKey("key") + arrayItemSuffix + boolTrueSuffix}, {{"var", boolTrueSuffix}});
        ValidateTokens("strict $.key[1, 2, 3] == $var", {encodeKey("key") + arrayItemSuffix + nullSuffix}, {{"var", nullSuffix}});
        ValidateTokens("strict $.a.b[0].c == $var", {encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c") + numSuffix(7)}, {{"var", numSuffix(7)}});

        // Wildcard member access: path finishes, literal not appended
        ValidateTokens("strict $.* == $var", {""}, {{"var", strSuffix("x")}});
        ValidateTokens("strict $.a.* == $var", {encodeKey("a")}, {{"var", numSuffix(1)}});
        ValidateTokens("strict $var == $.*", {""}, {{"var", strSuffix("x")}});
        ValidateTokens("strict $var == $.a.*", {encodeKey("a")}, {{"var", numSuffix(1)}});

        // Methods: path finishes, literal not appended
        ValidateTokens("strict $.key.size() == $var", {encodeKey("key")}, {{"var", numSuffix(3)}});
        ValidateTokens("strict $.key.abs() == $var", {encodeKey("key")}, {{"var", numSuffix(1)}});
        ValidateTokens("strict $.key.type() == $var", {encodeKey("key")}, {{"var", strSuffix("number")}});
        ValidateTokens("strict $.a.b.floor() == $var", {encodeKey("a") + encodeKey("b")}, {{"var", numSuffix(0)}});

        // Unary arithmetic on path: path finishes, literal not appended
        ValidateTokens("strict -$.key == $var", {encodeKey("key")}, {{"var", numSuffix(1)}});
        ValidateTokens("strict +$.key == $var", {encodeKey("key")}, {{"var", numSuffix(0)}});

        // Multiple variables: AND
        ValidateTokens("strict ($.a == $v1) && ($.b == $v2)",
            {encodeKey("a") + strSuffix("x"), encodeKey("b") + numSuffix(1)},
            {{"v1", strSuffix("x")}, {"v2", numSuffix(1)}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == $v) && ($.b == $v)",
            {encodeKey("a") + nullSuffix, encodeKey("b") + nullSuffix},
            {{"v", nullSuffix}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == $v1) && ($.b == $v2) && ($.c == $v3)",
            {encodeKey("a") + strSuffix("a"), encodeKey("b") + numSuffix(2), encodeKey("c") + boolTrueSuffix},
            {{"v1", strSuffix("a")}, {"v2", numSuffix(2)}, {"v3", boolTrueSuffix}}, {},
            ECallableType::JsonValue, EMode::And);

        // Multiple variables: OR
        ValidateTokens("strict ($.a == $v1) || ($.b == $v2)",
            {encodeKey("a") + strSuffix("x"), encodeKey("b") + numSuffix(1)},
            {{"v1", strSuffix("x")}, {"v2", numSuffix(1)}}, {},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("strict ($.key == $v1) || ($.key == $v2)",
            {encodeKey("key") + strSuffix("a"), encodeKey("key") + strSuffix("b")},
            {{"v1", strSuffix("a")}, {"v2", strSuffix("b")}}, {},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("strict ($.a == $v1) || ($.b == $v2) || ($.c == $v3)",
            {encodeKey("a") + boolTrueSuffix, encodeKey("b") + nullSuffix, encodeKey("c") + numSuffix(0)},
            {{"v1", boolTrueSuffix}, {"v2", nullSuffix}, {"v3", numSuffix(0)}}, {},
            ECallableType::JsonValue, EMode::Or);

        // Mixed: variable and literal
        ValidateTokens("strict ($.a == $var) && ($.b == 42)",
            {encodeKey("a") + strSuffix("x"), encodeKey("b") + numSuffix(42)},
            {{"var", strSuffix("x")}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == \"hello\") && ($.b == $var)",
            {encodeKey("a") + strSuffix("hello"), encodeKey("b") + numSuffix(3.14)},
            {{"var", numSuffix(3.14)}}, {},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == $var) || ($.b == true)",
            {encodeKey("a") + nullSuffix, encodeKey("b") + boolTrueSuffix},
            {{"var", nullSuffix}}, {},
            ECallableType::JsonValue, EMode::Or);

        // Variable not in map
        ValidateTokens("strict $.key == $var", {encodeKey("key")}, {}, {}, ECallableType::JsonValue);
        ValidateTokens("strict $var == $.key", {encodeKey("key")}, {}, {}, ECallableType::JsonValue);

        // Variable exists but queried variable is missing
        ValidateTokens("strict $.key == $missing", {encodeKey("key")},
            {{"other", strSuffix("x")}}, {}, ECallableType::JsonValue);

        // One variable present, other missing in AND
        ValidateTokens("strict ($.a == $v1) && ($.b == $v2)", {encodeKey("a") + strSuffix("x"), encodeKey("b")},
            {{"v1", strSuffix("x")}}, {}, ECallableType::JsonValue, EMode::And);

        // Variable in non-literal context: standalone path
        ValidateError("$var", varContextError, {{"var", strSuffix("x")}});
        ValidateError("$var.key", varContextError, {{"var", strSuffix("x")}});
        ValidateError("$var[0]", varContextError, {{"var", strSuffix("x")}});
        ValidateError("$var.a.b.c", varContextError, {{"var", strSuffix("x")}});

        // Variable in arithmetic: treated as empty literal, path only
        ValidateTokens("strict $.key + $var", {encodeKey("key")}, {{"var", numSuffix(1)}});
        ValidateTokens("strict $var + $.key", {encodeKey("key")}, {{"var", numSuffix(1)}});
        ValidateTokens("strict $.key - $var", {encodeKey("key")}, {{"var", numSuffix(1)}});
        ValidateTokens("strict $.key * $var", {encodeKey("key")}, {{"var", numSuffix(2)}});
        ValidateTokens("strict $.key / $var", {encodeKey("key")}, {{"var", numSuffix(2)}});
        ValidateTokens("strict $.key % $var", {encodeKey("key")}, {{"var", numSuffix(2)}});
        ValidateTokens("strict $.a.b + $var", {encodeKey("a") + encodeKey("b")}, {{"var", numSuffix(5)}});

        // Both sides are variables: treated as two empty literals -> emptyError
        ValidateError("$v1 + $v2", emptyError, {{"v1", numSuffix(1)}, {"v2", numSuffix(2)}});
        ValidateError("$v1 * $v2", emptyError, {{"v1", numSuffix(3)}, {"v2", numSuffix(4)}});

        // Variable in comparison operators: variable is treated as a dropped literal
        ValidateTokens("strict $.key < $var", {encodeKey("key")}, {{"var", numSuffix(5)}});
        ValidateTokens("strict $.key <= $var", {encodeKey("key")}, {{"var", numSuffix(5)}});
        ValidateTokens("strict $.key > $var", {encodeKey("key")}, {{"var", numSuffix(0)}});
        ValidateTokens("strict $.key >= $var", {encodeKey("key")}, {{"var", numSuffix(0)}});
        ValidateTokens("strict $.key != $var", {encodeKey("key")}, {{"var", strSuffix("x")}});

        // Variable on left side
        ValidateTokens("strict $var < $.key", {encodeKey("key")}, {{"var", numSuffix(5)}});
        ValidateTokens("strict $var > $.key", {encodeKey("key")}, {{"var", numSuffix(0)}});
        ValidateTokens("strict $var != $.key", {encodeKey("key")}, {{"var", strSuffix("x")}});
        ValidateTokens("strict $var <= $.a.b", {encodeKey("a") + encodeKey("b")}, {{"var", numSuffix(3)}});

        // Both sides variables: emptyError
        ValidateError("$v1 < $v2", emptyError,
            {{"v1", numSuffix(1)}, {"v2", numSuffix(2)}}, {}, ECallableType::JsonValue);
        ValidateError("$v1 != $v2", emptyError,
            {{"v1", strSuffix("a")}, {"v2", strSuffix("b")}}, {}, ECallableType::JsonValue);

        // Filter predicate with variable
        ValidateTokens("strict $.a ? (@.b == $var)", {encodeKey("a") + encodeKey("b") + strSuffix("x")},
            {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);
        ValidateTokens("strict $.a ? (@.b == $var)", {encodeKey("a") + encodeKey("b") + numSuffix(42)},
            {{"var", numSuffix(42)}}, {}, ECallableType::JsonExists);
        ValidateTokens("strict $.a ? (@.b == $var)", {encodeKey("a") + encodeKey("b") + boolTrueSuffix},
            {{"var", boolTrueSuffix}}, {}, ECallableType::JsonExists);
        ValidateTokens("strict $.a ? (@.b == $var)", {encodeKey("a") + encodeKey("b") + nullSuffix},
            {{"var", nullSuffix}}, {}, ECallableType::JsonExists);

        // Variable on left side in filter equality
        ValidateTokens("strict $.a ? ($var == @.b)", {encodeKey("a") + encodeKey("b") + strSuffix("x")},
            {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);

        // Deeper filter paths
        ValidateTokens("strict $.a.b ? (@.c == $var)", {encodeKey("a") + encodeKey("b") + encodeKey("c") + numSuffix(1)},
            {{"var", numSuffix(1)}}, {}, ECallableType::JsonExists);
        ValidateTokens("strict $.key ? (@.sub == $var)", {encodeKey("key") + encodeKey("sub") + strSuffix("val")},
            {{"var", strSuffix("val")}}, {}, ECallableType::JsonExists);

        // Variable in filter comparison (dropped, path only)
        ValidateTokens("strict $.a ? (@.b < $var)", {encodeKey("a") + encodeKey("b")},
            {{"var", numSuffix(10)}}, {}, ECallableType::JsonExists);
        ValidateTokens("strict $.a ? (@.b != $var)", {encodeKey("a") + encodeKey("b")},
            {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);

        // Multiple variables in filter AND
        ValidateTokens("strict $.a ? (@.b == $v1 && @.c == $v2)",
            {encodeKey("a") + encodeKey("b") + strSuffix("x"), encodeKey("a") + encodeKey("c") + numSuffix(1)},
            {{"v1", strSuffix("x")}, {"v2", numSuffix(1)}}, {}, ECallableType::JsonExists);

        // Multiple variables in filter OR
        ValidateTokens("strict $.a ? ((@.b == $v1) || (@.b == $v2))",
            {encodeKey("a") + encodeKey("b") + strSuffix("a"), encodeKey("a") + encodeKey("b") + strSuffix("b")},
            {{"v1", strSuffix("a")}, {"v2", strSuffix("b")}}, {}, ECallableType::JsonExists);

        // Missing variable in filter -> skipped
        ValidateTokens("strict $.a ? (@.b == $var)", {encodeKey("a") + encodeKey("b")}, {}, {}, ECallableType::JsonExists);
    }

    // ParamVariables map: var name -> YQL param name (e.g. "$value")
    Y_UNIT_TEST(CollectPath_ParamVariables) {
        // Basic
        ValidateTokens("strict $.key == $var", {TToken{encodeKey("key"), "$value"}}, {}, {{"var", "$value"}});
        ValidateTokens("strict $.key == $var", {TToken{encodeKey("key"), "$p"}}, {}, {{"var", "$p"}});

        // Reversed order
        ValidateTokens("strict $var == $.key", {TToken{encodeKey("key"), "$value"}}, {}, {{"var", "$value"}});

        // Context object as path
        ValidateTokens("strict $ == $var", {TToken{"", "$p"}}, {}, {{"var", "$p"}});
        ValidateTokens("strict $var == $", {TToken{"", "$p"}}, {}, {{"var", "$p"}});

        // Deeper member access paths
        ValidateTokens("strict $.a.b == $var", {TToken{encodeKey("a") + encodeKey("b"), "$p"}}, {}, {{"var", "$p"}});
        ValidateTokens("strict $.a.b.c == $var", {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$param"}}, {}, {{"var", "$param"}});
        ValidateTokens("strict $.aba.\"caba\" == $var", {TToken{encodeKey("aba") + encodeKey("caba"), "$val"}}, {}, {{"var", "$val"}});
        ValidateTokens("strict $var == $.a.b.c", {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, {}, {{"var", "$p"}});

        // Array access
        ValidateTokens("strict $.key[0] == $var", {TToken{encodeKey("key") + arrayItemSuffix, "$v"}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.key[last] == $var", {TToken{encodeKey("key") + arrayItemSuffix, "$v"}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.key[1, 2, 3] == $var", {TToken{encodeKey("key") + arrayItemSuffix, "$v"}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.a.b[0].c == $var", {TToken{encodeKey("a") + encodeKey("b") + arrayItemSuffix + encodeKey("c"), "$v"}}, {}, {{"var", "$v"}});

        // Wildcard member access
        ValidateTokens("strict $.* == $var", {TToken{"", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.a.* == $var", {TToken{encodeKey("a"), ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $var == $.*", {TToken{"", ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $var == $.a.*", {TToken{encodeKey("a"), ""}}, {}, {{"var", "$v"}});

        // Methods finish the path
        ValidateTokens("strict $.key.size() == $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.key.abs() == $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.key.type() == $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict $.a.b.floor() == $var", {TToken{encodeKey("a") + encodeKey("b"), ""}}, {}, {{"var", "$v"}});

        // Unary arithmetic on path
        ValidateTokens("strict -$.key == $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$v"}});
        ValidateTokens("strict +$.key == $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$v"}});

        // Multiple param variables: AND
        ValidateTokens("strict ($.a == $v1) && ($.b == $v2)",
            {TToken{encodeKey("a"), "$p1"}, TToken{encodeKey("b"), "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == $v) && ($.b == $v)",
            {TToken{encodeKey("a"), "$p"}, TToken{encodeKey("b"), "$p"}}, {},
            {{"v", "$p"}},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == $v1) && ($.b == $v2) && ($.c == $v3)",
            {TToken{encodeKey("a"), "$p1"}, TToken{encodeKey("b"), "$p2"}, TToken{encodeKey("c"), "$p3"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}, {"v3", "$p3"}},
            ECallableType::JsonValue, EMode::And);

        // Multiple param variables: OR
        ValidateTokens("strict ($.a == $v1) || ($.b == $v2)",
            {TToken{encodeKey("a"), "$p1"}, TToken{encodeKey("b"), "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("strict ($.key == $v1) || ($.key == $v2)",
            {TToken{encodeKey("key"), "$p1"}, TToken{encodeKey("key"), "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonValue, EMode::Or);
        ValidateTokens("strict ($.a == $v1) || ($.b == $v2) || ($.c == $v3)",
            {TToken{encodeKey("a"), "$p1"}, TToken{encodeKey("b"), "$p2"}, TToken{encodeKey("c"), "$p3"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}, {"v3", "$p3"}},
            ECallableType::JsonValue, EMode::Or);

        // Mixed: param variable and plain literal in AND
        ValidateTokens("strict ($.a == $var) && ($.b == 42)",
            {TToken{encodeKey("a"), "$p"}, TToken{encodeKey("b") + numSuffix(42), ""}}, {},
            {{"var", "$p"}},
            ECallableType::JsonValue, EMode::And);
        ValidateTokens("strict ($.a == \"hello\") && ($.b == $var)",
            {TToken{encodeKey("a") + strSuffix("hello"), ""}, TToken{encodeKey("b"), "$p"}}, {},
            {{"var", "$p"}},
            ECallableType::JsonValue, EMode::And);

        // Mixed: param variable and plain literal in OR
        ValidateTokens("strict ($.a == $var) || ($.b == true)",
            {TToken{encodeKey("a"), "$p"}, TToken{encodeKey("b") + boolTrueSuffix, ""}}, {},
            {{"var", "$p"}},
            ECallableType::JsonValue, EMode::Or);

        // Param variable in filter predicate
        ValidateTokens("strict $.a ? (@.b == $var)", {TToken{encodeKey("a") + encodeKey("b"), "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("strict $.a ? ($var == @.b)", {TToken{encodeKey("a") + encodeKey("b"), "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);

        // Deeper filter paths
        ValidateTokens("strict $.a.b ? (@.c == $var)", {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("strict $.key ? (@.sub == $var)", {TToken{encodeKey("key") + encodeKey("sub"), "$p"}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);

        // Param variable in filter comparison
        ValidateTokens("strict $.a ? (@.b < $var)", {TToken{encodeKey("a") + encodeKey("b"), ""}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("strict $.a ? (@.b != $var)", {TToken{encodeKey("a") + encodeKey("b"), ""}}, {},
            {{"var", "$p"}}, ECallableType::JsonExists);

        // Multiple param variables in filter AND
        ValidateTokens("strict $.a ? (@.b == $v1 && @.c == $v2)",
            {TToken{encodeKey("a") + encodeKey("b"), "$p1"}, TToken{encodeKey("a") + encodeKey("c"), "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonExists, EMode::And);

        // Multiple param variables in filter OR
        ValidateTokens("strict $.a ? ((@.b == $v1) || (@.b == $v2))",
            {TToken{encodeKey("a") + encodeKey("b"), "$p1"}, TToken{encodeKey("a") + encodeKey("b"), "$p2"}}, {},
            {{"v1", "$p1"}, {"v2", "$p2"}},
            ECallableType::JsonExists, EMode::Or);

        // Param variable in arithmetic
        ValidateTokens("strict $.key + $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$p"}}, ECallableType::JsonExists);
        ValidateTokens("strict $var + $.key", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$p"}}, ECallableType::JsonExists);

        // Param variable in comparison operators
        ValidateTokens("strict $.key < $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$p"}}, ECallableType::JsonValue);
        ValidateTokens("strict $.key != $var", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$p"}}, ECallableType::JsonValue);
        ValidateTokens("strict $var < $.key", {TToken{encodeKey("key"), ""}}, {}, {{"var", "$p"}}, ECallableType::JsonValue);

        // Param variable not in the map: variable is ignored
        ValidateTokens("strict $.key == $var", {TToken{encodeKey("key"), ""}});
        ValidateTokens("strict $var == $.key", {TToken{encodeKey("key"), ""}});

        // Variable in non-literal context is still an error with paramVariables
        ValidateError("$var", varContextError, {}, {{"var", "$p"}}, ECallableType::JsonValue);
        ValidateError("$var.key", varContextError, {}, {{"var", "$p"}}, ECallableType::JsonValue);

        // param variable on both sides of == is an error (two literals)
        ValidateError("$v1 == $v2", compError, {}, {{"v1", "$p1"}, {"v2", "$p2"}}, ECallableType::JsonValue);

        // Deduplication
        ValidateTokens("strict ($.a.b.c == $var) && ($.a.b.c == $var)",
            {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, {}, {{"var", "$p"}});
        ValidateTokens("strict ($.a.b.c == $var) || ($.a.b.c == $var)",
            {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, {}, {{"var", "$p"}});

        ValidateTokens("strict ($.key == $v1) && ($.key == $v2)",
            {TToken{encodeKey("key"), "$p"}}, {}, {{"v1", "$p"}, {"v2", "$p"}});
        ValidateTokens("strict ($.key == $v1) || ($.key == $v2)",
            {TToken{encodeKey("key"), "$p"}}, {}, {{"v1", "$p"}, {"v2", "$p"}});
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
        ValidateJsonValue("strict $.key == \"hello\".type()", {encodeKey("key")});
        ValidateJsonValue("strict \"hello\".type() == $.key", {encodeKey("key")});
        ValidateJsonValue("strict $.key == 42.0.abs()", {encodeKey("key")});
        ValidateJsonValue("strict $.a == true.type()", {encodeKey("a")});
        ValidateJsonValue("strict $.a == null.type()", {encodeKey("a")});
        ValidateJsonValue("strict $.a.b == (-1).abs()", {encodeKey("a") + encodeKey("b")});

        // Comparison: path.method() == literal.method()
        ValidateJsonValue("strict $.key.abs() == \"hello\".type()", {encodeKey("key")});
        ValidateJsonValue("strict $.a.b.floor() == 42.0.abs()", {encodeKey("a") + encodeKey("b")});

        // Comparison: path == variable.method()
        ValidateTokens("strict $.key == $var.type()", {encodeKey("key")}, {{"var", strSuffix("number")}});
        ValidateTokens("strict $var.type() == $.key", {encodeKey("key")}, {{"var", strSuffix("number")}});
        ValidateTokens("strict $.key == $var.abs()", {encodeKey("key")}, {{"var", numSuffix(5)}});
        ValidateTokens("strict $.a.b == $var.floor()", {encodeKey("a") + encodeKey("b")}, {{"var", numSuffix(0)}});

        // Comparison: path == param-variable.method()
        ValidateTokens("strict $.key == $var.type()", {encodeKey("key")}, {}, {{"var", "$p"}});
        ValidateTokens("strict $var.type() == $.key", {encodeKey("key")}, {}, {{"var", "$p"}});

        // Both sides non-path (literal.method() == literal)
        ValidateError(R"(strict "hello".type() == "string")", emptyError);
        ValidateError("42.0.abs() == 42", emptyError);

        // Filter: @.b == literal.method()
        ValidateJsonExists("strict $.a ? (@.b == \"hello\".type())", {encodeKey("a") + encodeKey("b")});
        ValidateJsonExists("strict $.a ? (@.b == 42.0.abs())", {encodeKey("a") + encodeKey("b")});
        ValidateTokens("strict $.a ? (@.b == $var.type())", {encodeKey("a") + encodeKey("b")}, {{"var", strSuffix("x")}}, {}, ECallableType::JsonExists);
    }

    Y_UNIT_TEST(CollectPath_MultiTokenSuffix) {
        // MemberAccess
        ValidateJsonValue("strict ($.a + $.b).c == 1", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict exists(($.a + $.b).c)", {encodeKey("a"), encodeKey("b")});

        // WildcardMemberAccess
        ValidateJsonValue("strict exists(($.a + $.b).*)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a + $.b).* starts with \"x\"", {encodeKey("a"), encodeKey("b")});

        // ArrayAccess
        ValidateJsonValue("strict exists(($.a + $.b)[0])", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict exists(($.a + $.b)[0].key)", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict exists(($.a + $.b)[*])", {encodeKey("a"), encodeKey("b")});

        // Binary operations
        ValidateJsonValue("strict ($.a + $.b) == 3", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($.a - $.b) == true", {encodeKey("a"), encodeKey("b")});

        // Filter
        ValidateJsonValue("strict ($ ? (@.a > 0) ? (@.b < 10)).c", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict ($ ? (@.a > 0) ? (@.b < 10)) == 1", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $ ? (@.a > 0 && @.b < 10).c", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $ ? (@.a > 0 && @.b < 10) == 1", {encodeKey("a"), encodeKey("b")});

        // Methods
        ValidateJsonValue("strict ($.k1 > $.k2).abs()", {encodeKey("k1"), encodeKey("k2")});
        ValidateJsonValue("strict $ ? (@.a > 0 && @.b < 10).type()", {encodeKey("a"), encodeKey("b")});
        ValidateJsonValue("strict $ ? (@.a > 0 && @.b < 10).type() == \"object\"", {encodeKey("a"), encodeKey("b")});
    }

    // Tokens with no ancestor–descendant relation survive both AND and OR merge intact.
    Y_UNIT_TEST(MergeAndOr_DisjointPaths) {
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("c") + encodeKey("d")})),
            {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("c") + encodeKey("d")})),
            {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a"), encodeKey("b")}, EMode::And), MakeTokens({encodeKey("c")})),
            {encodeKey("a"), encodeKey("b"), encodeKey("c")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a"), encodeKey("b")}, EMode::Or), MakeTokens({encodeKey("c")})),
            {encodeKey("a"), encodeKey("b"), encodeKey("c")}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d")}, EMode::And),
                     MakeTokens({encodeKey("e") + encodeKey("f"), encodeKey("g") + encodeKey("h")}, EMode::And)),
            {encodeKey("a") + encodeKey("b"), encodeKey("c") + encodeKey("d"), encodeKey("e") + encodeKey("f"), encodeKey("g") + encodeKey("h")}, EMode::And);

    }

    // AND keeps the deepest descendant (leaf); OR keeps the shallowest ancestor (root).
    Y_UNIT_TEST(MergeAndOr_DirectAncestorDescendant) {
        // parent in left operand
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")})),
            {encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And);

        // parent in right operand
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And);

        // grandparent pruned (two levels up)
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a")}), MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")})),
            {encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")})),
            {encodeKey("a") + encodeKey("b")}, EMode::Or);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b")}, EMode::Or);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a")}), MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")})),
            {encodeKey("a")}, EMode::Or);
    }

    // Identical tokens collapse to a single entry; equal tokens are NOT each other's prefix.
    Y_UNIT_TEST(MergeAndOr_IdenticalTokens) {
        // single-token sets: set deduplication leaves 1 token -> mode stays NotSet
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b")}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b")}, EMode::NotSet);

        // multi-token sets: deduplication, no pruning (b and c are siblings, not prefix-related)
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("c")}, EMode::And),
                     MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("c")}, EMode::And)),
            {encodeKey("a") + encodeKey("b"), encodeKey("c")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("c")}, EMode::Or),
                    MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("c")}, EMode::Or)),
            {encodeKey("a") + encodeKey("b"), encodeKey("c")}, EMode::Or);
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
        auto left = MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("a") + encodeKey("d")}, EMode::And);
        auto right = MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("e"), encodeKey("e") + encodeKey("f")}, EMode::And);
        CheckMerge(
            MergeAnd(std::move(left), std::move(right)),
            {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("e"), encodeKey("a") + encodeKey("d"), encodeKey("e") + encodeKey("f")}, EMode::And);

        left = MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("a") + encodeKey("d")}, EMode::Or);
        right = MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("e"), encodeKey("e") + encodeKey("f")}, EMode::Or);
        CheckMerge(
            MergeOr(std::move(left), std::move(right)),
            {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("d"), encodeKey("e") + encodeKey("f")}, EMode::Or);
    }

    // Two completely independent subtrees each with an ancestor–descendant pair.
    Y_UNIT_TEST(MergeAndOr_MultipleBranches) {
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And),
                     MakeTokens({encodeKey("x") + encodeKey("y"), encodeKey("x") + encodeKey("y") + encodeKey("z")}, EMode::And)),
            {encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("x") + encodeKey("y") + encodeKey("z")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::Or),
                    MakeTokens({encodeKey("x") + encodeKey("y"), encodeKey("x") + encodeKey("y") + encodeKey("z")}, EMode::Or)),
            {encodeKey("a") + encodeKey("b"), encodeKey("x") + encodeKey("y")}, EMode::Or);
    }

    // A path-only token is a string prefix of a path+literal token for the same field.
    // AND should keep the value-specific (longer) token; OR should keep the path-only (shorter).
    Y_UNIT_TEST(MergeAndOr_LiteralSuffix) {
        const TString ab = encodeKey("a") + encodeKey("b");
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
            MergeAnd(MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}}),
                     MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), "$p"}}, EMode::NotSet, "AND dedup: equal param tokens collapse to one");

        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}}),
                    MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), "$p"}}, EMode::NotSet, "OR dedup: equal param tokens collapse to one");

        // Same path, same Param, in a multi-token set: dedup still collapses them.
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{encodeKey("a"), "$p"}, TToken{encodeKey("b"), "$q"}}, EMode::And),
                     MakeParamTokens({TToken{encodeKey("a"), "$p"}, TToken{encodeKey("b"), "$q"}}, EMode::And)),
            {TToken{encodeKey("a"), "$p"}, TToken{encodeKey("b"), "$q"}}, EMode::And, "AND dedup: multi-token param set collapses duplicates");

        // AND pruning with param as leaf
        CheckMergeFull(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}),
                     MakeParamTokens({TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, EMode::And, "AND pruning: non-param ancestor dropped, param leaf kept");

        CheckMergeFull(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}),
                     MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), "$p"}}, EMode::And, "AND pruning: path-only token dropped, param token kept");

        // Order does not matter for AND
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}),
                     MakeTokens({encodeKey("a") + encodeKey("b")})),
            {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, EMode::And, "AND pruning: order reversed, non-param ancestor still dropped");

        // Two levels up
        CheckMergeFull(
            MergeAnd(MakeTokens({encodeKey("a")}),
                     MakeParamTokens({TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}, EMode::And, "AND pruning: grandparent non-param dropped by param grandchild");

        // OR pruning with non-param ancestor
        CheckMergeFull(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}),
                    MakeParamTokens({TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), ""}}, EMode::Or, "OR pruning: non-param ancestor kept, param descendant dropped");

        CheckMergeFull(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}),
                    MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b")}}, EMode::Or, "OR pruning: path-only token kept, param token dropped");

        // Order does not matter for OR
        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}}),
                    MakeTokens({encodeKey("a") + encodeKey("b")})),
            {TToken{encodeKey("a") + encodeKey("b"), ""}}, EMode::Or, "OR pruning: order reversed, non-param ancestor still kept");

        // Two levels down
        CheckMergeFull(
            MergeOr(MakeTokens({encodeKey("a")}),
                    MakeParamTokens({TToken{encodeKey("a") + encodeKey("b") + encodeKey("c"), "$p"}})),
            {TToken{encodeKey("a"), ""}}, EMode::Or, "OR pruning: root non-param kept, two-level-deep param descendant dropped");

        // Disjoint paths
        CheckMergeFull(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}),
                     MakeParamTokens({TToken{encodeKey("c") + encodeKey("d"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), ""}, TToken{encodeKey("c") + encodeKey("d"), "$p"}}, EMode::And, "AND disjoint: unrelated paths both kept");

        CheckMergeFull(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}),
                    MakeParamTokens({TToken{encodeKey("c") + encodeKey("d"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), ""}, TToken{encodeKey("c") + encodeKey("d"), "$p"}}, EMode::Or, "OR disjoint: unrelated paths both kept");

        // Disjoint param tokens
        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{encodeKey("a"), "$p1"}}),
                     MakeParamTokens({TToken{encodeKey("b"), "$p2"}})),
            {TToken{encodeKey("a"), "$p1"}, TToken{encodeKey("b"), "$p2"}}, EMode::And, "AND disjoint params: two unrelated param tokens both kept");

        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{encodeKey("a"), "$p1"}}),
                    MakeParamTokens({TToken{encodeKey("b"), "$p2"}})),
            {TToken{encodeKey("a"), "$p1"}, TToken{encodeKey("b"), "$p2"}}, EMode::Or, "OR disjoint params: two unrelated param tokens both kept");

        // Same path, different Params
        CheckMergeFull(
            MergeOr(MakeParamTokens({TToken{encodeKey("key"), "$p1"}}),
                    MakeParamTokens({TToken{encodeKey("key"), "$p2"}})),
            {TToken{encodeKey("key"), "$p1"}, TToken{encodeKey("key"), "$p2"}}, EMode::Or, "OR same path different params: both kept as distinct constraints");

        CheckMergeFull(
            MergeAnd(MakeParamTokens({TToken{encodeKey("key"), "$p1"}}),
                     MakeParamTokens({TToken{encodeKey("key"), "$p2"}})),
            {TToken{encodeKey("key"), "$p1"}, TToken{encodeKey("key"), "$p2"}}, EMode::And, "AND same path different params: both kept as distinct constraints");

        // Param token with literal-suffixed non-param sibling on the same path
        CheckMergeFull(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b") + numSuffix(5.0)}),
                     MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), "$p"}, TToken{encodeKey("a") + encodeKey("b") + numSuffix(5.0), ""}}, EMode::And, "AND literal-suffix sibling: param and literal-suffixed token both kept");

        CheckMergeFull(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b") + numSuffix(5.0)}),
                    MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), "$p"}, TToken{encodeKey("a") + encodeKey("b") + numSuffix(5.0), ""}}, EMode::Or, "OR literal-suffix sibling: param and literal-suffixed token both kept");

        // Non-param path-only token is a prefix of the literal-suffixed token AND of the param token with the same base path
        CheckMergeFull(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + numSuffix(5.0)}, EMode::And),
                     MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}})),
            {TToken{encodeKey("a") + encodeKey("b"), "$p"}, TToken{encodeKey("a") + encodeKey("b") + numSuffix(5.0), ""}}, EMode::And, "AND prefix+literal+param: path-only prefix dropped, leaf and param kept");

        CheckMergeFull(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}),
                    MakeParamTokens({TToken{encodeKey("a") + encodeKey("b"), "$p"}, TToken{encodeKey("a") + encodeKey("b") + numSuffix(5.0), ""}})),
            {TToken{encodeKey("a") + encodeKey("b"), ""}}, EMode::Or, "OR prefix+literal+param: root non-param kept, descendants dropped");
    }

    // When one operand carries an incompatible mode, the merge falls back to OR mode,
    // so OR pruning (keep roots) is applied even inside MergeAnd.
    Y_UNIT_TEST(MergeAnd_ModeMixAppliesOrPruning) {
        // Left has Or mode -> hasMix -> final mode Or -> OR pruning keeps the shorter token
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}, EMode::Or),
                     MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And)),
            {encodeKey("a") + encodeKey("b")}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And),
                     MakeTokens({encodeKey("a") + encodeKey("b")}, EMode::Or)),
            {encodeKey("a") + encodeKey("b")}, EMode::Or);
    }

    // A chain of three levels (grandparent -> parent -> child) in a single merge call.
    Y_UNIT_TEST(MergeAndOr_DeepChain) {
        // AND: grandparent and parent are both prefixes of child -> only child survives
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a"), encodeKey("a") + encodeKey("b")}, EMode::And),
                     MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c")})),
            {encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::And);

        // OR: root covers all -> only root survives
        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a")}),
                    MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + encodeKey("c")}, EMode::Or)),
            {encodeKey("a")}, EMode::Or);
    }

    // Hierarchical tokens mixed with completely unrelated tokens.
    Y_UNIT_TEST(MergeAndOr_MixedHierarchyAndDisjoint) {
        // {a.b, a.b.c, x} AND {a.b.c.d, y} -> {a.b.c.d, x, y}
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("x")}, EMode::And),
                     MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d"), encodeKey("y")}, EMode::And)),
            {encodeKey("a") + encodeKey("b") + encodeKey("c") + encodeKey("d"), encodeKey("x"), encodeKey("y")}, EMode::And);

        // {a, a.b, x} OR {a.b.c, y} -> {a, x, y}
        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a"), encodeKey("a") + encodeKey("b"), encodeKey("x")}, EMode::Or),
                    MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("c"), encodeKey("y")}, EMode::Or)),
            {encodeKey("a"), encodeKey("x"), encodeKey("y")}, EMode::Or);
    }

    Y_UNIT_TEST(MergeAndOr_EmptyOperands) {
        // One operand is empty: result is the other operand's single token, NotSet mode
        CheckMerge(
            MergeAnd(MakeTokens({}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b")}, EMode::NotSet);

        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({})),
            {encodeKey("a") + encodeKey("b")}, EMode::NotSet);

        CheckMerge(
            MergeAnd(MakeTokens({}), MakeTokens({})),
            {}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b")}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({})),
            {encodeKey("a") + encodeKey("b")}, EMode::NotSet);

        CheckMerge(
            MergeOr(MakeTokens({}), MakeTokens({})),
            {}, EMode::NotSet);
    }

    Y_UNIT_TEST(MergeAndOr_ErrorPropagation) {
        {
            auto r = MergeAnd(MakeError("left error"), MakeTokens({encodeKey("a") + encodeKey("b")}));
            UNIT_ASSERT_C(r.IsError(), "AND: expected error from left");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "left error");
        }
        {
            auto r = MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeError("right error"));
            UNIT_ASSERT_C(r.IsError(), "AND: expected error from right");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "right error");
        }
        {
            auto r = MergeOr(MakeError("left error"), MakeTokens({encodeKey("a") + encodeKey("b")}));
            UNIT_ASSERT_C(r.IsError(), "OR: expected error from left");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "left error");
        }
        {
            auto r = MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeError("right error"));
            UNIT_ASSERT_C(r.IsError(), "OR: expected error from right");
            UNIT_ASSERT_STRING_CONTAINS(r.GetError().GetMessage(), "right error");
        }
    }

    // Sibling paths sharing only a common ancestor but not a prefix relation between themselves.
    Y_UNIT_TEST(MergeAndOr_Siblings) {
        // a.b and a.c share ancestor a but neither is a prefix of the other
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("a") + encodeKey("c")})),
            {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b")}), MakeTokens({encodeKey("a") + encodeKey("c")})),
            {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")}, EMode::Or);

        // Mix: one sibling has a deeper descendant
        // {a.b, a.c} AND {a.b.d, a.c} -> AND: a.b.d covers a.b; a.c deduplicates -> {a.b.d, a.c}
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")}, EMode::And),
                     MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("d"), encodeKey("a") + encodeKey("c")}, EMode::And)),
            {encodeKey("a") + encodeKey("b") + encodeKey("d"), encodeKey("a") + encodeKey("c")}, EMode::And);

        // {a.b, a.c} OR {a.b.d, a.c} -> OR: a.b covers a.b.d; a.c deduplicates -> {a.b, a.c}
        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")}, EMode::Or),
                    MakeTokens({encodeKey("a") + encodeKey("b") + encodeKey("d"), encodeKey("a") + encodeKey("c")}, EMode::Or)),
            {encodeKey("a") + encodeKey("b"), encodeKey("a") + encodeKey("c")}, EMode::Or);
    }

    Y_UNIT_TEST(MergeAndOr_ValueTokens) {
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b") + strSuffix("x")}), MakeTokens({encodeKey("a") + encodeKey("b") + strSuffix("xyz")})),
            {encodeKey("a") + encodeKey("b") + strSuffix("x"), encodeKey("a") + encodeKey("b") + strSuffix("xyz")}, EMode::And);
        CheckMerge(
            MergeAnd(MakeTokens({encodeKey("a") + encodeKey("b") + strSuffix("x")}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b") + strSuffix("x")}, EMode::And);

        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b") + strSuffix("x")}), MakeTokens({encodeKey("a") + encodeKey("b") + strSuffix("xyz")})),
            {encodeKey("a") + encodeKey("b") + strSuffix("x"), encodeKey("a") + encodeKey("b") + strSuffix("xyz")}, EMode::Or);
        CheckMerge(
            MergeOr(MakeTokens({encodeKey("a") + encodeKey("b") + strSuffix("x")}), MakeTokens({encodeKey("a") + encodeKey("b")})),
            {encodeKey("a") + encodeKey("b")}, EMode::Or);
    }

    // Literal suffixes at a path and at root ($); bool false is \0\0 (must not be confused with path bytes).
    Y_UNIT_TEST(MergeAndOr_LiteralValues) {
        const TString ab = encodeKey("a") + encodeKey("b");
        const TString abStrX = ab + strSuffix("x");
        const TString abStrXyz = ab + strSuffix("xyz");
        const TString abStrEmpty = ab + strSuffix("");
        const TString abNum5 = ab + numSuffix(5.0);
        const TString abNum7 = ab + numSuffix(7.0);
        const TString abTrue = ab + boolTrueSuffix;
        const TString abFalse = ab + boolFalseSuffix;
        const TString abNull = ab + nullSuffix;

        const TString rootStrX = strSuffix("x");
        const TString rootStrA = strSuffix("a");
        const TString rootStrAb = strSuffix("ab");
        const TString rootStrEmpty = strSuffix("");
        const TString rootNum0 = numSuffix(0.0);
        const TString rootNum1 = numSuffix(1.0);

        // Same path, same kind, different values -> both kept (no byte-prefix collapse).
        CheckMergeSymmetric(abStrX, abStrXyz, {abStrX, abStrXyz});
        CheckMergeSymmetric(abNum5, abNum7, {abNum5, abNum7});
        CheckMergeSymmetric(rootStrA, rootStrAb, {rootStrA, rootStrAb});
        CheckMergeSymmetric(rootNum0, rootNum1, {rootNum0, rootNum1});

        // Same path: path-only vs each literal kind.
        CheckPathAndOrMerge(ab, abStrX, {abStrX}, {ab});
        CheckPathAndOrMerge(ab, abNum5, {abNum5}, {ab});
        CheckPathAndOrMerge(ab, abTrue, {abTrue}, {ab});
        CheckPathAndOrMerge(ab, abFalse, {abFalse}, {ab});
        CheckPathAndOrMerge(ab, abNull, {abNull}, {ab});

        // Same path: different literal kinds (false suffix is \0\0).
        CheckMergeSymmetric(abStrX, abFalse, {abStrX, abFalse});
        CheckMergeSymmetric(abStrX, abTrue, {abStrX, abTrue});
        CheckMergeSymmetric(abStrX, abNull, {abStrX, abNull});
        CheckMergeSymmetric(abStrX, abNum5, {abStrX, abNum5});
        CheckMergeSymmetric(abFalse, abTrue, {abFalse, abTrue});
        CheckMergeSymmetric(abFalse, abNull, {abFalse, abNull});
        CheckMergeSymmetric(abFalse, abNum5, {abFalse, abNum5});
        CheckMergeSymmetric(abTrue, abNull, {abTrue, abNull});
        CheckMergeSymmetric(abNum5, abTrue, {abNum5, abTrue});
        CheckMergeSymmetric(abStrEmpty, abFalse, {abStrEmpty, abFalse});

        // Root-only literals: pairwise distinct kinds and values.
        CheckMergeSymmetric(boolFalseSuffix, boolTrueSuffix, {boolFalseSuffix, boolTrueSuffix});
        CheckMergeSymmetric(boolFalseSuffix, nullSuffix, {boolFalseSuffix, nullSuffix});
        CheckMergeSymmetric(boolFalseSuffix, rootStrX, {boolFalseSuffix, rootStrX});
        CheckMergeSymmetric(boolFalseSuffix, rootStrEmpty, {boolFalseSuffix, rootStrEmpty});
        CheckMergeSymmetric(boolFalseSuffix, rootNum0, {boolFalseSuffix, rootNum0});
        CheckMergeSymmetric(boolTrueSuffix, nullSuffix, {boolTrueSuffix, nullSuffix});
        CheckMergeSymmetric(boolTrueSuffix, rootStrX, {boolTrueSuffix, rootStrX});
        CheckMergeSymmetric(nullSuffix, rootNum1, {nullSuffix, rootNum1});
        CheckMergeSymmetric(nullSuffix, rootStrX, {nullSuffix, rootStrX});
        CheckMergeSymmetric(rootStrX, rootNum1, {rootStrX, rootNum1});

        // Root literal vs keyed path / empty-key path (no \0-byte false positive).
        CheckMergeSymmetric(boolFalseSuffix, abFalse, {boolFalseSuffix, abFalse});
        CheckMergeSymmetric(boolFalseSuffix, ab, {boolFalseSuffix, ab});
        CheckMergeSymmetric(boolTrueSuffix, "\1", {boolTrueSuffix, "\1"});
        CheckMergeSymmetric(boolFalseSuffix, abStrX, {boolFalseSuffix, abStrX});
        CheckMergeSymmetric(nullSuffix, abNull, {nullSuffix, abNull});

        // Root literal vs without path
        CheckPathAndOrMerge(rootStrX, "", {rootStrX}, {""});

        // Multi-token merge: literals on the same path must survive together under AND.
        CheckMerge(
            MergeAnd(MakeTokens({ab, abFalse, abStrX, abTrue, abNull, abNum5}, EMode::And),
                     MakeTokens({abNum7}, EMode::And)),
            {abFalse, abStrX, abTrue, abNull, abNum5, abNum7}, EMode::And);

        // OR: path-only on $.a.b covers all literals on that path.
        CheckMerge(
            MergeOr(MakeTokens({ab, abFalse, abStrX}, EMode::Or),
                    MakeTokens({abTrue, abNull, abNum5}, EMode::Or)),
            {ab}, EMode::Or);

        // Root-only batch: no token is a prefix of another.
        CheckMerge(
            MergeOr(MakeTokens({boolFalseSuffix, boolTrueSuffix, nullSuffix, rootStrX, rootNum0}, EMode::Or),
                    MakeTokens({rootStrAb, rootNum1}, EMode::Or)),
            {boolFalseSuffix, boolTrueSuffix, nullSuffix, rootStrX, rootStrAb, rootNum0, rootNum1}, EMode::Or);

        CheckMerge(
            MergeAnd(MakeTokens({boolFalseSuffix, boolTrueSuffix, nullSuffix}, EMode::And),
                     MakeTokens({rootStrX, rootNum0}, EMode::And)),
            {boolFalseSuffix, boolTrueSuffix, nullSuffix, rootStrX, rootNum0}, EMode::And);
    }

    // Keys may embed a zero byte; LEB128 length prefixes must not be confused with the
    // literal separator (\0) or with unrelated keys that share raw byte prefixes.
    Y_UNIT_TEST(MergeAndOr_ZeroByte) {
        const TString keyA0 = keyWithNull("a", "");
        const TString keyA0b = keyWithNull("a", "b");
        const TString keyAb = "ab";

        const TString pathA0 = encodeKey(keyA0);
        const TString pathA0b = encodeKey(keyA0b);
        const TString pathA = encodeKey("a");
        const TString pathAb = encodeKey(keyAb);

        // Same encoded length (2-byte keys), different content.
        CheckMergeSymmetric(pathA0, pathAb, {pathA0, pathAb});

        // "a" must not match key "a\0b"
        CheckMergeSymmetric(pathA, pathA0b, {pathA, pathA0b});

        // Real ancestor–descendant: null byte only in the deepest key segment.
        const TString parent = encodePath({"a", "b"});
        const TString child = encodePath({"a", "b", keyA0b});
        CheckPathAndOrMerge(parent, child, {child}, {parent});

        // Zero byte inside a key must not be parsed as the literal separator.
        const TString pathALiteral = encodeKey("a") + strSuffix("x");
        CheckMergeSymmetric(pathA0, pathALiteral, {pathA0, pathALiteral});
        CheckMergeSymmetric(pathA0b, pathALiteral, {pathA0b, pathALiteral});

        // Path-only vs path+literal on a key that contains \0.
        const TString pathA0bLitX = pathA0b + strSuffix("x");
        CheckPathAndOrMerge(pathA0b, pathA0bLitX, {pathA0bLitX}, {pathA0b});

        // Same keyed path, different literal values.
        const TString pathA0bLitY = pathA0b + strSuffix("y");
        CheckMergeSymmetric(pathA0bLitX, pathA0bLitY, {pathA0bLitX, pathA0bLitY});

        // bool-false literal suffix (\0\0) must not collide with \0 inside a key name.
        const TString pathA0False = pathA0 + boolFalseSuffix;
        CheckPathAndOrMerge(pathA0, pathA0False, {pathA0False}, {pathA0});
        CheckMergeSymmetric(boolFalseSuffix, pathA0False, {boolFalseSuffix, pathA0False});

        // Long keys (>= 127 bytes force multi-byte LEB128 length encoding).
        TString longKey128;
        longKey128.append(64, 'x');
        longKey128.push_back('\0');
        longKey128.append(63, 'y');

        TString longKey128Alt;
        longKey128Alt.append(65, 'x');
        longKey128Alt.append(63, 'y');

        const TString pathLong128 = encodeKey(longKey128);
        const TString pathLong128Alt = encodeKey(longKey128Alt);
        UNIT_ASSERT(pathLong128.size() > 128);
        UNIT_ASSERT(pathLong128Alt.size() > 128);

        CheckMergeSymmetric(pathLong128, pathLong128Alt, {pathLong128, pathLong128Alt});

        const TString pathParentLong = encodePath({"prefix"});
        const TString pathChildLong = encodePath({"prefix", longKey128});
        CheckPathAndOrMerge(pathParentLong, pathChildLong, {pathChildLong}, {pathParentLong});

        TString longKey200;
        longKey200.append(100, 'a');
        longKey200.push_back('\0');
        longKey200.append(99, 'b');

        const TString pathLong200 = encodeKey(longKey200);
        const TString pathLong200Lit = pathLong200 + strSuffix("value");
        UNIT_ASSERT(pathLong200.size() > 200);
        CheckPathAndOrMerge(pathLong200, pathLong200Lit, {pathLong200Lit}, {pathLong200});
    }

    // Ensure different-length key names don't create false prefix matches.
    // Key "ab" (2 bytes, length prefix \x02) must NOT match as a prefix for key "a" (\x01).
    Y_UNIT_TEST(MergeAndOr_DifferentLengthKeys) {
        // \2a\2b  = path $.a.b  (keys "a" and "b", each 1 char, encoded as 2)
        // \3ab\2c = path $.ab.c (key "ab" is 2 chars, encoded as 3)
        const TString pathAB  = encodeKey("a") + encodeKey("b");
        const TString pathABC = encodeKey("ab") + encodeKey("c");  // $.ab.c — unrelated to $.a.b

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
        const TString a = encodeKey("a");
        const TString ab = encodeKey("a") + encodeKey("b");
        const TString b = encodeKey("b");

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

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("\"literal string\"", error), (TVector<TString>{strSuffix("literal string"), laxMarker + strSuffix("literal string")}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString obj = "{\"id\":42042,\"brand\":\"bricks\",\"part_count\":1401,\"price\":null,\"parts\":"
            "[{\"id\":32526,\"count\":7,\"name\":\"3x5\"},{\"id\":32523,\"count\":17,\"name\":\"1x3\"}]}";
        auto tokens = TokenizeJson(obj, error);
        std::sort(tokens.begin(), tokens.end());
        UNIT_ASSERT_VALUES_EQUAL(tokens, (TVector<TString>{
            laxMarker + encodeKey("id"),
            laxMarker + encodeKey("id") + numSuffix(42042),
            laxMarker + encodeKey("brand"),
            laxMarker + encodeKey("brand") + strSuffix("bricks"),
            laxMarker + encodeKey("parts"),
            laxMarker + encodeKey("parts") + encodeKey("id"),
            laxMarker + encodeKey("parts") + encodeKey("id"),
            laxMarker + encodeKey("parts") + encodeKey("id") + numSuffix(32526),
            laxMarker + encodeKey("parts") + encodeKey("id") + numSuffix(32523),
            laxMarker + encodeKey("parts") + encodeKey("name"),
            laxMarker + encodeKey("parts") + encodeKey("name"),
            laxMarker + encodeKey("parts") + encodeKey("name") + strSuffix("1x3"),
            laxMarker + encodeKey("parts") + encodeKey("name") + strSuffix("3x5"),
            laxMarker + encodeKey("parts") + encodeKey("count"),
            laxMarker + encodeKey("parts") + encodeKey("count"),
            laxMarker + encodeKey("parts") + encodeKey("count") + numSuffix(7),
            laxMarker + encodeKey("parts") + encodeKey("count") + numSuffix(17),
            laxMarker + encodeKey("price"),
            laxMarker + encodeKey("price") + nullSuffix,
            laxMarker + encodeKey("part_count"),
            laxMarker + encodeKey("part_count") + numSuffix(1401),

            encodeKey("id"),
            encodeKey("id") + numSuffix(42042),
            encodeKey("brand"),
            encodeKey("brand") + strSuffix("bricks"),
            encodeKey("parts"),
            encodeKey("parts") + arrayItemSuffix,
            encodeKey("parts") + arrayItemSuffix + encodeKey("id"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("id"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("id") + numSuffix(32526),
            encodeKey("parts") + arrayItemSuffix + encodeKey("id") + numSuffix(32523),
            encodeKey("parts") + arrayItemSuffix + encodeKey("name"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("name"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("name") + strSuffix("1x3"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("name") + strSuffix("3x5"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("count"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("count"),
            encodeKey("parts") + arrayItemSuffix + encodeKey("count") + numSuffix(7),
            encodeKey("parts") + arrayItemSuffix + encodeKey("count") + numSuffix(17),
            encodeKey("price"),
            encodeKey("price") + nullSuffix,
            encodeKey("part_count"),
            encodeKey("part_count") + numSuffix(1401),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString emptyKeyObj = "{\"\":{\"a\":\"b\"}}";
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(emptyKeyObj, error), (TVector<TString>{
            encodeKey(""),
            encodePath({"", "a"}),
            encodePath({"", "a"}) + strSuffix("b"),

            laxMarker + encodeKey(""),
            laxMarker + encodePath({"", "a"}),
            laxMarker + encodePath({"", "a"}) + strSuffix("b"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        TString longKey;
        longKey.resize(1000);
        for (size_t i = 0; i < longKey.size(); i++)
            longKey[i] = 'a';
        TString longKeyObj = "{\"" + longKey + "\":{\"short\":\"b\"}}";
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson(longKeyObj, error), (TVector<TString>{
            encodeKey(longKey),
            encodePath({longKey, "short"}),
            encodePath({longKey, "short"}) + strSuffix("b"),

            laxMarker + encodeKey(longKey),
            laxMarker + encodePath({longKey, "short"}),
            laxMarker + encodePath({longKey, "short"}) + strSuffix("b"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Empty object has no indexable tokens
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{}", error), TVector<TString>{});
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Empty array has no indexable tokens
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[]", error), TVector<TString>{arrayItemSuffix});
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Root-level number literal
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("42", error), (TVector<TString>{numSuffix(42), laxMarker + numSuffix(42)}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Root-level boolean literals
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("true", error), (TVector<TString>{boolTrueSuffix, laxMarker + boolTrueSuffix}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("false", error), (TVector<TString>{boolFalseSuffix, laxMarker + boolFalseSuffix}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Root-level null literal
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("null", error), (TVector<TString>{nullSuffix, laxMarker + nullSuffix}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Root-level array: elements are tokenized at the root prefix (no key added for array indices)
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[1, 2, 3]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + numSuffix(1),
            arrayItemSuffix + numSuffix(2),
            arrayItemSuffix + numSuffix(3),

            laxMarker + numSuffix(1),
            laxMarker + numSuffix(2),
            laxMarker + numSuffix(3),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Array of arrays: nested arrays are flattened to the same path prefix
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[[1, 2], [3, 4]]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix + numSuffix(1),
            arrayItemSuffix + arrayItemSuffix + numSuffix(2),
            arrayItemSuffix + arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix + numSuffix(3),
            arrayItemSuffix + arrayItemSuffix + numSuffix(4),

            laxMarker + numSuffix(1),
            laxMarker + numSuffix(2),
            laxMarker + numSuffix(3),
            laxMarker + numSuffix(4),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Deeply nested arrays
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[[1, [2, 3]], 4]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix + numSuffix(1),
            arrayItemSuffix + arrayItemSuffix + arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix + arrayItemSuffix + numSuffix(2),
            arrayItemSuffix + arrayItemSuffix + arrayItemSuffix + numSuffix(3),
            arrayItemSuffix + numSuffix(4),

            laxMarker + numSuffix(1),
            laxMarker + numSuffix(2),
            laxMarker + numSuffix(3),
            laxMarker + numSuffix(4),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Mixed-type array
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[1, \"hello\", true, false, null]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + numSuffix(1),
            arrayItemSuffix + strSuffix("hello"),
            arrayItemSuffix + boolTrueSuffix,
            arrayItemSuffix + boolFalseSuffix,
            arrayItemSuffix + nullSuffix,

            laxMarker + numSuffix(1),
            laxMarker + strSuffix("hello"),
            laxMarker + boolTrueSuffix,
            laxMarker + boolFalseSuffix,
            laxMarker + nullSuffix,
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Array of empty containers has no indexable tokens
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[[], {}, []]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix,
            arrayItemSuffix + arrayItemSuffix,
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Array of objects: each object's keys appear at the same path depth (array adds no prefix)
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[{\"a\":1},{\"a\":2}]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + encodeKey("a"),
            arrayItemSuffix + encodeKey("a") + numSuffix(1),
            arrayItemSuffix + encodeKey("a"),
            arrayItemSuffix + encodeKey("a") + numSuffix(2),

            laxMarker + encodeKey("a"),
            laxMarker + encodeKey("a") + numSuffix(1),
            laxMarker + encodeKey("a"),
            laxMarker + encodeKey("a") + numSuffix(2),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Simple root-level object
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"a\":1}", error), (TVector<TString>{
            encodeKey("a"), encodeKey("a") + numSuffix(1),

            laxMarker + encodeKey("a"), laxMarker + encodeKey("a") + numSuffix(1),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Object covering all scalar value types (binary JSON sorts keys alphabetically)
        {
            auto objAllTypes = TokenizeJson("{\"s\":\"val\",\"b\":true,\"n\":null}", error);
            std::sort(objAllTypes.begin(), objAllTypes.end());
            UNIT_ASSERT_VALUES_EQUAL(error, "");
            TVector<TString> expected{
                encodeKey("b"), encodeKey("b") + boolTrueSuffix,
                encodeKey("n"), encodeKey("n") + nullSuffix,
                encodeKey("s"), encodeKey("s") + strSuffix("val"),

                laxMarker + encodeKey("b"), laxMarker + encodeKey("b") + boolTrueSuffix,
                laxMarker + encodeKey("n"), laxMarker + encodeKey("n") + nullSuffix,
                laxMarker + encodeKey("s"), laxMarker + encodeKey("s") + strSuffix("val"),
            };
            std::sort(expected.begin(), expected.end());
            UNIT_ASSERT_VALUES_EQUAL(objAllTypes, expected);
        }

        // Empty key with a scalar value
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"\":42}", error), (TVector<TString>{
            encodeKey(""), encodeKey("") + numSuffix(42),

            laxMarker + encodeKey(""), laxMarker + encodeKey("") + numSuffix(42),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Object with an empty string value
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"a\":\"\"}", error), (TVector<TString>{
            encodeKey("a"), encodeKey("a") + strSuffix(""),

            laxMarker + encodeKey("a"), laxMarker + encodeKey("a") + strSuffix(""),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Object whose values are empty containers: only path-prefix tokens, no value tokens
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"a\":{},\"b\":[]}", error), (TVector<TString>{
            encodeKey("a"),
            encodeKey("b"),
            encodeKey("b") + arrayItemSuffix,

            laxMarker + encodeKey("a"),
            laxMarker + encodeKey("b"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Unicode string value (ASCII key, Unicode value)
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"key\":\"Привет\"}", error), (TVector<TString>{
            encodeKey("key"), encodeKey("key") + strSuffix("Привет"),

            laxMarker + encodeKey("key"), laxMarker + encodeKey("key") + strSuffix("Привет"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Multiple unicode string values
        {
            auto unicodeVals = TokenizeJson("{\"a\":\"Привет\",\"b\":\"Мир\"}", error);
            std::sort(unicodeVals.begin(), unicodeVals.end());
            UNIT_ASSERT_VALUES_EQUAL(error, "");
            TVector<TString> expected{
                encodeKey("a"), encodeKey("a") + strSuffix("Привет"),
                encodeKey("b"), encodeKey("b") + strSuffix("Мир"),

                laxMarker + encodeKey("a"), laxMarker + encodeKey("a") + strSuffix("Привет"),
                laxMarker + encodeKey("b"), laxMarker + encodeKey("b") + strSuffix("Мир"),
            };
            std::sort(expected.begin(), expected.end());
            UNIT_ASSERT_VALUES_EQUAL(unicodeVals, expected);
        }

        // Unicode key (ASCII value)
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":\"val\"}", error), (TVector<TString>{
            encodeKey("ключ"), encodeKey("ключ") + strSuffix("val"),

            laxMarker + encodeKey("ключ"), laxMarker + encodeKey("ключ") + strSuffix("val"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Unicode key and unicode value
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":\"значение\"}", error), (TVector<TString>{
            encodeKey("ключ"), encodeKey("ключ") + strSuffix("значение"),

            laxMarker + encodeKey("ключ"), laxMarker + encodeKey("ключ") + strSuffix("значение"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Nested unicode key
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":{\"поле\":\"v\"}}", error), (TVector<TString>{
            encodeKey("ключ"),
            encodePath({"ключ", "поле"}),
            encodePath({"ключ", "поле"}) + strSuffix("v"),

            laxMarker + encodeKey("ключ"),
            laxMarker + encodePath({"ключ", "поле"}),
            laxMarker + encodePath({"ключ", "поле"}) + strSuffix("v"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Unicode key with numeric value
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":42}", error), (TVector<TString>{
            encodeKey("ключ"), encodeKey("ключ") + numSuffix(42),

            laxMarker + encodeKey("ключ"), laxMarker + encodeKey("ключ") + numSuffix(42),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Unicode key with boolean value
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":true}", error), (TVector<TString>{
            encodeKey("ключ"), encodeKey("ключ") + boolTrueSuffix,

            laxMarker + encodeKey("ключ"), laxMarker + encodeKey("ключ") + boolTrueSuffix,
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Unicode key with null value
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":null}", error), (TVector<TString>{
            encodeKey("ключ"), encodeKey("ключ") + nullSuffix,

            laxMarker + encodeKey("ключ"), laxMarker + encodeKey("ключ") + nullSuffix,
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Unicode value in array
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("[\"Привет\",\"Мир\"]", error), (TVector<TString>{
            arrayItemSuffix,
            arrayItemSuffix + strSuffix("Привет"),
            arrayItemSuffix + strSuffix("Мир"),

            laxMarker + strSuffix("Привет"),
            laxMarker + strSuffix("Мир"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Array with unicode values under unicode key
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("{\"ключ\":[\"а\",\"б\"]}", error), (TVector<TString>{
            encodeKey("ключ"),
            encodeKey("ключ") + arrayItemSuffix,
            encodeKey("ключ") + arrayItemSuffix + strSuffix("а"),
            encodeKey("ключ") + arrayItemSuffix + strSuffix("б"),

            laxMarker + encodeKey("ключ"),
            laxMarker + encodeKey("ключ") + strSuffix("а"),
            laxMarker + encodeKey("ключ") + strSuffix("б"),
        }));
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        // Root-level unicode string literal
        UNIT_ASSERT_VALUES_EQUAL(TokenizeJson("\"Привет\"", error), (TVector<TString>{strSuffix("Привет"), laxMarker + strSuffix("Привет")}));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
    }

    Y_UNIT_TEST(FormatJsonIndexToken) {
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

        // unicode key (FormatJsonIndexToken must round-trip it)
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"ключ"}), ""), R"({"path":"ключ"})");
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"ключ", "поле"}), ""), R"({"path":"ключ.поле"})");
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(encodePath({"ключ"}) + strSuffix("значение"), ""), R"({"path":"ключ","literal":"значение"})");

        // unicode string literal only
        UNIT_ASSERT_VALUES_EQUAL(FormatJsonIndexToken(strSuffix("Привет"), ""), R"({"literal":"Привет"})");
    }

    Y_UNIT_TEST(Unicode) {
        // Unicode string VALUE in JSONPath (ASCII key, Unicode value)
        // Equality: path + strSuffix with unicode bytes
        ValidateJsonValue(R"(strict $.key == "Привет")", {encodePath({"key"}) + strSuffix("Привет")});
        ValidateJsonValue(R"(strict "Привет" == $.key)", {encodePath({"key"}) + strSuffix("Привет")});
        ValidateJsonValue(R"(strict $.key == "Мир")", {encodePath({"key"}) + strSuffix("Мир")});
        // Inequality / range: path only (literal dropped)
        ValidateJsonValue(R"(strict $.key != "Привет")", {encodePath({"key"})});
        ValidateJsonValue(R"(strict $.key < "Я")", {encodePath({"key"})});
        ValidateJsonValue(R"(strict $.key > "А")", {encodePath({"key"})});
        ValidateJsonValue(R"(strict $.key <= "Я")", {encodePath({"key"})});
        ValidateJsonValue(R"(strict $.key >= "А")", {encodePath({"key"})});

        // Unicode KEY in JSONPath (quoted key)
        // $."ключ" — the JSONPath parser strips the quotes, key = "ключ"
        ValidateJsonExists(R"(strict $."ключ")", {encodePath({"ключ"})});
        ValidateJsonValue(R"(strict $."ключ" == "val")", {encodePath({"ключ"}) + strSuffix("val")});
        ValidateJsonValue(R"(strict $."ключ" != "val")", {encodePath({"ключ"})});
        // starts with / like_regex with unicode key: path only
        ValidateJsonValue(R"(strict $."ключ" starts with "пр")", {encodePath({"ключ"})});
        ValidateJsonValue(R"(strict $."ключ" like_regex "пр.*")", {encodePath({"ключ"})});

        // Unicode KEY and unicode VALUE
        ValidateJsonValue(R"(strict $."ключ" == "значение")", {encodePath({"ключ"}) + strSuffix("значение")});
        ValidateJsonValue(R"(strict "значение" == $."ключ")", {encodePath({"ключ"}) + strSuffix("значение")});

        // Nested unicode keys: $."ключ"."поле"
        ValidateJsonExists(R"(strict $."ключ"."поле")", {encodePath({"ключ", "поле"})});
        ValidateJsonValue(R"(strict $."ключ"."поле" == "v")", {encodePath({"ключ", "поле"}) + strSuffix("v")});
        ValidateJsonValue(R"(strict $."ключ"."поле" == "значение")", {encodePath({"ключ", "поле"}) + strSuffix("значение")});

        // Unicode value in filter equality (JSON_EXISTS)
        ValidateJsonExists(R"(strict $.key ? (@ == "Привет"))", {encodePath({"key"}) + strSuffix("Привет")});
        ValidateJsonExists(R"(strict $ ? (@.key == "Мир"))", {encodePath({"key"}) + strSuffix("Мир")});
        ValidateJsonExists(R"(strict $ ? (@."ключ" == "значение"))", {encodePath({"ключ"}) + strSuffix("значение")});
        // Inequality in filter: path only
        ValidateJsonExists(R"(strict $.key ? (@ != "Привет"))", {encodePath({"key"})});
        ValidateJsonExists(R"(strict $ ? (@.key != "Привет"))", {encodePath({"key"})});

        // Unicode value in filter starts with / like_regex (path only)
        ValidateJsonExists(R"(strict $.a ? (@.key starts with "При"))", {encodePath({"a", "key"})});
        ValidateJsonExists(R"(strict $.a ? (@.key like_regex "При.*"))", {encodePath({"a", "key"})});
        ValidateJsonExists(R"(strict $.a ? (@ starts with "При"))", {encodePath({"a"})});
        ValidateJsonExists(R"(strict $.a ? (@ like_regex "При.*"))", {encodePath({"a"})});

        // Unicode key with filter predicate
        ValidateJsonExists(R"(strict $."ключ" ? (@ == "значение"))", {encodePath({"ключ"}) + strSuffix("значение")});
        ValidateJsonExists(R"(strict $."ключ" ? (@."поле" == "v"))", {encodePath({"ключ", "поле"}) + strSuffix("v")});

        // Unicode in filter AND/OR
        ValidateJsonExists(R"(strict $ ? (@.key == "Привет" && @.other == "Мир"))",
            {encodePath({"key"}) + strSuffix("Привет"), encodePath({"other"}) + strSuffix("Мир")});
        ValidateJsonExists(R"(strict $ ? ((@.key == "Привет") || (@.key == "Мир")))",
            {encodePath({"key"}) + strSuffix("Привет"), encodePath({"key"}) + strSuffix("Мир")});
        ValidateJsonExists(R"(strict $ ? (@."ключ" == "а" && @."поле" == "б"))",
            {encodePath({"ключ"}) + strSuffix("а"), encodePath({"поле"}) + strSuffix("б")});

        // Unicode in starts with / like_regex at JsonValue path level (path only)
        ValidateJsonValue(R"(strict $.key starts with "При")", {encodePath({"key"})});
        ValidateJsonValue(R"(strict $.key like_regex "При.*")", {encodePath({"key"})});
        ValidateJsonValue(R"(strict $."ключ" starts with "При")", {encodePath({"ключ"})});
        ValidateJsonValue(R"(strict $."ключ" like_regex "При.*")", {encodePath({"ключ"})});

        // Unicode value via variables (PASSING): variable map value is the encoded suffix
        ValidateTokens(R"(strict $.key == $var)", {encodePath({"key"}) + strSuffix("Привет")},
            {{"var", strSuffix("Привет")}});
        ValidateTokens(R"(strict $.k1 ? (@.k2 == $var))", {encodePath({"k1", "k2"}) + strSuffix("Привет")},
            {{"var", strSuffix("Привет")}}, {}, ECallableType::JsonExists);
        ValidateTokens(R"(strict $ ? (@."ключ" == $var))", {encodePath({"ключ"}) + strSuffix("значение")},
            {{"var", strSuffix("значение")}}, {}, ECallableType::JsonExists);

        // Unicode AND/OR at JsonValue level
        ValidateJsonValue(R"(strict ($.k1 == "Привет") && ($.k2 == "Мир"))",
            {encodePath({"k1"}) + strSuffix("Привет"), encodePath({"k2"}) + strSuffix("Мир")});
        ValidateJsonValue(R"(strict ($.k1 == "Привет") || ($.k2 == "Мир"))",
            {encodePath({"k1"}) + strSuffix("Привет"), encodePath({"k2"}) + strSuffix("Мир")});
        ValidateJsonValue(R"(strict ($."ключ" == "значение") && ($.k2 == "val"))",
            {encodePath({"ключ"}) + strSuffix("значение"), encodePath({"k2"}) + strSuffix("val")});
    }
}

Y_UNIT_TEST_SUITE(Covered) {
    // Helper: parse, collect, and return IsCovered()
    bool CheckCovered(const TString& jsonPath, ECallableType callableType,
        const TVarMap& variables = {}, const TVarMap& paramVariables = {})
    {
        NYql::TIssues issues;
        const TJsonPathPtr path = NYql::NJsonPath::ParseJsonPath(jsonPath, issues, 1);
        UNIT_ASSERT_C(issues.Empty(), "Parse errors for path: " + jsonPath + ": " + issues.ToOneLineString());

        auto result = CollectJsonPath(path, callableType, variables, paramVariables);
        UNIT_ASSERT_C(!result.IsError(), "Collect errors for path: " + jsonPath + ": " + result.GetError().GetMessage());

        return result.IsCovered();
    }

    // --- Covered = true ---

    Y_UNIT_TEST(JsonExistsSimpleKey) {
        // $.key for JSON_EXISTS -> token \4key = exact existence check
        UNIT_ASSERT(CheckCovered("$.key", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(JsonExistsNestedKey) {
        UNIT_ASSERT(CheckCovered("$.a.b.c", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(EqualStringLiteral) {
        // $.key == "val" -> path + literal suffix = exact value match
        UNIT_ASSERT(CheckCovered(R"($ ? (@.key == "val"))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(EqualNumberLiteral) {
        UNIT_ASSERT(CheckCovered(R"($ ? (@.key == 42))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(EqualBoolLiteral) {
        UNIT_ASSERT(CheckCovered(R"($ ? (@.key == true))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(EqualNullLiteral) {
        UNIT_ASSERT(CheckCovered(R"($ ? (@.key == null))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(EqualVariable) {
        // $.key == $param -> path + param = exact value match at runtime
        UNIT_ASSERT(CheckCovered(R"($ ? (@.key == $var))", ECallableType::JsonExists,
            {{"var", strSuffix("hello")}}, {}));
    }

    Y_UNIT_TEST(EqualParamVariable) {
        UNIT_ASSERT(CheckCovered(R"($ ? (@.key == $p))", ECallableType::JsonExists,
            {}, {{"p", "$p"}}));
    }

    Y_UNIT_TEST(AndOfCoveredPredicates) {
        UNIT_ASSERT(CheckCovered(R"($ ? (@.a == "x" && @.b == "y"))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(OrOfCoveredPredicates) {
        UNIT_ASSERT(CheckCovered(R"($ ? (@.a == "x" || @.b == "y"))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(JsonValueEqualLiteral) {
        UNIT_ASSERT(CheckCovered(R"($.key == "val")", ECallableType::JsonValue));
    }

    // --- Covered = false ---

    Y_UNIT_TEST(WildcardMember) {
        // $.* -> prefix token = matches all members
        UNIT_ASSERT(!CheckCovered("$.*", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(ArrayAccess) {
        // $.key[0] -> array subscript not in token
        UNIT_ASSERT(!CheckCovered("$.key[0]", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(WildcardArrayAccess) {
        UNIT_ASSERT(!CheckCovered("$.key[*]", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(MethodSize) {
        // $.key.size() -> method result not in token
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key.size() == 1))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(LessThan) {
        // $.key < 5 -> range not encodable
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key < 5))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(GreaterThan) {
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key > 5))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(LessEqual) {
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key <= 5))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(GreaterEqual) {
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key >= 5))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(NotEqual) {
        // $.key != "x" -> negation not encodable
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key != "x"))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(ExistsPredicate) {
        // exists($.key) is a predicate wrapping, not a simple path
        UNIT_ASSERT(!CheckCovered(R"($ ? (exists(@.key)))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(StartsWithPredicate) {
        // startsWith is a predicate that loses precision
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.key starts with "val"))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(PathEqualPath) {
        // $.a == $.b -> path==path, no literal
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.a == @.b))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(AndWithNotCovered) {
        // AND where one operand is not covered
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.a == "x" && @.b > 5))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(OrWithNotCovered) {
        // OR where one operand is not covered
        UNIT_ASSERT(!CheckCovered(R"($ ? (@.a == "x" || @.b > 5))", ECallableType::JsonExists));
    }

    Y_UNIT_TEST(MergeAndCoverage) {
        // MergeAnd of two covered results should be covered
        auto left = MakeTokens({"a"});
        auto right = MakeTokens({"b"});
        auto result = MergeAnd(std::move(left), std::move(right));
        UNIT_ASSERT(result.IsCovered());
    }

    Y_UNIT_TEST(MergeAndNotCoveredPropagates) {
        // MergeAnd where right is not covered
        auto left = MakeTokens({"a"});
        auto right = MakeTokens({"b"});
        right.SetNotCovered();
        auto result = MergeAnd(std::move(left), std::move(right));
        UNIT_ASSERT(!result.IsCovered());
    }

    Y_UNIT_TEST(MergeOrCoverage) {
        // MergeOr of two covered results should be covered
        auto left = MakeTokens({"a"});
        auto right = MakeTokens({"b"});
        auto result = MergeOr(std::move(left), std::move(right));
        UNIT_ASSERT(result.IsCovered());
    }

    Y_UNIT_TEST(MergeOrNotCoveredPropagates) {
        auto left = MakeTokens({"a"});
        auto right = MakeTokens({"b"});
        right.SetNotCovered();
        auto result = MergeOr(std::move(left), std::move(right));
        UNIT_ASSERT(!result.IsCovered());
    }
}

}  // namespace NKikimr::NJsonIndex
