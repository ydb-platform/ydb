#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <yql/essentials/types/binary_json/format.h>

#include <compare>
#include <set>
#include <variant>

namespace NKikimr {

namespace NJsonIndex {

// YQL json numbers are double precision floats. The maximum supported integer is +-2^53.
static constexpr i64 MaxSupportedInt = 9007199254740992ll;

// A token paired with an optional parameter name
struct TToken {
public:
    // Constructs a token with the given path token (full or partial path-prefix token)
    explicit TToken(const TString& pathToken)
        : PathToken(pathToken)
    {
    }

    // Constructs a token with the given path token and parameter name (partial path-prefix and YQL param name)
    explicit TToken(const TString& pathToken, const TString& paramName)
        : PathToken(pathToken)
        , ParamName(paramName)
    {
    }

    auto operator<=>(const TToken& other) const {
        return std::tie(PathToken, ParamName) <=> std::tie(other.PathToken, other.ParamName);
    }

    bool operator==(const TToken& other) const {
        return std::tie(PathToken, ParamName) == std::tie(other.PathToken, other.ParamName);
    }

public:
    // Full or partial path-prefix token
    TString PathToken;

    // YQL param name. Empty for regular tokens
    TString ParamName;
};

using TTokens = std::set<TToken>;

// Result of the JSON index collection process
// Contains tokens for the JSON index or an error if the collection process failed
class TCollectResult {
public:
    using TError = NYql::TIssue;

    enum class ETokensMode {
        NotSet = 0,
        And = 1,
        Or = 2,
    };

public:
    // Constructs a collect result with the given tokens
    TCollectResult(TTokens&& tokens);

    // Constructs a collect result with the given token
    TCollectResult(TString&& token);

    // Constructs a collect result with the given error
    TCollectResult(TError&& issue);

    // Returns the collected tokens for the JSON index
    const TTokens& GetTokens() const;

    // Returns the collected tokens for the JSON index for modification
    TTokens& GetTokens();

    // Returns the error if the collection process failed
    const TError& GetError() const;

    // Returns true if the collection process failed
    bool IsError() const;

    // Returns true if the collection process can be continued
    // If it is true, the result token can be extended with the next token (e.g. literal)
    // It does not affect collecting multiple tokens (AND, OR, etc.)
    bool CanCollect() const;

    // Stops the collection process
    // It means that the result token is completed and cannot be extended
    // It does not affect collecting multiple tokens (AND, OR, etc.)
    void StopCollecting();

    // Returns the tokens mode
    ETokensMode GetTokensMode() const;

    // Sets the tokens mode
    void SetTokensMode(ETokensMode mode);

private:
    std::variant<TTokens, TError> Result;
    ETokensMode TokensMode = ETokensMode::NotSet;
    bool Stopped = false;
};

// Type of the callable function that is used for the JSON index collection
// It is given from the predicate of the SELECT statement
enum class ECallableType {
    JsonExists = 0,
    JsonValue = 1,
    JsonQuery = 2,
};

// Tokenizes the given JSON string into a list of tokens
// The tokenization result is filled into the JSON index table
TVector<TString> TokenizeJson(const TStringBuf jsonStr, TString& error);

// Tokenizes the given binary JSON into a list of tokens
// The tokenization result is filled into the JSON index table
TVector<TString> TokenizeBinaryJson(const TStringBuf text);

// Builds tokens for the given jsonpath expression
// The tokens are used for searching in the JSON index
TCollectResult CollectJsonPath(const NYql::NJsonPath::TJsonPathPtr path, ECallableType callableType,
    const std::unordered_map<TString, TString>& variables, const std::unordered_map<TString, TString>& paramVariables = {});

// Merges two collect results with AND semantics (all tokens must match)
TCollectResult MergeAnd(TCollectResult left, TCollectResult right);

// Merges two collect results with OR semantics (any token must match)
TCollectResult MergeOr(TCollectResult left, TCollectResult right);

// Appends NULL, binary JSON entry type byte, and scalar payload (the index token layout)
void AppendJsonIndexLiteral(TString& out, NBinaryJson::EEntryType type, TStringBuf stringPayload = {},
    const double* numberPayload = nullptr);

// Formats an index token and optional parameter name as a JSON-like object string.
// The path portion is decoded from length-prefixed key segments joined by '.'.
// A literal suffix (after the \0 separator) is decoded by its EEntryType byte.
// Examples:
//   ("\3k1\3k2", "")     -> {"path": "k1.k2"}
//   ("\3k1\3k2\0\1", "") -> {"path": "k1.k2", "literal": true}
//   ("\3k1\3k2", "$p")   -> {"path": "k1.k2", "param": "$p"}
//   ("\0\0", "")         -> {"literal": false}
//   ("", "$p")           -> {"param": "$p"}
//   ("", "")             -> {}
TString FormatJsonIndexToken(const TString& pathToken, const TString& paramName);

}  // namespace NJsonIndex

}  // namespace NKikimr
