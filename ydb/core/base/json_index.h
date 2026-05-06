#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>
#include <yql/essentials/types/binary_json/format.h>

#include <set>
#include <variant>

namespace NKikimr {

namespace NJsonIndex {

// Result of the JSON index collection process
// Contains tokens for the JSON index or an error if the collection process failed
class TCollectResult {
public:
    using TTokens = std::set<TString>;
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
TCollectResult CollectJsonPath(const NYql::NJsonPath::TJsonPathPtr path, ECallableType callableType);

// Merges two collect results with AND semantics (all tokens must match)
TCollectResult MergeAnd(TCollectResult left, TCollectResult right);

// Merges two collect results with OR semantics (any token must match)
TCollectResult MergeOr(TCollectResult left, TCollectResult right);

// Appends NULL, binary JSON entry type byte, and scalar payload (the index token layout)
void AppendJsonIndexLiteral(TString& out, NBinaryJson::EEntryType type, TStringBuf stringPayload = {},
    const double* numberPayload = nullptr);

}  // namespace NJsonIndex

}  // namespace NKikimr
