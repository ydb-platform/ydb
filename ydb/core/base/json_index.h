#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>

#include <variant>

namespace NKikimr {

namespace NJsonIndex {

// Result of the JSON index collection process
// Contains tokens for the JSON index or an error if the collection process failed
class TCollectResult {
public:
    using TTokens = TVector<TString>;
    using TError = NYql::TIssue;

    enum class ETokensMode {
        NotSet = 0,
        And = 1,
        Or = 2,
    };

public:
    TCollectResult();

    TCollectResult(TTokens&& tokens);

    TCollectResult(TString&& token);

    TCollectResult(TError&& issue);

    const TTokens& GetTokens() const;

    TTokens& GetTokens();

    const TError& GetError() const;

    bool IsError() const;

    bool IsFinished() const;

    bool CanCollect() const;

    void Finish();

    ETokensMode GetTokensMode() const;

    void SetTokensMode(ETokensMode mode);

private:
    std::variant<TTokens, TError> Result;
    ETokensMode TokensMode = ETokensMode::NotSet;
    bool Finished = false;
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


}  // namespace NJsonIndex

}  // namespace NKikimr
