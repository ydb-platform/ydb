#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>

#include <variant>

namespace NKikimr {

namespace NJsonIndex {

class TResult {
public:
    using TTokens = TVector<TString>;
    using TError = NYql::TIssue;

    TResult();

    TResult(TTokens&& tokens);

    TResult(TString&& token);

    TResult(TError&& issue);

    const TTokens& GetTokens() const;

    TTokens& GetTokens();

    const TError& GetError() const;

    bool IsError() const;

    bool IsFinished() const;

    bool CanCollect() const;

    void Finish();

private:
    std::variant<TTokens, TError> Result;
    bool Finished = false;
};

class TQueryCollector {
    enum class ECollectMode {
        None = 0,
        Context = 1,
    };

public:
    enum class ECallableType {
        JsonExists = 0,
        JsonValue = 1,
        JsonQuery = 2
    };

public:
    TQueryCollector(const NYql::NJsonPath::TJsonPathPtr path, ECallableType callableType);

    TResult Collect();

private:
    TResult Collect(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);

    TResult ContextObject();

    TResult MemberAccess(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);
    TResult WildcardMemberAccess(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);

    TResult ArrayAccess(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);
    TResult WildcardArrayAccess(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);
    TResult LastArrayIndex(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);

    TResult UnaryArithmeticOp(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);
    TResult BinaryArithmeticOp(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);

    TResult UnaryNot(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryAnd(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryOr(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryLess(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryLessEqual(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryGreater(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryGreaterEqual(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryEqual(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryNotEqual(const NYql::NJsonPath::TJsonPathItem& item);

    TResult Methods(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);
    TResult Predicates(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);

    TResult FilterObject(const NYql::NJsonPath::TJsonPathItem& item);
    TResult FilterPredicate(const NYql::NJsonPath::TJsonPathItem& item);

    TResult EvaluateLiteral(const NYql::NJsonPath::TJsonPathItem& item, ECollectMode collectMode);
    TResult Variable(const NYql::NJsonPath::TJsonPathItem& item);

private:
    NYql::NJsonPath::TJsonPathReader Reader;
    ECallableType CallableType;
};

TVector<TString> BuildSearchTerms(const TString& jsonPathStr);

TVector<TString> TokenizeJson(const TStringBuf jsonStr, TString& error);
TVector<TString> TokenizeBinaryJson(const TStringBuf text);

}  // namespace NJsonIndex

}  // namespace NKikimr
