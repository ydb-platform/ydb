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
    enum class EMode {
        Context = 0,
        Filter = 1,
        Predicate = 2,
        Literal = 3
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
    TResult Collect(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult CollectEqualOperands(const NYql::NJsonPath::TJsonPathItem& leftItem,
        const NYql::NJsonPath::TJsonPathItem& rightItem);

    TResult CollectArithmeticOperand(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult ContextObject(EMode mode);

    TResult MemberAccess(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult WildcardMemberAccess(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult ArrayAccess(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult WildcardArrayAccess(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult LastArrayIndex(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult UnaryArithmeticOp(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryArithmeticOp(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult UnaryNot(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryAnd(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryOr(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryLess(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryLessEqual(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryGreater(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryGreaterEqual(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryEqual(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult BinaryNotEqual(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult Methods(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult Predicates(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult FilterObject(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult FilterPredicate(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    TResult Literal(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);
    TResult Variable(const NYql::NJsonPath::TJsonPathItem& item, EMode mode);

    bool ArePredicatesAllowed(EMode mode) const;

private:
    NYql::NJsonPath::TJsonPathReader Reader;
    ECallableType CallableType;
};

TVector<TString> BuildSearchTerms(const TString& jsonPathStr);

TVector<TString> TokenizeJson(const TStringBuf jsonStr, TString& error);
TVector<TString> TokenizeBinaryJson(const TStringBuf text);

}  // namespace NJsonIndex

}  // namespace NKikimr
