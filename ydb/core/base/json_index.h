#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>

#include <variant>

namespace NKikimr {

namespace NJsonIndex {

class TResult {
public:
    using TQueries = TVector<TString>;
    using TError = NYql::TIssue;

    TResult(const TQueries& queries);

    TResult(TQueries&& queries);

    TResult(const TString& query);

    TResult(TString&& query);

    TResult(TError&& issue);

    const TQueries& GetQueries() const;

    TQueries& GetQueries();

    const TError& GetError() const;

    bool IsError() const;

    bool IsDone() const;

    void MarkDone();

private:
    std::variant<TQueries, TError> Result;
    bool Done = false;
};

class TQueryCollector {
public:
    TQueryCollector(const NYql::NJsonPath::TJsonPathPtr path);

    TResult Collect();

private:
    TResult Collect(const NYql::NJsonPath::TJsonPathItem& item);

    // Evaluates a literal node directly, without requiring a preceding ContextObject.
    // Use this for sub-expressions that are unconditionally literal by design
    // (e.g. the prefix argument of starts_with). Returns an error for non-literal nodes.
    TResult EvaluateLiteral(const NYql::NJsonPath::TJsonPathItem& item);

    TResult Finalize(const NYql::NJsonPath::TJsonPathItem& item);

    TResult FinalizeEmpty(const NYql::NJsonPath::TJsonPathItem& item);

    // The next methods are used to build the query step by step.
    TResult ContextObject();

    TResult MemberAccess(const NYql::NJsonPath::TJsonPathItem& item);

    TResult ArrayAccess(const NYql::NJsonPath::TJsonPathItem& item);
    TResult WildcardArrayAccess(const NYql::NJsonPath::TJsonPathItem& item);
    TResult LastArrayIndex(const NYql::NJsonPath::TJsonPathItem& item);

    TResult NullLiteral();
    TResult BooleanLiteral(const NYql::NJsonPath::TJsonPathItem& item);
    TResult NumberLiteral(const NYql::NJsonPath::TJsonPathItem& item);
    TResult StringLiteral(const NYql::NJsonPath::TJsonPathItem& item);

    TResult UnaryMinus(const NYql::NJsonPath::TJsonPathItem& item);
    TResult UnaryPlus(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryAdd(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinarySubstract(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryMultiply(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryDivide(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryModulo(const NYql::NJsonPath::TJsonPathItem& item);

    TResult UnaryNot(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryAnd(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryOr(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryLess(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryLessEqual(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryGreater(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryGreaterEqual(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryEqual(const NYql::NJsonPath::TJsonPathItem& item);
    TResult BinaryNotEqual(const NYql::NJsonPath::TJsonPathItem& item);

    TResult StartsWithPredicate(const NYql::NJsonPath::TJsonPathItem& item);
    TResult IsUnknownPredicate(const NYql::NJsonPath::TJsonPathItem& item);
    TResult ExistsPredicate(const NYql::NJsonPath::TJsonPathItem& item);
    TResult LikeRegexPredicate(const NYql::NJsonPath::TJsonPathItem& item);

    TResult FilterObject(const NYql::NJsonPath::TJsonPathItem& item);
    TResult FilterPredicate(const NYql::NJsonPath::TJsonPathItem& item);

    TResult Variable(const NYql::NJsonPath::TJsonPathItem& item);

private:
    NYql::NJsonPath::TJsonPathReader Reader;
};

TVector<TString> BuildSearchTerms(const TString& jsonPathStr);

TVector<TString> TokenizeJson(const TStringBuf jsonStr, TString& error);
TVector<TString> TokenizeBinaryJson(const TStringBuf text);

}  // namespace NJsonIndex

}  // namespace NKikimr
