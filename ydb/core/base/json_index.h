#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/jsonpath/parser/parser.h>

#include <variant>

namespace NKikimr {

namespace NJsonIndex {

using namespace NYql::NJsonPath;
using namespace NYql;

class TResult {
public:
    using TQueries = TVector<TString>;
    using TError = TIssue;

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
    TQueryCollector(const TJsonPathPtr path);

    TResult Collect();

private:
    TResult Collect(const TJsonPathItem& item);

    // Evaluates a literal node directly, without requiring a preceding ContextObject.
    // Use this for sub-expressions that are unconditionally literal by design
    // (e.g. the prefix argument of starts_with). Returns an error for non-literal nodes.
    TResult EvaluateLiteral(const TJsonPathItem& item);

    TResult Finalize(const TJsonPathItem& item);

    // The next methods are used to build the query step by step.
    TResult ContextObject();

    TResult MemberAccess(const TJsonPathItem& item);

    TResult ArrayAccess(const TJsonPathItem& item);
    TResult WildcardArrayAccess(const TJsonPathItem& item);
    TResult LastArrayIndex(const TJsonPathItem& item);

    TResult NullLiteral();
    TResult BooleanLiteral(const TJsonPathItem& item);
    TResult NumberLiteral(const TJsonPathItem& item);
    TResult StringLiteral(const TJsonPathItem& item);

    TResult UnaryMinus(const TJsonPathItem& item);
    TResult UnaryPlus(const TJsonPathItem& item);
    TResult BinaryAdd(const TJsonPathItem& item);
    TResult BinarySubstract(const TJsonPathItem& item);
    TResult BinaryMultiply(const TJsonPathItem& item);
    TResult BinaryDivide(const TJsonPathItem& item);
    TResult BinaryModulo(const TJsonPathItem& item);

    TResult UnaryNot(const TJsonPathItem& item);
    TResult BinaryAnd(const TJsonPathItem& item);
    TResult BinaryOr(const TJsonPathItem& item);
    TResult BinaryLess(const TJsonPathItem& item);
    TResult BinaryLessEqual(const TJsonPathItem& item);
    TResult BinaryGreater(const TJsonPathItem& item);
    TResult BinaryGreaterEqual(const TJsonPathItem& item);
    TResult BinaryEqual(const TJsonPathItem& item);
    TResult BinaryNotEqual(const TJsonPathItem& item);

    TResult StartsWithPredicate(const TJsonPathItem& item);
    TResult IsUnknownPredicate(const TJsonPathItem& item);
    TResult ExistsPredicate(const TJsonPathItem& item);
    TResult LikeRegexPredicate(const TJsonPathItem& item);

    TResult FilterObject(const TJsonPathItem& item);
    TResult FilterPredicate(const TJsonPathItem& item);

    TResult Variable(const TJsonPathItem& item);

private:
    TJsonPathReader Reader;
};

TVector<TString> BuildSearchTerms(const TString& jsonPathStr);

TVector<TString> TokenizeJson(const TStringBuf jsonStr, TString& error);
TVector<TString> TokenizeBinaryJson(const TStringBuf text);

}  // namespace NJsonIndex

}  // namespace NKikimr
