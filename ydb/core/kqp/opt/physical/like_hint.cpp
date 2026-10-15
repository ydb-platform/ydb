#include "like_hint.h"

#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

namespace {

const NYql::TExprNode* UnwrapQuote(const NYql::TExprNode* node) {
    while (node && node->IsCallable("quote") && node->ChildrenSize() == 1) {
        node = node->Child(0);
    }
    return node;
}

const NYql::TExprNode* SkipJust(const NYql::TExprNode* node) {
    while (node && node->IsCallable("Just") && node->ChildrenSize() >= 1) {
        node = SkipJust(UnwrapQuote(node->Child(0)));
    }
    return node;
}

bool IsFalseBool(const NYql::TExprNode& node) {
    return node.IsCallable("Bool") && node.ChildrenSize() >= 1 && node.Head().Content() == "false";
}

bool IsRe2Udf(const NYql::TExprNode& node, const std::string_view name) {
    const auto& udf = NYql::SkipCallables(node, {"AssumeStrict"});
    return udf.IsCallable("Udf") && udf.ChildrenSize() >= 1 && udf.Head().Content().StartsWith(name);
}

std::optional<bool> ReadCaseSensitive(const NYql::TExprNode& node) {
    const auto* current = UnwrapQuote(&node);
    if (!current) {
        return std::nullopt;
    }
    if (current->ChildrenSize() >= 2) {
        const auto* name = UnwrapQuote(current->Child(0));
        if (name && name->IsAtom("CaseSensitive")) {
            const auto* value = SkipJust(UnwrapQuote(current->Child(1)));
            if (value && value->IsCallable("Bool") && value->ChildrenSize() >= 1 && value->Head().IsAtom()) {
                return value->Head().Content() == "true";
            }
        }
    }
    for (const auto& child : current->Children()) {
        if (const auto found = ReadCaseSensitive(*child)) {
            return found;
        }
    }
    return std::nullopt;
}

bool ExtractLiteral(const NYql::TExprNode& node, TString& value) {
    const auto* literal = SkipJust(UnwrapQuote(&node));
    if (!literal || !literal->IsCallable({"String", "Utf8"}) || literal->ChildrenSize() < 1 || !literal->Head().IsAtom()) {
        return false;
    }
    value = TString(literal->Head().Content());
    return true;
}

// Optional columns are applied via Map/FlatMap (UDF AutoMap). The matched column is that input,
// or Unwrap(Member) when the match is applied directly.
const NYql::TExprNode* MemberColumn(const NYql::TExprNode* node) {
    node = SkipJust(UnwrapQuote(node));
    while (node && node->IsCallable({"Unwrap", "SafeCast"})) {
        node = SkipJust(UnwrapQuote(node->Child(0)));
    }
    if (node && node->IsCallable("Member")) {
        return node;
    }
    return nullptr;
}

struct TLocatedMatch {
    const NYql::TExprNode* Apply = nullptr;
    const NYql::TExprNode* Column = nullptr;
};

std::optional<TLocatedMatch> LocateMatch(const NYql::TExprNode* node, const NYql::TExprNode* boundArg, const NYql::TExprNode* boundColumn) {
    node = UnwrapQuote(node);
    if (!node) {
        return std::nullopt;
    }
    if (node->IsCallable("Coalesce") && node->ChildrenSize() >= 2 && IsFalseBool(node->Tail())) {
        return LocateMatch(node->Child(0), boundArg, boundColumn);
    }
    if (node->IsCallable("Just")) {
        return LocateMatch(&node->Head(), boundArg, boundColumn);
    }
    if (node->IsCallable({"Map", "FlatMap"}) && node->ChildrenSize() >= 2 && node->Tail().IsLambda()) {
        const auto& lambda = node->Tail();
        if (lambda.Head().ChildrenSize() != 1) {
            return std::nullopt;
        }
        const auto* inputColumn = MemberColumn(node->Child(0));
        if (!inputColumn) {
            return std::nullopt;
        }
        return LocateMatch(&lambda.Tail(), &lambda.Head().Head(), inputColumn);
    }
    if (!node->IsCallable("Apply") || node->ChildrenSize() != 2 || !IsRe2Udf(node->Head(), "Re2.Match")) {
        return std::nullopt;
    }
    const NYql::TExprNode* column = (boundArg && &node->Tail() == boundArg) ? boundColumn : MemberColumn(&node->Tail());
    if (!column) {
        return std::nullopt;
    }
    return TLocatedMatch{node, column};
}

}   // namespace

std::optional<TLikeHint> TryParseLikeHint(const NYql::TExprNode& predicate) {
    const auto located = LocateMatch(&predicate, nullptr, nullptr);
    if (!located) {
        return std::nullopt;
    }

    const auto& udf = NYql::SkipCallables(located->Apply->Head(), {"AssumeStrict"});
    const auto* runConfig = UnwrapQuote(udf.Child(1));
    if (!runConfig || runConfig->ChildrenSize() < 2) {
        return std::nullopt;
    }
    const auto* patternApply = UnwrapQuote(runConfig->Child(0));
    if (!patternApply || !patternApply->IsCallable("Apply") || patternApply->ChildrenSize() < 2 ||
        !IsRe2Udf(patternApply->Head(), "Re2.PatternFromLike")) {
        return std::nullopt;
    }

    TLikeHint hint;
    hint.Column = located->Column;
    if (!ExtractLiteral(*patternApply->Child(1), hint.Pattern)) {
        return std::nullopt;
    }
    if (patternApply->ChildrenSize() >= 3) {
        if (!ExtractLiteral(*patternApply->Child(2), hint.Escape) || hint.Escape.size() != 1) {
            return std::nullopt;
        }
    }
    const auto caseSensitive = ReadCaseSensitive(*runConfig->Child(1));
    if (!caseSensitive) {
        return std::nullopt;
    }
    hint.CaseSensitive = *caseSensitive;
    return hint;
}

}   // namespace NKikimr::NKqp::NOpt
