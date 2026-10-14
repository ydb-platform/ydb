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
        node = node->Child(0);
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
    if (node.ChildrenSize() >= 2 && node.Child(0)->IsAtom("CaseSensitive")) {
        const auto* value = SkipJust(node.Child(1));
        if (value && value->IsCallable("Bool") && value->ChildrenSize() >= 1 && value->Head().IsAtom()) {
            return value->Head().Content() == "true";
        }
    }
    for (const auto& child : node.Children()) {
        if (const auto found = ReadCaseSensitive(*child)) {
            return found;
        }
    }
    return std::nullopt;
}

bool ExtractLiteral(const NYql::TExprNode& node, TString& value) {
    if (!node.IsCallable({"String", "Utf8"}) || node.ChildrenSize() < 1 || !node.Head().IsAtom()) {
        return false;
    }
    value = TString(node.Head().Content());
    return true;
}

}   // namespace

std::optional<TLikeHint> TryParseLikeHint(const NYql::TExprNode& predicate) {
    const NYql::TExprNode* body = &predicate;
    if (body->IsCallable("Coalesce") && body->ChildrenSize() >= 2 && IsFalseBool(body->Tail())) {
        body = body->Child(0);
    }
    if (!body->IsCallable("Apply") || body->ChildrenSize() != 2 || !IsRe2Udf(body->Head(), "Re2.Match")) {
        return std::nullopt;
    }
    const NYql::TExprNode* column = &body->Tail();
    if (!column->IsCallable("Member")) {
        return std::nullopt;
    }

    const auto& udf = NYql::SkipCallables(body->Head(), {"AssumeStrict"});
    const auto* runConfig = UnwrapQuote(udf.Child(1));
    if (!runConfig || runConfig->ChildrenSize() < 2) {
        return std::nullopt;
    }
    const auto* patternApply = runConfig->Child(0);
    if (!patternApply->IsCallable("Apply") || patternApply->ChildrenSize() < 2 || !IsRe2Udf(patternApply->Head(), "Re2.PatternFromLike")) {
        return std::nullopt;
    }

    TLikeHint hint;
    hint.Column = column;
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
