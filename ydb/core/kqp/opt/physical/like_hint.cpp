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

std::optional<bool> ReadBool(const NYql::TExprNode* node) {
    node = SkipJust(UnwrapQuote(node));
    if (!node || !node->IsCallable("Bool") || node->ChildrenSize() < 1 || !node->Head().IsAtom()) {
        return std::nullopt;
    }
    if (node->Head().Content() == "true") {
        return true;
    }
    if (node->Head().Content() == "false") {
        return false;
    }
    return std::nullopt;
}

// SQL LIKE/ILIKE sets only CaseSensitive. Any other Re2 option can change what a match means
// (Literal=true matches the pattern text), so the LIKE implication no longer holds.
bool IsRe2OptionName(const std::string_view name) {
    return name == "CaseSensitive" || name == "DotNl" || name == "Literal" || name == "LogErrors" ||
        name == "LongestMatch" || name == "MaxMem" || name == "NeverCapture" || name == "NeverNl" ||
        name == "OneLine" || name == "PerlClasses" || name == "PosixSyntax" || name == "Utf8" ||
        name == "WordBoundary";
}

struct TOptionScan {
    std::optional<bool> CaseSensitive;
    bool Unsupported = false;
};

void ScanOptions(const NYql::TExprNode& node, TOptionScan& scan) {
    const auto* current = UnwrapQuote(&node);
    if (!current || scan.Unsupported) {
        return;
    }
    if (current->ChildrenSize() >= 2) {
        const auto* name = UnwrapQuote(current->Child(0));
        const auto* valueNode = SkipJust(UnwrapQuote(current->Child(1)));
        // Type nodes mention every option name. A setting is a bool or an integer value.
        const bool isSetting = valueNode && (valueNode->IsCallable("Bool") || valueNode->IsCallable("Uint64") ||
            valueNode->IsCallable("Int64") || valueNode->IsCallable("Ui64"));
        if (name && name->IsAtom() && isSetting && IsRe2OptionName(name->Content())) {
            if (name->Content() != "CaseSensitive") {
                scan.Unsupported = true;
                return;
            }
            const auto value = ReadBool(valueNode);
            if (!value || (scan.CaseSensitive && *scan.CaseSensitive != *value)) {
                scan.Unsupported = true;
                return;
            }
            scan.CaseSensitive = *value;
            return;
        }
    }
    for (const auto& child : current->Children()) {
        ScanOptions(*child, scan);
        if (scan.Unsupported) {
            return;
        }
    }
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
    TOptionScan options;
    ScanOptions(*runConfig->Child(1), options);
    if (options.Unsupported || !options.CaseSensitive) {
        return std::nullopt;
    }
    hint.CaseSensitive = *options.CaseSensitive;
    return hint;
}

}   // namespace NKikimr::NKqp::NOpt
