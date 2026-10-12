#include "kqp_opt_phy_olap_filter.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <algorithm>

namespace NKikimr::NKqp::NOpt {

namespace {

constexpr ui32 MinHintBytes = 3;

bool IsAsciiBytes(const TStringBuf bytes) {
    return std::all_of(bytes.begin(), bytes.end(), [](const char c) { return static_cast<unsigned char>(c) < 128; });
}

bool IsFalseBool(const TExprNode& node) {
    return node.IsCallable("Bool") && node.ChildrenSize() >= 1 && node.Head().Content() == "false";
}

const TExprNode* UnwrapQuote(const TExprNode* node) {
    while (node && node->IsCallable("quote") && node->ChildrenSize() == 1) {
        node = node->Child(0);
    }
    return node;
}

const TExprNode& SkipAssumed(const TExprNode& node) {
    return SkipCallables(node, {"AssumeStrict"});
}

bool IsPatternFromLikeUdf(const TExprNode& node) {
    const auto& udf = SkipAssumed(node);
    return udf.IsCallable("Udf") && udf.ChildrenSize() >= 1 && udf.Head().Content().StartsWith("Re2.PatternFromLike");
}

bool IsRe2MatchUdf(const TExprNode& node) {
    const auto& udf = SkipAssumed(node);
    return udf.IsCallable("Udf") && udf.ChildrenSize() >= 2 && udf.Head().IsAtom("Re2.Match");
}

const TExprNode* SkipJust(const TExprNode* node) {
    while (node && node->IsCallable("Just") && node->ChildrenSize() >= 1) {
        node = node->Child(0);
    }
    return node;
}

std::optional<bool> ReadCaseSensitive(const TExprNode& node) {
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

bool ExtractLiteral(const TExprNode& node, TString& value, bool& utf8) {
    if (!node.IsCallable({"String", "Utf8"}) || node.ChildrenSize() < 1 || !node.Head().IsAtom()) {
        return false;
    }
    value = TString(node.Head().Content());
    utf8 = node.IsCallable("Utf8");
    return true;
}

struct TLiteralRun {
    TString Bytes;
    bool AtStart = false;
    bool AtEnd = false;
};

// Split the original LIKE literal. `%` and `_` end a run; an escaped character is literal.
// Returns nothing when the escape is not a single character or the pattern ends with it.
std::optional<TVector<TLiteralRun>> SplitLiteralRuns(const TString& pattern, const TString& escape) {
    if (escape.size() > 1) {
        return std::nullopt;
    }
    const bool hasEscape = !escape.empty();
    const char escapeChar = hasEscape ? escape.front() : '\0';
    TVector<TLiteralRun> runs;
    TString current;
    bool atStart = true;
    bool escaped = false;
    for (const char c : pattern) {
        if (escaped) {
            current.push_back(c);
            escaped = false;
            continue;
        }
        if (hasEscape && c == escapeChar) {
            escaped = true;
            continue;
        }
        if (c == '%' || c == '_') {
            if (!current.empty()) {
                runs.push_back(TLiteralRun{current, atStart, false});
                current.clear();
            }
            atStart = false;
            continue;
        }
        current.push_back(c);
    }
    if (escaped) {
        return std::nullopt;
    }
    if (!current.empty()) {
        runs.push_back(TLiteralRun{std::move(current), atStart, true});
    }
    return runs;
}

struct TLikeMatch {
    TExprNode::TPtr Column;
    TString Pattern;
    bool Utf8 = false;
    bool CaseSensitive = true;
    TString Escape;
};

std::optional<TLikeMatch> ParseLikeMatch(const TExprNode& node, const TExprNode* lambdaArg) {
    if (!node.IsCallable("Apply") || node.ChildrenSize() < 2 || !IsRe2MatchUdf(node.Head())) {
        return std::nullopt;
    }
    auto column = node.ChildPtr(node.ChildrenSize() - 1);
    if (!column->IsCallable("Member") || column->ChildrenSize() < 2 || !lambdaArg || column->Child(0) != lambdaArg) {
        return std::nullopt;
    }

    const auto& udf = SkipAssumed(node.Head());
    const auto* runConfig = UnwrapQuote(udf.Child(1));
    if (!runConfig || runConfig->ChildrenSize() < 1) {
        return std::nullopt;
    }
    const auto* patternApply = runConfig->Child(0);
    if (!patternApply->IsCallable("Apply") || !IsPatternFromLikeUdf(patternApply->Head())) {
        return std::nullopt;
    }
    if (patternApply->ChildrenSize() < 2) {
        return std::nullopt;
    }

    TLikeMatch match;
    match.Column = column;
    if (!ExtractLiteral(*patternApply->Child(1), match.Pattern, match.Utf8)) {
        return std::nullopt;
    }
    if (patternApply->ChildrenSize() >= 3) {
        bool escapeUtf8 = false;
        if (!ExtractLiteral(*patternApply->Child(2), match.Escape, escapeUtf8) || match.Escape.size() != 1) {
            return std::nullopt;
        }
    }
    if (runConfig->ChildrenSize() < 2) {
        return std::nullopt;
    }
    const auto caseSensitive = ReadCaseSensitive(*runConfig->Child(1));
    if (!caseSensitive) {
        return std::nullopt;
    }
    match.CaseSensitive = *caseSensitive;
    return match;
}

bool SameColumnMember(const TExprNode& member, const TExprNode& column) {
    return member.IsCallable("Member") && column.IsCallable("Member") && member.ChildrenSize() >= 2 && column.ChildrenSize() >= 2 &&
        member.Tail().Content() == column.Tail().Content();
}

bool SiblingHasStartsWith(const TExprNode* parent, const TExprNode& column, const TString& bytes) {
    if (!parent || !parent->IsCallable("And")) {
        return false;
    }
    for (const auto& child : parent->Children()) {
        const TExprNode* node = child.Get();
        if (node->IsCallable("Coalesce") && node->ChildrenSize() >= 1) {
            node = node->Child(0);
        }
        if (!node->IsCallable({"StartsWith", "StartsWithIgnoreCase"}) || node->ChildrenSize() < 2) {
            continue;
        }
        if (!SameColumnMember(*node->Child(0), column)) {
            continue;
        }
        const auto* literal = node->Child(1);
        if (literal->IsCallable({"String", "Utf8"}) && literal->ChildrenSize() >= 1 && literal->Head().Content() == bytes) {
            return true;
        }
    }
    return false;
}

const TTypeAnnotationNode* BoolType(TExprContext& ctx, const bool optional) {
    const TTypeAnnotationNode* boolType = ctx.MakeType<TDataExprType>(EDataSlot::Bool);
    if (optional) {
        return ctx.MakeType<TOptionalExprType>(boolType);
    }
    return boolType;
}

TExprNode::TPtr MakeFalse(TExprContext& ctx, const TPositionHandle pos) {
    auto node = ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "false", TNodeFlags::Default)});
    node->SetTypeAnn(BoolType(ctx, false));
    return node;
}

const TTypeAnnotationNode* AndResultType(const TExprNode& node, TExprContext& ctx) {
    for (const auto& child : node.Children()) {
        const TTypeAnnotationNode* type = child->GetTypeAnn();
        if (type && type->GetKind() == ETypeAnnotationKind::Optional) {
            return BoolType(ctx, true);
        }
    }
    return BoolType(ctx, false);
}

TExprNode::TPtr MakeHint(const TLikeMatch& match, const TString& op, const TString& bytes, TExprContext& ctx, const TPositionHandle pos,
                         const TExprNode::TPtr& falseNode, TPushdownOptions& options) {
    const TTypeAnnotationNode* memberType = match.Column->GetTypeAnn();
    const bool optionalMember = memberType && memberType->GetKind() == ETypeAnnotationKind::Optional;
    auto literal = ctx.NewCallable(pos, match.Utf8 ? "Utf8" : "String", {ctx.NewAtom(pos, bytes)});
    literal->SetTypeAnn(ctx.MakeType<TDataExprType>(match.Utf8 ? EDataSlot::Utf8 : EDataSlot::String));
    // Reuse the match column node so the hint sees the same row argument.
    auto compare = ctx.NewCallable(pos, op, {match.Column, literal});
    compare->SetTypeAnn(BoolType(ctx, optionalMember));
    options.IndexOnlyHints.insert(compare.Get());
    auto coalesce = ctx.NewCallable(pos, "Coalesce", {compare, falseNode});
    coalesce->SetTypeAnn(BoolType(ctx, false));
    return coalesce;
}

// ASCII ignore-case kernels do not implement Re2 Unicode simple case folding (`k` / U+212A, `s` / U+017F).
// The same deviation is already accepted by OptimizeSimpleILIKE. Hints are still implied for ASCII bytes.
TVector<TExprNode::TPtr> BuildHints(const TLikeMatch& match, const TExprNode* siblingParent, TExprContext& ctx, const TPositionHandle pos,
                                    const TExprNode::TPtr& falseNode, TPushdownOptions& options) {
    const auto runs = SplitLiteralRuns(match.Pattern, match.Escape);
    if (!runs) {
        return {};
    }
    const TString startsOp = match.CaseSensitive ? "StartsWith" : "StartsWithIgnoreCase";
    const TString endsOp = match.CaseSensitive ? "EndsWith" : "EndsWithIgnoreCase";
    const TString containsOp = match.CaseSensitive ? "StringContains" : "StringContainsIgnoreCase";
    TVector<TExprNode::TPtr> hints;
    for (const auto& run : *runs) {
        if (run.Bytes.size() < MinHintBytes) {
            continue;
        }
        if (!match.CaseSensitive && !IsAsciiBytes(run.Bytes)) {
            continue;
        }
        if (run.AtStart) {
            if (!SiblingHasStartsWith(siblingParent, *match.Column, run.Bytes)) {
                hints.push_back(MakeHint(match, startsOp, run.Bytes, ctx, pos, falseNode, options));
            }
        }
        if (run.AtEnd && !run.AtStart) {
            hints.push_back(MakeHint(match, endsOp, run.Bytes, ctx, pos, falseNode, options));
        }
        if (!run.AtStart && !run.AtEnd) {
            hints.push_back(MakeHint(match, containsOp, run.Bytes, ctx, pos, falseNode, options));
        }
    }
    return hints;
}

const TExprNode* MatchOf(const TExprNode& node, const TExprNode* lambdaArg) {
    if (ParseLikeMatch(node, lambdaArg)) {
        return &node;
    }
    if (node.IsCallable("Coalesce") && node.ChildrenSize() >= 2 && IsFalseBool(node.Tail()) && ParseLikeMatch(node.Head(), lambdaArg)) {
        return &node.Head();
    }
    return nullptr;
}

} // namespace

TExprNode::TPtr RewriteComplexLikeForIndex(const TExprNode::TPtr& predicate, const TExprNode* lambdaArg, TExprContext& ctx,
                                           TPushdownOptions& options) {
    if (!options.PushdownComplexLike || !predicate) {
        return predicate;
    }
    const auto pos = predicate->Pos();
    const auto falseNode = MakeFalse(ctx, pos);

    if (predicate->IsCallable("And")) {
        TVector<TExprNode::TPtr> hints;
        TVector<TExprNode::TPtr> rest;
        rest.reserve(predicate->ChildrenSize());
        for (const auto& child : predicate->ChildrenList()) {
            if (const auto* matchNode = MatchOf(*child, lambdaArg)) {
                if (const auto parsed = ParseLikeMatch(*matchNode, lambdaArg)) {
                    auto built = BuildHints(*parsed, predicate.Get(), ctx, pos, falseNode, options);
                    hints.insert(hints.end(), built.begin(), built.end());
                }
            }
            rest.push_back(child);
        }
        if (hints.empty()) {
            return predicate;
        }
        // Hints go first so a later non-strict conjunct cannot keep them out of the pushable prefix.
        hints.insert(hints.end(), rest.begin(), rest.end());
        auto rewritten = ctx.NewCallable(pos, "And", std::move(hints));
        rewritten->SetTypeAnn(AndResultType(*rewritten, ctx));
        return rewritten;
    }

    if (predicate->IsCallable("Coalesce") && predicate->ChildrenSize() >= 2 && IsFalseBool(predicate->Tail()) &&
        predicate->Head().IsCallable("And")) {
        auto inner = RewriteComplexLikeForIndex(predicate->HeadPtr(), lambdaArg, ctx, options);
        if (inner == predicate->HeadPtr()) {
            return predicate;
        }
        // Coalesce(And, false) does not split. A Bool And is the same filter.
        if (inner->GetTypeAnn() && inner->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional) {
            return inner;
        }
        auto coalesce = ctx.NewCallable(pos, "Coalesce", {inner, falseNode});
        coalesce->SetTypeAnn(BoolType(ctx, false));
        return coalesce;
    }

    if (const auto* matchNode = MatchOf(*predicate, lambdaArg)) {
        const auto parsed = ParseLikeMatch(*matchNode, lambdaArg);
        if (!parsed) {
            return predicate;
        }
        auto hints = BuildHints(*parsed, nullptr, ctx, pos, falseNode, options);
        if (hints.empty()) {
            return predicate;
        }
        hints.push_back(predicate);
        auto rewritten = ctx.NewCallable(pos, "And", std::move(hints));
        rewritten->SetTypeAnn(AndResultType(*rewritten, ctx));
        return rewritten;
    }

    return predicate;
}

} // namespace NKikimr::NKqp::NOpt
