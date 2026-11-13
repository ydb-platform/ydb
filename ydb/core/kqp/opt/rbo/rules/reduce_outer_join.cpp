#include "kqp_rules_include.h"

namespace NKikimr::NKqp {
namespace {

// This is SQL null propagation, not NYql::IsStrict (which concerns errors).
// Unknown functions, including null-safe equality, provide no proof.
bool IsNullOn(const TExprNode& node, const TExprNode* row, const TUnorderedIUs& nulls) {
    if (node.IsCallable("Member") && node.Child(0) == row) {
        return nulls.Contains(GetMemberId(node));
    }
    if (node.IsCallable({"Nothing", "Null", "PgNull"})) {
        return true;
    }
    if (node.IsCallable("Coalesce")) {
        return AllOf(node.Children(), [&](const auto& child) { return IsNullOn(*child, row, nulls); });
    }
    if (node.IsCallable({"==", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/", "%",
                         "Cast", "SafeCast", "StrictCast", "Convert", "Not", "Minus", "Abs",
                         "StringContains", "StartsWith", "EndsWith", "StringContainsIgnoreCase",
                         "StartsWithIgnoreCase", "EndsWithIgnoreCase"})) {
        return AnyOf(node.Children(), [&](const auto& child) { return IsNullOn(*child, row, nulls); });
    }
    return false;
}

// Prove that the predicate cannot be TRUE (or FALSE under NOT) when all
// specified columns are NULL. In particular, every OR branch must reject them;
// the branches need not reference the same column of the nullable input.
bool RejectsNulls(const TExprNode& node, const TExprNode* row, const TUnorderedIUs& nulls, bool negated = false) {
    if (IsNullOn(node, row, nulls)) {
        return true;
    }
    if (node.IsCallable("Bool")) {
        return node.Head().Content() == (negated ? "true" : "false");
    }
    if (node.IsCallable("Not")) {
        return RejectsNulls(node.Head(), row, nulls, !negated);
    }
    if (node.IsCallable({"And", "Or", "Coalesce"})) {
        const auto rejects = [&](const auto& child) { return RejectsNulls(*child, row, nulls, negated); };
        const bool any = (node.IsCallable("And") && !negated) || (node.IsCallable("Or") && negated);
        return any ? AnyOf(node.Children(), rejects) : AllOf(node.Children(), rejects);
    }
    if (node.IsCallable("Exists") && IsNullOn(node.Head(), row, nulls)) {
        return !negated;
    }
    return false;
}

bool RejectsNulls(const TExpression& expr, const TUnorderedIUs& nulls) {
    const auto lambda = NYql::NNodes::TCoLambda(expr.GetLambda());
    return RejectsNulls(*lambda.Body().Ptr(), lambda.Args().Arg(0).Ptr().Get(), nulls);
}

bool JoinRejectsNulls(const TOpJoin& join, const TUnorderedIUs& nulls) {
    for (const auto& key : join.JoinKeys.Items()) {
        if (!key.EqualNulls && (nulls.Contains(key.first) || nulls.Contains(key.second))) {
            return true;
        }
    }
    return AnyOf(join.JoinFilters, [&](const auto& filter) { return RejectsNulls(filter, nulls); });
}

bool UnmatchedRowsAreRejected(TOpJoin& join, size_t nullInput) {
    auto nulls = join.GetChild(nullInput)->GetOutputIUs();
    IOperator* current = &join;
    // Follow a single consumer. A shared producer must not be specialized for
    // just one of its consumers; Replicate ports are boundaries as well.
    while (current->Parents.size() == 1 && !nulls.Empty()) {
        const auto [parent, childIndex] = current->Parents.front();
        switch (parent->Kind) {
            case EOperator::Filter:
                if (RejectsNulls(CastOperator<TOpFilter>(*parent).GetFilterExpression(), nulls)) {
                    return true;
                }
                break;
            case EOperator::Join: {
                const auto& ancestor = CastOperator<TOpJoin>(*parent);
                const auto& kind = ancestor.JoinKind;
                // ON rejects rows only on non-preserved inputs. For example,
                // a LEFT join's ON can simplify its RHS, but never its LHS.
                const bool rejectsInput = kind == "Inner" || kind == "Cross" || kind == "LeftSemi" || kind == "RightSemi" ||
                    (childIndex == 1 && (kind == "Left" || kind == "LeftOnly")) ||
                    (childIndex == 0 && (kind == "Right" || kind == "RightOnly"));
                if (JoinRejectsNulls(ancestor, nulls)) {
                    if (rejectsInput) {
                        return true;
                    }
                    // A preserved row that cannot match also null-extends the
                    // other input. A higher predicate can reject those NULLs.
                    if (kind == "Left" || kind == "Right" || kind == "Full") {
                        nulls.UnionWith(ancestor.GetChild(childIndex ^ 1)->GetOutputIUs());
                    }
                }
                break;
            }
            case EOperator::Map: {
                auto mappedNulls = nulls;
                for (const auto& [id, element] : CastOperator<TOpMap>(*parent).GetMapElements().Items()) {
                    const auto lambda = NYql::NNodes::TCoLambda(element.GetExpression().GetLambda());
                    if (IsNullOn(*lambda.Body().Ptr(), lambda.Args().Arg(0).Ptr().Get(), nulls)) {
                        mappedNulls.Add(id);
                    }
                }
                nulls = std::move(mappedNulls);
                break;
            }
            case EOperator::Sort:
                if (CastOperator<TOpSort>(*parent).IsTopSort()) {
                    return false;
                }
                break;
            default:
                // LIMIT, aggregation, windows and shared/subquery boundaries
                // can observe rows before an ancestor predicate rejects them.
                return false;
        }
        nulls.IntersectWith(parent->GetOutputIUs());
        current = parent;
    }
    return false;
}

} // namespace

bool TReduceOuterJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::Join) {
        return false;
    }
    const auto& kind = CastOperator<TOpJoin>(input)->JoinKind;
    return kind == "Left" || kind == "Right" || kind == "Full";
}

bool TReduceOuterJoinRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);
    if (!QuickMatch(input)) {
        return false;
    }
    auto& join = *CastOperator<TOpJoin>(input);
    // Preserving one input means emitting its unmatched rows with NULLs on
    // the other input. FULL preserves both sides; prove rejection separately.
    const bool preserveLeft = (join.JoinKind == "Left" || join.JoinKind == "Full") && !UnmatchedRowsAreRejected(join, 1);
    const bool preserveRight = (join.JoinKind == "Right" || join.JoinKind == "Full") && !UnmatchedRowsAreRejected(join, 0);
    const TString kind = preserveLeft ? (preserveRight ? "Full" : "Left") : (preserveRight ? "Right" : "Inner");
    if (join.JoinKind == kind) {
        return false;
    }
    join.JoinKind = kind;
    // The rule stage invalidates derived properties and repeats, so reducing
    // one join also exposes its ON predicates to simplify lower outer joins.
    return true;
}

} // namespace NKikimr::NKqp
