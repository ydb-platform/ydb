#include "intervals_converter.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <limits>

namespace NYql::NPushdown {

using namespace NNodes;

namespace {

struct TContext {
    TStringBuilder& Err;
    TExprContext& Ctx;
};

bool ConvertPredicate(
    const TExprBase& predicate,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
);

std::optional<ui64> TryConvertExpressionToInt(
    const TExprBase& expression,
    TContext& ctx
) {
    if (auto just = expression.Maybe<TCoJust>()) {
        return TryConvertExpressionToInt(TExprBase(just.Cast().Input()), ctx);
    }
    if (auto atom = expression.Maybe<TCoIntegralCtor>()) {
        bool hasSign;
        bool isSigned;
        ui64 valueAbs;
        ExtractIntegralValue(atom.Ref(), false, hasSign, isSigned, valueAbs);
        if (hasSign) {
            return 0;   // TODO
        }
        return valueAbs;
    }
    if (auto atom = expression.Maybe<TCoTimestamp>()) {   
        return FromString<ui64>(atom.Cast().Literal().Value());
    }

    ctx.Err << "Unsupported expression: " << expression.Raw()->Content();
    return std::nullopt;
}

void IntersectIntervals(
    TDisjointIntervalTree<ui64>& tree,
    const TDisjointIntervalTree<ui64>& other)
{
    // A ∩ B = A \ complement(B)
    // Erase from tree every gap that is not covered by other.

    if (other.Empty()) {
        tree.Clear();
        return;
    }

    ui64 prev = std::numeric_limits<ui64>::min();
    for (auto it = other.begin(); it != other.end(); ++it) {
        ui64 gapBegin = prev;
        ui64 gapEnd = it->first;
        if (gapBegin < gapEnd) {
            tree.EraseInterval(gapBegin, gapEnd);
        }
        prev = it->second;
    }

    // Trailing gap: [last interval end, MAX)
    if (prev < std::numeric_limits<ui64>::max()) {
        tree.EraseInterval(prev, std::numeric_limits<ui64>::max());
    }
}

void MergeIntervals(
    TDisjointIntervalTree<ui64>& tree,
    const TDisjointIntervalTree<ui64>& other)
{
    for (const auto [begin, end] : other) {
        // Erase the range first to remove any overlapping intervals,
        // then insert the interval from other.
        tree.EraseInterval(begin, end);
        tree.InsertInterval(begin, end);
    }
}

void InsertInterval(TDisjointIntervalTree<ui64>& tree, ui64 begin, ui64 end) {
    if (begin == end) {
        return;
    }
    tree.InsertInterval(begin, end);
}

bool ConvertComparePredicate(
    const TCoCompare& compare,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
) {
    bool leftIsMember = compare.Left().Maybe<TCoMember>().IsValid();
    bool rightIsMember = compare.Right().Maybe<TCoMember>().IsValid();
    if (!(leftIsMember && !rightIsMember || !leftIsMember && rightIsMember)) {
        return false;
    }
    
    auto valueExpr = compare.Right();
    bool inverted = rightIsMember;
    if (inverted) {
        valueExpr = compare.Left();
    }
    
    auto optValue = TryConvertExpressionToInt(valueExpr, ctx);
    if (!optValue) {
        return false;
    }
    ui64 value = *optValue;
    tree.Clear();

    if (compare.Maybe<TCoCmpEqual>()) {
        tree.Insert(value);
        return true;
    } else if (compare.Maybe<TCoCmpNotEqual>()) {
        InsertInterval(tree, std::numeric_limits<ui64>::min(), value);
        InsertInterval(tree, value + 1, std::numeric_limits<ui64>::max());
        return true;
    } else if (compare.Maybe<TCoCmpLess>()) {
        if (!inverted) {
            InsertInterval(tree, std::numeric_limits<ui64>::min(), value);             // a < 100
        } else {
            InsertInterval(tree, value + 1, std::numeric_limits<ui64>::max());         // 100 < a
        }
        return true;
    } else if (compare.Maybe<TCoCmpLessOrEqual>()) {
        if (!inverted) {
            InsertInterval(tree, std::numeric_limits<ui64>::min(), value + 1);         // a <= 100
        } else {
            InsertInterval(tree, value, std::numeric_limits<ui64>::max());             // 100 <= a
        }
        return true;
    } else if (compare.Maybe<TCoCmpGreater>()) {
        if (!inverted) {
            InsertInterval(tree, value + 1, std::numeric_limits<ui64>::max());         // a > 100
        } else {
            InsertInterval(tree, std::numeric_limits<ui64>::min(), value);             // 100 > a
        }
        return true;
    } else if (compare.Maybe<TCoCmpGreaterOrEqual>()) {
        if (!inverted) {
            InsertInterval(tree, value, std::numeric_limits<ui64>::max());             // a >= 100
        } else {
            InsertInterval(tree, std::numeric_limits<ui64>::min(), value + 1);         // 100 >= a
        }
        return true;
    }
    ctx.Err << "unknown compare operation: " << compare.Raw()->Content();
    return false;
}

bool ConvertCoalescePredicate(
    const TCoCoalesce& coalesce,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
) {
    auto value = coalesce.Value().Maybe<TCoBool>();
    if (value && TStringBuf(value.Cast().Literal()) == "false"sv) {
        return ConvertPredicate(TExprBase(coalesce.Predicate()), tree, ctx);
    }
    return false;
}

bool ConvertAndPredicate(
    const TCoAnd& predicate,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
) {
    tree.InsertInterval(std::numeric_limits<ui64>::min(), std::numeric_limits<ui64>::max());
    for (const auto& child : predicate.Ptr()->Children()) {
        TDisjointIntervalTree<ui64> itemTree;
        if (!ConvertPredicate(TExprBase(child), itemTree, ctx)) {
            return false;
        }
        IntersectIntervals(tree, itemTree);
    }
    return true;
}

bool ConvertOrPredicate(
    const TCoOr& predicate,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
) {
    for (const auto& child : predicate.Ptr()->Children()) {
        TDisjointIntervalTree<ui64> itemTree;
        if (!ConvertPredicate(TExprBase(child), itemTree, ctx)) {
            return false;
        }
        MergeIntervals(tree, itemTree);
    }
    return true;
}

bool ConvertInPredicate(
    const TCoSqlIn& sqlIn,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
) {
    const TExprBase& expr = sqlIn.Collection();
    const TExprBase& lookup = sqlIn.Lookup();
    bool isMember = lookup.Maybe<TCoMember>().IsValid();
    if (!isMember) {
        return false;
    }

    TExprNode::TPtr collection;
    if (expr.Ref().IsList()) {
        collection = expr.Ptr();
    } else if (auto maybeAsList = expr.Maybe<TCoAsList>()) {
        collection = maybeAsList.Cast().Ptr();
    } else {
        ctx.Err << "unknown source for in: " << expr.Ref().Content();
        return false;
    }
    std::set<ui64> values;
    for (auto& child : collection->Children()) {
        auto value = TryConvertExpressionToInt(TExprBase(child), ctx);
        if (!value){
            ctx.Err << "unknown value for in: " << child->Content();
            return false;
        }
        values.insert(*value);
    }
    if (values.empty()) {
        ctx.Err << "TCoSqlIn with empty collection";
        return false;
    }
    InsertInterval(tree, *values.begin(), *values.rbegin() + 1);
    return true;
}


bool ConvertPredicate(
    const TExprBase& predicate,
    TDisjointIntervalTree<ui64>& tree,
    TContext& ctx
) {
    if (auto compare = predicate.Maybe<TCoCompare>()) {
        return ConvertComparePredicate(compare.Cast(), tree, ctx);
    } else if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
        return ConvertCoalescePredicate(coalesce.Cast(), tree, ctx);
    } else if (auto andExpr = predicate.Maybe<TCoAnd>()) {
        return ConvertAndPredicate(andExpr.Cast(), tree, ctx);
    } else if(auto orExpr = predicate.Maybe<TCoOr>()) {
        return ConvertOrPredicate(orExpr.Cast(), tree, ctx);
    } else if (auto sqlIn = predicate.Maybe<TCoSqlIn>()) {
        return ConvertInPredicate(sqlIn.Cast(), tree, ctx);
    }
    ctx.Err << "unknown predicate: " << predicate.Raw()->Content();
    return false;
}

}

bool ConvertPredicateToIntervals(
    TExprContext& ctx,
    const TExprBase& predicateBody,
    TDisjointIntervalTree<ui64>& tree,
    TStringBuilder& err
) {
    TContext context = {.Err = err, .Ctx = ctx};
    return ConvertPredicate(predicateBody, tree, context);
}

} // namespace NYql::NPushdown
