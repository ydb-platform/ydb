#include "intervals_converter.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include <limits>

namespace NYql::NPushdown {

using namespace NNodes;

namespace {

struct TContext {
    TStringBuilder& Err;
};

bool ConvertPredicate(
    const TExprBase& predicate,
    TDisjointIntervalTree<i64>& tree,
    TContext& ctx
);

std::optional<i64> TryConvertExpressionToInt(
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

        bool allow = (!hasSign && valueAbs <= (ui64)Max<i64>()) || (hasSign && valueAbs <= (ui64)Max<i64>() + 1);
        if (allow) {
            return hasSign ? -(i64)valueAbs : (i64)valueAbs;
        }
    }
    if (auto atom = expression.Maybe<TCoTimestamp>()) {   
        return FromString<i64>(atom.Cast().Literal().Value());
    }
    ctx.Err << "Unsupported expression: " << expression.Raw()->Content() << ";";
    return std::nullopt;
}

void IntersectIntervals(
    TDisjointIntervalTree<i64>& tree,
    TDisjointIntervalTree<i64>& other)
{
    if (tree.GetNumIntervals() < other.GetNumIntervals()) {
        std::swap(tree, other);
    }
    // A ∩ B = A \ complement(B)
    // Erase from tree every gap that is not covered by other.

    if (other.Empty()) {
        tree.Clear();
        return;
    }

    i64 prev = Min<i64>();
    for (auto it = other.begin(); it != other.end(); ++it) {
        i64 gapBegin = prev;
        i64 gapEnd = it->first;
        if (gapBegin < gapEnd) {
            tree.EraseInterval(gapBegin, gapEnd);
        }
        prev = it->second;
    }

    // Trailing gap: [last interval end, MAX)
    if (prev < Max<i64>()) {
        tree.EraseInterval(prev, Max<i64>());
    }
}

void MergeIntervals(
    TDisjointIntervalTree<i64>& tree,
    TDisjointIntervalTree<i64>& other)
{
    if (tree.GetNumIntervals() < other.GetNumIntervals()) {
        std::swap(tree, other);
    }
    for (const auto [begin, end] : other) {
        // Erase the range first to remove any overlapping intervals,
        // then insert the interval from other.
        tree.EraseInterval(begin, end);
        tree.InsertInterval(begin, end);
    }
}

void InsertInterval(TDisjointIntervalTree<i64>& tree, i64 begin, i64 end) {
    if (begin == end) {
        return;
    }
    tree.InsertInterval(begin, end);
}

bool ConvertComparePredicate(
    const TCoCompare& compare,
    TDisjointIntervalTree<i64>& tree,
    TContext& ctx
) {
    bool leftIsMember = compare.Left().Maybe<TCoMember>().IsValid();
    bool rightIsMember = compare.Right().Maybe<TCoMember>().IsValid();
    if (!(rightIsMember ^ leftIsMember)) {
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
    i64 value = *optValue;
    tree.Clear();

    auto inc = [] (i64 value) {
        return value != Max<i64>() ? value + 1 : value;
    };
    if (compare.Maybe<TCoCmpEqual>()) {
        InsertInterval(tree, value, inc(value));
        return true;
    } else if (compare.Maybe<TCoCmpNotEqual>()) {
        InsertInterval(tree, Min<i64>(), value);
        InsertInterval(tree, inc(value), Max<i64>());
        return true;
    } else if (compare.Maybe<TCoCmpLess>()) {
        if (!inverted) {
            InsertInterval(tree, Min<i64>(), value);             // a < 100
        } else {
            InsertInterval(tree, inc(value), Max<i64>());         // 100 < a
        }
        return true;
    } else if (compare.Maybe<TCoCmpLessOrEqual>()) {
        if (!inverted) {
            InsertInterval(tree, Min<i64>(), inc(value));         // a <= 100
        } else {
            InsertInterval(tree, value, Max<i64>());             // 100 <= a
        }
        return true;
    } else if (compare.Maybe<TCoCmpGreater>()) {
        if (!inverted) {
            InsertInterval(tree, inc(value), Max<i64>());         // a > 100
        } else {
            InsertInterval(tree, Min<i64>(), value);             // 100 > a
        }
        return true;
    } else if (compare.Maybe<TCoCmpGreaterOrEqual>()) {
        if (!inverted) {
            InsertInterval(tree, value, Max<i64>());             // a >= 100
        } else {
            InsertInterval(tree, Min<i64>(), inc(value));         // 100 >= a
        }
        return true;
    }
    ctx.Err << "unknown compare operation: " << compare.Raw()->Content() << ";";
    return false;
}

bool ConvertCoalescePredicate(
    const TCoCoalesce& coalesce,
    TDisjointIntervalTree<i64>& tree,
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
    TDisjointIntervalTree<i64>& tree,
    TContext& ctx
) {
    tree.InsertInterval(Min<i64>(), Max<i64>());
    for (const auto& child : predicate.Ptr()->Children()) {
        TDisjointIntervalTree<i64> itemTree;
        if (!ConvertPredicate(TExprBase(child), itemTree, ctx)) {
            return false;
        }
        IntersectIntervals(tree, itemTree);
    }
    return true;
}

bool ConvertOrPredicate(
    const TCoOr& predicate,
    TDisjointIntervalTree<i64>& tree,
    TContext& ctx
) {
    for (const auto& child : predicate.Ptr()->Children()) {
        TDisjointIntervalTree<i64> itemTree;
        if (!ConvertPredicate(TExprBase(child), itemTree, ctx)) {
            return false;
        }
        MergeIntervals(tree, itemTree);
    }
    return true;
}

bool ConvertInPredicate(
    const TCoSqlIn& sqlIn,
    TDisjointIntervalTree<i64>& tree,
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
        ctx.Err << "unknown source for in: " << expr.Ref().Content() << ";";
        return false;
    }

    i64 min = Max<i64>();
    i64 max = Min<i64>();
    for (auto& child : collection->Children()) {
        auto value = TryConvertExpressionToInt(TExprBase(child), ctx);
        if (!value){
            ctx.Err << "unknown value for in: " << child->Content() << ";";
            return false;
        }
        min = std::min(min, *value);
        max = std::max(max, *value);
    }
    if (min == Max<i64>()) {
        ctx.Err << "TCoSqlIn with empty collection;";
        return false;
    }
    InsertInterval(tree, min, max != Max<i64>() ? max + 1: max);
    return true;
}

bool ConvertPredicate(
    const TExprBase& predicate,
    TDisjointIntervalTree<i64>& tree,
    TContext& ctx
) {
    if (auto compare = predicate.Maybe<TCoCompare>()) {
        return ConvertComparePredicate(compare.Cast(), tree, ctx);
    }
    if (auto coalesce = predicate.Maybe<TCoCoalesce>()) {
        return ConvertCoalescePredicate(coalesce.Cast(), tree, ctx);
    }
    if (auto andExpr = predicate.Maybe<TCoAnd>()) {
        return ConvertAndPredicate(andExpr.Cast(), tree, ctx);
    }
    if(auto orExpr = predicate.Maybe<TCoOr>()) {
        return ConvertOrPredicate(orExpr.Cast(), tree, ctx);
    }
    if (auto sqlIn = predicate.Maybe<TCoSqlIn>()) {
        return ConvertInPredicate(sqlIn.Cast(), tree, ctx);
    }
    ctx.Err << "unknown predicate: " << predicate.Raw()->Content() << ";";
    return false;
}

} // anonymous namespace

bool ConvertPredicateToIntervals(
    const TExprBase& predicateBody,
    TDisjointIntervalTree<i64>& tree,
    TStringBuilder& err
) {
    TContext context = {.Err = err};
    return ConvertPredicate(predicateBody, tree, context);
}

} // namespace NYql::NPushdown
