#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

namespace NYql::NPushdown {

bool ConvertPredicateToIntervals(
    TExprContext& ctx,
    const NNodes::TExprBase& predicateBody,
    TDisjointIntervalTree<ui64>& tree,
    TStringBuilder& err);

} // namespace NYql::NPushdown
