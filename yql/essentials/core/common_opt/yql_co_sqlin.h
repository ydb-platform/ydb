#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

using TShouldConvertSqlInToJoinPredicate = std::function<bool(const NNodes::TCoSqlIn&, bool /* negated */)>;

TExprNode::TPtr TryConvertSqlInPredicatesToJoins(const NNodes::TCoFlatMapBase& flatMap,
    TShouldConvertSqlInToJoinPredicate shouldConvertSqlInToJoin, TExprContext& ctx, bool prefixOnly = false);

} // namespace NYql
