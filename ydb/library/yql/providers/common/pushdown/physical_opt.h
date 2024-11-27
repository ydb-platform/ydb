#pragma once

#include "predicate_node.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_pos_handle.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NYql::NPushdown {

NPushdown::TPredicateNode MakePushdownNode(const NNodes::TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos, const TSettings& settings);
NNodes::TMaybeNode<NNodes::TCoLambda> MakePushdownPredicate(const NNodes::TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos, const TSettings& settings);

} // namespace NYql::NPushdown
