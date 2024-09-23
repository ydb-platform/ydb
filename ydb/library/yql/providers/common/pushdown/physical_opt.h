#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_pos_handle.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NYql::NPushdown {

NNodes::TMaybeNode<NNodes::TCoLambda> MakePushdownPredicate(const NNodes::TCoLambda& lambda, TExprContext& ctx, const TPositionHandle& pos, const TSettings& settings);

} // namespace NYql::NPushdown
