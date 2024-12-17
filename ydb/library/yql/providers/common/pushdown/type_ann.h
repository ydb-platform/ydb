#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_pos_handle.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql::NPushdown {

IGraphTransformer::TStatus AnnotateFilterPredicate(
    const TExprNode::TPtr& input,
    size_t childIndex,
    const TStructExprType* itemType,
    TExprContext& ctx);


} // namespace NYql::NPushdown
