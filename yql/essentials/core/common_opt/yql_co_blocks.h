#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

struct TTypeAnnotationContext;

IGraphTransformer::TStatus OptimizeBlocks(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    TTypeAnnotationContext& typeCtx);

} // NYql
