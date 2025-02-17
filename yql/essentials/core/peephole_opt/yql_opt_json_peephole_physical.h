#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {
    TExprNode::TPtr ExpandJsonValue(const TExprNode::TPtr& node, TExprContext& ctx);
    TExprNode::TPtr ExpandJsonExists(const TExprNode::TPtr& node, TExprContext& ctx);
    TExprNode::TPtr ExpandJsonQuery(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);
} // namespace NYql