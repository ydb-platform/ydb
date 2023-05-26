#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {
    TExprNode::TPtr ExpandJsonValue(const TExprNode::TPtr& node, TExprContext& ctx);
    TExprNode::TPtr ExpandJsonExists(const TExprNode::TPtr& node, TExprContext& ctx);
    TExprNode::TPtr ExpandJsonQuery(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);
} // namespace NYql