#pragma once
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

TExprNode::TPtr ExpandMatchRecognize(const TExprNode::TPtr &node, TExprContext &ctx, TTypeAnnotationContext& typeAnnCtx);

} //namespace NYql
