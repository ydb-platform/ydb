#pragma once

#include <yql/essentials/core/yql_type_annotation.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

namespace NYql {

TExprNode::TPtr ExpandMrPartitions(NNodes::TYtRead read, TExprContext& ctx, TTypeAnnotationContext& types);

}
