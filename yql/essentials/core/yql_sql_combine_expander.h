#pragma once

#include "yql_type_annotation.h"

namespace NYql {

struct TTypeAnnotationContext;
TExprNode::TPtr ExpandSqlCombine(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);

} // namespace NYql
