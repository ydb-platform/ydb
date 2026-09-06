#pragma once

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NKqp::NPhysicalConvertionUtils {

// Expands one typed StrictCast over Data or Optional wrappers around it.
// The result can contain smaller StrictCast nodes, so callers must
// type-annotate and repeat until no StrictCast remains.
NYql::TExprNode::TPtr ExpandScalarStrictCast(const NYql::TExprNode::TPtr &input,
                                             NYql::TExprContext &ctx);

} // namespace NKikimr::NKqp::NPhysicalConvertionUtils
