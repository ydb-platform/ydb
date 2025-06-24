#pragma once

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

// For given optional type returns valid value of its underlying type.
// Block<Optional<X>, Shape::Any> -> X
TExprNode::TPtr MakeValidValue(const TTypeAnnotationNode* type, TPositionHandle pos, TExprContext& ctx);

// Checks if valid value is supported for the given type
bool IsValidValueSupported(const TTypeAnnotationNode* type);

} // namespace NYql
