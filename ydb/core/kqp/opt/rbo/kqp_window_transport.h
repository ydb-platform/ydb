#pragma once

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NKqp::NWindowTransport {

// Returns the one exact window definition that is safe to transport beside an
// RBO scalar expression, or null for every unsupported/ambiguous shape.
NYql::TExprNode::TPtr FindTransportSafeWindowDefinition(
    const NYql::TExprNode::TPtr& expression,
    const NYql::TExprNode::TPtr& windowSetting);

} // namespace NKikimr::NKqp::NWindowTransport
