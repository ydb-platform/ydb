#pragma once

#include <ydb/library/yql/providers/function/gateway/dq_function_gateway.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NDqFunction {

    // Called before UDF type resolution. Returns the original node for ordinary UDFs,
    // nullptr on invalid service invocation, or a typed-contract service marker.
    TExprNode::TPtr RewriteServiceExpression(const TExprNode::TPtr& apply,
                                             const TDqFunctionGatewayFactory& factory, TExprContext& ctx);

} // namespace NYql::NDqFunction
