#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr {
namespace NMiniKQL {

class IFunctionRegistry;

}
}

namespace NYql {

IGraphTransformer::TStatus EvaluateExpression(const TExprNode::TPtr& input, TExprNode::TPtr& output, TTypeAnnotationContext& types, TExprContext& ctx,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry, IGraphTransformer* calcTransfomer = nullptr);

}
