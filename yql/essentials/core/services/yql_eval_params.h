#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

IGraphTransformer::TStatus EvaluateParameters(const TExprNode::TPtr& input, TExprNode::TPtr& output, TTypeAnnotationContext& types, TExprContext& ctx,
                                              const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);

bool ExtractParametersMetaAsYson(const TExprNode::TPtr& input, TTypeAnnotationContext& types, TExprContext& ctx, NYT::TNode& paramsMetaMap);

} // namespace NYql
