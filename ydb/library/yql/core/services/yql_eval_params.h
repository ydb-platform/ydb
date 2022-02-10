#pragma once

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <ydb/library/yql/ast/yql_expr.h>

namespace NKikimr {
namespace NMiniKQL {

class IFunctionRegistry;

}
}

namespace NYql {

IGraphTransformer::TStatus EvaluateParameters(const TExprNode::TPtr& input, TExprNode::TPtr& output, TTypeAnnotationContext& types, TExprContext& ctx,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);

bool ExtractParametersMetaAsYson(const TExprNode::TPtr& input, TTypeAnnotationContext& types, TExprContext& ctx, NYT::TNode& paramsMetaMap);

}
