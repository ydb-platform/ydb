#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/yql_type_annotation.h>

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

THolder<IGraphTransformer> CreateEvaluateExpressionTransformer(
    TTypeAnnotationContext& types,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    IGraphTransformer* calcTransformer = nullptr,
    TTypeAnnCallableFactory typeAnnCallableFactory = {});

} // namespace NYql
