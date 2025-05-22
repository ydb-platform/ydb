#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

bool CheckBlockIOSupportedTypes(
    const TTypeAnnotationNode& containerType,
    const TSet<TString>& supportedTypes,
    const TSet<NUdf::EDataSlot>& supportedDataTypes,
    std::function<void(const TString&)> unsupportedTypeHandler,
    size_t wideFlowLimit,
    bool allowNestedOptionals = true
);

NNodes::TCoLambda WrapLambdaWithBlockInput(NNodes::TCoLambda lambda, TExprContext& ctx);

}
