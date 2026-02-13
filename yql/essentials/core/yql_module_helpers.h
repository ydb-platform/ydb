#pragma once

#include "yql_type_annotation.h"

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

const TExprNode::TPtr* ImportFreezed(
    const TPosition& position,
    const TString& path,
    const TString& name,
    TExprContext& ctx,
    TTypeAnnotationContext& types);

// TODO(YQL-20436): reuse it anywhere.
TExprNode::TPtr ImportDeeplyCopied(
    const TPosition& position,
    const TString& path,
    const TString& name,
    TExprContext& ctx,
    TTypeAnnotationContext& types);

} // namespace NYql
