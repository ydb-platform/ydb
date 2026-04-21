#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

TExprNode::TPtr ExpandYqlTraitsFactory(
    const TExprNode::TPtr& factory,
    TExprNode::TPtr listType,
    TExprNode::TPtr extractor,
    TExprContext& ctxExpr,
    TTypeAnnotationContext& ctxTypes);

size_t DefValIndex(const TExprNode::TPtr& traits);

TExprNode::TPtr ExpandResultType(
    const TExprNode::TPtr& traits,
    const TExprNode::TPtr& body,
    TExprContext& ctxExpr);

} // namespace NYql
