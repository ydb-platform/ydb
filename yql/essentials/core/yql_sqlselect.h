#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

using TAggsRewriter = std::function<TExprNode::TPtr(TExprNode::TPtr arg, TExprNode::TPtr row)>;

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

TExprNode::TPtr ExpandSqlWindowCall(
    const TExprNode::TPtr& call,
    TExprNode::TPtr listType,
    TExprNode::TPtr keyExtractor,
    TAggsRewriter rewrite,
    TExprContext& ctxExpr,
    TTypeAnnotationContext& ctxTypes);

} // namespace NYql
