#pragma once

#include <yql/essentials/ast/yql_constraint.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_graph_transformer.h>

namespace NYql {

[[nodiscard]] TMaybe<TPartOfConstraintBase::TPathType> TryExtractDirectEventTimePath(
    NNodes::TExprBase node,
    NNodes::TExprBase arg);

[[nodiscard]] TMaybe<TPartOfConstraintBase::TPathType> TryExtractEventTimeFromConstraints(
    const TExprNode& body);

[[nodiscard]] TMaybe<TPartOfConstraintBase::TPathType> TryExtractEventTimeFromConstraints(
    const TExprNode& body,
    const TStreamingConstraintNode& streaming);

[[nodiscard]] IGraphTransformer::TStatus UpdateLambdaEventTimeLineage(
    TExprNode::TPtr& lambda,
    TExprContext& ctx);

[[nodiscard]] IGraphTransformer::TStatus TryExtractEventTime(
    TExprNode::TPtr& node,
    const TExprNode& eventTimeExpr,
    TExprContext& ctx,
    TMaybe<TPartOfConstraintBase::TPathType>& eventTime);

} // namespace NYql
