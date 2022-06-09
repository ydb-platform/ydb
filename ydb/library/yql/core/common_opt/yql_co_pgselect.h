#pragma once

#include "yql_co.h"

namespace NYql {

TExprNode::TPtr ExpandPgSelect(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgSelectSublink(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx,
    ui32 subLinkId, const TExprNode::TPtr& outer);

TExprNode::TPtr ExpandPositionalUnionAll(const TExprNode& node, const TVector<TColumnOrder>& columnOrders,
    TExprNode::TListType children, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgLike(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgIn(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgBetween(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

} // namespace NYql
