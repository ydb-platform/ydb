#pragma once

#include "yql_co.h"

namespace NYql {

TExprNode::TPtr ExpandPgSelect(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgSelectSublink(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx,
    ui32 subLinkId, const TExprNode::TListType& outerInputs, const TVector<TString>& outerInputAliases);

TExprNode::TPtr ExpandPositionalUnionAll(const TExprNode& input, const TVector<TColumnOrder>& columnOrders,
    TExprNode::TListType children, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr NormalizeColumnOrder(const TExprNode::TPtr& node, const TColumnOrder& sourceColumnOrder,
    const TColumnOrder& targetColumnOrder, TExprContext& ctx);

TExprNode::TPtr ExpandPgLike(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgIn(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgBetween(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgGroupRef(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgGrouping(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandPgIterate(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

} // namespace NYql
