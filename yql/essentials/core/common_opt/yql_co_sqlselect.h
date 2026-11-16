#pragma once

#include "yql_co.h"

#include <tuple>
#include <utility>

namespace NYql {

std::tuple<TNodeMap<ui32>, THashMap<ui32, const TExprNode*>> GatherSubLinksWithOriginal(
    const TExprNode::TPtr& root,
    const TExprNode::TPtr& originalRoot);

std::pair<TExprNode::TPtr, TExprNode::TPtr> RewriteSubLinksPartial(
    TPositionHandle pos,
    const TExprNode::TPtr& list,
    const TExprNode::TPtr& root,
    const TExprNode::TPtr& originalArg,
    const TNodeMap<ui32>& subLinks,
    const THashMap<ui32, const TExprNode*>& originalSubLinks,
    const TVector<TString>& inputAliases,
    const TExprNode::TListType& cleanedInputs,
    TExprContext& ctx,
    TOptimizeContext& optCtx,
    bool isYql,
    const TString& leftPrefix = {},
    TVector<TString>* sublinkColumns = nullptr);

TExprNode::TPtr ExpandSqlSelect(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandSqlSelectSublink(
    const TExprNode::TPtr& node,
    const TExprNode::TPtr& originalNode,
    TExprContext& ctx,
    TOptimizeContext& optCtx,
    ui32 subLinkId,
    const TExprNode::TListType& outerInputs,
    const TVector<TString>& outerInputAliases);

TExprNode::TPtr ExpandPositionalSelectOp(const TExprNode& input, const TVector<TColumnOrder>& columnOrders,
    TExprNode::TListType children, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr NormalizeColumnOrder(const TExprNode::TPtr& node, const TColumnOrder& sourceColumnOrder,
    const TColumnOrder& targetColumnOrder, TExprContext& ctx);

TExprNode::TPtr ExpandSqlGroupRef(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandSqlGrouping(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr ExpandSqlIterate(const TExprNode::TPtr& node, TExprContext& ctx, TOptimizeContext& optCtx);

TExprNode::TPtr CombineSetItems(TPositionHandle pos, const TExprNode::TPtr& left, const TExprNode::TPtr& right, const TStringBuf& op, TExprContext& ctx);

} // namespace NYql
