#pragma once

#include "type_ann_impl.h"
#include "type_ann_sql.h"

namespace NYql::NTypeAnnImpl {

IGraphTransformer::TStatus PromoteYqlAggOptions(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

TVector<TExprNode::TPtr> InferYqlGroupRefTypes(
    const TExprNode& groupExprs, const TExprNode& groupSets, TExprContext& ctx);

IGraphTransformer::TStatus InferYqlImplicitUsingJoinColumns(
    const TExprNode::TPtr& predicate,
    const TInputs& groupInputs,
    const TVector<ui32>& lhsIndexes,
    const TVector<ui32>& rhsIndexes,
    TVector<std::pair<TString, TString>>& implicitUsing,
    TExtContext& ctx);

IGraphTransformer::TStatus YqlAggFactoryWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus YqlAggWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus YqlWinFactoryWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus YqlAggWinWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

IGraphTransformer::TStatus YqlWinWrapper(
    const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

} // namespace NYql::NTypeAnnImpl
