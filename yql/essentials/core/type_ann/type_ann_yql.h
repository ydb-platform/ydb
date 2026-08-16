#pragma once

#include "type_ann_impl.h"
#include "type_ann_sql.h"

namespace NYql::NTypeAnnImpl {

struct TYqlFromSettings {
    bool IsCTE = false;

    static TMaybe<TYqlFromSettings> Parse(const TExprNode::TPtr& settings, TExtContext& ctx);
};

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

IGraphTransformer::TStatus InferYqlInferUnionType(
    TPositionHandle pos,
    const TExprNode::TListType& children,
    TColumnOrder& resultColumnOrder,
    const TStructExprType*& resultStructType,
    TExtContext& ctx,
    bool& areColumnsOrdered,
    bool& isUniversal);

/// NB: this is a light version only for a simple and sound static analysis.
TMaybe<TVector<std::pair<TString, /*isSynthetic=*/bool>>>
InferYqlSimpleColumnOrder(const TExprNode::TPtr& input);

IGraphTransformer::TStatus ValidateYqlExplicitColumnOrders(
    const TExprNode::TPtr& input,
    TExprNode::TPtr& output,
    TExtContext& ctx,
    TPositionHandle position,
    const TVector<TPositionHandle>& expectedPositions,
    const TVector<TString>& expectedOrder,
    const TVector<std::pair<TString, /*isSynthetic=*/bool>>& actualOrder);

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
