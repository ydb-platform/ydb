#pragma once

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql::NDq {

using TProviderCollectFunction =
    std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>;

/*
 * Main routine that checks:
 * 1. Do we have an equiJoin
 * 2. Is the cost already computed
 * 3. Are all the costs of equiJoin inputs computed?
 *
 * Then it optimizes the join tree.
*/
NYql::NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NYql::NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    const TOptimizerHints& hints = {},
    bool enableShuffleElimination = false,
    NYql::TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr
);

NYql::NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NYql::NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    int& equiJoinCounter,
    const TOptimizerHints& hints = {},
    bool enableShuffleElimination = false,
    NYql::TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr
);

void CollectInterestingOrderingsFromJoinTree(
    const NYql::NNodes::TExprBase& equiJoinNode,
    TFDStorage& fdStorage,
    TTypeAnnotationContext& typeCtx
);

IOptimizerNew* MakeNativeOptimizerNew(
    IProviderContext& ctx,
    const ui32 maxDPHypDPTableSize,
    TExprContext& ectx,
    bool enableShuffleElimination,
    TSimpleSharedPtr<TOrderingsStateMachine> orderingsFSM = nullptr,
    TTableAliasMap* tableAliases = nullptr
);

} // namespace NYql::NDq
