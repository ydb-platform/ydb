#pragma once

#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp {

using TProviderCollectFunction =
    std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const NYql::TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>;

bool KqpCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TKqpStatsStore& kqpStats,
    const NYql::NNodes::TCoEquiJoin& equiJoin,
    const TProviderCollectFunction& collector
);

/*
 * Main routine that checks:
 * 1. Do we have an equiJoin
 * 2. Is the cost already computed
 * 3. Are all the costs of equiJoin inputs computed?
 *
 * Then it optimizes the join tree.
*/
NYql::NNodes::TExprBase KqpOptimizeEquiJoinWithCosts(
    const NYql::NNodes::TExprBase& node,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typesCtx,
    TKqpStatsStore& kqpStats,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    const TOptimizerHints& hints = {},
    bool enableShuffleElimination = false,
    TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr
);

NYql::NNodes::TExprBase KqpOptimizeEquiJoinWithCosts(
    const NYql::NNodes::TExprBase& node,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typesCtx,
    TKqpStatsStore& kqpStats,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    int& equiJoinCounter,
    const TOptimizerHints& hints = {},
    bool enableShuffleElimination = false,
    TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels = nullptr
);

void CollectInterestingOrderingsFromJoinTree(
    const NYql::NNodes::TExprBase& equiJoinNode,
    TFDStorage& fdStorage,
    NYql::TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats
);

IOptimizerNew* MakeNativeOptimizerNew(
    IProviderContext& ctx,
    const TCBOSettings& settings,
    NYql::TExprContext& ectx,
    bool enableShuffleElimination,
    TSimpleSharedPtr<TOrderingsStateMachine> orderingsFSM = nullptr,
    TTableAliasMap* tableAliases = nullptr
);

} // namespace NKikimr::NKqp
