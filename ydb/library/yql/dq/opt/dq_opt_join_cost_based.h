#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

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
    TCardinalityHints hints = TCardinalityHints(),
    TJoinAlgoHints joinHints = TJoinAlgoHints()
);

NYql::NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NYql::NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect,
    int& equiJoinCounter,
    TCardinalityHints hints = TCardinalityHints(),
    TJoinAlgoHints joinHints = TJoinAlgoHints()
);

} // namespace NYql::NDq
