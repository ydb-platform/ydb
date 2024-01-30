#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer.h>

#include <functional>

namespace NYql {
    struct TTypeAnnotationContext;
    struct TDqSettings;
    struct IProviderContext;
    struct TRelOptimizerNode;
    struct TOptimizerStatistics;
}

namespace NYql::NDq {

NNodes::TExprBase DqRewriteAggregate(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typesCtx, bool compactForDistinct, bool usePhases, const bool useFinalizeByKey);

NNodes::TExprBase DqRewriteTakeSortToTopSort(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents);

NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NNodes::TExprBase& node, 
    TExprContext& ctx, 
    TTypeAnnotationContext& typesCtx, 
    ui32 optLevel, 
    ui32 maxDPccpDPTableSize,
    IProviderContext& providerCtx, 
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& providerCollect);

NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    const std::function<IOptimizer*(IOptimizer::TInput&&)>& optFactory,
    ui32 optLevel);

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, TExprContext& ctx);

NNodes::TExprBase DqEnforceCompactPartition(NNodes::TExprBase node, NNodes::TExprList frames, TExprContext& ctx);

NNodes::TExprBase DqExpandWindowFunctions(NNodes::TExprBase node, TExprContext& ctx, bool enforceCompact);

NNodes::TExprBase DqMergeQueriesWithSinks(NNodes::TExprBase dqQueryNode, TExprContext& ctx);

NNodes::TExprBase DqFlatMapOverExtend(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqSqlInDropCompact(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqReplicateFieldSubset(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents);

IGraphTransformer::TStatus DqWrapRead(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx, TTypeAnnotationContext& typesCtx, const TDqSettings& config);

NNodes::TExprBase DqExpandMatchRecognize(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx);

IOptimizer* MakeNativeOptimizer(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log);

} // namespace NYql::NDq
