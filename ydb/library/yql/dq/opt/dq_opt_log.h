#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <functional>

namespace NYql {
    class IOptimizationContext;
    struct TTypeAnnotationContext;
    struct TDqSettings;
    struct IProviderContext;
    struct TRelOptimizerNode;
    struct TOptimizerStatistics;
}

namespace NYql::NDq {

NNodes::TExprBase DqRewriteAggregate(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
    bool compactForDistinct, bool usePhases, const bool useFinalizeByKey, const bool allowSpilling);

NNodes::TExprBase DqRewriteTakeSortToTopSort(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents);

NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& optimizer,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& providerCollect,
    const TOptimizerHints& hints = {}
);

NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& optimizer,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& providerCollect,
    int& equiJoinCounter,
    const TOptimizerHints& hints = {}
);

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, TExprContext& ctx);

NNodes::TExprBase DqEnforceCompactPartition(NNodes::TExprBase node, NNodes::TExprList frames, TExprContext& ctx);

NNodes::TExprBase DqExpandWindowFunctions(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typesCtx, bool enforceCompact);

NNodes::TExprBase DqMergeQueriesWithSinks(NNodes::TExprBase dqQueryNode, TExprContext& ctx);

NNodes::TExprBase DqFlatMapOverExtend(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqSqlInDropCompact(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqReplicateFieldSubset(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents);

IGraphTransformer::TStatus DqWrapIO(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx, TTypeAnnotationContext& typesCtx, const TDqSettings& config);

NNodes::TExprBase DqExpandMatchRecognize(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx);

IOptimizerNew* MakeNativeOptimizerNew(IProviderContext& ctx, const ui32 maxDPccpDPTableSize);

NNodes::TMaybeNode<NNodes::TExprBase> UnorderedOverDqReadWrap(NNodes::TExprBase node, TExprContext& ctx, const std::function<const TParentsMap*()>& getParents, bool enableDqReplicate, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> ExtractMembersOverDqReadWrap(NNodes::TExprBase node, TExprContext& ctx, const std::function<const TParentsMap*()>& getParents, bool enableDqReplicate, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> TakeOrSkipOverDqReadWrap(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> ExtendOverDqReadWrap(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> DqReadWideWrapFieldSubset(NNodes::TExprBase node, TExprContext& ctx, const std::function<const TParentsMap*()>& getParents, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> DqReadWrapByProvider(NNodes::TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> ExtractMembersOverDqReadWrapMultiUsage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const std::function<const TParentsMap*()>& getParents, TTypeAnnotationContext& typeAnnCtx);

NNodes::TMaybeNode<NNodes::TExprBase> UnorderedOverDqReadWrapMultiUsage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const std::function<const TParentsMap*()>& getParents, TTypeAnnotationContext& typeAnnCtx);

} // namespace NYql::NDq
