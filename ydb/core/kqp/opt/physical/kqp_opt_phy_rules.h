#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <yql/essentials/ast/yql_expr.h>

/*
 * This file contains declaration of all rule functions for physical optimizer
 */

namespace NKikimr::NKqp::NOpt {

NYql::NNodes::TExprBase KqpRewriteReadTableSysView(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteReadTableFullText(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteLookupTablePhy(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpBuildReadTableStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpBuildReadTableFullTextIndexStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpBuildReadTableRangesStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap& parents);

NYql::NNodes::TExprBase KqpBuildSequencerStages(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpBuildStreamLookupTableStages(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpBuildStreamIdxLookupJoinStagesKeepSorted(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx, bool ruleEnabled, const NKikimr::NKqp::TKqpStatsStore* kqpStats = nullptr);

NYql::NNodes::TExprBase KqpBuildStreamIdxLookupJoinStagesKeepSortedFSM(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx, bool ruleEnabled, const NKikimr::NKqp::TKqpStatsStore* kqpStats = nullptr);

NYql::NNodes::TExprBase KqpBuildStreamIdxLookupJoinStages(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpRemoveRedundantSortOverReadTable(
    NYql::NNodes::TExprBase node,
    NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx
);

NYql::NNodes::TExprBase KqpRemoveRedundantSortOverReadTableFSM(
    NYql::NNodes::TExprBase node,
    NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx,
    const NYql::TTypeAnnotationContext& typeCtx
);

NYql::NNodes::TExprBase KqpBuildTopStageRemoveSort(NYql::NNodes::TExprBase node,  NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx, NYql::TTypeAnnotationContext& typeCtx, const NYql::TParentsMap& parentsMap,
    bool allowStageMultiUsage, bool ruleEnabled, const NKikimr::NKqp::TKqpStatsStore* kqpStats = nullptr);

NYql::NNodes::TExprBase KqpBuildTopStageRemoveSortFSM(NYql::NNodes::TExprBase node,  NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx, NYql::TTypeAnnotationContext& typeCtx, const NYql::TParentsMap& parentsMap,
    bool allowStageMultiUsage, bool ruleEnabled, const NKikimr::NKqp::TKqpStatsStore* kqpStats = nullptr);

NYql::NNodes::TExprBase KqpApplyLimitToFullTextIndex(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyLimitToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

/// `OptimizeExpr` visits children before parents, so `KqpApplyLimitToReadTable` on `Take` may run too late (read is
/// already wrapped into a DQ stage). When building the read stage, walk ancestors and push `ItemsLimit` like
/// `KqpApplyLimitToReadTable` would if it saw the read first.
NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpTryInjectAncestorTakeItemsLimitIntoReadRanges(
    NYql::NNodes::TExprBase readNode,
    const NYql::TParentsMap& parents,
    NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

/// Peels one layer of ExtractMembers / flow / block / ExpandMap / Map wrappers (shared by OLAP distinct and limit pushdown).
bool KqpPeelOneRxMapOrFlowWrapper(NYql::NNodes::TExprBase& probe);

/// Replaces `oldRead` with `newRead` under `root`, preserving the same wrapper stack.
NYql::NNodes::TExprBase KqpSubstituteReadPreservingRxWrappers(
    NYql::NNodes::TExprBase root,
    const NYql::TExprNode* oldReadRaw,
    NYql::NNodes::TExprBase newRead,
    NYql::TExprContext& ctx,
    NYql::TPositionHandle pos);

NYql::NNodes::TExprBase KqpApplyLimitToOlapReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyVectorTopKToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyVectorTopKToStageWithSource(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpPushOlapFilter(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx, NYql::IGraphTransformer &typeAnnTransformer);

NYql::NNodes::TExprBase KqpPushOlapProjections(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

NYql::NNodes::TExprBase KqpDisableOlapBlocksOnLimit(NYql::NNodes::TExprBase node, NYql::TTypeAnnotationContext& typesCtx, ui32 columnsLimit);

NYql::NNodes::TExprBase KqpPushOlapAggregate(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpPushOlapDistinct(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

/// Physical peephole (post stage-build): `Take` / `Limit` injects `Limit` into an already fused `TKqpOlapDistinct` when
/// SQL `LIMIT` sits outside the `AggregateCombine` shape handled by `KqpPushOlapDistinct`; `WideCombiner` covers distinct+inner `Take`/`Limit`.
NYql::NNodes::TExprBase KqpPushOlapDistinctPeephole(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TKikimrConfiguration::TPtr& config);

NYql::NNodes::TExprBase KqpPushDownOlapGroupByKeys(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpPushOlapLength(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpFloatUpStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpPropagatePrecomuteScalarRowset(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx, const NYql::TParentsMap& parentsMap, bool allowStageMultiUsage);

NYql::NNodes::TExprBase KqpBuildWriteConstraint(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx, const NYql::TParentsMap& parentsMap, bool allowStageMultiUsage);

NYql::NNodes::TExprBase KqpAddColumnForEmptyColumnsOlapRead(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

bool AllowFuseJoinInputs(NYql::NNodes::TExprBase node);

bool UseSource(const TKqpOptimizeContext& kqpCtx, const NYql::TKikimrTableDescription& tableDesc);

} // NKikimr::NKqp::NOpt
