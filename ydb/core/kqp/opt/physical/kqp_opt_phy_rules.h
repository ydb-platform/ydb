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
    NYql::TTypeAnnotationContext& typeCtx, bool ruleEnabled);

NYql::NNodes::TExprBase KqpBuildStreamIdxLookupJoinStagesKeepSortedFSM(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx, bool ruleEnabled);

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
    bool allowStageMultiUsage, bool ruleEnabled);

NYql::NNodes::TExprBase KqpBuildTopStageRemoveSortFSM(NYql::NNodes::TExprBase node,  NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx, NYql::TTypeAnnotationContext& typeCtx, const NYql::TParentsMap& parentsMap,
    bool allowStageMultiUsage, bool ruleEnabled);

NYql::NNodes::TExprBase KqpApplyLimitToFullTextIndex(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyLimitToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

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

NYql::NNodes::TExprBase KqpPushOlapAggregate(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

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
