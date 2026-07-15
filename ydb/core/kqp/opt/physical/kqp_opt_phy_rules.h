#pragma once

#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>

/*
 * This file contains declaration of all rule functions for physical optimizer
 */

namespace NYql {

struct TTypeAnnotationContext;
class IOptimizationContext;
class TKikimrTableDescription;

} // namespace NYql

namespace NKikimr::NKqp {

class TKqpStatsStore;

namespace NOpt {

struct TKqpOptimizeContext;

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

NYql::NNodes::TExprBase KqpApplyLimitToOlapReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyVectorTopKToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyVectorTopKToStageWithSource(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpPushOlapFilter(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

NYql::NNodes::TExprBase KqpPushOlapProjections(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

NYql::NNodes::TExprBase KqpDisableOlapBlocksOnLimit(NYql::NNodes::TExprBase node, NYql::TTypeAnnotationContext& typesCtx, ui32 columnsLimit);

NYql::NNodes::TExprBase KqpPushOlapAggregate(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpPushOlapDistinct(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

/// Count ERROR/FATAL issues already in `IssueManager` completed set (does not call `GetIssues()`).
ui32 KqpCountFatalCompletedIssues(const NYql::TExprContext& ctx);

bool KqpValidateOlapForceDistinctCombinesPragmaOnRoot(const NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

/// Forced OLAP DISTINCT read fallback and related diagnostics. `graphRoot` must be the query root (walk parents from
/// the read node inside `TKqpPhysicalOptTransformer`).
NYql::NNodes::TExprBase KqpPushOlapDistinctOnBlockReadForGraph(NYql::NNodes::TExprBase readNode,
    const NYql::TExprNode* graphRoot, NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx);

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

} // NOpt

} // NKikimr::NKqp
