#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

/*
 * This file contains declaration of all rule functions for logical optimizer
 */

namespace NYql {

class TKikimrTableDescription;
struct TTypeAnnotationContext;
struct TKikimrConfiguration;

namespace NCommon {

class TKeyRange;

} // namespace NCommon

} // namespace NYql

namespace NKikimr::NKqp {

struct TOptimizerHints;

namespace NOpt {

struct TKqpOptimizeContext;

NYql::NNodes::TKqlKeyRange BuildKeyRangeExpr(const NYql::NCommon::TKeyRange& keyRange,
    const NYql::TKikimrTableDescription& tableDesc, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpPushExtractedPredicateToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TExprBase KqpTopSortSelectIndex(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpJoinToIndexLookup(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, bool useCBO, const NKikimr::NKqp::TOptimizerHints& hints);

NYql::NNodes::TExprBase KqpRewriteSqlInToEquiJoin(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const TIntrusivePtr<NYql::TKikimrConfiguration>& config);

NYql::NNodes::TExprBase KqpRewriteSqlInCompactToJoin(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpRewriteIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteLookupIndex(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteStreamLookupIndex(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteLookupTable(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteTopSortOverIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext&,
    NYql::TTypeAnnotationContext& typesCtx, const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpPushLimitOverFullText(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpRewriteFlatMapOverFullTextMatch(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpSelectJsonIndex(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpRewriteFlatMapOverJsonRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteTopSortOverFlatMap(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpRewriteHybridRankTopSort(const NYql::NNodes::TExprBase& node,
    NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteTakeOverIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext&,
    const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TExprBase KqpRewriteFlatMapOverIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext&,
    const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TExprBase KqpDeleteOverLookup(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext &kqpCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TExprBase KqpExcessUpsertInputColumns(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpDropTakeOverLookupTable(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyExtractMembersToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TParentsMap& parentsMap, bool allowMultiUsage);

NYql::NNodes::TExprBase KqpApplyExtractMembersToReadOlapTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TParentsMap& parentsMap, bool allowMultiUsage);

NYql::NNodes::TExprBase KqpTopSortOverExtend(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TParentsMap& parents);

NYql::NNodes::TExprBase KqpUpsertRowsInputRewrite(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

} // namespace NOpt

} // namespace NKikimr::NKqp
