#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>

/*
 * This file contains declaration of all rule functions for logical optimizer
 */

namespace NYql::NCommon {
    class TKeyRange;
}

namespace NKikimr::NKqp::NOpt {

NYql::NNodes::TKqlKeyRange BuildKeyRangeExpr(const NYql::NCommon::TKeyRange& keyRange,
    const NYql::TKikimrTableDescription& tableDesc, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpPushExtractedPredicateToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TExprBase KqpJoinToIndexLookup(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, bool useCBO);

NYql::NNodes::TExprBase KqpRewriteSqlInToEquiJoin(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const NYql::TKikimrConfiguration::TPtr& config);

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
    const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap& parentsMap);

NYql::NNodes::TExprBase KqpRewriteTopSortOverFlatMap(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpRewriteTakeOverIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext&,
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

NYql::NNodes::TExprBase KqpApplyExtractMembersToReadTableRanges(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TParentsMap& parentsMap, bool allowMultiUsage);

NYql::NNodes::TExprBase KqpApplyExtractMembersToLookupTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TParentsMap& parentsMap, bool allowMultiUsage);

NYql::NNodes::TExprBase KqpTopSortOverExtend(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const NYql::TParentsMap& parents);

NYql::NNodes::TExprBase KqpUpsertRowsInputRewrite(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

} // namespace NKikimr::NKqp::NOpt
