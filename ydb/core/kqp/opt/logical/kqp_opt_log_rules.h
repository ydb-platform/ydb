#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>

/*
 * This file contains declaration of all rule functions for logical optimizer
 */

namespace NKikimr::NKqp::NOpt { 

NYql::NNodes::TExprBase KqpPushPredicateToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext &ctx,
    const TKqpOptimizeContext &kqpCtx);

NYql::NNodes::TExprBase KqpPushExtractedPredicateToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

NYql::NNodes::TExprBase KqpApplyExtractMembersToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpApplyExtractMembersToReadOlapTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpApplyExtractMembersToReadTableRanges(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpJoinToIndexLookup(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const NYql::TKikimrConfiguration::TPtr& config);

NYql::NNodes::TExprBase KqpRewriteSqlInToEquiJoin(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, const NYql::TKikimrConfiguration::TPtr& config);

NYql::NNodes::TExprBase KqpRewriteSqlInCompactToJoin(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpRewriteIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteLookupIndex(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteTopSortOverIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext&,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpRewriteTakeOverIndexRead(const NYql::NNodes::TExprBase& node, NYql::TExprContext&,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpDeleteOverLookup(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, 
    const TKqpOptimizeContext &kqpCtx); 
 
NYql::NNodes::TExprBase KqpExcessUpsertInputColumns(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx); 
 
} // namespace NKikimr::NKqp::NOpt 
