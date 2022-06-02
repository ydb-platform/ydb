#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>

/*
 * This file contains declaration of all rule functions for physical optimizer
 */

namespace NKikimr::NKqp::NOpt {

NYql::NNodes::TExprBase KqpBuildReadTableStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpBuildReadTableRangesStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpBuildLookupTableStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpRemoveRedundantSortByPk(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpApplyLimitToReadTable(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx);

NYql::NNodes::TExprBase KqpPushOlapFilter(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, NYql::TTypeAnnotationContext& typesCtx);

NYql::NNodes::TExprBase KqpFloatUpStage(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase KqpPropagatePrecomuteScalarRowset(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx, const NYql::TParentsMap& parentsMap, bool allowStageMultiUsage);

} // NKikimr::NKqp::NOpt
