#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <yql/essentials/ast/yql_expr.h>

/*
 * This file contains declaration of all rule functions for peephole optimizer
 */

namespace NKikimr::NKqp::NOpt {

NYql::NNodes::TExprBase KqpBuildWideReadTable(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typesCtx);
NYql::NNodes::TExprBase KqpRewriteWriteConstraint(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);
NYql::NNodes::TExprBase KqpEliminateWideMapForLargeOlapTable(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typesCtx);
NYql::NNodes::TExprBase KqpEliminateWideMapPackUnpack(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typesCtx);

NYql::IGraphTransformer::TStatus KqpBuildStreamingFlow(
    ui64 txIdx, const NYql::NNodes::TKqpPhysicalTx& tx, NYql::TExprNode::TPtr& output, THashSet<std::pair<ui64, ui64>>& streamingTxResults,
    const NYql::TKikimrConfiguration& config, const NYql::TKikimrTablesData& tables, TStringBuf cluster, NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp::NOpt
