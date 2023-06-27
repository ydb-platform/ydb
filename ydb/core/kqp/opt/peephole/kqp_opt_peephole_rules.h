#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>

/*
 * This file contains declaration of all rule functions for peephole optimizer
 */

namespace NKikimr::NKqp::NOpt {

NYql::NNodes::TExprBase KqpBuildWideReadTable(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typesCtx);
NYql::NNodes::TExprBase KqpRewriteWriteConstraint(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp::NOpt
