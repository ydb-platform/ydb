#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>

#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>

namespace NYql {

struct TTypeAnnotationContext;

} // namespace NYql

namespace NKikimr::NKqp {

class TKqpStatsStore;
struct TOptimizerHints;

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    NYql::NDq::EHashJoinMode mode,
    bool useCBO,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    const TOptimizerHints& hints);

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase> KqpRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    NYql::NDq::EHashJoinMode mode,
    bool useCBO,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    int& joinCounter,
    const TOptimizerHints& hints);

} // namespace NKikimr::NKqp
