#pragma once

#include <ydb/library/yql/dq/opt/dq_opt_join.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

namespace NKikimr::NKqp {

NYql::NNodes::TExprBase KqpRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    NYql::NDq::EHashJoinMode mode,
    bool useCBO,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    const TOptimizerHints& hints = {});

NYql::NNodes::TExprBase KqpRewriteEquiJoin(
    const NYql::NNodes::TExprBase& node,
    NYql::NDq::EHashJoinMode mode,
    bool useCBO,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx,
    TKqpStatsStore& kqpStats,
    int& joinCounter,
    const TOptimizerHints& hints = {});

} // namespace NKikimr::NKqp
