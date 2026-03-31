#pragma once

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>
#include <ydb/core/kqp/opt/cbo/kqp_statistics.h>

namespace NKikimr::NKqp {

NYql::NNodes::TExprBase DqRewriteEquiJoin(const NYql::NNodes::TExprBase& node, NYql::NDq::EHashJoinMode mode, bool useCBO, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx, TKqpStatsStore& kqpStats, const TOptimizerHints& hints = {});

NYql::NNodes::TExprBase DqRewriteEquiJoin(const NYql::NNodes::TExprBase& node, NYql::NDq::EHashJoinMode mode, bool useCBO, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx, TKqpStatsStore& kqpStats, int& joinCounter, const TOptimizerHints& hints = {});

NYql::NNodes::TExprBase DqBuildPhyJoin(const NYql::NNodes::TDqJoin& join, bool pushLeftStage, NYql::TExprContext& ctx, NYql::IOptimizationContext& optCtx, bool useGraceCoreForMap, bool buildCollectStage = true);

NYql::NNodes::TExprBase DqBuildJoin(
    const NYql::NNodes::TExprBase& node,
    NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx,
    const NYql::TParentsMap& parentsMap,
    bool allowStageMultiUsage,
    bool pushLeftStage,
    NYql::NDq::EHashJoinMode hashJoin = NYql::NDq::EHashJoinMode::Off,
    bool shuffleMapJoin = true,
    bool useGraceCoreForMap = false,
    bool useBlockHashJoin = false,
    bool shuffleElimination = false,
    bool shuffleEliminationWithMap = false,
    bool buildCollectStage=true,
    bool blockHashJoinBuildSideLeft = false
);

NYql::NNodes::TExprBase DqBuildHashJoin(const NYql::NNodes::TDqJoin& join, NYql::NDq::EHashJoinMode mode, NYql::TExprContext& ctx, NYql::IOptimizationContext& optCtx, bool shuffleElimination, bool shuffleEliminationWithMap, bool useBlockHashJoin = false, bool blockHashJoinBuildSideLeft = false);

NYql::NNodes::TExprBase DqBuildBlockHashJoin(const NYql::NNodes::TDqJoin& join, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase DqBuildJoinDict(const NYql::NNodes::TDqJoin& join, NYql::TExprContext& ctx);

NYql::NNodes::TDqJoin DqSuppressSortOnJoinInput(const NYql::NNodes::TDqJoin& node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase DqRewriteStreamEquiJoinWithLookup(const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx);

NYql::NNodes::TExprBase DqRewriteRightJoinToLeft(const NYql::NNodes::TExprBase node, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase DqRewriteLeftPureJoin(const NYql::NNodes::TExprBase node, NYql::TExprContext& ctx, const NYql::TParentsMap& parentsMap, bool allowStageMultiUsage);

} // namespace NKikimr::NKqp
