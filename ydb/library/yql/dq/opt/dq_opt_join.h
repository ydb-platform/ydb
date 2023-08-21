#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NYql::NDq {

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, TExprContext& ctx);

NNodes::TExprBase DqBuildPhyJoin(const NNodes::TDqJoin& join, bool pushLeftStage, TExprContext& ctx, IOptimizationContext& optCtx);

NNodes::TExprBase DqBuildJoin(const NNodes::TExprBase& node, TExprContext& ctx,
    IOptimizationContext& optCtx, const TParentsMap& parentsMap, bool allowStageMultiUsage, bool pushLeftStage, EHashJoinMode hashJoin = EHashJoinMode::Off);

NNodes::TExprBase DqBuildHashJoin(const NNodes::TDqJoin& join, EHashJoinMode mode, TExprContext& ctx, IOptimizationContext& optCtx);

NNodes::TExprBase DqBuildJoinDict(const NNodes::TDqJoin& join, TExprContext& ctx);

NNodes::TDqJoin DqSuppressSortOnJoinInput(const NNodes::TDqJoin& node, TExprContext& ctx);

}
