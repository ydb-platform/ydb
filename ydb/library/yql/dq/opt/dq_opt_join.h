#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NYql {

struct TOptimizerStatistics;
struct TRelOptimizerNode;

namespace NDq {

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, bool useCBO, TExprContext& ctx, const TTypeAnnotationContext& typeCtx);

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, bool useCBO, TExprContext& ctx, const TTypeAnnotationContext& typeCtx, int& joinCounter);

NNodes::TExprBase DqBuildPhyJoin(const NNodes::TDqJoin& join, bool pushLeftStage, TExprContext& ctx, IOptimizationContext& optCtx, bool useGraceCoreForMap);

NNodes::TExprBase DqBuildJoin(const NNodes::TExprBase& node, TExprContext& ctx,
    IOptimizationContext& optCtx, const TParentsMap& parentsMap, bool allowStageMultiUsage, bool pushLeftStage, EHashJoinMode hashJoin = EHashJoinMode::Off, bool shuffleMapJoin = true, bool useGraceCoreForMap = false);

NNodes::TExprBase DqBuildHashJoin(const NNodes::TDqJoin& join, EHashJoinMode mode, TExprContext& ctx, IOptimizationContext& optCtx);

NNodes::TExprBase DqBuildJoinDict(const NNodes::TDqJoin& join, TExprContext& ctx);

NNodes::TDqJoin DqSuppressSortOnJoinInput(const NNodes::TDqJoin& node, TExprContext& ctx);

bool DqCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TTypeAnnotationContext& typesCtx,
    const NNodes::TCoEquiJoin& equiJoin,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& collector);

} // namespace NDq
} // namespace NYql
