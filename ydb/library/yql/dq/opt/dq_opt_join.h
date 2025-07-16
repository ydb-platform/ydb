
#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NYql {

struct TOptimizerStatistics;
struct TRelOptimizerNode;

namespace NDq {

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, bool useCBO, TExprContext& ctx, TTypeAnnotationContext& typeCtx, const TOptimizerHints& hints = {});

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, bool useCBO, TExprContext& ctx, TTypeAnnotationContext& typeCtx, int& joinCounter, const TOptimizerHints& hints = {});

NNodes::TExprBase DqBuildPhyJoin(const NNodes::TDqJoin& join, bool pushLeftStage, TExprContext& ctx, IOptimizationContext& optCtx, bool useGraceCoreForMap, bool buildCollectStage=true);

NNodes::TExprBase DqBuildJoin(
    const NNodes::TExprBase& node,
    TExprContext& ctx,
    IOptimizationContext& optCtx,
    const TParentsMap& parentsMap,
    bool allowStageMultiUsage,
    bool pushLeftStage,
    EHashJoinMode hashJoin = EHashJoinMode::Off,
    bool shuffleMapJoin = true,
    bool useGraceCoreForMap = false,
    bool useBlockHashJoin = false,
    bool shuffleElimination = false,
    bool shuffleEliminationWithMap = false,
    bool buildCollectStage=true
);

NNodes::TExprBase DqBuildHashJoin(const NNodes::TDqJoin& join, EHashJoinMode mode, TExprContext& ctx, IOptimizationContext& optCtx, bool shuffleElimination, bool shuffleEliminationWithMap, bool useBlockHashJoin = false);

// Updated DqBuildBlockHashJoin function signature with all necessary parameters
NNodes::TExprBase DqBuildBlockHashJoin(
    const NNodes::TDqJoin& join, 
    const TStructExprType* leftStructType, 
    const TStructExprType* rightStructType, 
    const std::map<std::string_view, ui32>& leftNames, 
    const std::map<std::string_view, ui32>& rightNames,
    const TVector<NNodes::TCoAtom>& leftJoinKeys, 
    const TVector<NNodes::TCoAtom>& rightJoinKeys,
    NNodes::TCoArgument leftInputArg, 
    NNodes::TCoArgument rightInputArg, 
    TExprContext& ctx
);

NNodes::TExprBase DqBuildJoinDict(const NNodes::TDqJoin& join, TExprContext& ctx);

NNodes::TDqJoin DqSuppressSortOnJoinInput(const NNodes::TDqJoin& node, TExprContext& ctx);

bool DqCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TTypeAnnotationContext& typesCtx,
    const NNodes::TCoEquiJoin& equiJoin,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& collector);

} // namespace NDq
} // namespace NYql

