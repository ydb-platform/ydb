#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <yql/essentials/core/yql_cost_function.h>

#include <functional>

namespace NYql {

class IOptimizationContext;
struct TTypeAnnotationContext;
struct TRelOptimizerNode;
struct TOptimizerStatistics;

namespace NDq {

struct TEquiJoinCallbacks {
    std::function<EJoinAlgoType(const TVector<TString>&)>   GetAlgoHint       = {};
    std::function<void(const TVector<TString>&)>            OnAlgoHintApplied = {};
    std::function<void(const TExprNode*, const TExprNode*)> TransferStats     = {};
};

NNodes::TMaybeNode<NNodes::TExprBase> DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, bool useCBO, TExprContext& ctx, TTypeAnnotationContext& typeCtx, const TEquiJoinCallbacks& callbacks = {});

NNodes::TMaybeNode<NNodes::TExprBase> DqRewriteEquiJoin(const NNodes::TExprBase& node, EHashJoinMode mode, bool useCBO, TExprContext& ctx, TTypeAnnotationContext& typeCtx, int& joinCounter, const TEquiJoinCallbacks& callbacks = {});

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
    bool buildCollectStage=true,
    bool blockHashJoinBuildSideLeft = false
);

NNodes::TExprBase DqBuildHashJoin(const NNodes::TDqJoin& join, EHashJoinMode mode, TExprContext& ctx, IOptimizationContext& optCtx, bool shuffleElimination, bool shuffleEliminationWithMap, bool useBlockHashJoin = false, bool blockHashJoinBuildSideLeft = false);

NNodes::TExprBase DqBuildBlockHashJoin(const NNodes::TDqJoin& join, TExprContext& ctx);

NNodes::TExprBase DqBuildJoinDict(const NNodes::TDqJoin& join, TExprContext& ctx);

NNodes::TDqJoin DqSuppressSortOnJoinInput(const NNodes::TDqJoin& node, TExprContext& ctx);

bool DqCollectJoinRelationsWithStats(
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels,
    TTypeAnnotationContext& typesCtx,
    const NNodes::TCoEquiJoin& equiJoin,
    const std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>& collector);

NNodes::TExprBase DqRewriteStreamEquiJoinWithLookup(const NNodes::TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typeCtx);

} // namespace NDq
} // namespace NYql
