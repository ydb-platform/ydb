#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NYql::NDq {

NNodes::TMaybeNode<NNodes::TDqStage> DqPushLambdaToStage(const NNodes::TDqStage &stage, 
    const NNodes::TCoAtom& outputIndex, NNodes::TCoLambda& lambda, 
    const TVector<NNodes::TDqConnection>& lambdaInputs, TExprContext& ctx, IOptimizationContext& optCtx);
 
NNodes::TMaybeNode<NNodes::TDqConnection> DqPushLambdaToStageUnionAll(const NNodes::TDqConnection& connection, NNodes::TCoLambda& lambda, 
    const TVector<NNodes::TDqConnection>& lambdaInputs, TExprContext& ctx, IOptimizationContext& optCtx);

NNodes::TExprBase DqPushSkipNullMembersToStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqPushExtractMembersToStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqPushOrderedLMapToStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);

NNodes::TExprBase DqPushLMapToStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);

NNodes::TExprBase DqBuildExtFunctionStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);

NNodes::TExprBase DqBuildFlatmapStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, 
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqPushCombineToStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqBuildPartitionsStage(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap);
 
NNodes::TExprBase DqBuildPartitionStage(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parentsMap);
 
NNodes::TExprBase DqBuildAggregationResultStage(NNodes::TExprBase node, TExprContext& ctx, 
    IOptimizationContext& optCtx); 
 
NNodes::TExprBase DqBuildTopSortStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqBuildSortStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqBuildSkipStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);

NNodes::TExprBase DqBuildTakeStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqBuildTakeSkipStage(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx,
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);

NNodes::TExprBase DqRewriteLengthOfStageOutput(NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx);

NNodes::TExprBase DqRewriteRightJoinToLeft(const NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqPushJoinToStage(const NNodes::TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, 
    const TParentsMap& parentsMap, bool allowStageMultiUsage = true);
 
NNodes::TExprBase DqBuildPhyJoin(const NNodes::TDqJoin& join, bool pushLeftStage, TExprContext& ctx,
    IOptimizationContext& optCtx);
 
NNodes::TExprBase DqBuildJoinDict(const NNodes::TDqJoin& join, TExprContext& ctx);

TMaybe<std::pair<NNodes::TExprBase, NNodes::TDqConnection>>  ExtractPureExprStage(TExprNode::TPtr input, 
    TExprContext& ctx);

NNodes::TExprBase DqBuildPureExprStage(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqBuildExtendStage(NNodes::TExprBase node, TExprContext& ctx); 

NNodes::TExprBase DqBuildPrecomputeStage(NNodes::TExprBase node, TExprContext& ctx);

NYql::NNodes::TExprBase DqBuildHasItems(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx);

NYql::NNodes::TExprBase DqBuildScalarPrecompute(NYql::NNodes::TExprBase node, NYql::TExprContext& ctx,
    NYql::IOptimizationContext& optCtx);

} // namespace NYql::NDq
