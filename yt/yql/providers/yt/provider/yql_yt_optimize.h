#pragma once

#include "yql_yt_provider.h"
#include "yql_yt_provider_impl.h"

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <tuple>

namespace NYql {

// writer -> (reader, YtSection, YtOutput, YtPath)
using TOpDeps = TNodeMap<TVector<std::tuple<const TExprNode*, const TExprNode*, const TExprNode*, const TExprNode*>>>;

NNodes::TMaybeNode<NNodes::TYtSection> UpdateSectionWithSettings(NNodes::TExprBase world, NNodes::TYtSection section, NNodes::TYtDSink dataSink,
    TYqlRowSpecInfo::TPtr outRowSpec, bool keepSortness, bool allowWorldDeps, bool allowMaterialize, TSyncMap& syncList, const TYtState::TPtr& state, TExprContext& ctx);

NNodes::TYtSection MakeEmptySection(NNodes::TYtSection section, NNodes::TYtDSink dataSink,
                                    bool keepSortness, const TYtState::TPtr& state, TExprContext& ctx);

TExprNode::TPtr OptimizeReadWithSettings(const TExprNode::TPtr& node, bool allowWorldDeps, bool allowMaterialize, TSyncMap& syncList,
    const TYtState::TPtr& state, TExprContext& ctx);

IGraphTransformer::TStatus UpdateTableContentMemoryUsage(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    const TYtState::TPtr& state, TExprContext& ctx, bool estimateTableContentWeight);

IGraphTransformer::TStatus PeepHoleOptimizeBeforeExec(TExprNode::TPtr input, TExprNode::TPtr& output,
    const TYtState::TPtr& state, bool& hasNonDeterministicFunctions, TExprContext& ctx, bool estimateTableContentWeight);

TMaybe<bool> CanFuseLambdas(const NNodes::TCoLambda& innerLambda, const NNodes::TCoLambda& outerLambda, TExprContext& ctx, const TYtState::TPtr& state);

NNodes::TMaybeNode<NNodes::TExprBase> FuseMapToMapReduce(NNodes::TExprBase node, TExprContext& ctx,
    const TOptimizeTransformerBase::TGetParents& getParents, const TYtState::TPtr& state);

} //NYql
