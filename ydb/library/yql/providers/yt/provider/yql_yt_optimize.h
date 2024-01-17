#pragma once

#include "yql_yt_provider.h"
#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>

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
    const TYtState::TPtr& state, TExprContext& ctx);

IGraphTransformer::TStatus PeepHoleOptimizeBeforeExec(TExprNode::TPtr input, TExprNode::TPtr& output,
    const TYtState::TPtr& state, bool& hasNonDeterministicFunctions, TExprContext& ctx);

} //NYql
