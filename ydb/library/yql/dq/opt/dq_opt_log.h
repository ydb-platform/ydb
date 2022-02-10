#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>

#include <functional>

namespace NYql {
    struct TTypeAnnotationContext;
}

namespace NYql::NDq {

NNodes::TExprBase DqRewriteAggregate(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TExprBase DqRewriteTakeSortToTopSort(NNodes::TExprBase node, TExprContext& ctx, const TParentsMap& parents);

NNodes::TExprBase DqRewriteEquiJoin(const NNodes::TExprBase& node, TExprContext& ctx); 
 
NNodes::TExprBase DqEnforceCompactPartition(NNodes::TExprBase node, NNodes::TExprList frames, TExprContext& ctx);

NNodes::TExprBase DqExpandWindowFunctions(NNodes::TExprBase node, TExprContext& ctx, bool enforceCompact);

NNodes::TExprBase DqMergeQueriesWithSinks(NNodes::TExprBase dqQueryNode, TExprContext& ctx);

NNodes::TExprBase DqFlatMapOverExtend(NNodes::TExprBase node, TExprContext& ctx);

NNodes::TMaybeNode<NNodes::TExprBase> DqUnorderedInStage(NNodes::TExprBase node,
    const std::function<bool(const TExprNode*)>& stopTraverse, TExprContext& ctx, TTypeAnnotationContext* typeCtx);

} // namespace NYql::NDq
