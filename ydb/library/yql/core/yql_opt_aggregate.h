#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

TExprNode::TPtr ExpandAggregate(bool allowPickle, const TExprNode::TPtr& node, TExprContext& ctx, bool forceCompact = false);
inline TExprNode::TPtr ExpandAggregatePeephole(const TExprNode::TPtr& node, TExprContext& ctx) {
    return ExpandAggregate(false, node, ctx, true);
}

}

